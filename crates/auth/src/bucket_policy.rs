use s3_types::CanonicalUserId;
use serde_json::Value;

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
        let action = action.as_str();
        self.statements.iter().any(|statement| {
            statement.matches_action(action) && statement.references_existing_object_tag_condition()
        })
    }

    #[must_use]
    pub fn requires_request_object_tags_for_action(&self, action: PolicyAction) -> bool {
        let action = action.as_str();
        self.statements.iter().any(|statement| {
            statement.matches_action(action) && statement.references_request_object_tag_condition()
        })
    }

    pub fn validate_evaluable_object_conditions(&self) -> Result<(), BucketPolicyError> {
        for statement in &self.statements {
            if statement.references_evaluable_object_action()
                && !statement.conditions_supported_for_evaluable_object_actions()
            {
                return Err(BucketPolicyError::Malformed {
                    reason: "unsupported Condition for currently enforced bucket policy action",
                });
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn evaluate(&self, request: &PolicyRequest<'_>) -> PolicyEvaluation {
        let action = request.action.as_str();
        let resource = request.resource_arn();
        let mut saw_allow = false;

        for statement in &self.statements {
            let Some(effect) = statement.request_effect(request, action, &resource) else {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyAction {
    GetBucketPolicyStatus,
    GetBucketPublicAccessBlock,
    GetBucketObjectLockConfiguration,
    ListBucket,
    GetObject,
    GetObjectVersion,
    GetObjectAcl,
    GetObjectVersionAcl,
    GetObjectTagging,
    GetObjectVersionTagging,
    GetObjectRetention,
    GetObjectLegalHold,
    PutObject,
    PutObjectTagging,
    PutObjectVersionTagging,
    PutObjectRetention,
    PutObjectLegalHold,
    BypassGovernanceRetention,
    DeleteObject,
    DeleteObjectVersion,
    DeleteObjectTagging,
    DeleteObjectVersionTagging,
}

impl PolicyAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GetBucketPolicyStatus => "s3:GetBucketPolicyStatus",
            Self::GetBucketPublicAccessBlock => "s3:GetBucketPublicAccessBlock",
            Self::GetBucketObjectLockConfiguration => "s3:GetBucketObjectLockConfiguration",
            Self::ListBucket => "s3:ListBucket",
            Self::GetObject => "s3:GetObject",
            Self::GetObjectVersion => "s3:GetObjectVersion",
            Self::GetObjectAcl => "s3:GetObjectAcl",
            Self::GetObjectVersionAcl => "s3:GetObjectVersionAcl",
            Self::GetObjectTagging => "s3:GetObjectTagging",
            Self::GetObjectVersionTagging => "s3:GetObjectVersionTagging",
            Self::GetObjectRetention => "s3:GetObjectRetention",
            Self::GetObjectLegalHold => "s3:GetObjectLegalHold",
            Self::PutObject => "s3:PutObject",
            Self::PutObjectTagging => "s3:PutObjectTagging",
            Self::PutObjectVersionTagging => "s3:PutObjectVersionTagging",
            Self::PutObjectRetention => "s3:PutObjectRetention",
            Self::PutObjectLegalHold => "s3:PutObjectLegalHold",
            Self::BypassGovernanceRetention => "s3:BypassGovernanceRetention",
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
pub struct PolicyRequest<'a> {
    action: PolicyAction,
    bucket: &'a str,
    key: &'a str,
    bucket_resource: bool,
    requester_principal: Option<&'a str>,
    requester_canonical_user_id: Option<&'a CanonicalUserId>,
    existing_object_tags: &'a [PolicyTag<'a>],
    request_object_tags: &'a [PolicyTag<'a>],
    copy_source: Option<&'a str>,
    metadata_directive: Option<&'a str>,
    canned_acl: Option<&'a str>,
    sse_customer_algorithm: Option<&'a str>,
    grant_read: Option<&'a str>,
    grant_write: Option<&'a str>,
    grant_read_acp: Option<&'a str>,
    grant_write_acp: Option<&'a str>,
    grant_full_control: Option<&'a str>,
}

impl<'a> PolicyRequest<'a> {
    #[must_use]
    pub const fn new(
        action: PolicyAction,
        bucket: &'a str,
        key: &'a str,
        requester_principal: Option<&'a str>,
        requester_canonical_user_id: Option<&'a CanonicalUserId>,
    ) -> Self {
        Self {
            action,
            bucket,
            key,
            bucket_resource: false,
            requester_principal,
            requester_canonical_user_id,
            existing_object_tags: &[],
            request_object_tags: &[],
            copy_source: None,
            metadata_directive: None,
            canned_acl: None,
            sse_customer_algorithm: None,
            grant_read: None,
            grant_write: None,
            grant_read_acp: None,
            grant_write_acp: None,
            grant_full_control: None,
        }
    }

    #[must_use]
    pub const fn for_bucket(
        action: PolicyAction,
        bucket: &'a str,
        requester_principal: Option<&'a str>,
        requester_canonical_user_id: Option<&'a CanonicalUserId>,
    ) -> Self {
        Self {
            action,
            bucket,
            key: "",
            bucket_resource: true,
            requester_principal,
            requester_canonical_user_id,
            existing_object_tags: &[],
            request_object_tags: &[],
            copy_source: None,
            metadata_directive: None,
            canned_acl: None,
            sse_customer_algorithm: None,
            grant_read: None,
            grant_write: None,
            grant_read_acp: None,
            grant_write_acp: None,
            grant_full_control: None,
        }
    }

    #[must_use]
    pub const fn action(&self) -> PolicyAction {
        self.action
    }

    #[must_use]
    pub const fn requester_principal(&self) -> Option<&'a str> {
        self.requester_principal
    }

    #[must_use]
    pub const fn requester_canonical_user_id(&self) -> Option<&'a CanonicalUserId> {
        self.requester_canonical_user_id
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
    fn existing_object_tag_value(&self, key: &str) -> Option<&'a str> {
        self.existing_object_tags
            .iter()
            .find(|tag| tag.key == key)
            .map(|tag| tag.value)
    }

    #[must_use]
    pub fn with_existing_object_tags(mut self, existing_object_tags: &'a [PolicyTag<'a>]) -> Self {
        self.existing_object_tags = existing_object_tags;
        self
    }

    #[must_use]
    pub fn with_request_object_tags(mut self, request_object_tags: &'a [PolicyTag<'a>]) -> Self {
        self.request_object_tags = request_object_tags;
        self
    }

    #[must_use]
    pub fn with_copy_source(mut self, copy_source: Option<&'a str>) -> Self {
        self.copy_source = copy_source;
        self
    }

    #[must_use]
    pub fn with_metadata_directive(mut self, metadata_directive: Option<&'a str>) -> Self {
        self.metadata_directive = metadata_directive;
        self
    }

    #[must_use]
    pub fn with_canned_acl(mut self, canned_acl: Option<&'a str>) -> Self {
        self.canned_acl = canned_acl;
        self
    }

    #[must_use]
    pub fn with_sse_customer_algorithm(mut self, sse_customer_algorithm: Option<&'a str>) -> Self {
        self.sse_customer_algorithm = sse_customer_algorithm;
        self
    }

    #[must_use]
    pub fn with_grant_read(mut self, grant_read: Option<&'a str>) -> Self {
        self.grant_read = grant_read;
        self
    }

    #[must_use]
    pub fn with_grant_write(mut self, grant_write: Option<&'a str>) -> Self {
        self.grant_write = grant_write;
        self
    }

    #[must_use]
    pub fn with_grant_read_acp(mut self, grant_read_acp: Option<&'a str>) -> Self {
        self.grant_read_acp = grant_read_acp;
        self
    }

    #[must_use]
    pub fn with_grant_write_acp(mut self, grant_write_acp: Option<&'a str>) -> Self {
        self.grant_write_acp = grant_write_acp;
        self
    }

    #[must_use]
    pub fn with_grant_full_control(mut self, grant_full_control: Option<&'a str>) -> Self {
        self.grant_full_control = grant_full_control;
        self
    }

    #[must_use]
    fn copy_source(&self) -> Option<&'a str> {
        self.copy_source
    }

    #[must_use]
    fn metadata_directive(&self) -> Option<&'a str> {
        self.metadata_directive
    }

    #[must_use]
    fn canned_acl(&self) -> Option<&'a str> {
        self.canned_acl
    }

    #[must_use]
    fn sse_customer_algorithm(&self) -> Option<&'a str> {
        self.sse_customer_algorithm
    }

    #[must_use]
    fn request_object_tag_value(&self, key: &str) -> Option<&'a str> {
        self.request_object_tags
            .iter()
            .find(|tag| tag.key == key)
            .map(|tag| tag.value)
    }

    #[must_use]
    fn grant_read(&self) -> Option<&'a str> {
        self.grant_read
    }

    #[must_use]
    fn grant_write(&self) -> Option<&'a str> {
        self.grant_write
    }

    #[must_use]
    fn grant_read_acp(&self) -> Option<&'a str> {
        self.grant_read_acp
    }

    #[must_use]
    fn grant_write_acp(&self) -> Option<&'a str> {
        self.grant_write_acp
    }

    #[must_use]
    fn grant_full_control(&self) -> Option<&'a str> {
        self.grant_full_control
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEvaluation {
    ExplicitDeny,
    ExplicitAllow,
    NoMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyVersion {
    V2008_10_17,
    V2012_10_17,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyStatement {
    sid: Option<String>,
    effect: PolicyEffect,
    principal: PolicyPrincipal,
    actions: Vec<String>,
    resources: Vec<String>,
    conditions: Vec<PolicyConditionClause>,
}

impl PolicyStatement {
    #[must_use]
    pub fn sid(&self) -> Option<&str> {
        self.sid.as_deref()
    }

    #[must_use]
    pub fn effect(&self) -> PolicyEffect {
        self.effect
    }

    #[must_use]
    pub fn principal(&self) -> &PolicyPrincipal {
        &self.principal
    }

    #[must_use]
    pub fn actions(&self) -> &[String] {
        &self.actions
    }

    #[must_use]
    pub fn resources(&self) -> &[String] {
        &self.resources
    }

    #[must_use]
    pub fn conditions(&self) -> &[PolicyConditionClause] {
        &self.conditions
    }

    fn allows_public_access(&self) -> bool {
        if self.effect != PolicyEffect::Allow {
            return false;
        }

        if self.principal.is_fixed_non_public() {
            return false;
        }

        !conditions_constrain_public_principal(&self.conditions)
    }

    fn request_effect(
        &self,
        request: &PolicyRequest<'_>,
        action: &str,
        resource: &str,
    ) -> Option<PolicyEffect> {
        if !self.matches_principal(request)
            || !self.matches_action(action)
            || !self.matches_resource(resource)
        {
            return None;
        }

        match self.condition_match_result(request) {
            ConditionMatchResult::Matches => Some(self.effect),
            ConditionMatchResult::NoMatch => None,
            ConditionMatchResult::Unsupported => {
                (self.effect == PolicyEffect::Deny).then_some(PolicyEffect::Deny)
            }
        }
    }

    fn matches_principal(&self, request: &PolicyRequest<'_>) -> bool {
        self.principal.matches_request(request)
    }

    fn matches_action(&self, action: &str) -> bool {
        self.actions
            .iter()
            .any(|pattern| action_pattern_matches(pattern, action))
    }

    fn matches_resource(&self, resource: &str) -> bool {
        self.resources
            .iter()
            .any(|pattern| wildcard_matches(pattern, resource))
    }

    fn references_existing_object_tag_condition(&self) -> bool {
        self.conditions
            .iter()
            .any(|clause| clause.key.starts_with("s3:ExistingObjectTag/"))
    }

    fn references_request_object_tag_condition(&self) -> bool {
        self.conditions
            .iter()
            .any(|clause| clause.key.starts_with("s3:RequestObjectTag/"))
    }

    fn references_evaluable_object_action(&self) -> bool {
        self.actions.iter().any(|pattern| {
            EVALUABLE_OBJECT_POLICY_ACTIONS
                .iter()
                .any(|action| action_pattern_matches(pattern, action.as_str()))
        })
    }

    fn conditions_supported_for_evaluable_object_actions(&self) -> bool {
        self.conditions
            .iter()
            .all(condition_clause_supported_for_evaluable_object_actions)
    }

    fn condition_match_result(&self, request: &PolicyRequest<'_>) -> ConditionMatchResult {
        let mut saw_unsupported = false;
        for clause in &self.conditions {
            match condition_clause_matches_request(clause, request) {
                ConditionMatchResult::Matches => {}
                ConditionMatchResult::NoMatch => return ConditionMatchResult::NoMatch,
                ConditionMatchResult::Unsupported => saw_unsupported = true,
            }
        }
        if saw_unsupported {
            ConditionMatchResult::Unsupported
        } else {
            ConditionMatchResult::Matches
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

const EVALUABLE_OBJECT_POLICY_ACTIONS: [PolicyAction; 18] = [
    PolicyAction::GetObject,
    PolicyAction::GetObjectVersion,
    PolicyAction::GetObjectAcl,
    PolicyAction::GetObjectVersionAcl,
    PolicyAction::GetObjectTagging,
    PolicyAction::GetObjectVersionTagging,
    PolicyAction::GetObjectRetention,
    PolicyAction::GetObjectLegalHold,
    PolicyAction::PutObject,
    PolicyAction::PutObjectTagging,
    PolicyAction::PutObjectVersionTagging,
    PolicyAction::PutObjectRetention,
    PolicyAction::PutObjectLegalHold,
    PolicyAction::BypassGovernanceRetention,
    PolicyAction::DeleteObject,
    PolicyAction::DeleteObjectVersion,
    PolicyAction::DeleteObjectTagging,
    PolicyAction::DeleteObjectVersionTagging,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConditionMatchResult {
    Matches,
    NoMatch,
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

        let requester_principal = request.requester_principal();
        let requester_canonical_user_id = request.requester_canonical_user_id();

        self.aws.iter().any(|value| {
            requester_principal
                .is_some_and(|requester| aws_principal_matches_request(requester, value))
        }) || self
            .service
            .iter()
            .any(|value| requester_principal == Some(value.as_str()))
            || self.canonical_user.iter().any(|value| {
                requester_canonical_user_id.is_some_and(|requester| requester.as_str() == value)
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConditionClause {
    operator: String,
    key: String,
    values: Vec<String>,
}

impl PolicyConditionClause {
    #[must_use]
    pub fn operator(&self) -> &str {
        &self.operator
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum BucketPolicyError {
    #[error("malformed policy: {reason}")]
    Malformed { reason: &'static str },
}

impl BucketPolicyError {
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Malformed { reason } => reason,
        }
    }
}

pub fn parse_bucket_policy(policy: &str) -> Result<BucketPolicy, BucketPolicyError> {
    let value: Value = serde_json::from_str(policy).map_err(|_| BucketPolicyError::Malformed {
        reason: "invalid JSON",
    })?;
    let object = value.as_object().ok_or(BucketPolicyError::Malformed {
        reason: "top-level policy must be an object",
    })?;

    let version = match object.get("Version") {
        Some(Value::String(version)) => Some(parse_version(version)?),
        Some(_) => {
            return Err(BucketPolicyError::Malformed {
                reason: "Version must be a string",
            });
        }
        None => None,
    };

    let statements = parse_statements(object.get("Statement").ok_or(
        BucketPolicyError::Malformed {
            reason: "missing Statement",
        },
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
        _ => Err(BucketPolicyError::Malformed {
            reason: "unsupported Version value",
        }),
    }
}

fn parse_statements(value: &Value) -> Result<Vec<PolicyStatement>, BucketPolicyError> {
    match value {
        Value::Array(statements) => statements.iter().map(parse_statement).collect(),
        Value::Object(_) => Ok(vec![parse_statement(value)?]),
        _ => Err(BucketPolicyError::Malformed {
            reason: "Statement must be an object or array",
        }),
    }
}

fn parse_statement(value: &Value) -> Result<PolicyStatement, BucketPolicyError> {
    let object = value.as_object().ok_or(BucketPolicyError::Malformed {
        reason: "statement must be an object",
    })?;

    if object.contains_key("NotPrincipal")
        || object.contains_key("NotAction")
        || object.contains_key("NotResource")
    {
        return Err(BucketPolicyError::Malformed {
            reason: "NotPrincipal, NotAction, and NotResource are not supported",
        });
    }

    let sid = match object.get("Sid") {
        Some(Value::String(sid)) => Some(sid.clone()),
        Some(_) => {
            return Err(BucketPolicyError::Malformed {
                reason: "Sid must be a string",
            });
        }
        None => None,
    };

    let effect = match object.get("Effect") {
        Some(Value::String(effect)) => parse_effect(effect)?,
        Some(_) => {
            return Err(BucketPolicyError::Malformed {
                reason: "Effect must be a string",
            });
        }
        None => {
            return Err(BucketPolicyError::Malformed {
                reason: "missing Effect",
            });
        }
    };

    let principal = parse_principal(object.get("Principal").ok_or(
        BucketPolicyError::Malformed {
            reason: "missing Principal",
        },
    )?)?;

    let actions = parse_string_or_array(
        object.get("Action").ok_or(BucketPolicyError::Malformed {
            reason: "missing Action",
        })?,
        "Action must be a string or array of strings",
    )?;
    let resources = parse_string_or_array(
        object.get("Resource").ok_or(BucketPolicyError::Malformed {
            reason: "missing Resource",
        })?,
        "Resource must be a string or array of strings",
    )?;
    validate_resource_applicability(&actions, &resources)?;
    let conditions = match object.get("Condition") {
        Some(value) => parse_conditions(value)?,
        None => Vec::new(),
    };

    Ok(PolicyStatement {
        sid,
        effect,
        principal,
        actions,
        resources,
        conditions,
    })
}

fn validate_resource_applicability(
    actions: &[String],
    resources: &[String],
) -> Result<(), BucketPolicyError> {
    let has_bucket_action = actions.iter().any(|pattern| {
        SUPPORTED_BUCKET_POLICY_BUCKET_ACTIONS
            .iter()
            .any(|action| action_pattern_matches(pattern, action.as_str()))
    });
    let has_object_action = actions.iter().any(|pattern| {
        SUPPORTED_BUCKET_POLICY_OBJECT_ACTIONS
            .iter()
            .any(|action| action_pattern_matches(pattern, action.as_str()))
    });

    if !has_bucket_action && !has_object_action {
        return Ok(());
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

    if (has_bucket_action && !has_bucket_resource) || (has_object_action && !has_object_resource) {
        return Err(BucketPolicyError::Malformed {
            reason: "Action does not apply to any resource(s) in statement",
        });
    }

    Ok(())
}

const SUPPORTED_BUCKET_POLICY_BUCKET_ACTIONS: [PolicyAction; 4] = [
    PolicyAction::GetBucketPolicyStatus,
    PolicyAction::GetBucketPublicAccessBlock,
    PolicyAction::GetBucketObjectLockConfiguration,
    PolicyAction::ListBucket,
];

const SUPPORTED_BUCKET_POLICY_OBJECT_ACTIONS: [PolicyAction; 18] = [
    PolicyAction::GetObject,
    PolicyAction::GetObjectVersion,
    PolicyAction::GetObjectAcl,
    PolicyAction::GetObjectVersionAcl,
    PolicyAction::GetObjectTagging,
    PolicyAction::GetObjectVersionTagging,
    PolicyAction::GetObjectRetention,
    PolicyAction::GetObjectLegalHold,
    PolicyAction::PutObject,
    PolicyAction::PutObjectTagging,
    PolicyAction::PutObjectVersionTagging,
    PolicyAction::PutObjectRetention,
    PolicyAction::PutObjectLegalHold,
    PolicyAction::BypassGovernanceRetention,
    PolicyAction::DeleteObject,
    PolicyAction::DeleteObjectVersion,
    PolicyAction::DeleteObjectTagging,
    PolicyAction::DeleteObjectVersionTagging,
];

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
        _ => Err(BucketPolicyError::Malformed {
            reason: "Effect must be Allow or Deny",
        }),
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

            Ok(PolicyPrincipal {
                aws: vec![principal.clone()],
                ..PolicyPrincipal::default()
            })
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
                                principal.aws.push(value);
                            }
                        }
                    }
                    "Service" => principal.service.extend(values),
                    "CanonicalUser" => principal.canonical_user.extend(values),
                    _ => {
                        return Err(BucketPolicyError::Malformed {
                            reason: "unsupported Principal type",
                        });
                    }
                }
            }
            if principal.has_any() {
                Ok(principal)
            } else {
                Err(BucketPolicyError::Malformed {
                    reason: "Principal must not be empty",
                })
            }
        }
        _ => Err(BucketPolicyError::Malformed {
            reason: "Principal must be a string or object",
        }),
    }
}

fn parse_conditions(value: &Value) -> Result<Vec<PolicyConditionClause>, BucketPolicyError> {
    let operators = value.as_object().ok_or(BucketPolicyError::Malformed {
        reason: "Condition must be an object",
    })?;
    let mut clauses = Vec::new();
    for (operator, operands) in operators {
        let operand_object = operands.as_object().ok_or(BucketPolicyError::Malformed {
            reason: "Condition operator value must be an object",
        })?;
        for (key, value) in operand_object {
            clauses.push(PolicyConditionClause {
                operator: operator.clone(),
                key: key.clone(),
                values: parse_string_or_array(
                    value,
                    "Condition value must be a string or array of strings",
                )?,
            });
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
                return Err(BucketPolicyError::Malformed {
                    reason: "array field must not be empty",
                });
            }
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                let value = value
                    .as_str()
                    .ok_or(BucketPolicyError::Malformed { reason: field_name })?;
                parsed.push(value.to_string());
            }
            Ok(parsed)
        }
        _ => Err(BucketPolicyError::Malformed { reason: field_name }),
    }
}

fn conditions_constrain_public_principal(conditions: &[PolicyConditionClause]) -> bool {
    conditions.iter().any(is_non_public_condition_clause)
}

fn condition_clause_matches_request(
    clause: &PolicyConditionClause,
    request: &PolicyRequest<'_>,
) -> ConditionMatchResult {
    if let Some(tag_key) = clause.key.strip_prefix("s3:ExistingObjectTag/") {
        return string_equals_condition_matches(clause, request.existing_object_tag_value(tag_key));
    }
    if let Some(tag_key) = clause.key.strip_prefix("s3:RequestObjectTag/") {
        return string_condition_matches(clause, request.request_object_tag_value(tag_key));
    }

    match clause.key.as_str() {
        "s3:x-amz-copy-source" => string_condition_matches(clause, request.copy_source()),
        "s3:x-amz-metadata-directive" => {
            string_condition_matches(clause, request.metadata_directive())
        }
        "s3:x-amz-acl" => string_condition_matches(clause, request.canned_acl()),
        "s3:x-amz-server-side-encryption-customer-algorithm" => {
            string_condition_matches(clause, request.sse_customer_algorithm())
        }
        "s3:x-amz-grant-read" => string_condition_matches(clause, request.grant_read()),
        "s3:x-amz-grant-write" => string_condition_matches(clause, request.grant_write()),
        "s3:x-amz-grant-read-acp" => string_condition_matches(clause, request.grant_read_acp()),
        "s3:x-amz-grant-write-acp" => string_condition_matches(clause, request.grant_write_acp()),
        "s3:x-amz-grant-full-control" => {
            string_condition_matches(clause, request.grant_full_control())
        }
        _ => ConditionMatchResult::Unsupported,
    }
}

fn condition_clause_supported_for_evaluable_object_actions(clause: &PolicyConditionClause) -> bool {
    (clause.operator == "StringEquals" && clause.key.starts_with("s3:ExistingObjectTag/"))
        || (matches!(
            clause.operator.as_str(),
            "StringEquals" | "StringLike" | "StringNotEquals" | "Null"
        ) && matches!(
            clause.key.as_str(),
            "s3:x-amz-copy-source"
                | "s3:x-amz-metadata-directive"
                | "s3:x-amz-acl"
                | "s3:x-amz-server-side-encryption-customer-algorithm"
                | "s3:x-amz-grant-read"
                | "s3:x-amz-grant-write"
                | "s3:x-amz-grant-read-acp"
                | "s3:x-amz-grant-write-acp"
                | "s3:x-amz-grant-full-control"
        ))
        || (matches!(
            clause.operator.as_str(),
            "StringEquals" | "StringLike" | "StringNotEquals" | "Null"
        ) && clause.key.starts_with("s3:RequestObjectTag/"))
}

fn string_equals_condition_matches(
    clause: &PolicyConditionClause,
    actual: Option<&str>,
) -> ConditionMatchResult {
    if clause.operator != "StringEquals" {
        return ConditionMatchResult::Unsupported;
    }

    if actual.is_some_and(|actual| clause.values.iter().any(|expected| expected == actual)) {
        ConditionMatchResult::Matches
    } else {
        ConditionMatchResult::NoMatch
    }
}

fn string_condition_matches(
    clause: &PolicyConditionClause,
    actual: Option<&str>,
) -> ConditionMatchResult {
    match clause.operator.as_str() {
        "StringEquals" => {
            let Some(actual) = actual else {
                return ConditionMatchResult::NoMatch;
            };
            if clause.values.iter().any(|expected| expected == actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        "StringLike" => {
            let Some(actual) = actual else {
                return ConditionMatchResult::NoMatch;
            };
            if clause
                .values
                .iter()
                .any(|expected| wildcard_matches(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        "StringNotEquals" => match actual {
            Some(actual) => {
                if clause.values.iter().all(|expected| expected != actual) {
                    ConditionMatchResult::Matches
                } else {
                    ConditionMatchResult::NoMatch
                }
            }
            None => ConditionMatchResult::Matches,
        },
        "Null" => {
            let is_null = actual.is_none();
            if clause
                .values
                .iter()
                .any(|expected| match expected.as_str() {
                    "true" => is_null,
                    "false" => !is_null,
                    _ => false,
                })
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        _ => ConditionMatchResult::Unsupported,
    }
}

fn action_pattern_matches(pattern: &str, action: &str) -> bool {
    wildcard_matches(&pattern.to_ascii_lowercase(), &action.to_ascii_lowercase())
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value;
    }

    let segments: Vec<&str> = pattern.split('*').collect();
    if segments.iter().all(|segment| segment.is_empty()) {
        return true;
    }

    let mut search_start = 0;
    let mut start_index = 0;

    if !pattern.starts_with('*') {
        let first = segments
            .first()
            .expect("split always yields a first segment");
        let Some(_remaining) = value.strip_prefix(first) else {
            return false;
        };
        search_start = first.len();
        start_index = 1;
    }

    let end_index = if pattern.ends_with('*') {
        segments.len()
    } else {
        segments.len().saturating_sub(1)
    };
    for segment in &segments[start_index..end_index] {
        if segment.is_empty() {
            continue;
        }
        let Some(found) = value[search_start..].find(segment) else {
            return false;
        };
        search_start += found + segment.len();
    }

    if pattern.ends_with('*') {
        true
    } else {
        let last = segments.last().expect("split always yields a last segment");
        value[search_start..].ends_with(last)
    }
}

fn aws_principal_matches_request(requester_principal: &str, policy_value: &str) -> bool {
    if requester_principal == policy_value {
        return true;
    }

    let Some(policy_root_account_id) = root_account_principal_account_id(policy_value) else {
        return false;
    };

    requester_principal == policy_root_account_id
        || iam_principal_account_id(requester_principal) == Some(policy_root_account_id)
}

fn root_account_principal_account_id(value: &str) -> Option<&str> {
    let account_id = iam_principal_account_id(value)?;
    value.ends_with(":root").then_some(account_id)
}

fn iam_principal_account_id(value: &str) -> Option<&str> {
    let rest = value.strip_prefix("arn:")?;
    let mut parts = rest.splitn(6, ':');
    let _partition = parts.next()?;
    let service = parts.next()?;
    if service != "iam" {
        return None;
    }
    let _region = parts.next()?;
    let account_id = parts.next()?;
    let _resource = parts.next()?;
    (!account_id.is_empty()).then_some(account_id)
}

fn is_non_public_condition_clause(clause: &PolicyConditionClause) -> bool {
    match clause.key.as_str() {
        "aws:PrincipalOrgID"
        | "aws:SourceVpc"
        | "aws:SourceVpce"
        | "aws:SourceOwner"
        | "aws:SourceAccount"
        | "aws:userid"
        | "s3:DataAccessPointAccount" => {
            matches!(
                clause.operator.as_str(),
                "StringEquals" | "StringEqualsIgnoreCase" | "StringLike"
            ) && clause.values.iter().all(|value| is_fixed_value(value))
        }
        "aws:SourceArn" | "s3:DataAccessPointArn" => {
            matches!(
                clause.operator.as_str(),
                "ArnEquals" | "ArnLike" | "StringEquals" | "StringEqualsIgnoreCase" | "StringLike"
            ) && clause.values.iter().all(|value| is_fixed_value(value))
        }
        "aws:SourceIp" => {
            clause.operator == "IpAddress"
                && clause.values.iter().all(|value| is_fixed_source_ip(value))
        }
        _ => false,
    }
}

fn is_fixed_value(value: &str) -> bool {
    !value.contains('*') && !value.contains("${")
}

fn is_fixed_source_ip(value: &str) -> bool {
    if !is_fixed_value(value) {
        return false;
    }
    if let Some((_, prefix)) = value.split_once('/') {
        return prefix.parse::<u8>().is_ok();
    }
    value.parse::<std::net::IpAddr>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request<'a>(
        action: PolicyAction,
        bucket: &'a str,
        key: &'a str,
        requester_principal: Option<&'a str>,
        existing_object_tags: &'a [PolicyTag<'a>],
    ) -> PolicyRequest<'a> {
        PolicyRequest::new(action, bucket, key, requester_principal, None)
            .with_existing_object_tags(existing_object_tags)
    }

    fn bucket_request<'a>(
        action: PolicyAction,
        bucket: &'a str,
        requester_principal: Option<&'a str>,
    ) -> PolicyRequest<'a> {
        PolicyRequest::for_bucket(action, bucket, requester_principal, None)
    }

    #[test]
    fn parse_empty_statement_array() {
        let policy = parse_bucket_policy(r#"{"Version":"2012-10-17","Statement":[]}"#).unwrap();
        assert_eq!(policy.version(), Some(PolicyVersion::V2012_10_17));
        assert!(policy.statements().is_empty());
        assert!(!policy.is_public());
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
    fn fixed_source_vpc_constrains_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"aws:SourceVpc":"vpc-12345678"}}}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
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
            BucketPolicyError::Malformed {
                reason: "invalid JSON"
            }
        );
    }

    #[test]
    fn missing_statement_is_rejected() {
        let err = parse_bucket_policy(r#"{"Version":"2012-10-17"}"#).unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "missing Statement"
            }
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
            BucketPolicyError::Malformed {
                reason: "NotPrincipal, NotAction, and NotResource are not supported"
            }
        );
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
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}},{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectTagging","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();

        assert!(policy.requires_existing_object_tags_for_action(PolicyAction::GetObject));
        assert!(!policy.requires_existing_object_tags_for_action(PolicyAction::GetObjectTagging));
    }

    #[test]
    fn requires_request_object_tags_for_matching_action() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}},{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();

        assert!(policy.requires_request_object_tags_for_action(PolicyAction::PutObject));
        assert!(!policy.requires_request_object_tags_for_action(PolicyAction::GetObject));
    }

    #[test]
    fn request_object_tag_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let request_tags = [PolicyTag::new("security", "public")];
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn request_object_tag_null_condition_matches_missing_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:RequestObjectTag/security":"true"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn request_object_tag_string_not_equals_matches_mismatched_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let request_tags = [PolicyTag::new("security", "private")];
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn grant_full_control_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-grant-full-control":"id=owner-canonical-id"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
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
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn grant_full_control_string_not_equals_matches_mismatched_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:x-amz-grant-full-control":"id=owner-canonical-id"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
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
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
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
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn sse_customer_algorithm_string_not_equals_matches_mismatched_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:x-amz-server-side-encryption-customer-algorithm":"AES256"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
        )
        .with_sse_customer_algorithm(Some("AES192"));

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
            Err(BucketPolicyError::Malformed {
                reason: "unsupported Condition for currently enforced bucket policy action",
            })
        );
    }

    #[test]
    fn unsupported_deny_condition_is_treated_conservatively_at_evaluation_time() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"aws:PrincipalArn":"arn:aws:iam::444455556666:user/other"}}}]}"#,
        )
        .unwrap();
        let tags: [PolicyTag<'_>; 0] = [];
        let request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("444455556666"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn copy_source_condition_matches_string_like() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"StringLike":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
        )
        .unwrap();
        let request =
            PolicyRequest::new(PolicyAction::PutObject, "dst", "key", Some("caller"), None)
                .with_copy_source(Some("src/public/foo"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn metadata_directive_condition_requires_explicit_copy_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-metadata-directive":"COPY"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
        );
        let explicit_copy = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
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
        let request = PolicyRequest::new(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
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
    fn get_object_retention_existing_tag_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectRetention","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let tags = [PolicyTag::new("security", "public")];
        let request = request(
            PolicyAction::GetObjectRetention,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
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
    fn list_bucket_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_policy_status_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketPolicyStatus","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Action does not apply to any resource(s) in statement",
            }
        );
    }

    #[test]
    fn get_bucket_object_lock_configuration_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Action does not apply to any resource(s) in statement",
            }
        );
    }

    #[test]
    fn list_bucket_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Action does not apply to any resource(s) in statement",
            }
        );
    }

    #[test]
    fn get_object_bucket_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Action does not apply to any resource(s) in statement",
            }
        );
    }
}
