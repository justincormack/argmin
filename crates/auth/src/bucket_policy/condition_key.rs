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

use super::condition_op::{self, ActualValue, ConditionSetQualifier};
use super::{
    BucketTagValue, ConditionMatchResult, ExistingObjectTagValue, PolicyAction,
    PolicyConditionClause, PolicyRequest, PolicyValue, RequestBool, RequestField,
    RequestObjectTagKeysValue, RequestObjectTagValue,
};

/// Resolved value for a condition key in a given request.
///
/// Mirrors the existing evaluator's distinction between "known absent",
/// "present with this value", and "cannot be determined in this context".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ResolvedValue<'a> {
    Present(&'a str),
    PresentScalarValues(Vec<&'a str>),
    PresentValues(Vec<&'a str>),
    SourceIp(std::net::IpAddr),
    Numeric(u64),
    EpochSeconds(u64),
    Absent,
    Unavailable,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PolicyVariableResolution {
    Value(String),
    Absent,
    Unavailable,
}

/// Which operator families a condition key supports.
///
/// `AnyEvaluable` accepts the string and binary operators flagged with
/// `evaluable_on_evaluable_object_actions` in the operator table.
/// `StringEqualsOnly` narrows to the `StringEquals` / `StringEqualsIfExists`
/// fast path used by `s3:ExistingObjectTag/*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OperatorSupport {
    AnyEvaluable,
    ArnEqualsOnly,
    BoolOnly,
    DateOnly,
    IpOnly,
    NumericOnly,
    StringOrNumeric,
    StringEqualsOnly,
}

/// How the resolver matches a request condition key.
///
/// Exact and prefix keys compare case-insensitively, matching AWS IAM context
/// key semantics. Prefix keys pass the original-cased remainder to `resolve`
/// as the parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyMatch {
    Exact(&'static str),
    Prefix(&'static str),
}

impl KeyMatch {
    fn match_key<'a>(&self, key: &'a str) -> Option<&'a str> {
        match self {
            Self::Exact(name) => key.eq_ignore_ascii_case(name).then_some(""),
            Self::Prefix(prefix) => match_key_prefix(key, prefix),
        }
    }
}

/// Signature for a per-key value resolver.
///
/// The second argument is the parameter portion for prefix keys (for
/// example the tag name after `s3:ExistingObjectTag/`) or `""` for exact
/// keys.
pub(super) type ResolveFn = for<'a> fn(&PolicyRequest<'a>, &str) -> ResolvedValue<'a>;

/// Runtime input family a condition key needs when it is actually evaluable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionInput {
    ExistingObject,
    Request,
    Bucket,
    SourceIp,
    CurrentTime,
    SecureTransport,
    RequestedRegion,
    Referer,
    AuthType,
    SignatureVersion,
    SignatureAge,
    TlsVersion,
    ContentSha256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionActionStatus {
    Evaluable,
    AcceptedButNotEvaluable,
    PolicyInvalid,
    Unsupported,
}

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
    pub(super) input: Option<ConditionInput>,
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
        input: Some(ConditionInput::ExistingObject),
        resolve: resolve_existing_object_tag,
        evaluable_for_action: Some(existing_object_tag_evaluable_for_action),
        supported_for_action: Some(existing_object_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Prefix("s3:BucketTag/"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::Bucket),
        resolve: resolve_bucket_tag,
        evaluable_for_action: None,
        supported_for_action: Some(bucket_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Prefix("aws:ResourceTag/"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::Bucket),
        resolve: resolve_resource_tag,
        evaluable_for_action: None,
        supported_for_action: Some(bucket_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:SourceIp"),
        operator_support: OperatorSupport::IpOnly,
        input: Some(ConditionInput::SourceIp),
        resolve: resolve_source_ip,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:userid"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_userid,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:PrincipalArn"),
        operator_support: OperatorSupport::ArnEqualsOnly,
        input: None,
        resolve: resolve_principal_arn,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:TokenIssueTime"),
        operator_support: OperatorSupport::DateOnly,
        input: None,
        resolve: resolve_token_issue_time,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:PrincipalType"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_principal_type,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:username"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_username,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:CurrentTime"),
        operator_support: OperatorSupport::DateOnly,
        input: Some(ConditionInput::CurrentTime),
        resolve: resolve_current_time,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:EpochTime"),
        operator_support: OperatorSupport::NumericOnly,
        input: Some(ConditionInput::CurrentTime),
        resolve: resolve_current_time,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:SecureTransport"),
        operator_support: OperatorSupport::BoolOnly,
        input: Some(ConditionInput::SecureTransport),
        resolve: resolve_secure_transport,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:RequestedRegion"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::RequestedRegion),
        resolve: resolve_requested_region,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:referer"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::Referer),
        resolve: resolve_referer,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:authType"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::AuthType),
        resolve: resolve_auth_type,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:signatureversion"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::SignatureVersion),
        resolve: resolve_signature_version,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:signatureAge"),
        operator_support: OperatorSupport::NumericOnly,
        input: Some(ConditionInput::SignatureAge),
        resolve: resolve_signature_age,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:TlsVersion"),
        operator_support: OperatorSupport::NumericOnly,
        input: Some(ConditionInput::TlsVersion),
        resolve: resolve_tls_version,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-content-sha256"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::ContentSha256),
        resolve: resolve_content_sha256,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Prefix("s3:RequestObjectTag/"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::Request),
        resolve: resolve_request_object_tag,
        evaluable_for_action: None,
        supported_for_action: Some(request_object_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Prefix("aws:RequestTag/"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::Request),
        resolve: resolve_request_object_tag,
        evaluable_for_action: None,
        supported_for_action: Some(request_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:RequestObjectTagKeys"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::Request),
        resolve: resolve_request_tag_keys,
        evaluable_for_action: None,
        supported_for_action: Some(request_object_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("aws:TagKeys"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: Some(ConditionInput::Request),
        resolve: resolve_request_tag_keys,
        evaluable_for_action: None,
        supported_for_action: Some(tag_keys_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-copy-source"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_copy_source,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-metadata-directive"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_metadata_directive,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-acl"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_canned_acl,
        evaluable_for_action: None,
        supported_for_action: Some(canned_acl_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-server-side-encryption"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_server_side_encryption,
        evaluable_for_action: None,
        supported_for_action: Some(server_side_encryption_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-server-side-encryption-customer-algorithm"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_sse_customer_algorithm,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-website-redirect-location"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_website_redirect_location,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-read"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_grant_read,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-write"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
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
        input: None,
        resolve: resolve_grant_read_acp,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-write-acp"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_grant_write_acp,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-full-control"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_grant_full_control,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:if-match"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_if_match,
        evaluable_for_action: None,
        supported_for_action: Some(conditional_write_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:if-none-match"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_if_none_match,
        evaluable_for_action: None,
        supported_for_action: Some(conditional_write_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:ObjectCreationOperation"),
        operator_support: OperatorSupport::BoolOnly,
        input: None,
        resolve: resolve_object_creation_operation,
        evaluable_for_action: None,
        supported_for_action: Some(conditional_write_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:prefix"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_prefix,
        evaluable_for_action: None,
        supported_for_action: Some(list_condition_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:delimiter"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_delimiter,
        evaluable_for_action: None,
        supported_for_action: Some(list_condition_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:max-keys"),
        operator_support: OperatorSupport::StringOrNumeric,
        input: None,
        resolve: resolve_max_keys,
        evaluable_for_action: None,
        supported_for_action: Some(list_condition_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:locationconstraint"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_location_constraint,
        evaluable_for_action: None,
        supported_for_action: Some(location_constraint_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-object-ownership"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_object_ownership,
        evaluable_for_action: None,
        supported_for_action: Some(object_ownership_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:versionid"),
        operator_support: OperatorSupport::AnyEvaluable,
        input: None,
        resolve: resolve_version_id,
        evaluable_for_action: None,
        supported_for_action: Some(version_id_supported_for_action),
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

/// Whether `key` is an AWS-recognized S3 bucket-policy condition key.
///
/// This includes keys that Argmin recognizes but intentionally rejects at
/// policy-upload time until they are runtime-evaluable. Unknown keys are
/// rejected with AWS's `Policy has an invalid condition key` error, while
/// recognized-but-deferred keys continue down the explicit unsupported path.
pub(super) fn is_known_condition_key(key: &str) -> bool {
    lookup(key).is_some()
        || KNOWN_DEFERRED_EXACT_CONDITION_KEYS
            .iter()
            .any(|known| key.eq_ignore_ascii_case(known))
        || KNOWN_DEFERRED_PREFIX_CONDITION_KEYS
            .iter()
            .any(|prefix| match_key_prefix(key, prefix).is_some())
}

const KNOWN_DEFERRED_EXACT_CONDITION_KEYS: &[&str] = &[
    "aws:AssumedRoot",
    "aws:CalledVia",
    "aws:CalledViaAWSMCP",
    "aws:CalledViaFirst",
    "aws:CalledViaLast",
    "aws:ChatbotSourceArn",
    "aws:Ec2InstanceSourcePrivateIPv4",
    "aws:Ec2InstanceSourceVpc",
    "aws:FederatedProvider",
    "aws:IsMcpServiceAction",
    "aws:MultiFactorAuthAge",
    "aws:MultiFactorAuthPresent",
    "aws:PrincipalAccount",
    "aws:PrincipalIsAWSService",
    "aws:PrincipalOrgID",
    "aws:PrincipalOrgPaths",
    "aws:PrincipalServiceName",
    "aws:PrincipalServiceNamesList",
    "aws:ResourceAccount",
    "aws:ResourceOrgID",
    "aws:ResourceOrgPaths",
    "aws:SourceAccount",
    "aws:SourceArn",
    "aws:SourceIdentity",
    "aws:SourceOrgID",
    "aws:SourceOrgPaths",
    "aws:SourceOwner",
    "aws:SourceVpc",
    "aws:SourceVpcArn",
    "aws:SourceVpce",
    "aws:UserAgent",
    "aws:ViaAWSMCPService",
    "aws:ViaAWSService",
    "aws:VpcSourceIp",
    "aws:VpceAccount",
    "aws:VpceOrgID",
    "aws:VpceOrgPaths",
    "codebuild:BuildArn",
    "codebuild:ProjectArn",
    "ec2:RoleDelivery",
    "ec2:SourceInstanceArn",
    "glue:CredentialIssuingService",
    "glue:RoleAssumedBy",
    "identitystore:UserId",
    "lambda:SourceFunctionArn",
    "ssm:SourceInstanceArn",
    "s3:DataAccessPointAccount",
    "s3:DataAccessPointArn",
    "s3:AccessGrantScope",
    "s3:AccessGrantsInstanceArn",
    "s3:AccessGrantsLocationScope",
    "s3:AccessPointNetworkOrigin",
    "s3:ExistingJobOperation",
    "s3:ExistingJobPriority",
    "s3:annotation-prefix",
    "s3:deliverySourceArn",
    "s3:destinationRegion",
    "s3:InventoryAccessibleOptionalFields",
    "s3:InventoryField",
    "s3:isReplicationPauseRequest",
    "s3:JobSuspendedCause",
    "s3:logType",
    "s3:max-annotation-results",
    "s3:RequestJobOperation",
    "s3:RequestJobPriority",
    "s3:ResourceAccount",
    "s3:resourceArnBeingAuthorized",
    "s3:object-lock-mode",
    "s3:object-lock-legal-hold",
    "s3:object-lock-retain-until-date",
    "s3:object-lock-remaining-retention-days",
    "s3:x-amz-bucket-namespace",
    "s3:x-amz-object-annotation-directive",
    "s3:x-amz-object-if-match",
    "s3:x-amz-server-side-encryption-aws-kms-key-id",
    "s3:x-amz-storage-class",
];

const KNOWN_DEFERRED_PREFIX_CONDITION_KEYS: &[&str] = &["aws:PrincipalTag/", "s3:AccessPointTag/"];

fn match_key_prefix<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
    if key.len() < prefix.len() {
        return None;
    }
    let (candidate_prefix, param) = (key.get(..prefix.len())?, key.get(prefix.len()..)?);
    candidate_prefix
        .eq_ignore_ascii_case(prefix)
        .then_some(param)
}

fn resolve_existing_object_tag<'a>(request: &PolicyRequest<'a>, param: &str) -> ResolvedValue<'a> {
    match request.existing_object_tag_value(param) {
        ExistingObjectTagValue::Unavailable => ResolvedValue::Unavailable,
        ExistingObjectTagValue::Available(Some(value)) => ResolvedValue::Present(value),
        ExistingObjectTagValue::Available(None) => ResolvedValue::Absent,
    }
}

fn resolve_request_object_tag<'a>(request: &PolicyRequest<'a>, param: &str) -> ResolvedValue<'a> {
    match request.request_object_tag_value(param) {
        RequestObjectTagValue::Unavailable => ResolvedValue::Unavailable,
        RequestObjectTagValue::Available(values) if values.is_empty() => ResolvedValue::Absent,
        RequestObjectTagValue::Available(values) if values.len() == 1 => {
            ResolvedValue::Present(values[0])
        }
        RequestObjectTagValue::Available(values) => ResolvedValue::PresentScalarValues(values),
    }
}

fn resolve_request_tag_keys<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    match request.request_tag_keys() {
        RequestObjectTagKeysValue::Unavailable => ResolvedValue::Unavailable,
        RequestObjectTagKeysValue::Available(keys) if keys.is_empty() => ResolvedValue::Absent,
        RequestObjectTagKeysValue::Available(keys) => ResolvedValue::PresentValues(keys),
    }
}

fn resolve_bucket_tag<'a>(request: &PolicyRequest<'a>, param: &str) -> ResolvedValue<'a> {
    match request.bucket_tag_value(param) {
        BucketTagValue::Unavailable => ResolvedValue::Unavailable,
        BucketTagValue::Available(Some(value)) => ResolvedValue::Present(value),
        BucketTagValue::Available(None) => ResolvedValue::Absent,
    }
}

fn resolve_resource_tag<'a>(request: &PolicyRequest<'a>, param: &str) -> ResolvedValue<'a> {
    match request.resource_tag_value(param) {
        BucketTagValue::Unavailable => ResolvedValue::Unavailable,
        BucketTagValue::Available(Some(value)) => ResolvedValue::Present(value),
        BucketTagValue::Available(None) => ResolvedValue::Absent,
    }
}

fn resolve_source_ip<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    match request.source_ip() {
        Some(source_ip) => ResolvedValue::SourceIp(source_ip),
        None => ResolvedValue::Unavailable,
    }
}

fn resolve_userid<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    if request.requester_is_anonymous() {
        ResolvedValue::Present("anonymous")
    } else {
        match request.aws_userid() {
            Some(userid) => ResolvedValue::Present(userid),
            None => ResolvedValue::Unavailable,
        }
    }
}

fn resolve_principal_arn<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    match request.aws_principal_arn() {
        Some(principal_arn) => ResolvedValue::Present(principal_arn),
        None if request.requester_is_anonymous() => ResolvedValue::Absent,
        None => ResolvedValue::Unsupported,
    }
}

fn resolve_token_issue_time<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    match request.token_issue_time_epoch_seconds() {
        Some(epoch_seconds) => ResolvedValue::EpochSeconds(epoch_seconds),
        None => ResolvedValue::Absent,
    }
}

fn resolve_principal_type<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    if request.requester_is_anonymous() {
        ResolvedValue::Present("Anonymous")
    } else if request.aws_userid().is_some() {
        ResolvedValue::Present("AssumedRole")
    } else {
        ResolvedValue::Unavailable
    }
}

fn resolve_username<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    if request.requester_principal().is_some() {
        ResolvedValue::Unavailable
    } else {
        ResolvedValue::Absent
    }
}

fn resolve_current_time<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    match request.current_time_epoch_seconds() {
        Some(epoch_seconds) => ResolvedValue::EpochSeconds(epoch_seconds),
        None => ResolvedValue::Unavailable,
    }
}

fn resolve_secure_transport<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    match request.secure_transport() {
        RequestBool::Unavailable => ResolvedValue::Unavailable,
        RequestBool::Available(Some(true)) => ResolvedValue::Present("true"),
        RequestBool::Available(Some(false)) => ResolvedValue::Present("false"),
        RequestBool::Available(None) => ResolvedValue::Absent,
    }
}

fn resolve_requested_region<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.requested_region())
}

fn resolve_referer<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.referer())
}

fn resolve_auth_type<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.auth_type())
}

fn resolve_signature_version<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.signature_version())
}

fn resolve_signature_age<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    match request.signature_age_millis() {
        Some(Some(signature_age_millis)) => ResolvedValue::Numeric(signature_age_millis),
        Some(None) => ResolvedValue::Absent,
        None => ResolvedValue::Unavailable,
    }
}

fn resolve_tls_version<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.tls_version())
}

fn resolve_content_sha256<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.content_sha256())
}

fn resolve_copy_source<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.copy_source())
}

fn resolve_metadata_directive<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.metadata_directive())
}

fn resolve_canned_acl<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.canned_acl())
}

fn resolve_server_side_encryption<'a>(
    request: &PolicyRequest<'a>,
    _param: &str,
) -> ResolvedValue<'a> {
    request_field_to_resolved(request.server_side_encryption())
}

fn resolve_sse_customer_algorithm<'a>(
    request: &PolicyRequest<'a>,
    _param: &str,
) -> ResolvedValue<'a> {
    request_field_to_resolved(request.sse_customer_algorithm())
}

fn resolve_website_redirect_location<'a>(
    request: &PolicyRequest<'a>,
    _param: &str,
) -> ResolvedValue<'a> {
    request_field_to_resolved(request.website_redirect_location())
}

fn resolve_grant_read<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.grant_read())
}

fn resolve_grant_write<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.grant_write())
}

fn resolve_grant_read_acp<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.grant_read_acp())
}

fn resolve_grant_write_acp<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.grant_write_acp())
}

fn resolve_grant_full_control<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.grant_full_control())
}

fn resolve_if_match<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.if_match())
}

fn resolve_if_none_match<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.if_none_match())
}

fn resolve_object_creation_operation<'a>(
    request: &PolicyRequest<'a>,
    _param: &str,
) -> ResolvedValue<'a> {
    match request.object_creation_operation() {
        RequestBool::Unavailable => ResolvedValue::Unavailable,
        RequestBool::Available(Some(true)) => ResolvedValue::Present("true"),
        RequestBool::Available(Some(false)) => ResolvedValue::Present("false"),
        RequestBool::Available(None) => ResolvedValue::Absent,
    }
}

fn resolve_prefix<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.prefix())
}

fn resolve_delimiter<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.delimiter())
}

fn resolve_max_keys<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.max_keys())
}

fn resolve_location_constraint<'a>(
    _request: &PolicyRequest<'a>,
    _param: &str,
) -> ResolvedValue<'a> {
    ResolvedValue::Absent
}

fn resolve_object_ownership<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.object_ownership())
}

fn resolve_version_id<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    request_field_to_resolved(request.version_id())
}

fn request_field_to_resolved(value: RequestField<'_>) -> ResolvedValue<'_> {
    match value {
        RequestField::Unavailable => ResolvedValue::Unavailable,
        RequestField::Available(Some(value)) => ResolvedValue::Present(value),
        RequestField::Available(None) => ResolvedValue::Absent,
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
    matches!(
        action,
        PolicyAction::PutObject
            | PolicyAction::PutObjectTagging
            | PolicyAction::PutObjectVersionTagging
    )
}

fn tag_keys_supported_for_action(action: PolicyAction) -> bool {
    matches!(
        action,
        PolicyAction::TagResource | PolicyAction::UntagResource
    )
}

fn request_tag_supported_for_action(action: PolicyAction) -> bool {
    matches!(
        action,
        PolicyAction::TagResource | PolicyAction::UntagResource
    )
}

fn bucket_tag_supported_for_action(action: PolicyAction) -> bool {
    matches!(
        action,
        PolicyAction::GetObject
            | PolicyAction::GetObjectAcl
            | PolicyAction::GetObjectVersionAcl
            | PolicyAction::GetObjectTagging
            | PolicyAction::GetObjectVersionTagging
            | PolicyAction::GetObjectRetention
            | PolicyAction::GetObjectLegalHold
            | PolicyAction::PutObject
            | PolicyAction::PutObjectAcl
            | PolicyAction::PutObjectVersionAcl
            | PolicyAction::PutObjectTagging
            | PolicyAction::PutObjectVersionTagging
            | PolicyAction::PutObjectRetention
            | PolicyAction::PutObjectLegalHold
            | PolicyAction::BypassGovernanceRetention
            | PolicyAction::DeleteObject
            | PolicyAction::DeleteObjectVersion
            | PolicyAction::DeleteObjectTagging
            | PolicyAction::DeleteObjectVersionTagging
            | PolicyAction::GetBucketPolicy
            | PolicyAction::PutBucketPolicy
            | PolicyAction::DeleteBucket
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
            | PolicyAction::TagResource
            | PolicyAction::UntagResource
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

fn canned_acl_supported_for_action(action: PolicyAction) -> bool {
    request_header_supported_for_action_non_get(action)
        && !matches!(
            action,
            PolicyAction::PutObjectTagging | PolicyAction::PutObjectVersionTagging
        )
}

fn server_side_encryption_supported_for_action(action: PolicyAction) -> bool {
    request_header_supported_for_action_non_get(action)
        && !matches!(
            action,
            PolicyAction::PutObjectTagging | PolicyAction::PutObjectVersionTagging
        )
}

fn conditional_write_supported_for_action(action: PolicyAction) -> bool {
    matches!(action, PolicyAction::PutObject)
}

fn list_condition_supported_for_action(action: PolicyAction) -> bool {
    matches!(
        action,
        PolicyAction::ListBucket | PolicyAction::ListBucketVersions
    )
}

fn location_constraint_supported_for_action(_action: PolicyAction) -> bool {
    false
}

fn object_ownership_supported_for_action(action: PolicyAction) -> bool {
    matches!(action, PolicyAction::PutBucketOwnershipControls)
}

fn version_id_supported_for_action(action: PolicyAction) -> bool {
    matches!(
        action,
        PolicyAction::GetObjectVersion
            | PolicyAction::GetObjectVersionAttributes
            | PolicyAction::GetObjectVersionAcl
            | PolicyAction::GetObjectVersionTagging
            | PolicyAction::PutObjectVersionAcl
            | PolicyAction::PutObjectVersionTagging
            | PolicyAction::DeleteObjectVersion
            | PolicyAction::DeleteObjectVersionTagging
    )
}

pub(super) fn clause_action_status(
    clause: &PolicyConditionClause,
    action: PolicyAction,
) -> ConditionActionStatus {
    let Some((resolver, _param)) = lookup(clause.key.as_str()) else {
        return ConditionActionStatus::Unsupported;
    };
    let operator = clause.operator.as_str();
    let operator_ok = operator_supported_for_key(operator, resolver.operator_support);
    if !operator_ok {
        return ConditionActionStatus::Unsupported;
    }
    if resolver
        .supported_for_action
        .is_some_and(|predicate| !predicate(action))
    {
        return ConditionActionStatus::PolicyInvalid;
    }
    if resolver
        .evaluable_for_action
        .is_some_and(|predicate| !predicate(action))
    {
        return ConditionActionStatus::AcceptedButNotEvaluable;
    }
    ConditionActionStatus::Evaluable
}

/// Whether a condition clause is supported on a given action at
/// statement-validation time.
///
/// A clause is supported when its key has a resolver row, the operator is
/// compatible with the row's `operator_support`, and the row's
/// `supported_for_action` predicate (if any) admits the action. A clause can
/// still be supported even when AWS accepts it but does not evaluate it for
/// this action.
pub(super) fn supports_clause_for_action(
    clause: &PolicyConditionClause,
    action: PolicyAction,
) -> bool {
    matches!(
        clause_action_status(clause, action),
        ConditionActionStatus::Evaluable | ConditionActionStatus::AcceptedButNotEvaluable
    )
}

pub(super) fn clause_requires_input_for_action(
    clause: &PolicyConditionClause,
    action: PolicyAction,
    input: ConditionInput,
) -> bool {
    let Some((resolver, _param)) = lookup(clause.key.as_str()) else {
        return false;
    };
    resolver.input == Some(input)
        && matches!(
            clause_action_status(clause, action),
            ConditionActionStatus::Evaluable
        )
}

pub(super) fn clause_input(clause: &PolicyConditionClause) -> Option<ConditionInput> {
    lookup(clause.key.as_str()).and_then(|(resolver, _param)| resolver.input)
}

/// Evaluate a condition clause against a request.
///
/// Single entry point for the evaluator. Returns `Unsupported` if the
/// condition key is not in the table or if the operator is not in the
/// resolver's allowed set.
pub(super) fn evaluate_clause(
    clause: &PolicyConditionClause,
    request: &PolicyRequest<'_>,
    variables_enabled: bool,
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
    if !operator_supported_for_key(op.name, resolver.operator_support) {
        return ConditionMatchResult::Unsupported;
    }
    let resolved = (resolver.resolve)(request, param);
    let expanded_values;
    let values = if variables_enabled && condition_op::supports_policy_variables(op.kind) {
        expanded_values = clause
            .values
            .iter()
            .filter_map(|value| super::expand_policy_template(value, request))
            .collect::<Vec<_>>();
        expanded_values.as_slice()
    } else {
        expanded_values = clause
            .values
            .iter()
            .map(|value| PolicyValue::literal(value))
            .collect::<Vec<_>>();
        expanded_values.as_slice()
    };
    let actual = match resolved {
        ResolvedValue::Present(value) => ActualValue::Present(value),
        ResolvedValue::PresentScalarValues(actuals) => {
            return evaluate_scalar_values(*op, values, &actuals);
        }
        ResolvedValue::PresentValues(values) => ActualValue::PresentValues(values),
        ResolvedValue::SourceIp(source_ip) => ActualValue::SourceIp(source_ip),
        ResolvedValue::Numeric(value) => ActualValue::Numeric(value),
        ResolvedValue::EpochSeconds(epoch_seconds) => ActualValue::EpochSeconds(epoch_seconds),
        ResolvedValue::Absent => ActualValue::Absent,
        ResolvedValue::Unavailable => return ConditionMatchResult::InputUnavailable,
        ResolvedValue::Unsupported => return ConditionMatchResult::Unsupported,
    };
    (op.evaluate)(values, actual)
}

fn evaluate_scalar_values(
    op: condition_op::ConditionOpDef,
    operands: &[PolicyValue],
    actuals: &[&str],
) -> ConditionMatchResult {
    let actual_matches = |actual: &&str| {
        (op.evaluate)(operands, ActualValue::Present(actual)) == ConditionMatchResult::Matches
    };
    let matches = match op.set_qualifier {
        ConditionSetQualifier::ForAllValues => actuals.iter().all(actual_matches),
        ConditionSetQualifier::ForAnyValue => actuals.iter().any(actual_matches),
        ConditionSetQualifier::None => match op.kind {
            condition_op::ConditionOpKind::ArnEquals
            | condition_op::ConditionOpKind::BinaryEquals
            | condition_op::ConditionOpKind::StringEquals
            | condition_op::ConditionOpKind::StringEqualsIgnoreCase
            | condition_op::ConditionOpKind::StringLike => actuals.iter().any(actual_matches),
            condition_op::ConditionOpKind::StringNotEquals
            | condition_op::ConditionOpKind::StringNotEqualsIgnoreCase
            | condition_op::ConditionOpKind::StringNotLike => actuals.iter().all(actual_matches),
            condition_op::ConditionOpKind::Bool
            | condition_op::ConditionOpKind::DateEquals
            | condition_op::ConditionOpKind::DateNotEquals
            | condition_op::ConditionOpKind::DateLessThan
            | condition_op::ConditionOpKind::DateLessThanEquals
            | condition_op::ConditionOpKind::DateGreaterThan
            | condition_op::ConditionOpKind::DateGreaterThanEquals
            | condition_op::ConditionOpKind::NumericEquals
            | condition_op::ConditionOpKind::NumericNotEquals
            | condition_op::ConditionOpKind::NumericLessThan
            | condition_op::ConditionOpKind::NumericLessThanEquals
            | condition_op::ConditionOpKind::NumericGreaterThan
            | condition_op::ConditionOpKind::NumericGreaterThanEquals
            | condition_op::ConditionOpKind::IpAddress
            | condition_op::ConditionOpKind::NotIpAddress
            | condition_op::ConditionOpKind::Null => false,
        },
    };
    if matches {
        ConditionMatchResult::Matches
    } else {
        ConditionMatchResult::NoMatch
    }
}

pub(super) fn resolve_policy_variable(
    request: &PolicyRequest<'_>,
    key: &str,
) -> PolicyVariableResolution {
    let Some((resolver, param)) = lookup_policy_variable_key(key) else {
        return PolicyVariableResolution::Unavailable;
    };
    match (resolver.resolve)(request, param) {
        ResolvedValue::Present(value) => PolicyVariableResolution::Value(value.to_string()),
        ResolvedValue::SourceIp(source_ip) => {
            PolicyVariableResolution::Value(source_ip.to_string())
        }
        ResolvedValue::Numeric(value) => PolicyVariableResolution::Value(value.to_string()),
        ResolvedValue::EpochSeconds(epoch_seconds) => {
            PolicyVariableResolution::Value(epoch_seconds.to_string())
        }
        ResolvedValue::PresentScalarValues(_) | ResolvedValue::PresentValues(_) => {
            PolicyVariableResolution::Unavailable
        }
        ResolvedValue::Absent => PolicyVariableResolution::Absent,
        ResolvedValue::Unavailable | ResolvedValue::Unsupported => {
            PolicyVariableResolution::Unavailable
        }
    }
}

fn lookup_policy_variable_key(key: &str) -> Option<(&'static ConditionKeyResolver, &str)> {
    lookup(key)
}

fn operator_supported_for_key(operator: &str, support: OperatorSupport) -> bool {
    match support {
        OperatorSupport::AnyEvaluable => condition_op::lookup(operator).is_some_and(|op| {
            op.evaluable_on_evaluable_object_actions
                && (condition_op::is_string_condition_kind(op.kind)
                    || condition_op::is_binary_condition_kind(op.kind))
        }),
        OperatorSupport::ArnEqualsOnly => operator == "ArnEquals",
        OperatorSupport::StringOrNumeric => condition_op::lookup(operator).is_some_and(|op| {
            op.evaluable_on_evaluable_object_actions
                && (condition_op::is_string_condition_kind(op.kind)
                    || condition_op::is_numeric_condition_kind(op.kind))
        }),
        OperatorSupport::DateOnly => condition_op::lookup(operator).is_some_and(|op| {
            op.evaluable_on_evaluable_object_actions
                && condition_op::is_date_condition_kind(op.kind)
        }),
        OperatorSupport::NumericOnly => condition_op::lookup(operator).is_some_and(|op| {
            op.evaluable_on_evaluable_object_actions
                && condition_op::is_numeric_condition_kind(op.kind)
        }),
        OperatorSupport::StringEqualsOnly => {
            matches!(operator, "StringEquals" | "StringEqualsIfExists")
        }
        OperatorSupport::BoolOnly => matches!(
            operator,
            "Bool" | "BoolIfExists" | "ForAllValues:Bool" | "ForAnyValue:Bool"
        ),
        OperatorSupport::IpOnly => condition_op::lookup(operator).is_some_and(|op| {
            op.evaluable_on_evaluable_object_actions && condition_op::is_ip_condition_kind(op.kind)
        }),
    }
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
    fn key_match_prefix_rejects_non_boundary_prefix_length() {
        let key = KeyMatch::Prefix("s3:ExistingObjectTag/");
        assert_eq!(key.match_key("aaaaaaaaaaaaaaaaaaaaé"), None);
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
    fn string_equals_only_keys_do_not_accept_ignore_case_variants() {
        let clause = PolicyConditionClause {
            operator: "StringEqualsIgnoreCase".to_string(),
            key: "s3:ExistingObjectTag/classification".to_string(),
            values: vec!["public".to_string()],
        };
        assert!(!supports_clause_for_action(
            &clause,
            PolicyAction::GetObject
        ));

        let clause = PolicyConditionClause {
            operator: "StringEqualsIgnoreCase".to_string(),
            key: "s3:RequestObjectTag/classification".to_string(),
            values: vec!["public".to_string()],
        };
        assert!(supports_clause_for_action(&clause, PolicyAction::PutObject));
    }

    #[test]
    fn binary_equals_is_supported_for_general_evaluable_keys_only() {
        let request_tag = PolicyConditionClause {
            operator: "BinaryEquals".to_string(),
            key: "s3:RequestObjectTag/classification".to_string(),
            values: vec!["cHVibGlj".to_string()],
        };
        assert!(supports_clause_for_action(
            &request_tag,
            PolicyAction::PutObject
        ));

        let existing_tag = PolicyConditionClause {
            operator: "BinaryEquals".to_string(),
            key: "s3:ExistingObjectTag/classification".to_string(),
            values: vec!["cHVibGlj".to_string()],
        };
        assert!(!supports_clause_for_action(
            &existing_tag,
            PolicyAction::GetObject
        ));

        let max_keys = PolicyConditionClause {
            operator: "BinaryEquals".to_string(),
            key: "s3:max-keys".to_string(),
            values: vec!["Mg==".to_string()],
        };
        assert!(!supports_clause_for_action(
            &max_keys,
            PolicyAction::ListBucket
        ));
    }

    #[test]
    fn lookup_multivalue_tag_keys_returns_exact_resolvers() {
        let (request_tag_keys, param) = lookup("s3:RequestObjectTagKeys").unwrap();
        assert_eq!(param, "");
        assert_eq!(
            request_tag_keys.operator_support,
            OperatorSupport::AnyEvaluable
        );
        assert!(request_tag_keys.supported_for_action.is_some());

        let (aws_tag_keys, param) = lookup("aws:TagKeys").unwrap();
        assert_eq!(param, "");
        assert_eq!(aws_tag_keys.operator_support, OperatorSupport::AnyEvaluable);
        let predicate = aws_tag_keys
            .supported_for_action
            .expect("aws:TagKeys has an action support predicate");
        assert!(predicate(PolicyAction::TagResource));
        assert!(predicate(PolicyAction::UntagResource));
        assert!(!predicate(PolicyAction::PutObjectTagging));
    }

    #[test]
    fn lookup_request_tag_returns_tag_resource_resolver() {
        let (resolver, param) = lookup("aws:RequestTag/classification").unwrap();
        assert_eq!(param, "classification");
        assert_eq!(resolver.operator_support, OperatorSupport::AnyEvaluable);
        let predicate = resolver
            .supported_for_action
            .expect("aws:RequestTag has an action support predicate");
        assert!(predicate(PolicyAction::TagResource));
        assert!(predicate(PolicyAction::UntagResource));
        assert!(!predicate(PolicyAction::PutObjectTagging));
    }

    #[test]
    fn lookup_resource_tag_returns_bucket_tag_resolver() {
        let (resolver, param) = lookup("aws:ResourceTag/classification").unwrap();
        assert_eq!(param, "classification");
        assert_eq!(resolver.operator_support, OperatorSupport::AnyEvaluable);
        assert!(resolver.evaluable_for_action.is_none());
        assert!(resolver.supported_for_action.is_some());
    }

    #[test]
    fn lookup_time_keys_returns_exact_resolvers() {
        let (current_time, param) = lookup("aws:CurrentTime").unwrap();
        assert_eq!(param, "");
        assert_eq!(current_time.operator_support, OperatorSupport::DateOnly);
        assert_eq!(current_time.input, Some(ConditionInput::CurrentTime));

        let (epoch_time, param) = lookup("aws:EpochTime").unwrap();
        assert_eq!(param, "");
        assert_eq!(epoch_time.operator_support, OperatorSupport::NumericOnly);
        assert_eq!(epoch_time.input, Some(ConditionInput::CurrentTime));

        let (token_issue_time, param) = lookup("aws:TokenIssueTime").unwrap();
        assert_eq!(param, "");
        assert_eq!(token_issue_time.operator_support, OperatorSupport::DateOnly);
        assert_eq!(token_issue_time.input, None);

        let (principal_arn, param) = lookup("aws:PrincipalArn").unwrap();
        assert_eq!(param, "");
        assert_eq!(
            principal_arn.operator_support,
            OperatorSupport::ArnEqualsOnly
        );
        assert_eq!(principal_arn.input, None);
    }

    #[test]
    fn lookup_condition_keys_is_case_insensitive() {
        let (source_ip, param) = lookup("AWS:SourceIP").unwrap();
        assert_eq!(param, "");
        assert_eq!(source_ip.input, Some(ConditionInput::SourceIp));

        let (secure_transport, param) = lookup("aws:securetransport").unwrap();
        assert_eq!(param, "");
        assert_eq!(
            secure_transport.input,
            Some(ConditionInput::SecureTransport)
        );

        let (requested_region, param) = lookup("AWS:RequestedRegion").unwrap();
        assert_eq!(param, "");
        assert_eq!(
            requested_region.input,
            Some(ConditionInput::RequestedRegion)
        );

        let (referer, param) = lookup("AWS:Referer").unwrap();
        assert_eq!(param, "");
        assert_eq!(referer.input, Some(ConditionInput::Referer));
    }

    #[test]
    fn lookup_prefix_condition_keys_is_case_insensitive() {
        let (resolver, param) = lookup("S3:ExistingObjectTag/Classification").unwrap();
        assert_eq!(param, "Classification");
        assert_eq!(resolver.input, Some(ConditionInput::ExistingObject));

        let (resolver, param) = lookup("AWS:RequestTag/Classification").unwrap();
        assert_eq!(param, "Classification");
        assert_eq!(resolver.input, Some(ConditionInput::Request));
    }

    #[test]
    fn lookup_request_property_keys_returns_exact_resolvers() {
        let (secure_transport, param) = lookup("aws:SecureTransport").unwrap();
        assert_eq!(param, "");
        assert_eq!(secure_transport.operator_support, OperatorSupport::BoolOnly);
        assert_eq!(
            secure_transport.input,
            Some(ConditionInput::SecureTransport)
        );

        let (requested_region, param) = lookup("aws:RequestedRegion").unwrap();
        assert_eq!(param, "");
        assert_eq!(
            requested_region.operator_support,
            OperatorSupport::AnyEvaluable
        );
        assert_eq!(
            requested_region.input,
            Some(ConditionInput::RequestedRegion)
        );

        let (referer, param) = lookup("aws:referer").unwrap();
        assert_eq!(param, "");
        assert_eq!(referer.operator_support, OperatorSupport::AnyEvaluable);
        assert_eq!(referer.input, Some(ConditionInput::Referer));
    }

    #[test]
    fn request_property_keys_accept_expected_operator_families() {
        let secure_clause = PolicyConditionClause {
            operator: "Bool".to_string(),
            key: "aws:SecureTransport".to_string(),
            values: vec!["true".to_string()],
        };
        assert!(supports_clause_for_action(
            &secure_clause,
            PolicyAction::GetObject
        ));

        let secure_string_clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "aws:SecureTransport".to_string(),
            values: vec!["true".to_string()],
        };
        assert!(!supports_clause_for_action(
            &secure_string_clause,
            PolicyAction::GetObject
        ));

        for key in ["aws:RequestedRegion", "aws:referer"] {
            let clause = PolicyConditionClause {
                operator: "StringLike".to_string(),
                key: key.to_string(),
                values: vec!["*".to_string()],
            };
            assert!(supports_clause_for_action(&clause, PolicyAction::GetObject));
        }
    }

    #[test]
    fn time_keys_accept_only_their_operator_families() {
        let date_clause = PolicyConditionClause {
            operator: "DateLessThan".to_string(),
            key: "aws:CurrentTime".to_string(),
            values: vec!["2024-01-01T00:00:00Z".to_string()],
        };
        assert!(supports_clause_for_action(
            &date_clause,
            PolicyAction::GetObject
        ));

        let token_issue_time_clause = PolicyConditionClause {
            operator: "DateGreaterThan".to_string(),
            key: "aws:TokenIssueTime".to_string(),
            values: vec!["2024-01-01T00:00:00Z".to_string()],
        };
        assert!(supports_clause_for_action(
            &token_issue_time_clause,
            PolicyAction::GetObject
        ));

        let principal_arn_clause = PolicyConditionClause {
            operator: "ArnEquals".to_string(),
            key: "aws:PrincipalArn".to_string(),
            values: vec!["arn:aws:iam::123456789012:role/test".to_string()],
        };
        assert!(supports_clause_for_action(
            &principal_arn_clause,
            PolicyAction::GetObject
        ));
        let unpinned_principal_arn_clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            ..principal_arn_clause
        };
        assert!(!supports_clause_for_action(
            &unpinned_principal_arn_clause,
            PolicyAction::GetObject
        ));

        for (key, action) in [
            ("s3:ExistingObjectTag/test", PolicyAction::GetObject),
            ("s3:max-keys", PolicyAction::ListBucket),
        ] {
            let clause = PolicyConditionClause {
                operator: "ArnEquals".to_string(),
                key: key.to_string(),
                values: vec!["value".to_string()],
            };
            assert!(!supports_clause_for_action(&clause, action));
        }

        let string_clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "aws:CurrentTime".to_string(),
            values: vec!["2024-01-01T00:00:00Z".to_string()],
        };
        assert!(!supports_clause_for_action(
            &string_clause,
            PolicyAction::GetObject
        ));

        let numeric_clause = PolicyConditionClause {
            operator: "NumericLessThan".to_string(),
            key: "aws:EpochTime".to_string(),
            values: vec!["32503680000".to_string()],
        };
        assert!(supports_clause_for_action(
            &numeric_clause,
            PolicyAction::GetObject
        ));

        let date_epoch_clause = PolicyConditionClause {
            operator: "DateLessThan".to_string(),
            key: "aws:EpochTime".to_string(),
            values: vec!["2999-01-01T00:00:00Z".to_string()],
        };
        assert!(!supports_clause_for_action(
            &date_epoch_clause,
            PolicyAction::GetObject
        ));
    }

    #[test]
    fn lookup_exact_header_keys() {
        let (acl, param) = lookup("s3:x-amz-acl").unwrap();
        assert_eq!(param, "");
        assert_eq!(acl.operator_support, OperatorSupport::AnyEvaluable);
        assert!(acl.supported_for_action.is_some());

        let (sse, _) = lookup("s3:x-amz-server-side-encryption").unwrap();
        assert!(sse.supported_for_action.is_some());
    }

    #[test]
    fn lookup_unknown_key_returns_none() {
        assert!(lookup("aws:SourceVpc").is_none());
        assert!(lookup("").is_none());
    }

    // Snapshot of AWS S3 service-specific condition keys and AWS global
    // condition keys that are documented as S3-relevant, verified
    // 2026-07-12 against:
    // - https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html#amazons3-policy-keys
    // - https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html
    // - https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html
    const DOCUMENTED_S3_AND_GLOBAL_CONDITION_KEY_INVENTORY: &[&str] = &[
        "aws:AssumedRoot",
        "aws:CalledVia",
        "aws:CalledViaAWSMCP",
        "aws:CalledViaFirst",
        "aws:CalledViaLast",
        "aws:ChatbotSourceArn",
        "aws:CurrentTime",
        "aws:Ec2InstanceSourcePrivateIPv4",
        "aws:Ec2InstanceSourceVpc",
        "aws:EpochTime",
        "aws:FederatedProvider",
        "aws:IsMcpServiceAction",
        "aws:MultiFactorAuthAge",
        "aws:MultiFactorAuthPresent",
        "aws:PrincipalAccount",
        "aws:PrincipalArn",
        "aws:PrincipalIsAWSService",
        "aws:PrincipalOrgID",
        "aws:PrincipalOrgPaths",
        "aws:PrincipalServiceName",
        "aws:PrincipalServiceNamesList",
        "aws:PrincipalTag/department",
        "aws:PrincipalType",
        "aws:RequestedRegion",
        "aws:RequestTag/department",
        "aws:ResourceAccount",
        "aws:ResourceOrgID",
        "aws:ResourceOrgPaths",
        "aws:ResourceTag/department",
        "aws:SecureTransport",
        "aws:SourceAccount",
        "aws:SourceArn",
        "aws:SourceIdentity",
        "aws:SourceIp",
        "aws:SourceOrgID",
        "aws:SourceOrgPaths",
        "aws:SourceOwner",
        "aws:SourceVpc",
        "aws:SourceVpcArn",
        "aws:SourceVpce",
        "aws:TagKeys",
        "aws:TokenIssueTime",
        "aws:UserAgent",
        "aws:ViaAWSMCPService",
        "aws:ViaAWSService",
        "aws:VpcSourceIp",
        "aws:VpceAccount",
        "aws:VpceOrgID",
        "aws:VpceOrgPaths",
        "aws:referer",
        "aws:userid",
        "aws:username",
        "codebuild:BuildArn",
        "codebuild:ProjectArn",
        "ec2:RoleDelivery",
        "ec2:SourceInstanceArn",
        "glue:CredentialIssuingService",
        "glue:RoleAssumedBy",
        "identitystore:UserId",
        "lambda:SourceFunctionArn",
        "s3:AccessGrantScope",
        "s3:AccessGrantsInstanceArn",
        "s3:AccessGrantsLocationScope",
        "s3:AccessPointNetworkOrigin",
        "s3:AccessPointTag/environment",
        "s3:BucketTag/department",
        "s3:DataAccessPointAccount",
        "s3:DataAccessPointArn",
        "s3:ExistingJobOperation",
        "s3:ExistingJobPriority",
        "s3:ExistingObjectTag/department",
        "s3:InventoryAccessibleOptionalFields",
        "s3:InventoryField",
        "s3:JobSuspendedCause",
        "s3:ObjectCreationOperation",
        "s3:RequestJobOperation",
        "s3:RequestJobPriority",
        "s3:RequestObjectTag/department",
        "s3:RequestObjectTagKeys",
        "s3:ResourceAccount",
        "s3:TlsVersion",
        "s3:annotation-prefix",
        "s3:authType",
        "s3:delimiter",
        "s3:deliverySourceArn",
        "s3:destinationRegion",
        "s3:if-match",
        "s3:if-none-match",
        "s3:isReplicationPauseRequest",
        "s3:locationconstraint",
        "s3:logType",
        "s3:max-annotation-results",
        "s3:max-keys",
        "s3:object-lock-legal-hold",
        "s3:object-lock-mode",
        "s3:object-lock-remaining-retention-days",
        "s3:object-lock-retain-until-date",
        "s3:prefix",
        "s3:resourceArnBeingAuthorized",
        "s3:signatureAge",
        "s3:signatureversion",
        "s3:versionid",
        "s3:x-amz-acl",
        "s3:x-amz-bucket-namespace",
        "s3:x-amz-content-sha256",
        "s3:x-amz-copy-source",
        "s3:x-amz-grant-full-control",
        "s3:x-amz-grant-read",
        "s3:x-amz-grant-read-acp",
        "s3:x-amz-grant-write",
        "s3:x-amz-grant-write-acp",
        "s3:x-amz-metadata-directive",
        "s3:x-amz-object-annotation-directive",
        "s3:x-amz-object-if-match",
        "s3:x-amz-object-ownership",
        "s3:x-amz-server-side-encryption",
        "s3:x-amz-server-side-encryption-aws-kms-key-id",
        "s3:x-amz-server-side-encryption-customer-algorithm",
        "s3:x-amz-storage-class",
        "s3:x-amz-website-redirect-location",
        "ssm:SourceInstanceArn",
    ];

    #[test]
    fn documented_s3_and_global_condition_keys_are_classified() {
        for key in DOCUMENTED_S3_AND_GLOBAL_CONDITION_KEY_INVENTORY {
            assert!(is_known_condition_key(key), "{key} should be classified");
        }
    }

    #[test]
    fn known_condition_key_includes_deferred_aws_keys() {
        for key in KNOWN_DEFERRED_EXACT_CONDITION_KEYS {
            assert!(is_known_condition_key(key), "{key} should be recognized");
        }
        for prefix in KNOWN_DEFERRED_PREFIX_CONDITION_KEYS {
            let key = format!("{prefix}environment");
            assert!(is_known_condition_key(&key), "{key} should be recognized");
        }

        assert!(is_known_condition_key(
            "S3:Object-Lock-Remaining-Retention-Days"
        ));
        assert!(is_known_condition_key("s3:AccessPointTag/environment"));
        assert!(!is_known_condition_key("aaaaaaaaaaaaaaaaaaaaé"));
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
    fn canned_acl_is_not_supported_for_tagging_actions() {
        let (resolver, _) = lookup("s3:x-amz-acl").unwrap();
        let predicate = resolver
            .supported_for_action
            .expect("canned ACL has a support predicate");
        assert!(predicate(PolicyAction::PutObject));
        assert!(predicate(PolicyAction::PutObjectAcl));
        assert!(!predicate(PolicyAction::PutObjectTagging));
        assert!(!predicate(PolicyAction::PutObjectVersionTagging));
    }

    #[test]
    fn sse_is_not_supported_for_tagging_actions() {
        let (resolver, _) = lookup("s3:x-amz-server-side-encryption").unwrap();
        let predicate = resolver
            .supported_for_action
            .expect("SSE has a support predicate");
        assert!(predicate(PolicyAction::PutObject));
        assert!(predicate(PolicyAction::PutObjectAcl));
        assert!(!predicate(PolicyAction::PutObjectTagging));
        assert!(!predicate(PolicyAction::PutObjectVersionTagging));
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
    fn condition_action_status_distinguishes_validation_and_evaluability() {
        let clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "s3:ExistingObjectTag/security".to_string(),
            values: vec!["public".to_string()],
        };

        assert_eq!(
            clause_action_status(&clause, PolicyAction::GetObject),
            ConditionActionStatus::Evaluable
        );
        assert_eq!(
            clause_action_status(&clause, PolicyAction::GetObjectAttributes),
            ConditionActionStatus::AcceptedButNotEvaluable
        );
        assert_eq!(
            clause_action_status(&clause, PolicyAction::PutObject),
            ConditionActionStatus::PolicyInvalid
        );
    }

    #[test]
    fn condition_input_requirements_only_apply_to_evaluable_actions() {
        let clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "s3:ExistingObjectTag/security".to_string(),
            values: vec!["public".to_string()],
        };

        assert!(clause_requires_input_for_action(
            &clause,
            PolicyAction::GetObject,
            ConditionInput::ExistingObject
        ));
        assert!(!clause_requires_input_for_action(
            &clause,
            PolicyAction::GetObjectAttributes,
            ConditionInput::ExistingObject
        ));
        assert!(!clause_requires_input_for_action(
            &clause,
            PolicyAction::PutObject,
            ConditionInput::ExistingObject
        ));
    }

    #[test]
    fn resolver_table_classifies_condition_inputs() {
        for key in [
            "s3:RequestObjectTag/security",
            "aws:RequestTag/security",
            "s3:RequestObjectTagKeys",
            "aws:TagKeys",
        ] {
            let clause = PolicyConditionClause {
                operator: "StringEquals".to_string(),
                key: key.to_string(),
                values: vec!["allow".to_string()],
            };
            assert_eq!(clause_input(&clause), Some(ConditionInput::Request));
        }

        for key in ["s3:BucketTag/security", "aws:ResourceTag/security"] {
            let clause = PolicyConditionClause {
                operator: "StringEquals".to_string(),
                key: key.to_string(),
                values: vec!["allow".to_string()],
            };
            assert_eq!(clause_input(&clause), Some(ConditionInput::Bucket));
        }

        let time_clause = PolicyConditionClause {
            operator: "DateLessThan".to_string(),
            key: "aws:CurrentTime".to_string(),
            values: vec!["2999-01-01T00:00:00Z".to_string()],
        };
        assert_eq!(
            clause_input(&time_clause),
            Some(ConditionInput::CurrentTime)
        );

        let secure_transport_clause = PolicyConditionClause {
            operator: "Bool".to_string(),
            key: "aws:SecureTransport".to_string(),
            values: vec!["true".to_string()],
        };
        assert_eq!(
            clause_input(&secure_transport_clause),
            Some(ConditionInput::SecureTransport)
        );

        let requested_region_clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "aws:RequestedRegion".to_string(),
            values: vec!["us-east-1".to_string()],
        };
        assert_eq!(
            clause_input(&requested_region_clause),
            Some(ConditionInput::RequestedRegion)
        );

        let referer_clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "aws:referer".to_string(),
            values: vec!["https://example.com".to_string()],
        };
        assert_eq!(clause_input(&referer_clause), Some(ConditionInput::Referer));

        let header_clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "s3:x-amz-copy-source".to_string(),
            values: vec!["src/*".to_string()],
        };
        assert_eq!(clause_input(&header_clause), None);
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
        assert!(predicate(PolicyAction::PutObjectTagging));
        assert!(predicate(PolicyAction::PutObjectVersionTagging));
    }

    #[test]
    fn request_object_tag_unsupported_for_control_tagging_actions() {
        let (request_tag, _) = lookup("s3:RequestObjectTag/x").unwrap();
        let request_tag_predicate = request_tag
            .supported_for_action
            .expect("RequestObjectTag has a support predicate");
        assert!(!request_tag_predicate(PolicyAction::TagResource));
        assert!(!request_tag_predicate(PolicyAction::UntagResource));

        let (request_tag_keys, _) = lookup("s3:RequestObjectTagKeys").unwrap();
        let request_tag_keys_predicate = request_tag_keys
            .supported_for_action
            .expect("RequestObjectTagKeys has a support predicate");
        assert!(!request_tag_keys_predicate(PolicyAction::TagResource));
        assert!(!request_tag_keys_predicate(PolicyAction::UntagResource));
    }

    #[test]
    fn version_id_is_supported_for_version_scoped_object_actions() {
        let (resolver, param) = lookup("s3:versionid").unwrap();
        assert_eq!(param, "");
        assert_eq!(resolver.operator_support, OperatorSupport::AnyEvaluable);
        let predicate = resolver
            .supported_for_action
            .expect("VersionId has a support predicate");

        for action in [
            PolicyAction::GetObjectVersion,
            PolicyAction::GetObjectVersionAttributes,
            PolicyAction::GetObjectVersionAcl,
            PolicyAction::GetObjectVersionTagging,
            PolicyAction::PutObjectVersionAcl,
            PolicyAction::PutObjectVersionTagging,
            PolicyAction::DeleteObjectVersion,
            PolicyAction::DeleteObjectVersionTagging,
        ] {
            assert!(predicate(action), "{action:?}");
        }

        for action in [
            PolicyAction::GetObject,
            PolicyAction::GetObjectAttributes,
            PolicyAction::GetObjectAcl,
            PolicyAction::GetObjectTagging,
            PolicyAction::PutObject,
            PolicyAction::PutObjectAcl,
            PolicyAction::PutObjectTagging,
            PolicyAction::DeleteObject,
            PolicyAction::DeleteObjectTagging,
            PolicyAction::ListBucketVersions,
        ] {
            assert!(!predicate(action), "{action:?}");
        }
    }

    #[test]
    fn every_expected_key_has_a_row() {
        for key in [
            "s3:ExistingObjectTag/x",
            "aws:ResourceTag/x",
            "s3:RequestObjectTag/x",
            "aws:RequestTag/x",
            "s3:RequestObjectTagKeys",
            "aws:TagKeys",
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
            "s3:if-match",
            "s3:if-none-match",
            "s3:ObjectCreationOperation",
            "s3:prefix",
            "s3:delimiter",
            "s3:max-keys",
            "s3:locationconstraint",
            "s3:x-amz-object-ownership",
        ] {
            assert!(lookup(key).is_some(), "missing resolver for {key}");
        }
    }

    #[test]
    fn object_creation_operation_requires_bool_operator_on_put_object() {
        let clause = PolicyConditionClause {
            operator: "Bool".to_string(),
            key: "s3:ObjectCreationOperation".to_string(),
            values: vec!["true".to_string()],
        };
        assert!(supports_clause_for_action(&clause, PolicyAction::PutObject));

        for operator in ["BoolIfExists", "ForAllValues:Bool", "ForAnyValue:Bool"] {
            let clause = PolicyConditionClause {
                operator: operator.to_string(),
                key: "s3:ObjectCreationOperation".to_string(),
                values: vec!["true".to_string()],
            };
            assert!(
                supports_clause_for_action(&clause, PolicyAction::PutObject),
                "{operator}"
            );
        }

        let wrong_operator = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "s3:ObjectCreationOperation".to_string(),
            values: vec!["true".to_string()],
        };
        assert!(!supports_clause_for_action(
            &wrong_operator,
            PolicyAction::PutObject
        ));
        assert!(!supports_clause_for_action(
            &clause,
            PolicyAction::ListBucket
        ));
    }

    #[test]
    fn list_condition_keys_are_action_scoped() {
        let prefix = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "s3:prefix".to_string(),
            values: vec!["allowed/".to_string()],
        };
        let delimiter = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "s3:delimiter".to_string(),
            values: vec!["/".to_string()],
        };
        let max_keys = PolicyConditionClause {
            operator: "NumericEquals".to_string(),
            key: "s3:max-keys".to_string(),
            values: vec!["2".to_string()],
        };

        for clause in [&prefix, &delimiter, &max_keys] {
            assert!(supports_clause_for_action(clause, PolicyAction::ListBucket));
            assert!(supports_clause_for_action(
                clause,
                PolicyAction::ListBucketVersions
            ));
            assert!(!supports_clause_for_action(clause, PolicyAction::GetObject));
        }
    }

    #[test]
    fn max_keys_accepts_string_and_numeric_operators() {
        let string_clause = PolicyConditionClause {
            operator: "StringEquals".to_string(),
            key: "s3:max-keys".to_string(),
            values: vec!["2".to_string()],
        };
        let bool_clause = PolicyConditionClause {
            operator: "Bool".to_string(),
            key: "s3:max-keys".to_string(),
            values: vec!["true".to_string()],
        };

        assert!(supports_clause_for_action(
            &string_clause,
            PolicyAction::ListBucket
        ));
        for operator in [
            "NumericEquals",
            "NumericEqualsIfExists",
            "NumericNotEquals",
            "NumericNotEqualsIfExists",
            "NumericLessThan",
            "NumericLessThanIfExists",
            "NumericLessThanEquals",
            "NumericLessThanEqualsIfExists",
            "NumericGreaterThan",
            "NumericGreaterThanIfExists",
            "NumericGreaterThanEquals",
            "NumericGreaterThanEqualsIfExists",
        ] {
            let numeric_clause = PolicyConditionClause {
                operator: operator.to_string(),
                key: "s3:max-keys".to_string(),
                values: vec!["2".to_string()],
            };
            assert!(
                supports_clause_for_action(&numeric_clause, PolicyAction::ListBucket),
                "{operator} should be supported for s3:max-keys"
            );
        }
        assert!(!supports_clause_for_action(
            &bool_clause,
            PolicyAction::ListBucket
        ));
    }
}
