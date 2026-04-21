use auth::bucket_policy::{
    clause_supported_for_action_for_tests, parse_bucket_policy, BucketPolicy, ExistingObjectTags,
    PolicyAction, PolicyEvaluation, PolicyRequest, PolicyTag,
};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence};
use s3_types::CanonicalUserId;
use serde_json::{json, Map, Value};

const BUCKET: &str = "bucket";
const KEY: &str = "key";
const OTHER_BUCKET: &str = "other-bucket";
const OTHER_KEY: &str = "other-key";

const OWNER_ROOT: &str = "arn:aws:iam::111122223333:root";
const OWNER_USER: &str = "arn:aws:iam::111122223333:user/owner";
const ALT_ROOT: &str = "arn:aws:iam::444455556666:root";
const ALT_USER: &str = "arn:aws:iam::444455556666:user/alt";
const NO_MATCH_PRINCIPAL: &str = "arn:aws:iam::999988887777:user/no-match";

#[derive(Debug, Clone)]
struct DifferentialCase {
    request: GeneratedRequest,
    policy: GeneratedPolicy,
}

impl DifferentialCase {
    fn render(&self) -> String {
        format!(
            "policy={}\nrequest={}",
            self.policy.render(RenderStyle::CompactSingletons),
            self.request.render_builder()
        )
    }
}

#[derive(Debug, Clone)]
struct GeneratedPolicy {
    statements: Vec<GeneratedStatement>,
}

impl GeneratedPolicy {
    fn render(&self, style: RenderStyle) -> String {
        serde_json::to_string(&json!({
            "Version": "2012-10-17",
            "Statement": self
                .statements
                .iter()
                .map(|statement| statement.to_value(style))
                .collect::<Vec<_>>(),
        }))
        .expect("serialize generated policy")
    }

    fn append_guaranteed_non_matching_statement(&self) -> Self {
        let mut statements = self.statements.clone();
        statements.push(GeneratedStatement::guaranteed_non_match());
        Self { statements }
    }

    fn expanded_equivalent(&self) -> Self {
        let statements = self
            .statements
            .iter()
            .flat_map(GeneratedStatement::expand_equivalent)
            .collect::<Vec<_>>();
        Self { statements }
    }

    fn reversed(&self) -> Self {
        let mut statements = self.statements.clone();
        statements.reverse();
        Self { statements }
    }
}

#[derive(Debug, Clone, Copy)]
enum RenderStyle {
    CompactSingletons,
    AlwaysArray,
}

#[derive(Debug, Clone)]
struct GeneratedStatement {
    effect: GeneratedEffect,
    principal: GeneratedPrincipal,
    actions: Vec<GeneratedActionPattern>,
    resources: Vec<GeneratedResourcePattern>,
    conditions: Vec<GeneratedConditionClause>,
}

impl GeneratedStatement {
    fn to_value(&self, style: RenderStyle) -> Value {
        let mut object = Map::new();
        object.insert(
            "Effect".to_string(),
            Value::String(self.effect.as_str().to_string()),
        );
        object.insert("Principal".to_string(), self.principal.to_value(style));
        object.insert(
            "Action".to_string(),
            strings_to_value(
                &self
                    .actions
                    .iter()
                    .map(|action| action.as_str().to_string())
                    .collect::<Vec<_>>(),
                style,
            ),
        );
        object.insert(
            "Resource".to_string(),
            strings_to_value(
                &self
                    .resources
                    .iter()
                    .map(|resource| resource.as_str().to_string())
                    .collect::<Vec<_>>(),
                style,
            ),
        );
        if !self.conditions.is_empty() {
            object.insert(
                "Condition".to_string(),
                conditions_to_value(&self.conditions, style),
            );
        }
        Value::Object(object)
    }

    fn guaranteed_non_match() -> Self {
        Self {
            effect: GeneratedEffect::Deny,
            principal: GeneratedPrincipal::Aws(vec![GeneratedPrincipalValue::NoMatchUser]),
            actions: vec![GeneratedActionPattern::Literal("s3:GetBucketPolicy")],
            resources: vec![GeneratedResourcePattern::BucketArn(OTHER_BUCKET)],
            conditions: Vec::new(),
        }
    }

    fn expand_equivalent(&self) -> Vec<Self> {
        let principals = self.principal.expand_values();
        let mut expanded = Vec::new();
        for principal in principals {
            for action in &self.actions {
                for resource in &self.resources {
                    expanded.push(Self {
                        effect: self.effect,
                        principal: principal.clone(),
                        actions: vec![*action],
                        resources: vec![*resource],
                        conditions: self.conditions.clone(),
                    });
                }
            }
        }
        expanded
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedEffect {
    Allow,
    Deny,
}

impl GeneratedEffect {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "Allow",
            Self::Deny => "Deny",
        }
    }
}

#[derive(Debug, Clone)]
enum GeneratedPrincipal {
    Wildcard,
    Aws(Vec<GeneratedPrincipalValue>),
    CanonicalUser(Vec<GeneratedPrincipalValue>),
}

impl GeneratedPrincipal {
    fn to_value(&self, style: RenderStyle) -> Value {
        match self {
            Self::Wildcard => Value::String("*".to_string()),
            Self::Aws(values) => json!({
                "AWS": strings_to_value(
                    &values.iter().copied().map(GeneratedPrincipalValue::policy_value).map(str::to_string).collect::<Vec<_>>(),
                    style,
                )
            }),
            Self::CanonicalUser(values) => json!({
                "CanonicalUser": strings_to_value(
                    &values.iter().copied().map(GeneratedPrincipalValue::canonical_value).collect::<Vec<_>>(),
                    style,
                )
            }),
        }
    }

    fn expand_values(&self) -> Vec<Self> {
        match self {
            Self::Wildcard => vec![Self::Wildcard],
            Self::Aws(values) => values
                .iter()
                .copied()
                .map(|value| Self::Aws(vec![value]))
                .collect(),
            Self::CanonicalUser(values) => values
                .iter()
                .copied()
                .map(|value| Self::CanonicalUser(vec![value]))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedPrincipalValue {
    OwnerRoot,
    OwnerUser,
    AltRoot,
    AltUser,
    AltCanonicalUser,
    NoMatchUser,
}

impl GeneratedPrincipalValue {
    const fn policy_value(self) -> &'static str {
        match self {
            Self::OwnerRoot => OWNER_ROOT,
            Self::OwnerUser => OWNER_USER,
            Self::AltRoot => ALT_ROOT,
            Self::AltUser => ALT_USER,
            Self::AltCanonicalUser => ALT_USER,
            Self::NoMatchUser => NO_MATCH_PRINCIPAL,
        }
    }

    fn canonical_value(self) -> String {
        CanonicalUserId::from_principal(self.policy_value())
            .as_str()
            .to_string()
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedActionPattern {
    Literal(&'static str),
}

impl GeneratedActionPattern {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Literal(value) => value,
        }
    }

    fn policy_action(self) -> PolicyAction {
        match self {
            Self::Literal("s3:GetObject") => PolicyAction::GetObject,
            Self::Literal("s3:PutObject") => PolicyAction::PutObject,
            Self::Literal("s3:GetObjectTagging") => PolicyAction::GetObjectTagging,
            Self::Literal("s3:PutObjectTagging") => PolicyAction::PutObjectTagging,
            Self::Literal("s3:DeleteObject") => PolicyAction::DeleteObject,
            Self::Literal(_) => unreachable!("unsupported generated action pattern"),
        }
    }

    fn supports_clause(self, clause: &GeneratedConditionClause) -> bool {
        clause_supported_for_action_for_tests(
            clause.operator.as_str(),
            clause.key.as_str(),
            self.policy_action(),
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedResourcePattern {
    ObjectArn(&'static str, &'static str),
    ObjectBucketWildcard(&'static str),
    BucketArn(&'static str),
}

impl GeneratedResourcePattern {
    fn as_str(self) -> &'static str {
        match self {
            Self::ObjectArn(bucket, key) => match (bucket, key) {
                (BUCKET, KEY) => "arn:aws:s3:::bucket/key",
                (OTHER_BUCKET, OTHER_KEY) => "arn:aws:s3:::other-bucket/other-key",
                _ => unreachable!("unsupported generated object ARN"),
            },
            Self::ObjectBucketWildcard(bucket) => match bucket {
                BUCKET => "arn:aws:s3:::bucket/*",
                OTHER_BUCKET => "arn:aws:s3:::other-bucket/*",
                _ => unreachable!("unsupported generated bucket wildcard"),
            },
            Self::BucketArn(bucket) => match bucket {
                BUCKET => "arn:aws:s3:::bucket",
                OTHER_BUCKET => "arn:aws:s3:::other-bucket",
                _ => unreachable!("unsupported generated bucket ARN"),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedConditionOperator {
    StringEquals,
    StringEqualsIfExists,
    StringLike,
    StringLikeIfExists,
    StringNotEquals,
    StringNotEqualsIfExists,
    Null,
}

impl GeneratedConditionOperator {
    const fn as_str(self) -> &'static str {
        match self {
            Self::StringEquals => "StringEquals",
            Self::StringEqualsIfExists => "StringEqualsIfExists",
            Self::StringLike => "StringLike",
            Self::StringLikeIfExists => "StringLikeIfExists",
            Self::StringNotEquals => "StringNotEquals",
            Self::StringNotEqualsIfExists => "StringNotEqualsIfExists",
            Self::Null => "Null",
        }
    }
}

#[derive(Debug, Clone)]
struct GeneratedConditionClause {
    operator: GeneratedConditionOperator,
    key: GeneratedConditionKey,
    values: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
enum GeneratedConditionKey {
    ExistingTagClassification,
    ExistingTagRegion,
    RequestTagTeam,
    CopySource,
    MetadataDirective,
    CannedAcl,
    ServerSideEncryption,
    SseCustomerAlgorithm,
    GrantRead,
    GrantWrite,
    GrantReadAcp,
    GrantWriteAcp,
    GrantFullControl,
}

impl GeneratedConditionKey {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ExistingTagClassification => "s3:ExistingObjectTag/classification",
            Self::ExistingTagRegion => "s3:ExistingObjectTag/region",
            Self::RequestTagTeam => "s3:RequestObjectTag/team",
            Self::CopySource => "s3:x-amz-copy-source",
            Self::MetadataDirective => "s3:x-amz-metadata-directive",
            Self::CannedAcl => "s3:x-amz-acl",
            Self::ServerSideEncryption => "s3:x-amz-server-side-encryption",
            Self::SseCustomerAlgorithm => "s3:x-amz-server-side-encryption-customer-algorithm",
            Self::GrantRead => "s3:x-amz-grant-read",
            Self::GrantWrite => "s3:x-amz-grant-write",
            Self::GrantReadAcp => "s3:x-amz-grant-read-acp",
            Self::GrantWriteAcp => "s3:x-amz-grant-write-acp",
            Self::GrantFullControl => "s3:x-amz-grant-full-control",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedRequester {
    Anonymous,
    OwnerUser,
    AltUser,
}

impl GeneratedRequester {
    const fn principal(self) -> Option<&'static str> {
        match self {
            Self::Anonymous => None,
            Self::OwnerUser => Some(OWNER_USER),
            Self::AltUser => Some(ALT_USER),
        }
    }

    fn canonical(self) -> Option<CanonicalUserId> {
        self.principal().map(CanonicalUserId::from_principal)
    }
}

#[derive(Debug, Clone)]
struct GeneratedRequest {
    action: GeneratedRequestAction,
    requester: GeneratedRequester,
    existing_tags: Vec<(String, String)>,
    request_tags: Vec<(String, String)>,
    copy_source: Option<String>,
    metadata_directive: Option<String>,
    canned_acl: Option<String>,
    server_side_encryption: Option<String>,
    sse_customer_algorithm: Option<String>,
    grant_read: Option<String>,
    grant_write: Option<String>,
    grant_read_acp: Option<String>,
    grant_write_acp: Option<String>,
    grant_full_control: Option<String>,
}

impl GeneratedRequest {
    fn render_builder(&self) -> String {
        let mut out = format!(
            "PolicyRequest::for_object(PolicyAction::{}, \"{}\", \"{}\", {:?}, requester_canonical.as_ref(), ExistingObjectTags::{})",
            self.action.variant_name(),
            BUCKET,
            KEY,
            self.requester.principal(),
            if self.existing_tags.is_empty() {
                "Unavailable"
            } else {
                "Available(&[])"
            }
        );
        if !self.existing_tags.is_empty() {
            out = format!(
                "PolicyRequest::for_object(PolicyAction::{}, \"{}\", \"{}\", {:?}, requester_canonical.as_ref(), ExistingObjectTags::Available(&[{}]))",
                self.action.variant_name(),
                BUCKET,
                KEY,
                self.requester.principal(),
                self.existing_tags
                    .iter()
                    .map(|(k, v)| format!("PolicyTag::new({k:?}, {v:?})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if !self.request_tags.is_empty() {
            out.push_str(&format!(
                ".with_request_object_tags(&[{}])",
                self.request_tags
                    .iter()
                    .map(|(k, v)| format!("PolicyTag::new({k:?}, {v:?})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        push_optional_request_context(&mut out, "with_copy_source", self.copy_source.as_deref());
        push_optional_request_context(
            &mut out,
            "with_metadata_directive",
            self.metadata_directive.as_deref(),
        );
        push_optional_request_context(&mut out, "with_canned_acl", self.canned_acl.as_deref());
        push_optional_request_context(
            &mut out,
            "with_server_side_encryption",
            self.server_side_encryption.as_deref(),
        );
        push_optional_request_context(
            &mut out,
            "with_sse_customer_algorithm",
            self.sse_customer_algorithm.as_deref(),
        );
        push_optional_request_context(&mut out, "with_grant_read", self.grant_read.as_deref());
        push_optional_request_context(&mut out, "with_grant_write", self.grant_write.as_deref());
        push_optional_request_context(
            &mut out,
            "with_grant_read_acp",
            self.grant_read_acp.as_deref(),
        );
        push_optional_request_context(
            &mut out,
            "with_grant_write_acp",
            self.grant_write_acp.as_deref(),
        );
        push_optional_request_context(
            &mut out,
            "with_grant_full_control",
            self.grant_full_control.as_deref(),
        );
        out
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedRequestAction {
    GetObject,
    PutObject,
    GetObjectTagging,
    PutObjectTagging,
    CopyObject,
    DeleteObject,
}

impl GeneratedRequestAction {
    const fn policy_action(self) -> PolicyAction {
        match self {
            Self::GetObject => PolicyAction::GetObject,
            Self::PutObject => PolicyAction::PutObject,
            Self::GetObjectTagging => PolicyAction::GetObjectTagging,
            Self::PutObjectTagging => PolicyAction::PutObjectTagging,
            Self::CopyObject => PolicyAction::PutObject,
            Self::DeleteObject => PolicyAction::DeleteObject,
        }
    }

    const fn variant_name(self) -> &'static str {
        match self {
            Self::GetObject => "GetObject",
            Self::PutObject => "PutObject",
            Self::GetObjectTagging => "GetObjectTagging",
            Self::PutObjectTagging => "PutObjectTagging",
            Self::CopyObject => "PutObject",
            Self::DeleteObject => "DeleteObject",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestContextField {
    ExistingTagClassification,
    RequestTagTeam,
    CopySource,
    MetadataDirective,
    CannedAcl,
    ServerSideEncryption,
    SseCustomerAlgorithm,
    GrantRead,
    GrantWrite,
    GrantReadAcp,
    GrantWriteAcp,
    GrantFullControl,
}

impl RequestContextField {
    const fn all() -> &'static [Self] {
        &[
            Self::ExistingTagClassification,
            Self::RequestTagTeam,
            Self::CopySource,
            Self::MetadataDirective,
            Self::CannedAcl,
            Self::ServerSideEncryption,
            Self::SseCustomerAlgorithm,
            Self::GrantRead,
            Self::GrantWrite,
            Self::GrantReadAcp,
            Self::GrantWriteAcp,
            Self::GrantFullControl,
        ]
    }

    const fn condition_key(self) -> GeneratedConditionKey {
        match self {
            Self::ExistingTagClassification => GeneratedConditionKey::ExistingTagClassification,
            Self::RequestTagTeam => GeneratedConditionKey::RequestTagTeam,
            Self::CopySource => GeneratedConditionKey::CopySource,
            Self::MetadataDirective => GeneratedConditionKey::MetadataDirective,
            Self::CannedAcl => GeneratedConditionKey::CannedAcl,
            Self::ServerSideEncryption => GeneratedConditionKey::ServerSideEncryption,
            Self::SseCustomerAlgorithm => GeneratedConditionKey::SseCustomerAlgorithm,
            Self::GrantRead => GeneratedConditionKey::GrantRead,
            Self::GrantWrite => GeneratedConditionKey::GrantWrite,
            Self::GrantReadAcp => GeneratedConditionKey::GrantReadAcp,
            Self::GrantWriteAcp => GeneratedConditionKey::GrantWriteAcp,
            Self::GrantFullControl => GeneratedConditionKey::GrantFullControl,
        }
    }

    const fn action(self) -> GeneratedRequestAction {
        match self {
            Self::ExistingTagClassification => GeneratedRequestAction::GetObject,
            Self::RequestTagTeam => GeneratedRequestAction::PutObject,
            Self::CopySource => GeneratedRequestAction::CopyObject,
            Self::MetadataDirective => GeneratedRequestAction::CopyObject,
            Self::CannedAcl => GeneratedRequestAction::PutObject,
            Self::ServerSideEncryption => GeneratedRequestAction::PutObject,
            Self::SseCustomerAlgorithm => GeneratedRequestAction::PutObject,
            Self::GrantRead => GeneratedRequestAction::PutObject,
            Self::GrantWrite => GeneratedRequestAction::PutObject,
            Self::GrantReadAcp => GeneratedRequestAction::PutObject,
            Self::GrantWriteAcp => GeneratedRequestAction::PutObject,
            Self::GrantFullControl => GeneratedRequestAction::PutObject,
        }
    }

    const fn expected_value(self) -> &'static str {
        match self {
            Self::ExistingTagClassification => "public",
            Self::RequestTagTeam => "analytics",
            Self::CopySource => "bucket/key",
            Self::MetadataDirective => "COPY",
            Self::CannedAcl => "private",
            Self::ServerSideEncryption => "AES256",
            Self::SseCustomerAlgorithm => "AES256",
            Self::GrantRead => r#"id="grant-read-canonical""#,
            Self::GrantWrite => r#"id="grant-write-canonical""#,
            Self::GrantReadAcp => r#"id="grant-read-acp-canonical""#,
            Self::GrantWriteAcp => r#"id="grant-write-acp-canonical""#,
            Self::GrantFullControl => r#"id="grant-full-control-canonical""#,
        }
    }

    const fn mismatching_value(self) -> &'static str {
        match self {
            Self::ExistingTagClassification => "private",
            Self::RequestTagTeam => "platform",
            Self::CopySource => "other-bucket/other-key",
            Self::MetadataDirective => "REPLACE",
            Self::CannedAcl => "public-read",
            Self::ServerSideEncryption => "aws:kms",
            Self::SseCustomerAlgorithm => "aws:kms",
            Self::GrantRead => r#"id="someone-else-read""#,
            Self::GrantWrite => r#"id="someone-else-write""#,
            Self::GrantReadAcp => r#"id="someone-else-read-acp""#,
            Self::GrantWriteAcp => r#"id="someone-else-write-acp""#,
            Self::GrantFullControl => r#"id="someone-else-full-control""#,
        }
    }

    fn apply(self, request: &mut GeneratedRequest, value: Option<&str>) {
        match self {
            Self::ExistingTagClassification => {
                request.existing_tags = value
                    .into_iter()
                    .map(|value| ("classification".to_string(), value.to_string()))
                    .collect();
            }
            Self::RequestTagTeam => {
                request.request_tags = value
                    .into_iter()
                    .map(|value| ("team".to_string(), value.to_string()))
                    .collect();
            }
            Self::CopySource => request.copy_source = value.map(str::to_string),
            Self::MetadataDirective => request.metadata_directive = value.map(str::to_string),
            Self::CannedAcl => request.canned_acl = value.map(str::to_string),
            Self::ServerSideEncryption => {
                request.server_side_encryption = value.map(str::to_string);
            }
            Self::SseCustomerAlgorithm => {
                request.sse_customer_algorithm = value.map(str::to_string);
            }
            Self::GrantRead => request.grant_read = value.map(str::to_string),
            Self::GrantWrite => request.grant_write = value.map(str::to_string),
            Self::GrantReadAcp => request.grant_read_acp = value.map(str::to_string),
            Self::GrantWriteAcp => request.grant_write_acp = value.map(str::to_string),
            Self::GrantFullControl => request.grant_full_control = value.map(str::to_string),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldValueState {
    Missing,
    Matching,
    Mismatching,
}

impl FieldValueState {
    const fn expected_allows(self) -> bool {
        matches!(self, Self::Matching)
    }
}

#[derive(Debug, Clone, Copy)]
enum ObjectResourceScope {
    ExactObject,
    BucketWildcard,
    BucketOnly,
}

impl ObjectResourceScope {
    const fn pattern(self) -> GeneratedResourcePattern {
        match self {
            Self::ExactObject => GeneratedResourcePattern::ObjectArn(BUCKET, KEY),
            Self::BucketWildcard => GeneratedResourcePattern::ObjectBucketWildcard(BUCKET),
            Self::BucketOnly => GeneratedResourcePattern::BucketArn(BUCKET),
        }
    }
}

fn push_optional_request_context(out: &mut String, method: &str, value: Option<&str>) {
    if let Some(value) = value {
        out.push_str(&format!(".{method}(Some({value:?}))"));
    }
}

fn strings_to_value(values: &[String], style: RenderStyle) -> Value {
    match (style, values) {
        (RenderStyle::CompactSingletons, [value]) => Value::String(value.clone()),
        _ => Value::Array(values.iter().cloned().map(Value::String).collect()),
    }
}

fn conditions_to_value(conditions: &[GeneratedConditionClause], style: RenderStyle) -> Value {
    let mut operator_map = Map::<String, Value>::new();
    for clause in conditions {
        let operator = operator_map
            .entry(clause.operator.as_str().to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        let Value::Object(operand_map) = operator else {
            unreachable!("condition operator values are always objects");
        };
        operand_map.insert(
            clause.key.as_str().to_string(),
            strings_to_value(&clause.values, style),
        );
    }
    Value::Object(operator_map)
}

fn evaluate_generated(policy: &BucketPolicy, generated: &GeneratedRequest) -> PolicyEvaluation {
    let requester_canonical = generated.requester.canonical();
    let existing_tags = generated
        .existing_tags
        .iter()
        .map(|(key, value)| PolicyTag::new(key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let request_tags = generated
        .request_tags
        .iter()
        .map(|(key, value)| PolicyTag::new(key.as_str(), value.as_str()))
        .collect::<Vec<_>>();

    let request = PolicyRequest::for_object(
        generated.action.policy_action(),
        BUCKET,
        KEY,
        generated.requester.principal(),
        requester_canonical.as_ref(),
        ExistingObjectTags::Available(&existing_tags),
    )
    .with_request_object_tags(&request_tags)
    .with_copy_source(generated.copy_source.as_deref())
    .with_metadata_directive(generated.metadata_directive.as_deref())
    .with_canned_acl(generated.canned_acl.as_deref())
    .with_server_side_encryption(generated.server_side_encryption.as_deref())
    .with_sse_customer_algorithm(generated.sse_customer_algorithm.as_deref())
    .with_grant_read(generated.grant_read.as_deref())
    .with_grant_write(generated.grant_write.as_deref())
    .with_grant_read_acp(generated.grant_read_acp.as_deref())
    .with_grant_write_acp(generated.grant_write_acp.as_deref())
    .with_grant_full_control(generated.grant_full_control.as_deref());
    policy.evaluate(&request)
}

fn parse_generated_policy(
    generated: &GeneratedPolicy,
    style: RenderStyle,
) -> Result<BucketPolicy, String> {
    let rendered = generated.render(style);
    let parsed = parse_bucket_policy(&rendered)
        .map_err(|err| format!("failed to parse generated policy {rendered}: {err:?}"))?;
    parsed
        .validate_evaluable_object_conditions()
        .map_err(|err| {
            format!("generated policy failed evaluable-condition validation: {rendered}: {err:?}")
        })?;
    Ok(parsed)
}

fn differential_case_strategy() -> impl Strategy<Value = DifferentialCase> {
    (
        generated_request_strategy(),
        proptest::collection::vec(generated_statement_strategy(), 1..=3),
    )
        .prop_map(|(request, statements)| DifferentialCase {
            request,
            policy: GeneratedPolicy { statements },
        })
}

fn generated_request_strategy() -> impl Strategy<Value = GeneratedRequest> {
    (
        prop_oneof![
            Just(GeneratedRequestAction::GetObject),
            Just(GeneratedRequestAction::PutObject),
            Just(GeneratedRequestAction::GetObjectTagging),
            Just(GeneratedRequestAction::PutObjectTagging),
            Just(GeneratedRequestAction::CopyObject),
            Just(GeneratedRequestAction::DeleteObject),
        ],
        prop_oneof![
            Just(GeneratedRequester::Anonymous),
            Just(GeneratedRequester::OwnerUser),
            Just(GeneratedRequester::AltUser),
        ],
        (
            proptest::option::of(tag_value_strategy()),
            proptest::option::of(tag_value_strategy()),
            proptest::option::of(copy_source_value_strategy()),
            proptest::option::of(metadata_directive_value_strategy()),
            proptest::option::of(canned_acl_value_strategy()),
            proptest::option::of(server_side_encryption_value_strategy()),
        ),
        (
            proptest::option::of(sse_customer_algorithm_value_strategy()),
            proptest::option::of(grant_header_value_strategy()),
            proptest::option::of(grant_header_value_strategy()),
            proptest::option::of(grant_header_value_strategy()),
            proptest::option::of(grant_header_value_strategy()),
            proptest::option::of(grant_header_value_strategy()),
        ),
    )
        .prop_map(
            |(
                action,
                requester,
                (
                    existing_tag,
                    request_tag,
                    copy_source,
                    metadata_directive,
                    canned_acl,
                    server_side_encryption,
                ),
                (
                    sse_customer_algorithm,
                    grant_read,
                    grant_write,
                    grant_read_acp,
                    grant_write_acp,
                    grant_full_control,
                ),
            )| GeneratedRequest {
                action,
                requester,
                existing_tags: existing_tag
                    .into_iter()
                    .map(|value| ("classification".to_string(), value))
                    .collect(),
                request_tags: request_tag
                    .into_iter()
                    .map(|value| ("team".to_string(), value))
                    .collect(),
                copy_source,
                metadata_directive,
                canned_acl,
                server_side_encryption,
                sse_customer_algorithm,
                grant_read,
                grant_write,
                grant_read_acp,
                grant_write_acp,
                grant_full_control,
            },
        )
}

fn generated_statement_strategy() -> impl Strategy<Value = GeneratedStatement> {
    (
        prop_oneof![Just(GeneratedEffect::Allow), Just(GeneratedEffect::Deny)],
        generated_principal_strategy(),
        proptest::collection::vec(generated_action_pattern_strategy(), 1..=2),
        proptest::collection::vec(generated_resource_pattern_strategy(), 1..=2),
        proptest::collection::vec(generated_condition_clause_strategy(), 0..=2),
    )
        .prop_map(
            |(effect, principal, mut actions, mut resources, mut conditions)| {
                dedup_actions(&mut actions);
                dedup_resources(&mut resources);
                conditions
                    .retain(|clause| actions.iter().all(|action| action.supports_clause(clause)));
                GeneratedStatement {
                    effect,
                    principal,
                    actions,
                    resources,
                    conditions,
                }
            },
        )
}

fn generated_invalid_statement_strategy() -> impl Strategy<Value = GeneratedStatement> {
    (
        prop_oneof![Just(GeneratedEffect::Allow), Just(GeneratedEffect::Deny)],
        generated_principal_strategy(),
        generated_action_pattern_strategy(),
        generated_condition_clause_strategy(),
    )
        .prop_filter(
            "action and condition must be validation-incompatible",
            |(_, _, action, clause)| !action.supports_clause(clause),
        )
        .prop_map(|(effect, principal, action, clause)| GeneratedStatement {
            effect,
            principal,
            actions: vec![action],
            resources: vec![GeneratedResourcePattern::ObjectBucketWildcard(BUCKET)],
            conditions: vec![clause],
        })
}

fn generated_principal_strategy() -> impl Strategy<Value = GeneratedPrincipal> {
    prop_oneof![
        Just(GeneratedPrincipal::Wildcard),
        proptest::collection::vec(
            prop_oneof![
                Just(GeneratedPrincipalValue::OwnerRoot),
                Just(GeneratedPrincipalValue::OwnerUser),
                Just(GeneratedPrincipalValue::AltRoot),
                Just(GeneratedPrincipalValue::AltUser),
                Just(GeneratedPrincipalValue::NoMatchUser),
            ],
            1..=2,
        )
        .prop_map(GeneratedPrincipal::Aws),
        proptest::collection::vec(Just(GeneratedPrincipalValue::AltCanonicalUser), 1..=2)
            .prop_map(GeneratedPrincipal::CanonicalUser),
    ]
}

fn generated_action_pattern_strategy() -> impl Strategy<Value = GeneratedActionPattern> {
    prop_oneof![
        Just(GeneratedActionPattern::Literal("s3:GetObject")),
        Just(GeneratedActionPattern::Literal("s3:PutObject")),
        Just(GeneratedActionPattern::Literal("s3:GetObjectTagging")),
        Just(GeneratedActionPattern::Literal("s3:PutObjectTagging")),
        Just(GeneratedActionPattern::Literal("s3:DeleteObject")),
    ]
}

fn generated_resource_pattern_strategy() -> impl Strategy<Value = GeneratedResourcePattern> {
    prop_oneof![
        Just(GeneratedResourcePattern::ObjectArn(BUCKET, KEY)),
        Just(GeneratedResourcePattern::ObjectBucketWildcard(BUCKET)),
        Just(GeneratedResourcePattern::ObjectArn(OTHER_BUCKET, OTHER_KEY)),
        Just(GeneratedResourcePattern::ObjectBucketWildcard(OTHER_BUCKET)),
    ]
}

fn generated_condition_clause_strategy() -> impl Strategy<Value = GeneratedConditionClause> {
    prop_oneof![
        (
            prop_oneof![
                Just(GeneratedConditionOperator::StringEquals),
                Just(GeneratedConditionOperator::StringEqualsIfExists),
            ],
            prop_oneof![
                Just(GeneratedConditionKey::ExistingTagClassification),
                Just(GeneratedConditionKey::ExistingTagRegion),
            ],
            proptest::collection::vec(tag_value_strategy(), 1..=2),
        )
            .prop_map(|(operator, key, values)| GeneratedConditionClause {
                operator,
                key,
                values
            }),
        (
            prop_oneof![
                Just(GeneratedConditionOperator::StringEquals),
                Just(GeneratedConditionOperator::StringEqualsIfExists),
                Just(GeneratedConditionOperator::StringLike),
                Just(GeneratedConditionOperator::StringLikeIfExists),
                Just(GeneratedConditionOperator::StringNotEquals),
                Just(GeneratedConditionOperator::StringNotEqualsIfExists),
                Just(GeneratedConditionOperator::Null),
            ],
            prop_oneof![
                Just(GeneratedConditionKey::RequestTagTeam),
                Just(GeneratedConditionKey::CopySource),
                Just(GeneratedConditionKey::MetadataDirective),
                Just(GeneratedConditionKey::CannedAcl),
                Just(GeneratedConditionKey::ServerSideEncryption),
                Just(GeneratedConditionKey::SseCustomerAlgorithm),
                Just(GeneratedConditionKey::GrantRead),
                Just(GeneratedConditionKey::GrantWrite),
                Just(GeneratedConditionKey::GrantReadAcp),
                Just(GeneratedConditionKey::GrantWriteAcp),
                Just(GeneratedConditionKey::GrantFullControl),
            ],
            proptest::collection::vec(condition_value_strategy(), 1..=2),
        )
            .prop_map(|(operator, key, values)| GeneratedConditionClause {
                operator,
                key,
                values
            }),
    ]
}

fn tag_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("public".to_string()),
        Just("shared".to_string()),
        Just("internal".to_string()),
        Just("us-east-1".to_string()),
        Just("analytics".to_string()),
    ]
}

fn copy_source_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(format!("{BUCKET}/{KEY}")),
        Just(format!("{BUCKET}/prefix-*")),
        Just(format!("{OTHER_BUCKET}/{OTHER_KEY}")),
    ]
}

fn metadata_directive_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![Just("COPY".to_string()), Just("REPLACE".to_string())]
}

fn canned_acl_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("private".to_string()),
        Just("public-read".to_string()),
        Just("bucket-owner-full-control".to_string()),
    ]
}

fn server_side_encryption_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![Just("AES256".to_string()), Just("aws:kms".to_string())]
}

fn sse_customer_algorithm_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![Just("AES256".to_string()), Just("aws:kms".to_string())]
}

fn grant_header_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(format!(
            r#"id="{}""#,
            CanonicalUserId::from_principal(ALT_USER).as_str()
        )),
        Just(format!(
            r#"id="{}""#,
            CanonicalUserId::from_principal(OWNER_USER).as_str()
        )),
    ]
}

fn condition_value_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        tag_value_strategy(),
        copy_source_value_strategy(),
        metadata_directive_value_strategy(),
        canned_acl_value_strategy(),
        server_side_encryption_value_strategy(),
        sse_customer_algorithm_value_strategy(),
        grant_header_value_strategy(),
        Just("true".to_string()),
        Just("false".to_string()),
        Just("missing".to_string()),
        Just("*".to_string()),
    ]
}

fn request_context_field_strategy() -> impl Strategy<Value = RequestContextField> {
    prop::sample::select(RequestContextField::all().to_vec())
}

fn request_context_target_and_wrong_field_strategy(
) -> impl Strategy<Value = (RequestContextField, RequestContextField)> {
    request_context_field_strategy().prop_flat_map(|target| {
        let others = RequestContextField::all()
            .iter()
            .copied()
            .filter(|field| *field != target)
            .collect::<Vec<_>>();
        prop::sample::select(others).prop_map(move |wrong| (target, wrong))
    })
}

fn field_value_state_strategy() -> impl Strategy<Value = FieldValueState> {
    prop_oneof![
        Just(FieldValueState::Missing),
        Just(FieldValueState::Matching),
        Just(FieldValueState::Mismatching),
    ]
}

fn blank_request(action: GeneratedRequestAction) -> GeneratedRequest {
    GeneratedRequest {
        action,
        requester: GeneratedRequester::AltUser,
        existing_tags: Vec::new(),
        request_tags: Vec::new(),
        copy_source: None,
        metadata_directive: None,
        canned_acl: None,
        server_side_encryption: None,
        sse_customer_algorithm: None,
        grant_read: None,
        grant_write: None,
        grant_read_acp: None,
        grant_write_acp: None,
        grant_full_control: None,
    }
}

fn apply_state(field: RequestContextField, request: &mut GeneratedRequest, state: FieldValueState) {
    let value = match state {
        FieldValueState::Missing => None,
        FieldValueState::Matching => Some(field.expected_value()),
        FieldValueState::Mismatching => Some(field.mismatching_value()),
    };
    field.apply(request, value);
}

fn single_condition_policy(
    field: RequestContextField,
    resource: GeneratedResourcePattern,
) -> BucketPolicy {
    parse_generated_policy(
        &GeneratedPolicy {
            statements: vec![GeneratedStatement {
                effect: GeneratedEffect::Allow,
                principal: GeneratedPrincipal::Wildcard,
                actions: vec![GeneratedActionPattern::Literal(
                    field.action().policy_action().as_str(),
                )],
                resources: vec![resource],
                conditions: vec![GeneratedConditionClause {
                    operator: GeneratedConditionOperator::StringEquals,
                    key: field.condition_key(),
                    values: vec![field.expected_value().to_string()],
                }],
            }],
        },
        RenderStyle::CompactSingletons,
    )
    .expect("single-condition policy parses")
}

fn dedup_actions(actions: &mut Vec<GeneratedActionPattern>) {
    actions.dedup_by_key(|action| action.as_str());
}

fn dedup_resources(resources: &mut Vec<GeneratedResourcePattern>) {
    resources.dedup_by_key(|resource| resource.as_str());
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
        .. ProptestConfig::default()
    })]

    #[test]
    fn bucket_policy_differential_normalized_round_trip_preserves_evaluation(case in differential_case_strategy()) {
        let policy = parse_generated_policy(&case.policy, RenderStyle::CompactSingletons)
            .expect("generated policy parses");
        let normalized = parse_bucket_policy(&policy.normalized_json())
            .expect("normalized policy parses");
        let original_eval = evaluate_generated(&policy, &case.request);
        let normalized_eval = evaluate_generated(&normalized, &case.request);
        prop_assert_eq!(normalized_eval, original_eval, "normalized round-trip changed evaluation\n{}", case.render());
    }

    #[test]
    fn bucket_policy_differential_scalar_and_singleton_array_forms_are_equivalent(case in differential_case_strategy()) {
        let compact = parse_generated_policy(&case.policy, RenderStyle::CompactSingletons)
            .expect("compact policy parses");
        let array_form = parse_generated_policy(&case.policy, RenderStyle::AlwaysArray)
            .expect("array policy parses");
        let compact_eval = evaluate_generated(&compact, &case.request);
        let array_eval = evaluate_generated(&array_form, &case.request);
        prop_assert_eq!(array_eval, compact_eval, "scalar vs singleton-array form changed evaluation\ncompact={}\narray={}\nrequest={}", case.policy.render(RenderStyle::CompactSingletons), case.policy.render(RenderStyle::AlwaysArray), case.request.render_builder());
    }

    #[test]
    fn bucket_policy_differential_guaranteed_non_matching_statement_is_inert(case in differential_case_strategy()) {
        let policy = parse_generated_policy(&case.policy, RenderStyle::CompactSingletons)
            .expect("generated policy parses");
        let extended_policy = parse_generated_policy(
            &case.policy.append_guaranteed_non_matching_statement(),
            RenderStyle::CompactSingletons,
        )
        .expect("extended policy parses");
        let original_eval = evaluate_generated(&policy, &case.request);
        let extended_eval = evaluate_generated(&extended_policy, &case.request);
        prop_assert_eq!(extended_eval, original_eval, "adding a guaranteed non-matching statement changed evaluation\n{}", case.render());
    }

    #[test]
    fn bucket_policy_differential_equivalent_statement_expansion_preserves_evaluation(case in differential_case_strategy()) {
        let policy = parse_generated_policy(&case.policy, RenderStyle::CompactSingletons)
            .expect("generated policy parses");
        let expanded = parse_generated_policy(&case.policy.expanded_equivalent(), RenderStyle::CompactSingletons)
            .expect("expanded policy parses");
        let original_eval = evaluate_generated(&policy, &case.request);
        let expanded_eval = evaluate_generated(&expanded, &case.request);
        prop_assert_eq!(expanded_eval, original_eval, "statement expansion changed evaluation\noriginal={}\nexpanded={}\nrequest={}", case.policy.render(RenderStyle::CompactSingletons), case.policy.expanded_equivalent().render(RenderStyle::CompactSingletons), case.request.render_builder());
    }

    #[test]
    fn bucket_policy_differential_deny_precedence_is_stable_under_statement_reordering(case in differential_case_strategy()) {
        let policy = parse_generated_policy(&case.policy, RenderStyle::CompactSingletons)
            .expect("generated policy parses");
        let reversed = parse_generated_policy(&case.policy.reversed(), RenderStyle::CompactSingletons)
            .expect("reversed policy parses");
        let original_eval = evaluate_generated(&policy, &case.request);
        let reversed_eval = evaluate_generated(&reversed, &case.request);
        prop_assert_eq!(reversed_eval, original_eval, "statement reordering changed evaluation\noriginal={}\nreversed={}\nrequest={}", case.policy.render(RenderStyle::CompactSingletons), case.policy.reversed().render(RenderStyle::CompactSingletons), case.request.render_builder());
    }

    #[test]
    fn bucket_policy_differential_validation_rejects_invalid_action_condition_pairs(
        statement in generated_invalid_statement_strategy(),
    ) {
        let policy = GeneratedPolicy {
            statements: vec![statement],
        };
        let error = parse_generated_policy(&policy, RenderStyle::CompactSingletons)
            .expect_err("invalid generated policy must be rejected");
        prop_assert!(
            error.contains("unsupported Condition for currently enforced bucket policy action"),
            "unexpected validation error for invalid generated policy\npolicy={}\nerror={error}",
            policy.render(RenderStyle::CompactSingletons),
        );
    }

    #[test]
    fn bucket_policy_differential_phase2_request_context_exactness_matrix(
        noise in generated_request_strategy(),
        (target, wrong) in request_context_target_and_wrong_field_strategy(),
        target_state in field_value_state_strategy(),
    ) {
        let policy = single_condition_policy(target, GeneratedResourcePattern::ObjectBucketWildcard(BUCKET));

        let mut clean = blank_request(target.action());
        apply_state(target, &mut clean, target_state);

        let mut noisy = noise.clone();
        noisy.action = target.action();
        noisy.requester = GeneratedRequester::AltUser;
        apply_state(target, &mut noisy, target_state);
        wrong.apply(&mut noisy, Some(wrong.expected_value()));

        let clean_eval = evaluate_generated(&policy, &clean);
        let noisy_eval = evaluate_generated(&policy, &noisy);
        let expected = if target_state.expected_allows() {
            PolicyEvaluation::ExplicitAllow
        } else {
            PolicyEvaluation::NoMatch
        };

        prop_assert_eq!(
            noisy_eval,
            clean_eval,
            "unrelated request-context field changed evaluation\nfield={:?}\nwrong={:?}\nstate={:?}\nclean={}\nnoisy={}",
            target,
            wrong,
            target_state,
            clean.render_builder(),
            noisy.render_builder(),
        );
        prop_assert_eq!(
            noisy_eval,
            expected,
            "single-field request-context exactness drifted\nfield={:?}\nwrong={:?}\nstate={:?}\nrequest={}",
            target,
            wrong,
            target_state,
            noisy.render_builder(),
        );
    }
}

#[test]
fn bucket_policy_differential_phase2_copy_source_and_metadata_directive_matrix() {
    let policy = parse_generated_policy(
        &GeneratedPolicy {
            statements: vec![GeneratedStatement {
                effect: GeneratedEffect::Allow,
                principal: GeneratedPrincipal::Wildcard,
                actions: vec![GeneratedActionPattern::Literal("s3:PutObject")],
                resources: vec![GeneratedResourcePattern::ObjectBucketWildcard(BUCKET)],
                conditions: vec![
                    GeneratedConditionClause {
                        operator: GeneratedConditionOperator::StringEquals,
                        key: GeneratedConditionKey::CopySource,
                        values: vec!["bucket/key".to_string()],
                    },
                    GeneratedConditionClause {
                        operator: GeneratedConditionOperator::StringEquals,
                        key: GeneratedConditionKey::MetadataDirective,
                        values: vec!["COPY".to_string()],
                    },
                ],
            }],
        },
        RenderStyle::CompactSingletons,
    )
    .expect("copy-source/metadata policy parses");

    for resource_scope in [
        ObjectResourceScope::ExactObject,
        ObjectResourceScope::BucketWildcard,
        ObjectResourceScope::BucketOnly,
    ] {
        let scoped_policy = parse_generated_policy(
            &GeneratedPolicy {
                statements: vec![GeneratedStatement {
                    effect: GeneratedEffect::Allow,
                    principal: GeneratedPrincipal::Wildcard,
                    actions: vec![GeneratedActionPattern::Literal("s3:PutObject")],
                    resources: vec![resource_scope.pattern()],
                    conditions: vec![
                        GeneratedConditionClause {
                            operator: GeneratedConditionOperator::StringEquals,
                            key: GeneratedConditionKey::CopySource,
                            values: vec!["bucket/key".to_string()],
                        },
                        GeneratedConditionClause {
                            operator: GeneratedConditionOperator::StringEquals,
                            key: GeneratedConditionKey::MetadataDirective,
                            values: vec!["COPY".to_string()],
                        },
                    ],
                }],
            },
            RenderStyle::CompactSingletons,
        );

        if matches!(resource_scope, ObjectResourceScope::BucketOnly) {
            assert!(
                scoped_policy.is_err(),
                "object action unexpectedly accepted bucket-only resource applicability"
            );
            continue;
        }
        let scoped_policy = scoped_policy.expect("scoped copy-source/metadata policy parses");

        for copy_source_state in [
            FieldValueState::Missing,
            FieldValueState::Matching,
            FieldValueState::Mismatching,
        ] {
            for metadata_state in [
                FieldValueState::Missing,
                FieldValueState::Matching,
                FieldValueState::Mismatching,
            ] {
                let mut request = blank_request(GeneratedRequestAction::CopyObject);
                apply_state(
                    RequestContextField::CopySource,
                    &mut request,
                    copy_source_state,
                );
                apply_state(
                    RequestContextField::MetadataDirective,
                    &mut request,
                    metadata_state,
                );

                let eval = evaluate_generated(&scoped_policy, &request);
                let expected =
                    if copy_source_state.expected_allows() && metadata_state.expected_allows() {
                        PolicyEvaluation::ExplicitAllow
                    } else {
                        PolicyEvaluation::NoMatch
                    };

                assert_eq!(
                    eval,
                    expected,
                    "copy-source/metadata matrix drifted\nresource_scope={resource_scope:?}\ncopy_source_state={copy_source_state:?}\nmetadata_state={metadata_state:?}\nrequest={}",
                    request.render_builder(),
                );
            }
        }
    }

    let mut with_unrelated_noise = blank_request(GeneratedRequestAction::CopyObject);
    apply_state(
        RequestContextField::CopySource,
        &mut with_unrelated_noise,
        FieldValueState::Matching,
    );
    apply_state(
        RequestContextField::MetadataDirective,
        &mut with_unrelated_noise,
        FieldValueState::Matching,
    );
    RequestContextField::GrantRead.apply(
        &mut with_unrelated_noise,
        Some(RequestContextField::GrantRead.expected_value()),
    );
    RequestContextField::ServerSideEncryption.apply(
        &mut with_unrelated_noise,
        Some(RequestContextField::ServerSideEncryption.expected_value()),
    );
    RequestContextField::RequestTagTeam.apply(
        &mut with_unrelated_noise,
        Some(RequestContextField::RequestTagTeam.expected_value()),
    );
    assert_eq!(
        evaluate_generated(&policy, &with_unrelated_noise),
        PolicyEvaluation::ExplicitAllow,
        "unrelated request context changed copy-source/metadata evaluation\n{}",
        with_unrelated_noise.render_builder(),
    );
}

#[test]
fn bucket_policy_differential_phase2_bucket_vs_object_resource_applicability_matrix() {
    let object_actions = [
        PolicyAction::GetObject,
        PolicyAction::PutObject,
        PolicyAction::GetObjectTagging,
        PolicyAction::PutObjectTagging,
        PolicyAction::DeleteObject,
    ];
    let bucket_actions = [
        PolicyAction::GetBucketAcl,
        PolicyAction::PutBucketAcl,
        PolicyAction::GetBucketVersioning,
        PolicyAction::ListBucketVersions,
        PolicyAction::ListBucketMultipartUploads,
    ];

    for action in object_actions {
        for resource in [
            GeneratedResourcePattern::ObjectArn(BUCKET, KEY),
            GeneratedResourcePattern::ObjectBucketWildcard(BUCKET),
            GeneratedResourcePattern::BucketArn(BUCKET),
        ] {
            let policy = parse_generated_policy(
                &GeneratedPolicy {
                    statements: vec![GeneratedStatement {
                        effect: GeneratedEffect::Allow,
                        principal: GeneratedPrincipal::Wildcard,
                        actions: vec![GeneratedActionPattern::Literal(action.as_str())],
                        resources: vec![resource],
                        conditions: Vec::new(),
                    }],
                },
                RenderStyle::CompactSingletons,
            );
            if matches!(resource, GeneratedResourcePattern::BucketArn(_)) {
                assert!(
                    policy.is_err(),
                    "object action unexpectedly accepted bucket-only resource\naction={action:?}\nresource={:?}",
                    resource.as_str(),
                );
            } else {
                let policy = policy.expect("object action applicability policy parses");
                let request = PolicyRequest::for_object(
                    action,
                    BUCKET,
                    KEY,
                    Some(ALT_USER),
                    None,
                    ExistingObjectTags::Unavailable,
                );
                assert_eq!(
                    policy.evaluate(&request),
                    PolicyEvaluation::ExplicitAllow,
                    "object-action resource applicability drifted\naction={action:?}\nresource={:?}",
                    resource.as_str(),
                );
            }
        }
    }

    for action in bucket_actions {
        for resource in [
            GeneratedResourcePattern::BucketArn(BUCKET),
            GeneratedResourcePattern::ObjectBucketWildcard(BUCKET),
            GeneratedResourcePattern::ObjectArn(BUCKET, KEY),
        ] {
            let policy = parse_generated_policy(
                &GeneratedPolicy {
                    statements: vec![GeneratedStatement {
                        effect: GeneratedEffect::Allow,
                        principal: GeneratedPrincipal::Wildcard,
                        actions: vec![GeneratedActionPattern::Literal(action.as_str())],
                        resources: vec![resource],
                        conditions: Vec::new(),
                    }],
                },
                RenderStyle::CompactSingletons,
            );
            if matches!(resource, GeneratedResourcePattern::BucketArn(_)) {
                let policy = policy.expect("bucket action applicability policy parses");
                let request = PolicyRequest::for_bucket(
                    action,
                    BUCKET,
                    Some(ALT_USER),
                    None,
                    auth::bucket_policy::BucketTags::Unavailable,
                );
                assert_eq!(
                    policy.evaluate(&request),
                    PolicyEvaluation::ExplicitAllow,
                    "bucket-action resource applicability drifted\naction={action:?}\nresource={:?}",
                    resource.as_str(),
                );
            } else {
                assert!(
                    policy.is_err(),
                    "bucket action unexpectedly accepted object-scoped resource\naction={action:?}\nresource={:?}",
                    resource.as_str(),
                );
            }
        }
    }
}

#[test]
fn bucket_policy_differential_phase2_unsupported_condition_keys_are_rejected() {
    for unsupported_key in [
        "aws:PrincipalArn",
        "aws:SourceVpc",
        "aws:SourceVpce",
        "aws:SourceIp",
        "s3:VersionId",
    ] {
        let policy = parse_bucket_policy(
            &serde_json::to_string(&json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::bucket/*",
                    "Condition": {
                        "StringEquals": {
                            unsupported_key: "irrelevant"
                        }
                    }
                }]
            }))
            .expect("serialize unsupported-key policy"),
        )
        .expect("unsupported-key policy parses");

        assert!(
            policy.validate_evaluable_object_conditions().is_err(),
            "unsupported condition key unexpectedly entered evaluable subset: {unsupported_key}"
        );
    }
}

#[test]
fn bucket_policy_differential_generator_support_matches_production_table() {
    let actions = [
        GeneratedActionPattern::Literal("s3:GetObject"),
        GeneratedActionPattern::Literal("s3:PutObject"),
        GeneratedActionPattern::Literal("s3:GetObjectTagging"),
        GeneratedActionPattern::Literal("s3:PutObjectTagging"),
        GeneratedActionPattern::Literal("s3:DeleteObject"),
    ];
    let operators = [
        GeneratedConditionOperator::StringEquals,
        GeneratedConditionOperator::StringEqualsIfExists,
        GeneratedConditionOperator::StringLike,
        GeneratedConditionOperator::StringLikeIfExists,
        GeneratedConditionOperator::StringNotEquals,
        GeneratedConditionOperator::StringNotEqualsIfExists,
        GeneratedConditionOperator::Null,
    ];
    let keys = [
        GeneratedConditionKey::ExistingTagClassification,
        GeneratedConditionKey::ExistingTagRegion,
        GeneratedConditionKey::RequestTagTeam,
        GeneratedConditionKey::CopySource,
        GeneratedConditionKey::MetadataDirective,
        GeneratedConditionKey::CannedAcl,
        GeneratedConditionKey::ServerSideEncryption,
        GeneratedConditionKey::SseCustomerAlgorithm,
        GeneratedConditionKey::GrantRead,
        GeneratedConditionKey::GrantWrite,
        GeneratedConditionKey::GrantReadAcp,
        GeneratedConditionKey::GrantWriteAcp,
        GeneratedConditionKey::GrantFullControl,
    ];

    for action in actions {
        for operator in operators {
            for key in keys {
                let clause = GeneratedConditionClause {
                    operator,
                    key,
                    values: vec!["value".to_string()],
                };
                assert_eq!(
                    action.supports_clause(&clause),
                    clause_supported_for_action_for_tests(
                        operator.as_str(),
                        key.as_str(),
                        action.policy_action(),
                    ),
                    "generator/production support mismatch for action {} operator {} key {}",
                    action.as_str(),
                    operator.as_str(),
                    key.as_str(),
                );
            }
        }
    }
}
