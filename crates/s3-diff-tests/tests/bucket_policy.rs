use std::thread;
use std::time::Duration;

use auth::bucket_policy::{
    parse_bucket_policy, BucketPolicy, PolicyAction, PolicyEvaluation, PolicyRequest, PolicyTag,
};
use s3_tests::{
    aws_sdk_s3::{
        error::ProvideErrorMetadata,
        primitives::ByteStream,
        types::{
            BucketCannedAcl, BucketLocationConstraint, CreateBucketConfiguration,
            MetadataDirective, ObjectAttributes, ObjectCannedAcl, ObjectOwnership,
            OwnershipControls, OwnershipControlsRule, ServerSideEncryption, Tag, Tagging,
        },
        Client,
    },
    build_client_with_ca, enable_bucket_sse_c, sse_c_header_values, test_sse_c_key, unique_bucket,
    TestServer, CTX,
};
use s3_types::is_legacy_create_bucket_region;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteOutcome {
    Allow,
    Reject,
}

#[derive(Clone, Debug, Default)]
struct PutObjectShape {
    key: &'static str,
    body: &'static [u8],
    tagging: Option<String>,
    canned_acl: Option<&'static str>,
    server_side_encryption: Option<&'static str>,
    sse_customer_algorithm: Option<&'static str>,
    grant_read: Option<String>,
    grant_write: Option<String>,
    grant_read_acp: Option<String>,
    grant_write_acp: Option<String>,
    grant_full_control: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct AclMutationShape {
    canned_acl: Option<&'static str>,
    grant_read: Option<String>,
    grant_write: Option<String>,
    grant_read_acp: Option<String>,
    grant_write_acp: Option<String>,
    grant_full_control: Option<String>,
}

#[derive(Clone, Debug)]
enum ScenarioOperation {
    PutObject(PutObjectShape),
    GetObject {
        key: &'static str,
    },
    GetObjectAcl {
        key: &'static str,
    },
    GetObjectAttributes {
        key: &'static str,
    },
    GetObjectTagging {
        key: &'static str,
    },
    PutObjectTagging {
        key: &'static str,
        request_tag: &'static str,
    },
    PutBucketAcl(AclMutationShape),
    PutObjectAcl {
        key: &'static str,
        acl: AclMutationShape,
    },
    CopyObjectCopySource {
        source_key: &'static str,
    },
    CopyObjectMetadataDirective {
        source_key: &'static str,
        metadata_directive: Option<MetadataDirective>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GrantHeaderCondition {
    Read,
    Write,
    ReadAcp,
    WriteAcp,
    FullControl,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExistingTagReadAction {
    Object,
    ObjectAcl,
    ObjectAttributes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PolicyShape {
    ExistingTagRead(ExistingTagReadAction),
    GetObjectTaggingExistingPublic,
    PutObjectTaggingRequestPublic,
    PutObjectInlineTaggingRequestPublic,
    PutObjectAclPrivate,
    PutObjectGrantReadOwner,
    PutBucketAclGrant(GrantHeaderCondition),
    PutObjectAclGrant(GrantHeaderCondition),
    PutObjectSseAes256,
    PutObjectSseCustomerAlgorithmAes256,
    PutObjectAclNullDeny,
    PutObjectSseS3NullDeny,
    CopyObjectCopySourcePublic,
    CopyObjectMetadataDirectiveCopy,
    CopyObjectCopySourcePublicAndMetadataDirectiveCopy,
}

#[derive(Clone, Debug)]
struct Scenario {
    name: &'static str,
    policy_shape: PolicyShape,
    local_request: LocalRequest,
    operation: ScenarioOperation,
    local_expected: PolicyEvaluation,
    remote_expected: RemoteOutcome,
}

#[derive(Clone, Debug)]
struct LocalRequest {
    action: PolicyAction,
    existing_tags: Vec<(&'static str, &'static str)>,
    request_tags: Vec<(&'static str, &'static str)>,
    copy_source: Option<String>,
    metadata_directive: Option<&'static str>,
    canned_acl: Option<&'static str>,
    server_side_encryption: Option<&'static str>,
    sse_customer_algorithm: Option<&'static str>,
    grant_read: Option<String>,
    grant_write: Option<String>,
    grant_read_acp: Option<String>,
    grant_write_acp: Option<String>,
    grant_full_control: Option<String>,
    bucket_resource: bool,
}

impl LocalRequest {
    fn evaluate(&self, policy: &BucketPolicy, bucket: &str, principal: &str) -> PolicyEvaluation {
        let existing_tags = self
            .existing_tags
            .iter()
            .map(|(key, value)| PolicyTag::new(key, value))
            .collect::<Vec<_>>();
        let request_tags = self
            .request_tags
            .iter()
            .map(|(key, value)| PolicyTag::new(key, value))
            .collect::<Vec<_>>();

        let request = if self.bucket_resource {
            PolicyRequest::for_bucket(self.action, bucket, Some(principal), None)
        } else {
            PolicyRequest::new(self.action, bucket, "key", Some(principal), None)
        }
        .with_existing_object_tags(&existing_tags)
        .with_request_object_tags(&request_tags)
        .with_copy_source(self.copy_source.as_deref())
        .with_metadata_directive(self.metadata_directive)
        .with_canned_acl(self.canned_acl)
        .with_server_side_encryption(self.server_side_encryption)
        .with_sse_customer_algorithm(self.sse_customer_algorithm)
        .with_grant_read(self.grant_read.as_deref())
        .with_grant_write(self.grant_write.as_deref())
        .with_grant_read_acp(self.grant_read_acp.as_deref())
        .with_grant_write_acp(self.grant_write_acp.as_deref())
        .with_grant_full_control(self.grant_full_control.as_deref());
        policy.evaluate(&request)
    }
}

struct DiffEnv {
    external_owner: Client,
    external_alt: Client,
    local_owner: Client,
    local_alt: Client,
    external_region: String,
    local_server: TestServer,
}

impl DiffEnv {
    async fn setup() -> Option<Self> {
        std::env::var_os("S3_TEST_ENDPOINT")?;

        let _ = &*CTX;
        let external_region = CTX.region().to_string();
        let local_server = TestServer::start_https_in_region(&external_region).await;
        let local_owner = build_client_with_ca(
            local_server.endpoint(),
            s3_tests::server::TEST_ACCESS_KEY,
            s3_tests::server::TEST_SECRET_KEY,
            &external_region,
            local_server.tls_ca_pem(),
        );
        let local_alt = build_client_with_ca(
            local_server.endpoint(),
            s3_tests::server::ALT_ACCESS_KEY,
            s3_tests::server::ALT_SECRET_KEY,
            &external_region,
            local_server.tls_ca_pem(),
        );

        Some(Self {
            external_owner: CTX.client().clone(),
            external_alt: CTX.alt_client().clone(),
            local_owner,
            local_alt,
            external_region,
            local_server,
        })
    }

    async fn create_bucket_pair(&self) -> String {
        let bucket = unique_bucket();
        create_bucket_in_region(&self.external_owner, &bucket, &self.external_region).await;
        create_bucket_in_region(&self.local_owner, &bucket, &self.external_region).await;
        bucket
    }

    async fn cleanup_pair(&self, bucket: &str, keys: &[&str]) {
        for client in [&self.external_owner, &self.local_owner] {
            let _ = client.delete_bucket_policy().bucket(bucket).send().await;
            for key in keys {
                let _ = client.delete_object().bucket(bucket).key(*key).send().await;
            }
            let _ = client.delete_bucket().bucket(bucket).send().await;
        }
    }
}

fn object_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}/*")
}

fn bucket_resource(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

fn alt_root_principal(account_id: &str) -> String {
    format!("arn:aws:iam::{account_id}:root")
}

impl GrantHeaderCondition {
    fn policy_condition_key(self) -> &'static str {
        match self {
            Self::Read => "s3:x-amz-grant-read",
            Self::Write => "s3:x-amz-grant-write",
            Self::ReadAcp => "s3:x-amz-grant-read-acp",
            Self::WriteAcp => "s3:x-amz-grant-write-acp",
            Self::FullControl => "s3:x-amz-grant-full-control",
        }
    }

    fn request_header_name(self) -> &'static str {
        match self {
            Self::Read => "x-amz-grant-read",
            Self::Write => "x-amz-grant-write",
            Self::ReadAcp => "x-amz-grant-read-acp",
            Self::WriteAcp => "x-amz-grant-write-acp",
            Self::FullControl => "x-amz-grant-full-control",
        }
    }
}

impl ExistingTagReadAction {
    fn policy_action(self) -> PolicyAction {
        match self {
            Self::Object => PolicyAction::GetObject,
            Self::ObjectAcl => PolicyAction::GetObjectAcl,
            Self::ObjectAttributes => PolicyAction::GetObjectAttributes,
        }
    }
}

fn apply_grant_header_to_local_request(
    request: &mut LocalRequest,
    condition: GrantHeaderCondition,
    value: String,
) {
    match condition {
        GrantHeaderCondition::Read => request.grant_read = Some(value),
        GrantHeaderCondition::Write => request.grant_write = Some(value),
        GrantHeaderCondition::ReadAcp => request.grant_read_acp = Some(value),
        GrantHeaderCondition::WriteAcp => request.grant_write_acp = Some(value),
        GrantHeaderCondition::FullControl => request.grant_full_control = Some(value),
    }
}

fn apply_grant_header_to_acl_shape(
    shape: &mut AclMutationShape,
    condition: GrantHeaderCondition,
    value: String,
) {
    match condition {
        GrantHeaderCondition::Read => shape.grant_read = Some(value),
        GrantHeaderCondition::Write => shape.grant_write = Some(value),
        GrantHeaderCondition::ReadAcp => shape.grant_read_acp = Some(value),
        GrantHeaderCondition::WriteAcp => shape.grant_write_acp = Some(value),
        GrantHeaderCondition::FullControl => shape.grant_full_control = Some(value),
    }
}

fn put_object_grant_headers(shape: &PutObjectShape) -> Vec<(&'static str, String)> {
    let mut headers = Vec::new();
    if let Some(value) = &shape.grant_read {
        headers.push((
            GrantHeaderCondition::Read.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_write {
        headers.push((
            GrantHeaderCondition::Write.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_read_acp {
        headers.push((
            GrantHeaderCondition::ReadAcp.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_write_acp {
        headers.push((
            GrantHeaderCondition::WriteAcp.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_full_control {
        headers.push((
            GrantHeaderCondition::FullControl.request_header_name(),
            value.clone(),
        ));
    }
    headers
}

fn acl_mutation_grant_headers(shape: &AclMutationShape) -> Vec<(&'static str, String)> {
    let mut headers = Vec::new();
    if let Some(value) = &shape.grant_read {
        headers.push((
            GrantHeaderCondition::Read.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_write {
        headers.push((
            GrantHeaderCondition::Write.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_read_acp {
        headers.push((
            GrantHeaderCondition::ReadAcp.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_write_acp {
        headers.push((
            GrantHeaderCondition::WriteAcp.request_header_name(),
            value.clone(),
        ));
    }
    if let Some(value) = &shape.grant_full_control {
        headers.push((
            GrantHeaderCondition::FullControl.request_header_name(),
            value.clone(),
        ));
    }
    headers
}

fn bucket_policy_document(
    principal: &str,
    action: &str,
    resource: String,
    conditions: serde_json::Value,
) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": principal },
            "Action": action,
            "Resource": resource,
            "Condition": conditions,
        }],
    })
    .to_string()
}

fn copy_bucket_policy_document(
    principal: &str,
    bucket: &str,
    conditions: serde_json::Value,
) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": { "AWS": principal },
                "Action": "s3:PutObject",
                "Resource": object_resource(bucket),
                "Condition": conditions,
            },
            {
                "Effect": "Allow",
                "Principal": { "AWS": principal },
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{bucket}/src/*"),
            }
        ],
    })
    .to_string()
}

impl PolicyShape {
    fn requires_acl_capable_bucket(self) -> bool {
        matches!(
            self,
            Self::PutObjectAclPrivate
                | Self::PutObjectGrantReadOwner
                | Self::PutObjectAclNullDeny
                | Self::PutBucketAclGrant(_)
                | Self::PutObjectAclGrant(_)
        )
    }

    fn render(self, bucket: &str, principal: &str, owner_canonical_id: &str) -> String {
        match self {
            Self::ExistingTagRead(ExistingTagReadAction::ObjectAttributes) => serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": { "AWS": principal },
                    "Action": ["s3:GetObject", "s3:GetObjectAttributes"],
                    "Resource": object_resource(bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:ExistingObjectTag/security": "public"
                        }
                    },
                }],
            })
            .to_string(),
            Self::ExistingTagRead(action) => bucket_policy_document(
                principal,
                action.policy_action().as_str(),
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }),
            ),
            Self::GetObjectTaggingExistingPublic => bucket_policy_document(
                principal,
                "s3:GetObjectTagging",
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        "s3:ExistingObjectTag/security": "public"
                    }
                }),
            ),
            Self::PutObjectTaggingRequestPublic => bucket_policy_document(
                principal,
                "s3:PutObjectTagging",
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        "s3:RequestObjectTag/security": "public"
                    }
                }),
            ),
            Self::PutObjectInlineTaggingRequestPublic => serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": { "AWS": principal },
                    "Action": ["s3:PutObject", "s3:PutObjectTagging"],
                    "Resource": object_resource(bucket),
                    "Condition": {
                        "StringEquals": {
                            "s3:RequestObjectTag/security": "public"
                        }
                    },
                }],
            })
            .to_string(),
            Self::PutObjectAclPrivate => bucket_policy_document(
                principal,
                "s3:PutObject",
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        "s3:x-amz-acl": "private"
                    }
                }),
            ),
            Self::PutObjectGrantReadOwner => bucket_policy_document(
                principal,
                "s3:PutObject",
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        "s3:x-amz-grant-read": format!("id=\"{owner_canonical_id}\"")
                    }
                }),
            ),
            Self::PutBucketAclGrant(condition) => bucket_policy_document(
                principal,
                "s3:PutBucketAcl",
                bucket_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        condition.policy_condition_key(): format!("id=\"{owner_canonical_id}\"")
                    }
                }),
            ),
            Self::PutObjectAclGrant(condition) => bucket_policy_document(
                principal,
                "s3:PutObjectAcl",
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        condition.policy_condition_key(): format!("id=\"{owner_canonical_id}\"")
                    }
                }),
            ),
            Self::PutObjectSseAes256 => bucket_policy_document(
                principal,
                "s3:PutObject",
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        "s3:x-amz-server-side-encryption": "AES256"
                    }
                }),
            ),
            Self::PutObjectSseCustomerAlgorithmAes256 => bucket_policy_document(
                principal,
                "s3:PutObject",
                object_resource(bucket),
                serde_json::json!({
                    "StringEquals": {
                        "s3:x-amz-server-side-encryption-customer-algorithm": "AES256"
                    }
                }),
            ),
            Self::PutObjectAclNullDeny => serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": { "AWS": principal },
                        "Action": "s3:PutObject",
                        "Resource": object_resource(bucket),
                    },
                    {
                        "Effect": "Deny",
                        "Principal": { "AWS": principal },
                        "Action": "s3:PutObject",
                        "Resource": object_resource(bucket),
                        "Condition": {
                            "Null": {
                                "s3:x-amz-acl": "true"
                            }
                        }
                    }
                ],
            })
            .to_string(),
            Self::PutObjectSseS3NullDeny => serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": { "AWS": principal },
                        "Action": "s3:PutObject",
                        "Resource": object_resource(bucket),
                    },
                    {
                        "Effect": "Deny",
                        "Principal": { "AWS": principal },
                        "Action": "s3:PutObject",
                        "Resource": object_resource(bucket),
                        "Condition": {
                            "Null": {
                                "s3:x-amz-server-side-encryption": "true"
                            }
                        }
                    }
                ],
            })
            .to_string(),
            Self::CopyObjectCopySourcePublic => copy_bucket_policy_document(
                principal,
                bucket,
                serde_json::json!({
                    "StringLike": {
                        "s3:x-amz-copy-source": format!("{bucket}/src/public/*")
                    }
                }),
            ),
            Self::CopyObjectMetadataDirectiveCopy => copy_bucket_policy_document(
                principal,
                bucket,
                serde_json::json!({
                    "StringEquals": {
                        "s3:x-amz-metadata-directive": "COPY"
                    }
                }),
            ),
            Self::CopyObjectCopySourcePublicAndMetadataDirectiveCopy => {
                copy_bucket_policy_document(
                    principal,
                    bucket,
                    serde_json::json!({
                        "StringLike": {
                            "s3:x-amz-copy-source": format!("{bucket}/src/public/*")
                        },
                        "StringEquals": {
                            "s3:x-amz-metadata-directive": "COPY"
                        }
                    }),
                )
            }
        }
    }
}

fn build_scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "put-object inline request tag allow",
            policy_shape: PolicyShape::PutObjectInlineTaggingRequestPublic,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![("security", "public")],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-request-tag-allowed",
                body: b"allowed",
                tagging: Some("security=public".to_string()),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object inline request tag mismatch",
            policy_shape: PolicyShape::PutObjectInlineTaggingRequestPublic,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![("security", "private")],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-request-tag-denied",
                body: b"denied",
                tagging: Some("security=private".to_string()),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "get-object-tagging existing tag allow",
            policy_shape: PolicyShape::GetObjectTaggingExistingPublic,
            local_request: LocalRequest {
                action: PolicyAction::GetObjectTagging,
                existing_tags: vec![("security", "public")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObjectTagging {
                key: "existing-public",
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "get-object-tagging existing tag mismatch",
            policy_shape: PolicyShape::GetObjectTaggingExistingPublic,
            local_request: LocalRequest {
                action: PolicyAction::GetObjectTagging,
                existing_tags: vec![("security", "private")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObjectTagging {
                key: "existing-private",
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "get-object existing tag allow",
            policy_shape: PolicyShape::ExistingTagRead(ExistingTagReadAction::Object),
            local_request: LocalRequest {
                action: PolicyAction::GetObject,
                existing_tags: vec![("security", "public")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObject {
                key: "existing-public",
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "get-object existing tag mismatch",
            policy_shape: PolicyShape::ExistingTagRead(ExistingTagReadAction::Object),
            local_request: LocalRequest {
                action: PolicyAction::GetObject,
                existing_tags: vec![("security", "private")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObject {
                key: "existing-private",
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "get-object-acl existing tag allow",
            policy_shape: PolicyShape::ExistingTagRead(ExistingTagReadAction::ObjectAcl),
            local_request: LocalRequest {
                action: PolicyAction::GetObjectAcl,
                existing_tags: vec![("security", "public")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObjectAcl {
                key: "existing-public",
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "get-object-acl existing tag mismatch",
            policy_shape: PolicyShape::ExistingTagRead(ExistingTagReadAction::ObjectAcl),
            local_request: LocalRequest {
                action: PolicyAction::GetObjectAcl,
                existing_tags: vec![("security", "private")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObjectAcl {
                key: "existing-private",
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "get-object-attributes existing tag allow",
            policy_shape: PolicyShape::ExistingTagRead(ExistingTagReadAction::ObjectAttributes),
            local_request: LocalRequest {
                action: PolicyAction::GetObjectAttributes,
                existing_tags: vec![("security", "public")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObjectAttributes {
                key: "existing-public",
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "get-object-attributes existing tag mismatch",
            policy_shape: PolicyShape::ExistingTagRead(ExistingTagReadAction::ObjectAttributes),
            local_request: LocalRequest {
                action: PolicyAction::GetObjectAttributes,
                existing_tags: vec![("security", "private")],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::GetObjectAttributes {
                key: "existing-private",
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object-tagging request tag allow",
            policy_shape: PolicyShape::PutObjectTaggingRequestPublic,
            local_request: LocalRequest {
                action: PolicyAction::PutObjectTagging,
                existing_tags: vec![],
                request_tags: vec![("security", "public")],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObjectTagging {
                key: "tag-target",
                request_tag: "public",
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object-tagging request tag mismatch",
            policy_shape: PolicyShape::PutObjectTaggingRequestPublic,
            local_request: LocalRequest {
                action: PolicyAction::PutObjectTagging,
                existing_tags: vec![],
                request_tags: vec![("security", "private")],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObjectTagging {
                key: "tag-target",
                request_tag: "private",
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object acl private allow",
            policy_shape: PolicyShape::PutObjectAclPrivate,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: Some("private"),
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-acl-private-allowed",
                body: b"allowed",
                canned_acl: Some("private"),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object acl private mismatch",
            policy_shape: PolicyShape::PutObjectAclPrivate,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-acl-private-denied",
                body: b"denied",
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object grant-read allow",
            policy_shape: PolicyShape::PutObjectGrantReadOwner,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: Some(String::new()),
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-grant-read-allowed",
                body: b"allowed",
                grant_read: Some(String::new()),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object grant-read mismatch",
            policy_shape: PolicyShape::PutObjectGrantReadOwner,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-grant-read-denied",
                body: b"denied",
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-bucket-acl grant-read allow",
            policy_shape: PolicyShape::PutBucketAclGrant(GrantHeaderCondition::Read),
            local_request: LocalRequest {
                action: PolicyAction::PutBucketAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: Some(String::new()),
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: true,
            },
            operation: ScenarioOperation::PutBucketAcl(AclMutationShape {
                grant_read: Some(String::new()),
                ..AclMutationShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-bucket-acl grant-read mismatch",
            policy_shape: PolicyShape::PutBucketAclGrant(GrantHeaderCondition::Read),
            local_request: LocalRequest {
                action: PolicyAction::PutBucketAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: Some("private"),
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: true,
            },
            operation: ScenarioOperation::PutBucketAcl(AclMutationShape {
                canned_acl: Some("private"),
                ..AclMutationShape::default()
            }),
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-bucket-acl grant-full-control allow",
            policy_shape: PolicyShape::PutBucketAclGrant(GrantHeaderCondition::FullControl),
            local_request: LocalRequest {
                action: PolicyAction::PutBucketAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: Some(String::new()),
                bucket_resource: true,
            },
            operation: ScenarioOperation::PutBucketAcl(AclMutationShape {
                grant_full_control: Some(String::new()),
                ..AclMutationShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-bucket-acl grant-full-control mismatch",
            policy_shape: PolicyShape::PutBucketAclGrant(GrantHeaderCondition::FullControl),
            local_request: LocalRequest {
                action: PolicyAction::PutBucketAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: Some("private"),
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: true,
            },
            operation: ScenarioOperation::PutBucketAcl(AclMutationShape {
                canned_acl: Some("private"),
                ..AclMutationShape::default()
            }),
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object-acl grant-read allow",
            policy_shape: PolicyShape::PutObjectAclGrant(GrantHeaderCondition::Read),
            local_request: LocalRequest {
                action: PolicyAction::PutObjectAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: Some(String::new()),
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObjectAcl {
                key: "acl-target",
                acl: AclMutationShape {
                    grant_read: Some(String::new()),
                    ..AclMutationShape::default()
                },
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object-acl grant-read mismatch",
            policy_shape: PolicyShape::PutObjectAclGrant(GrantHeaderCondition::Read),
            local_request: LocalRequest {
                action: PolicyAction::PutObjectAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: Some("private"),
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObjectAcl {
                key: "acl-target",
                acl: AclMutationShape {
                    canned_acl: Some("private"),
                    ..AclMutationShape::default()
                },
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object-acl grant-full-control allow",
            policy_shape: PolicyShape::PutObjectAclGrant(GrantHeaderCondition::FullControl),
            local_request: LocalRequest {
                action: PolicyAction::PutObjectAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: Some(String::new()),
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObjectAcl {
                key: "acl-target",
                acl: AclMutationShape {
                    grant_full_control: Some(String::new()),
                    ..AclMutationShape::default()
                },
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object-acl grant-full-control mismatch",
            policy_shape: PolicyShape::PutObjectAclGrant(GrantHeaderCondition::FullControl),
            local_request: LocalRequest {
                action: PolicyAction::PutObjectAcl,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: Some("private"),
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObjectAcl {
                key: "acl-target",
                acl: AclMutationShape {
                    canned_acl: Some("private"),
                    ..AclMutationShape::default()
                },
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object sse-s3 allow",
            policy_shape: PolicyShape::PutObjectSseAes256,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: Some("AES256"),
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-sse-s3-allowed",
                body: b"allowed",
                server_side_encryption: Some("AES256"),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object sse-s3 mismatch",
            policy_shape: PolicyShape::PutObjectSseAes256,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-sse-s3-denied",
                body: b"denied",
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object sse-c allow",
            policy_shape: PolicyShape::PutObjectSseCustomerAlgorithmAes256,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: Some("AES256"),
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-sse-c-allowed",
                body: b"allowed",
                sse_customer_algorithm: Some("AES256"),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object sse-c mismatch",
            policy_shape: PolicyShape::PutObjectSseCustomerAlgorithmAes256,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-sse-c-denied",
                body: b"denied",
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object acl null deny missing header",
            policy_shape: PolicyShape::PutObjectAclNullDeny,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-acl-null-denied",
                body: b"denied",
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitDeny,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object acl null deny explicit private allowed",
            policy_shape: PolicyShape::PutObjectAclNullDeny,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: Some("private"),
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-acl-null-allowed",
                body: b"allowed",
                canned_acl: Some("private"),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "put-object sse-s3 null deny missing header",
            policy_shape: PolicyShape::PutObjectSseS3NullDeny,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-sse-null-denied",
                body: b"denied",
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitDeny,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "put-object sse-s3 null deny aes256 allowed",
            policy_shape: PolicyShape::PutObjectSseS3NullDeny,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: None,
                canned_acl: None,
                server_side_encryption: Some("AES256"),
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::PutObject(PutObjectShape {
                key: "put-sse-null-allowed",
                body: b"allowed",
                server_side_encryption: Some("AES256"),
                ..PutObjectShape::default()
            }),
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "copy-object copy-source allow",
            policy_shape: PolicyShape::CopyObjectCopySourcePublic,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::CopyObjectCopySource {
                source_key: "src/public/foo",
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "copy-object copy-source mismatch",
            policy_shape: PolicyShape::CopyObjectCopySourcePublic,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::CopyObjectCopySource {
                source_key: "src/private/foo",
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "copy-object metadata-directive allow",
            policy_shape: PolicyShape::CopyObjectMetadataDirectiveCopy,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: Some("COPY"),
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::CopyObjectMetadataDirective {
                source_key: "src/meta/foo",
                metadata_directive: Some(MetadataDirective::Copy),
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "copy-object metadata-directive missing is no-match",
            policy_shape: PolicyShape::CopyObjectMetadataDirectiveCopy,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::CopyObjectMetadataDirective {
                source_key: "src/meta/foo",
                metadata_directive: None,
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "copy-object copy-source and metadata-directive allow",
            policy_shape: PolicyShape::CopyObjectCopySourcePublicAndMetadataDirectiveCopy,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: Some("COPY"),
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::CopyObjectMetadataDirective {
                source_key: "src/public/foo",
                metadata_directive: Some(MetadataDirective::Copy),
            },
            local_expected: PolicyEvaluation::ExplicitAllow,
            remote_expected: RemoteOutcome::Allow,
        },
        Scenario {
            name: "copy-object copy-source and metadata-directive missing is no-match",
            policy_shape: PolicyShape::CopyObjectCopySourcePublicAndMetadataDirectiveCopy,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
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
                bucket_resource: false,
            },
            operation: ScenarioOperation::CopyObjectMetadataDirective {
                source_key: "src/public/foo",
                metadata_directive: None,
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
        Scenario {
            name: "copy-object copy-source and metadata-directive source mismatch",
            policy_shape: PolicyShape::CopyObjectCopySourcePublicAndMetadataDirectiveCopy,
            local_request: LocalRequest {
                action: PolicyAction::PutObject,
                existing_tags: vec![],
                request_tags: vec![],
                copy_source: None,
                metadata_directive: Some("COPY"),
                canned_acl: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                grant_read: None,
                grant_write: None,
                grant_read_acp: None,
                grant_write_acp: None,
                grant_full_control: None,
                bucket_resource: false,
            },
            operation: ScenarioOperation::CopyObjectMetadataDirective {
                source_key: "src/private/foo",
                metadata_directive: Some(MetadataDirective::Copy),
            },
            local_expected: PolicyEvaluation::NoMatch,
            remote_expected: RemoteOutcome::Reject,
        },
    ]
}

fn materialize_scenario(template: &Scenario, bucket: &str, owner_canonical_id: &str) -> Scenario {
    let mut scenario = template.clone();
    scenario.local_request.copy_source = match &scenario.operation {
        ScenarioOperation::CopyObjectCopySource { source_key }
        | ScenarioOperation::CopyObjectMetadataDirective { source_key, .. } => {
            Some(format!("{bucket}/{source_key}"))
        }
        _ => None,
    };
    let owner_grant_header = format!("id=\"{owner_canonical_id}\"");
    if matches!(scenario.policy_shape, PolicyShape::PutObjectGrantReadOwner) {
        if scenario.local_request.grant_read.is_some() {
            scenario.local_request.grant_read = Some(owner_grant_header.clone());
        }
        if let ScenarioOperation::PutObject(shape) = &mut scenario.operation {
            if shape.grant_read.is_some() {
                shape.grant_read = Some(owner_grant_header.clone());
            }
        }
    }
    if let PolicyShape::PutBucketAclGrant(condition) | PolicyShape::PutObjectAclGrant(condition) =
        scenario.policy_shape
    {
        let local_has_placeholder = match condition {
            GrantHeaderCondition::Read => scenario.local_request.grant_read.is_some(),
            GrantHeaderCondition::Write => scenario.local_request.grant_write.is_some(),
            GrantHeaderCondition::ReadAcp => scenario.local_request.grant_read_acp.is_some(),
            GrantHeaderCondition::WriteAcp => scenario.local_request.grant_write_acp.is_some(),
            GrantHeaderCondition::FullControl => {
                scenario.local_request.grant_full_control.is_some()
            }
        };
        if local_has_placeholder {
            apply_grant_header_to_local_request(
                &mut scenario.local_request,
                condition,
                owner_grant_header.clone(),
            );
        }
        match &mut scenario.operation {
            ScenarioOperation::PutBucketAcl(acl) => {
                let op_has_placeholder = match condition {
                    GrantHeaderCondition::Read => acl.grant_read.is_some(),
                    GrantHeaderCondition::Write => acl.grant_write.is_some(),
                    GrantHeaderCondition::ReadAcp => acl.grant_read_acp.is_some(),
                    GrantHeaderCondition::WriteAcp => acl.grant_write_acp.is_some(),
                    GrantHeaderCondition::FullControl => acl.grant_full_control.is_some(),
                };
                if op_has_placeholder {
                    apply_grant_header_to_acl_shape(acl, condition, owner_grant_header.clone());
                }
            }
            ScenarioOperation::PutObjectAcl { acl, .. } => {
                let op_has_placeholder = match condition {
                    GrantHeaderCondition::Read => acl.grant_read.is_some(),
                    GrantHeaderCondition::Write => acl.grant_write.is_some(),
                    GrantHeaderCondition::ReadAcp => acl.grant_read_acp.is_some(),
                    GrantHeaderCondition::WriteAcp => acl.grant_write_acp.is_some(),
                    GrantHeaderCondition::FullControl => acl.grant_full_control.is_some(),
                };
                if op_has_placeholder {
                    apply_grant_header_to_acl_shape(acl, condition, owner_grant_header.clone());
                }
            }
            _ => {}
        }
    }
    scenario
}

async fn create_bucket_in_region(client: &Client, bucket: &str, region: &str) {
    let mut request = client.create_bucket().bucket(bucket);
    if !is_legacy_create_bucket_region(region) {
        request = request.create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(region))
                .build(),
        );
    }
    request.send().await.expect("create bucket");
}

async fn set_object_writer_ownership(client: &Client, bucket: &str) {
    let rule = OwnershipControlsRule::builder()
        .object_ownership(ObjectOwnership::ObjectWriter)
        .build()
        .unwrap();
    let controls = OwnershipControls::builder().rules(rule).build().unwrap();
    client
        .put_bucket_ownership_controls()
        .bucket(bucket)
        .ownership_controls(controls)
        .send()
        .await
        .expect("set object writer ownership");
}

fn tag(key: &str, value: &str) -> Tag {
    Tag::builder().key(key).value(value).build().unwrap()
}

fn tagging(tags: Vec<Tag>) -> Tagging {
    Tagging::builder().set_tag_set(Some(tags)).build().unwrap()
}

async fn canonical_owner_id(client: &Client, bucket: &str) -> String {
    client
        .get_bucket_acl()
        .bucket(bucket)
        .send()
        .await
        .expect("get bucket acl for owner id")
        .owner()
        .and_then(|owner| owner.id())
        .expect("bucket owner id")
        .to_string()
}

async fn install_bucket_policy(client: &Client, bucket: &str, policy_json: &str) {
    client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy_json)
        .send()
        .await
        .expect("install bucket policy");
}

async fn setup_tagged_object(client: &Client, bucket: &str, key: &str, tag_value: &str) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await
        .expect("put tagged object");
    client
        .put_object_tagging()
        .bucket(bucket)
        .key(key)
        .tagging(tagging(vec![tag("security", tag_value)]))
        .send()
        .await
        .expect("put object tagging");
}

async fn setup_plain_object(client: &Client, bucket: &str, key: &str) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"body"))
        .send()
        .await
        .expect("put plain object");
}

async fn setup_copy_source_object(client: &Client, bucket: &str, key: &str) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"copy-source"))
        .send()
        .await
        .expect("put copy source object");
}

async fn observe_scenario(client: &Client, bucket: &str, scenario: &Scenario) -> RemoteOutcome {
    const MAX_ATTEMPTS: usize = 20;
    for attempt in 0..MAX_ATTEMPTS {
        let observed = match &scenario.operation {
            ScenarioOperation::PutObject(shape) => {
                let mut request = client
                    .put_object()
                    .bucket(bucket)
                    .key(shape.key)
                    .body(ByteStream::from_static(shape.body));
                if let Some(tagging) = &shape.tagging {
                    request = request.tagging(tagging);
                }
                if let Some(canned_acl) = shape.canned_acl {
                    request = request.acl(match canned_acl {
                        "private" => ObjectCannedAcl::Private,
                        other => panic!("unsupported canned acl {other}"),
                    });
                }
                if let Some(sse) = shape.server_side_encryption {
                    request = request.server_side_encryption(match sse {
                        "AES256" => ServerSideEncryption::Aes256,
                        other => panic!("unsupported sse {other}"),
                    });
                }
                if let Some(algorithm) = shape.sse_customer_algorithm {
                    let customer_key = test_sse_c_key();
                    let (key_b64, key_md5_b64) = sse_c_header_values(&customer_key);
                    request = request
                        .sse_customer_algorithm(algorithm)
                        .sse_customer_key(key_b64)
                        .sse_customer_key_md5(key_md5_b64);
                }
                let grant_headers = put_object_grant_headers(shape);
                let result = if grant_headers.is_empty() {
                    request.send().await
                } else {
                    request
                        .customize()
                        .mutate_request(move |req| {
                            for (header, value) in &grant_headers {
                                req.headers_mut().insert(*header, value.clone());
                            }
                        })
                        .send()
                        .await
                };
                handle_observed_result(scenario.name, attempt, result)
            }
            ScenarioOperation::GetObject { key } => handle_observed_result(
                scenario.name,
                attempt,
                client.get_object().bucket(bucket).key(*key).send().await,
            ),
            ScenarioOperation::GetObjectAcl { key } => handle_observed_result(
                scenario.name,
                attempt,
                client
                    .get_object_acl()
                    .bucket(bucket)
                    .key(*key)
                    .send()
                    .await,
            ),
            ScenarioOperation::GetObjectAttributes { key } => handle_observed_result(
                scenario.name,
                attempt,
                client
                    .get_object_attributes()
                    .bucket(bucket)
                    .key(*key)
                    .object_attributes(ObjectAttributes::ObjectSize)
                    .send()
                    .await,
            ),
            ScenarioOperation::GetObjectTagging { key } => handle_observed_result(
                scenario.name,
                attempt,
                client
                    .get_object_tagging()
                    .bucket(bucket)
                    .key(*key)
                    .send()
                    .await,
            ),
            ScenarioOperation::PutObjectTagging { key, request_tag } => handle_observed_result(
                scenario.name,
                attempt,
                client
                    .put_object_tagging()
                    .bucket(bucket)
                    .key(*key)
                    .tagging(tagging(vec![tag("security", request_tag)]))
                    .send()
                    .await,
            ),
            ScenarioOperation::PutBucketAcl(acl) => {
                let request = client.put_bucket_acl().bucket(bucket);
                let request = if let Some(canned_acl) = acl.canned_acl {
                    request.acl(match canned_acl {
                        "private" => BucketCannedAcl::Private,
                        other => panic!("unsupported bucket acl {other}"),
                    })
                } else {
                    request
                };
                let grant_headers = acl_mutation_grant_headers(acl);
                let result = if grant_headers.is_empty() {
                    request.send().await
                } else {
                    request
                        .customize()
                        .mutate_request(move |req| {
                            for (header, value) in &grant_headers {
                                req.headers_mut().insert(*header, value.clone());
                            }
                        })
                        .send()
                        .await
                };
                handle_observed_result(scenario.name, attempt, result)
            }
            ScenarioOperation::PutObjectAcl { key, acl } => {
                let request = client.put_object_acl().bucket(bucket).key(*key);
                let request = if let Some(canned_acl) = acl.canned_acl {
                    request.acl(match canned_acl {
                        "private" => ObjectCannedAcl::Private,
                        other => panic!("unsupported object acl {other}"),
                    })
                } else {
                    request
                };
                let grant_headers = acl_mutation_grant_headers(acl);
                let result = if grant_headers.is_empty() {
                    request.send().await
                } else {
                    request
                        .customize()
                        .mutate_request(move |req| {
                            for (header, value) in &grant_headers {
                                req.headers_mut().insert(*header, value.clone());
                            }
                        })
                        .send()
                        .await
                };
                handle_observed_result(scenario.name, attempt, result)
            }
            ScenarioOperation::CopyObjectCopySource { source_key } => handle_observed_result(
                scenario.name,
                attempt,
                client
                    .copy_object()
                    .bucket(bucket)
                    .key(format!("dst-{}", source_key.replace('/', "-")))
                    .copy_source(format!("{bucket}/{source_key}"))
                    .send()
                    .await,
            ),
            ScenarioOperation::CopyObjectMetadataDirective {
                source_key,
                metadata_directive,
            } => {
                let req = client
                    .copy_object()
                    .bucket(bucket)
                    .key(format!("dst-meta-{}", source_key.replace('/', "-")))
                    .copy_source(format!("{bucket}/{source_key}"));
                let result = match metadata_directive {
                    Some(value) => req.metadata_directive(value.clone()).send().await,
                    None => req.send().await,
                };
                handle_observed_result(scenario.name, attempt, result)
            }
        };

        match observed {
            Some(class) => return class,
            None if attempt + 1 < MAX_ATTEMPTS => {
                thread::sleep(Duration::from_millis(200));
            }
            None => panic!(
                "scenario {} failed with unexpected error after retries",
                scenario.name
            ),
        }
    }
    unreachable!()
}

async fn observe_scenario_until_expected(
    client: &Client,
    bucket: &str,
    scenario: &Scenario,
) -> RemoteOutcome {
    const MAX_ATTEMPTS: usize = 20;
    let mut last = None;
    for attempt in 0..MAX_ATTEMPTS {
        let observed = observe_scenario(client, bucket, scenario).await;
        if observed == scenario.remote_expected {
            return observed;
        }
        last = Some(observed);
        if attempt + 1 < MAX_ATTEMPTS {
            thread::sleep(Duration::from_millis(200));
        }
    }
    panic!(
        "scenario {} did not converge to expected classification {:?}; last observed {:?}",
        scenario.name, scenario.remote_expected, last
    );
}

fn handle_observed_result<T, E>(
    scenario_name: &str,
    attempt: usize,
    result: Result<T, E>,
) -> Option<RemoteOutcome>
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    match result {
        Ok(_) => Some(RemoteOutcome::Allow),
        Err(err)
            if err.code() == Some("AccessDenied")
                || err.code() == Some("NoSuchKey")
                || err.code() == Some("NoSuchTagSet") =>
        {
            Some(RemoteOutcome::Reject)
        }
        Err(err) => {
            eprintln!(
                "retrying scenario {scenario_name} after unexpected error on attempt {}: {err:?}",
                attempt + 1
            );
            None
        }
    }
}

async fn prepare_bucket_pair(env: &DiffEnv, bucket: &str) {
    for client in [&env.external_owner, &env.local_owner] {
        setup_tagged_object(client, bucket, "existing-public", "public").await;
        setup_tagged_object(client, bucket, "existing-private", "private").await;
        setup_plain_object(client, bucket, "tag-target").await;
        setup_plain_object(client, bucket, "acl-target").await;
        setup_copy_source_object(client, bucket, "src/public/foo").await;
        setup_copy_source_object(client, bucket, "src/private/foo").await;
        setup_copy_source_object(client, bucket, "src/meta/foo").await;
    }
}

const SCENARIO_CLEANUP_KEYS: &[&str] = &[
    "existing-public",
    "existing-private",
    "tag-target",
    "acl-target",
    "put-request-tag-allowed",
    "put-request-tag-denied",
    "put-acl-private-allowed",
    "put-acl-private-denied",
    "put-grant-read-allowed",
    "put-grant-read-denied",
    "put-sse-s3-allowed",
    "put-sse-s3-denied",
    "put-sse-c-allowed",
    "put-sse-c-denied",
    "put-acl-null-denied",
    "put-acl-null-allowed",
    "put-sse-null-denied",
    "put-sse-null-allowed",
    "src/public/foo",
    "src/private/foo",
    "src/meta/foo",
    "dst-src-public-foo",
    "dst-src-private-foo",
    "dst-meta-src-meta-foo",
    "dst-meta-src-public-foo",
    "dst-meta-src-private-foo",
];

fn run_selected_scenarios(selected_shapes: &[PolicyShape]) {
    s3_tests::run(async {
        let Some(env) = DiffEnv::setup().await else {
            return;
        };
        let external_principal = alt_root_principal(CTX.alt_account_id());
        let local_policy_principal = alt_root_principal(s3_tests::server::ALT_ACCOUNT_ID);
        let local_requester_principal = s3_tests::server::ALT_ACCOUNT_ID;

        let scenarios = build_scenarios()
            .into_iter()
            .filter(|scenario| selected_shapes.contains(&scenario.policy_shape))
            .collect::<Vec<_>>();
        for scenario_template in &scenarios {
            let bucket = env.create_bucket_pair().await;
            if scenario_template.policy_shape.requires_acl_capable_bucket() {
                set_object_writer_ownership(&env.external_owner, &bucket).await;
                set_object_writer_ownership(&env.local_owner, &bucket).await;
            }
            if matches!(
                scenario_template.policy_shape,
                PolicyShape::PutObjectSseCustomerAlgorithmAes256
            ) {
                enable_bucket_sse_c(&env.external_owner, &bucket).await;
                enable_bucket_sse_c(&env.local_owner, &bucket).await;
            }
            prepare_bucket_pair(&env, &bucket).await;
            let owner_canonical_id = canonical_owner_id(&env.external_owner, &bucket).await;
            let scenario = materialize_scenario(scenario_template, &bucket, &owner_canonical_id);
            let external_policy_json =
                scenario
                    .policy_shape
                    .render(&bucket, &external_principal, &owner_canonical_id);
            let local_policy_json =
                scenario
                    .policy_shape
                    .render(&bucket, &local_policy_principal, &owner_canonical_id);
            install_bucket_policy(&env.external_owner, &bucket, &external_policy_json).await;
            install_bucket_policy(&env.local_owner, &bucket, &local_policy_json).await;

            let policy = parse_bucket_policy(&local_policy_json).expect("policy parses");
            policy
                .validate_evaluable_object_conditions()
                .expect("policy remains in evaluable subset");
            let local_eval =
                scenario
                    .local_request
                    .evaluate(&policy, &bucket, local_requester_principal);

            let external =
                observe_scenario_until_expected(&env.external_alt, &bucket, &scenario).await;
            let local = observe_scenario_until_expected(&env.local_alt, &bucket, &scenario).await;

            assert_eq!(
                local_eval, scenario.local_expected,
                "local evaluator classification drifted for {}\npolicy={}\nrequest={:?}",
                scenario.name, local_policy_json, scenario.local_request
            );
            assert_eq!(
                external, scenario.remote_expected,
                "aws classification drifted for {}\npolicy={}\nrequest={:?}",
                scenario.name, external_policy_json, scenario.local_request
            );
            assert_eq!(
                local, external,
                "local server diverged from aws for {}\nexternal_policy={}\nlocal_policy={}\nrequest={:?}",
                scenario.name, external_policy_json, local_policy_json, scenario.local_request
            );

            env.cleanup_pair(&bucket, SCENARIO_CLEANUP_KEYS).await;
        }

        drop(env.local_server);
    });
}

#[test]
fn test_bucket_policy_tagging_conditions_match_aws() {
    run_selected_scenarios(&[
        PolicyShape::ExistingTagRead(ExistingTagReadAction::Object),
        PolicyShape::ExistingTagRead(ExistingTagReadAction::ObjectAcl),
        PolicyShape::GetObjectTaggingExistingPublic,
        PolicyShape::PutObjectTaggingRequestPublic,
        PolicyShape::PutObjectInlineTaggingRequestPublic,
    ]);
}

#[test]
fn test_bucket_policy_put_object_acl_and_grant_conditions_match_aws() {
    run_selected_scenarios(&[
        PolicyShape::PutObjectAclPrivate,
        PolicyShape::PutObjectGrantReadOwner,
        PolicyShape::PutObjectAclNullDeny,
        PolicyShape::PutBucketAclGrant(GrantHeaderCondition::Read),
        PolicyShape::PutBucketAclGrant(GrantHeaderCondition::FullControl),
        PolicyShape::PutObjectAclGrant(GrantHeaderCondition::Read),
        PolicyShape::PutObjectAclGrant(GrantHeaderCondition::FullControl),
    ]);
}

#[test]
fn test_bucket_policy_put_object_sse_conditions_match_aws() {
    run_selected_scenarios(&[
        PolicyShape::PutObjectSseAes256,
        PolicyShape::PutObjectSseCustomerAlgorithmAes256,
        PolicyShape::PutObjectSseS3NullDeny,
    ]);
}

#[test]
fn test_bucket_policy_copy_conditions_match_aws() {
    run_selected_scenarios(&[
        PolicyShape::CopyObjectCopySourcePublic,
        PolicyShape::CopyObjectMetadataDirectiveCopy,
        PolicyShape::CopyObjectCopySourcePublicAndMetadataDirectiveCopy,
    ]);
}
