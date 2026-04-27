use super::*;
use std::ops::Deref;

#[derive(Clone, Copy)]
pub(in crate::coordinator) struct BoeBucketSummary<'a>(&'a ModernBucketSummary);

impl<'a> BoeBucketSummary<'a> {
    pub(in crate::coordinator) fn new(bucket: &'a ModernBucketSummary) -> Option<Self> {
        if Coordinator::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            Some(Self(bucket))
        } else {
            None
        }
    }
}

impl Deref for BoeBucketSummary<'_> {
    type Target = ModernBucketSummary;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::coordinator) enum PreloadedBucketTags<'a> {
    Available(&'a [(String, String)]),
    Unavailable,
}

impl<'a> PreloadedBucketTags<'a> {
    pub(in crate::coordinator) fn new(tags: Option<&'a [(String, String)]>) -> Self {
        match tags {
            Some(tags) => Self::Available(tags),
            None => Self::Unavailable,
        }
    }

    fn for_policy_action(
        self,
        bucket: BoeBucketSummary<'_>,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<Option<&'a [(String, String)]>, ServerError> {
        let Some(policy) = policy else {
            return Ok(None);
        };
        if !(bucket.bucket_abac_enabled && policy.requires_bucket_tags_for_action(action)) {
            return Ok(None);
        }
        match self {
            Self::Available(tags) => Ok(Some(tags)),
            Self::Unavailable => Err(ServerError::InternalError {
                // This is a coordinator wiring bug, not an AWS-facing semantic branch.
                // Any BOE modern-auth path that evaluates a bucket-tag-conditioned policy
                // must have loaded the bucket tags before reaching the evaluator.
                reason: format!(
                    "BOE modern auth requires preloaded bucket tags for {action:?} when bucket ABAC is enabled"
                ),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernObjectReadAuthorization {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernObjectWriteAuthorization {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernWriteAction {
    PutObject,
    CreateMultipartUpload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernReadAction {
    ReadCurrent,
    ReadVersion,
    AttributesCurrent,
    AttributesVersion,
}

impl ModernReadAction {
    pub(in crate::coordinator) fn from_get_object_version(version_id: Option<VersionId>) -> Self {
        match version_id {
            Some(_) => Self::ReadVersion,
            None => Self::ReadCurrent,
        }
    }

    pub(in crate::coordinator) fn from_get_object_attributes_version(
        version_id: Option<VersionId>,
    ) -> Self {
        match version_id {
            Some(_) => Self::AttributesVersion,
            None => Self::AttributesCurrent,
        }
    }

    pub(in crate::coordinator) fn policy_action(self) -> auth::PolicyAction {
        match self {
            Self::ReadCurrent => auth::PolicyAction::GetObject,
            Self::ReadVersion => auth::PolicyAction::GetObjectVersion,
            Self::AttributesCurrent => auth::PolicyAction::GetObjectAttributes,
            Self::AttributesVersion => auth::PolicyAction::GetObjectVersionAttributes,
        }
    }
}

fn requester_is_modern_bucket_owner_account(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
) -> bool {
    let Some(account) = requester.account() else {
        return false;
    };
    if account.principal() == bucket.owner_principal {
        return true;
    }

    let Some(requester_account_id) = aws_account_id_from_principal(account.principal()) else {
        return false;
    };
    Coordinator::bucket_owner_account_id(&bucket.owner_principal) == Some(requester_account_id)
}

fn requester_can_modern_bucket_owner_account_admin(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
) -> bool {
    Coordinator::requester_can_bucket_admin(requester, &bucket.owner_principal)
        || (requester.authorization_profile() == auth::AuthorizationProfile::OwnerAccountAdmin
            && requester_is_modern_bucket_owner_account(requester, bucket))
}

fn modern_bucket_policy_allow_survives_restrict_public_buckets(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
) -> bool {
    if !bucket.bucket_policy_public
        || !Coordinator::restricts_public_buckets(bucket.public_access_block.as_ref())
    {
        return true;
    }

    requester_is_modern_bucket_owner_account(requester, bucket)
}

fn bucket_policy_decision_for_put_object_action_modern(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    action: auth::PolicyAction,
    policy_context: &PutObjectPolicyContext<'_>,
    policy: Option<&auth::BucketPolicy>,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };

    let request_object_tags = if policy.requires_request_object_tags_for_action(action) {
        match policy_context.request_object_tags_xml {
            Some(tags_xml) => Coordinator::parse_serialized_tag_set(tags_xml)?,
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let request_object_tags: Vec<auth::PolicyTag<'_>> = request_object_tags
        .iter()
        .map(|(tag_key, value)| auth::PolicyTag::new(tag_key, value))
        .collect();
    let bucket_tags = bucket_tags.for_policy_action(bucket, action, Some(policy))?;
    let bucket_tags: Vec<auth::PolicyTag<'_>> = bucket_tags
        .into_iter()
        .flat_map(|tags| tags.iter())
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let policy_request = auth::PolicyRequest::for_object(
        action,
        bucket.name.as_str(),
        key,
        requester.principal_opt(),
        requester.canonical_user_id(),
        auth::bucket_policy::ExistingObjectTags::Unavailable,
    )
    .with_bucket_tags(
        if bucket.bucket_abac_enabled && policy.requires_bucket_tags_for_action(action) {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
    )
    .with_request_object_tags(&request_object_tags)
    .with_copy_source(policy_context.copy_source)
    .with_metadata_directive(policy_context.metadata_directive)
    .with_canned_acl(policy_context.canned_acl)
    .with_server_side_encryption(
        policy_context
            .managed_encryption
            .map(ManagedEncryptionAlgorithm::as_str),
    )
    .with_sse_customer_algorithm(policy_context.sse_customer_algorithm)
    .with_grant_read(policy_context.grant_read)
    .with_grant_write(policy_context.grant_write)
    .with_grant_read_acp(policy_context.grant_read_acp)
    .with_grant_write_acp(policy_context.grant_write_acp)
    .with_grant_full_control(policy_context.grant_full_control);
    Ok(policy.evaluate(&policy_request))
}

pub(super) fn write_multipart_upload_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    upload: &MultipartUploadRecord,
    policy_context: &PutObjectPolicyContext<'_>,
    policy: Option<&auth::BucketPolicy>,
) -> Result<ModernObjectWriteAuthorization, ServerError> {
    let decision = bucket_policy_decision_for_put_object_action_modern(
        requester,
        bucket,
        bucket_tags,
        upload.key.as_str(),
        auth::PolicyAction::PutObject,
        policy_context,
        policy,
    )?;
    let allowed = match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    };
    Ok(if allowed {
        ModernObjectWriteAuthorization::Allowed
    } else {
        ModernObjectWriteAuthorization::Denied
    })
}

pub(super) fn put_object_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    action: ModernWriteAction,
    policy_context: &PutObjectPolicyContext<'_>,
    policy: Option<&auth::BucketPolicy>,
) -> Result<ModernObjectWriteAuthorization, ServerError> {
    if action == ModernWriteAction::CreateMultipartUpload && requester.is_anonymous() {
        return Ok(ModernObjectWriteAuthorization::Denied);
    }
    let decision = bucket_policy_decision_for_put_object_action_modern(
        requester,
        bucket,
        bucket_tags,
        key,
        auth::PolicyAction::PutObject,
        policy_context,
        policy,
    )?;
    let allowed = match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    };
    if !allowed {
        return Ok(ModernObjectWriteAuthorization::Denied);
    }

    if policy_context.request_object_tags_xml.is_some() {
        let tagging_decision = bucket_policy_decision_for_put_object_action_modern(
            requester,
            bucket,
            bucket_tags,
            key,
            auth::PolicyAction::PutObjectTagging,
            policy_context,
            policy,
        )?;
        let tagging_allowed = match tagging_decision {
            auth::PolicyEvaluation::ExplicitDeny => false,
            auth::PolicyEvaluation::ExplicitAllow
                if modern_bucket_policy_allow_survives_restrict_public_buckets(
                    requester, bucket,
                ) =>
            {
                true
            }
            auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                requester_can_modern_bucket_owner_account_admin(requester, bucket)
            }
        };
        if !tagging_allowed {
            return Ok(ModernObjectWriteAuthorization::Denied);
        }
    }

    Ok(ModernObjectWriteAuthorization::Allowed)
}

pub(super) fn delete_object_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    object: Option<&StoredObject>,
    action: auth::PolicyAction,
    policy: Option<&auth::BucketPolicy>,
) -> Result<bool, ServerError> {
    let decision = match object {
        Some(object) => bucket_policy_decision_for_object_with_preloaded_tags_modern(
            requester,
            bucket,
            bucket_tags,
            object,
            action,
            policy,
            ExistingObjectTagsMode::Available,
        )?,
        None => bucket_policy_decision_for_put_object_action_modern(
            requester,
            bucket,
            bucket_tags,
            key,
            action,
            &PutObjectPolicyContext::default(),
            policy,
        )?,
    };

    Ok(match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    })
}

fn bucket_policy_decision_for_object_with_preloaded_tags_modern(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    object: &StoredObject,
    action: auth::PolicyAction,
    policy: Option<&auth::BucketPolicy>,
    existing_object_tags_mode: ExistingObjectTagsMode,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };
    let bucket_tags = bucket_tags.for_policy_action(bucket, action, Some(policy))?;

    Coordinator::evaluate_bucket_policy_for_object_request(
        super::policy::ObjectPolicyEvaluationContext {
            requester,
            bucket_name: bucket.name.as_str(),
            bucket_abac_enabled: bucket.bucket_abac_enabled,
            action,
            policy_context: PutObjectPolicyContext::default(),
            policy,
        },
        object,
        existing_object_tags_mode,
        bucket_tags.unwrap_or(&[]),
    )
}

fn modern_read_object_default_allowed(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    object: &StoredObject,
    action: ModernReadAction,
) -> bool {
    match action {
        ModernReadAction::ReadCurrent | ModernReadAction::ReadVersion => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
        ModernReadAction::AttributesCurrent | ModernReadAction::AttributesVersion => requester
            .principal_opt()
            .is_some_and(|principal| principal == object.owner().principal.as_str()),
    }
}

fn modern_read_object_authorization_for_single_action(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    object: &StoredObject,
    action: ModernReadAction,
    policy: Option<&auth::BucketPolicy>,
    existing_object_tags_mode: ExistingObjectTagsMode,
) -> Result<ModernObjectReadAuthorization, ServerError> {
    let decision = bucket_policy_decision_for_object_with_preloaded_tags_modern(
        requester,
        bucket,
        bucket_tags,
        object,
        action.policy_action(),
        policy,
        existing_object_tags_mode,
    )?;
    let modern_default_allowed =
        modern_read_object_default_allowed(requester, bucket, object, action);

    let outcome = match decision {
        auth::PolicyEvaluation::ExplicitDeny => ModernObjectReadAuthorization::Denied,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            ModernObjectReadAuthorization::Allowed
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            if modern_default_allowed {
                ModernObjectReadAuthorization::Allowed
            } else {
                ModernObjectReadAuthorization::Denied
            }
        }
    };
    Ok(outcome)
}

fn combine_modern_read_authorization(
    first: ModernObjectReadAuthorization,
    second: ModernObjectReadAuthorization,
) -> ModernObjectReadAuthorization {
    match (first, second) {
        (ModernObjectReadAuthorization::Denied, _) | (_, ModernObjectReadAuthorization::Denied) => {
            ModernObjectReadAuthorization::Denied
        }
        (ModernObjectReadAuthorization::Allowed, ModernObjectReadAuthorization::Allowed) => {
            ModernObjectReadAuthorization::Allowed
        }
    }
}

pub(super) fn read_object_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    object: &StoredObject,
    action: ModernReadAction,
    policy: Option<&auth::BucketPolicy>,
) -> Result<ModernObjectReadAuthorization, ServerError> {
    match action {
        ModernReadAction::AttributesCurrent | ModernReadAction::AttributesVersion => {
            let read_action = match action {
                ModernReadAction::AttributesVersion => ModernReadAction::ReadVersion,
                ModernReadAction::AttributesCurrent => ModernReadAction::ReadCurrent,
                _ => unreachable!(),
            };
            let read = modern_read_object_authorization_for_single_action(
                requester,
                bucket,
                bucket_tags,
                object,
                read_action,
                policy,
                ExistingObjectTagsMode::Available,
            )?;
            let attrs = modern_read_object_authorization_for_single_action(
                requester,
                bucket,
                bucket_tags,
                object,
                action,
                policy,
                ExistingObjectTagsMode::Unavailable,
            )?;
            Ok(combine_modern_read_authorization(read, attrs))
        }
        _ => modern_read_object_authorization_for_single_action(
            requester,
            bucket,
            bucket_tags,
            object,
            action,
            policy,
            ExistingObjectTagsMode::Available,
        ),
    }
}
