use super::*;

pub(super) struct ObjectPolicyEvaluationContext<'a> {
    pub(super) requester: &'a Requester,
    pub(super) bucket_name: &'a str,
    pub(super) bucket_abac_enabled: bool,
    pub(super) action: auth::PolicyAction,
    pub(super) policy_context: PutObjectPolicyContext<'a>,
    pub(super) policy: &'a auth::BucketPolicy,
}

pub(super) struct ObjectPolicyRequestInput<'a> {
    pub(super) requester: &'a Requester,
    pub(super) bucket_name: &'a str,
    pub(super) bucket_abac_enabled: bool,
    pub(super) key: &'a str,
    pub(super) action: auth::PolicyAction,
    pub(super) policy_context: PutObjectPolicyContext<'a>,
    pub(super) policy: &'a auth::BucketPolicy,
    pub(super) existing_object_tags: auth::bucket_policy::ExistingObjectTags<'a>,
    pub(super) existing_object_tags_not_evaluable: bool,
    pub(super) bucket_tags: &'a [auth::PolicyTag<'a>],
    pub(super) request_object_tags: &'a [auth::PolicyTag<'a>],
    pub(super) version_id: Option<&'a str>,
}

struct BucketPolicyRequestInput<'a> {
    requester: &'a Requester,
    bucket_name: &'a str,
    bucket_abac_enabled: bool,
    action: auth::PolicyAction,
    policy: &'a auth::BucketPolicy,
    bucket_tags: auth::bucket_policy::BucketTags<'a>,
    request_tags: Option<&'a [auth::PolicyTag<'a>]>,
    policy_context: Option<PutObjectPolicyContext<'a>>,
    requested_max_keys: Option<&'a str>,
}

pub(super) fn bucket_policy_allow_survives_restrict_public_buckets(
    requester: &Requester,
    bucket: &BucketSummary,
) -> bool {
    if !bucket.bucket_policy_public
        || !Coordinator::restricts_public_buckets(bucket.public_access_block.as_ref())
    {
        return true;
    }

    Coordinator::requester_is_bucket_owner_account(requester, bucket)
}

pub(super) fn load_bucket_tags_for_policy_action(
    coord: &Coordinator,
    bucket: &BucketSummary,
    action: auth::PolicyAction,
    policy: &auth::BucketPolicy,
) -> Result<Option<Vec<(String, String)>>, ServerError> {
    if !(bucket.bucket_abac_enabled && policy.requires_bucket_tags_for_action(action)) {
        return Ok(None);
    }

    let tags = coord
        .storage_node()
        .get_bucket_subresource(&bucket.name, storage::BucketSubresourceKind::Tagging)
        .map_err(|error| match error {
            storage::BucketSnapshotLoadError::Store(
                storage::StoreError::MetadataCommandLogConflict { .. }
                | storage::StoreError::MetadataCommandLogGap { .. }
                | storage::StoreError::MetadataCommandPendingConflict { .. },
            ) => ServerError::OperationAborted,
            storage::BucketSnapshotLoadError::Store(error) => super::super::map_store_error(error),
            storage::BucketSnapshotLoadError::Metadata(ref error)
                if super::super::metadata_error_is_command_contention(error) =>
            {
                ServerError::OperationAborted
            }
            storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { name },
            ) => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            storage::BucketSnapshotLoadError::Metadata(other) => ServerError::Metadata(other),
        })?;
    match tags {
        Some(tags_xml) => Ok(Some(Coordinator::parse_serialized_tag_set(&tags_xml)?)),
        None => Ok(Some(Vec::new())),
    }
}

pub(super) fn policy_tags_from_pairs(tags: &[(String, String)]) -> Vec<auth::PolicyTag<'_>> {
    tags.iter()
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect()
}

fn bucket_tag_input_for_policy_action<'a>(
    bucket_abac_enabled: bool,
    policy: &auth::BucketPolicy,
    action: auth::PolicyAction,
    bucket_tags: &'a [auth::PolicyTag<'a>],
) -> auth::bucket_policy::BucketTags<'a> {
    if bucket_abac_enabled && policy.requires_bucket_tags_for_action(action) {
        auth::bucket_policy::BucketTags::Available(bucket_tags)
    } else {
        auth::bucket_policy::BucketTags::Unavailable
    }
}

pub(super) fn object_policy_request<'a>(
    input: ObjectPolicyRequestInput<'a>,
) -> Result<auth::PolicyRequest<'a>, ServerError> {
    let existing_object_tags_required = input
        .policy
        .requires_existing_object_tags_for_action(input.action);
    let existing_object_tags_unavailable = matches!(
        input.existing_object_tags,
        auth::bucket_policy::ExistingObjectTags::Unavailable
    );
    if existing_object_tags_required
        && existing_object_tags_unavailable
        && !input.existing_object_tags_not_evaluable
    {
        return Err(required_policy_input_unavailable_error(
            input.action,
            "existing object tags",
        ));
    }
    debug_assert!(
        !(existing_object_tags_required
            && existing_object_tags_unavailable
            && !input.existing_object_tags_not_evaluable),
        "bucket policy request for {:?} requires existing object tags",
        input.action
    );
    let source_ip_required = input.policy.requires_source_ip_for_action(input.action);
    if source_ip_required && input.requester.source_ip().is_none() {
        return Err(required_policy_input_unavailable_error(
            input.action,
            "source IP",
        ));
    }
    debug_assert!(
        !(source_ip_required && input.requester.source_ip().is_none()),
        "bucket policy request for {:?} requires source IP",
        input.action
    );
    let current_time_required = input.policy.requires_current_time_for_action(input.action);
    if current_time_required && input.requester.request_epoch_seconds().is_none() {
        return Err(required_policy_input_unavailable_error(
            input.action,
            "request time",
        ));
    }
    debug_assert!(
        !(current_time_required && input.requester.request_epoch_seconds().is_none()),
        "bucket policy request for {:?} requires request time",
        input.action
    );

    Ok(auth::PolicyRequest::for_object(
        input.action,
        input.bucket_name,
        input.key,
        input.requester.principal_opt(),
        input.requester.canonical_user_id(),
        input.existing_object_tags,
    )
    .with_source_ip(input.requester.source_ip())
    .with_current_time_epoch_seconds(input.requester.request_epoch_seconds())
    .with_bucket_tags(bucket_tag_input_for_policy_action(
        input.bucket_abac_enabled,
        input.policy,
        input.action,
        input.bucket_tags,
    ))
    .with_request_object_tags(input.request_object_tags)
    .with_copy_source(input.policy_context.copy_source)
    .with_metadata_directive(input.policy_context.metadata_directive)
    .with_canned_acl(input.policy_context.canned_acl)
    .with_server_side_encryption(
        input
            .policy_context
            .managed_encryption
            .map(ManagedEncryptionAlgorithm::as_str),
    )
    .with_sse_customer_algorithm(input.policy_context.sse_customer_algorithm)
    .with_grant_read(input.policy_context.grant_read)
    .with_grant_write(input.policy_context.grant_write)
    .with_grant_read_acp(input.policy_context.grant_read_acp)
    .with_grant_write_acp(input.policy_context.grant_write_acp)
    .with_grant_full_control(input.policy_context.grant_full_control)
    .with_if_match(input.policy_context.if_match)
    .with_if_none_match(input.policy_context.if_none_match)
    .with_object_creation_operation(input.policy_context.object_creation_operation)
    .with_version_id(input.version_id))
}

fn bucket_policy_request<'a>(
    input: BucketPolicyRequestInput<'a>,
) -> Result<auth::PolicyRequest<'a>, ServerError> {
    let bucket_tags_required =
        input.bucket_abac_enabled && input.policy.requires_bucket_tags_for_action(input.action);
    let bucket_tags_unavailable = matches!(
        input.bucket_tags,
        auth::bucket_policy::BucketTags::Unavailable
    );
    if bucket_tags_required && bucket_tags_unavailable {
        return Err(required_policy_input_unavailable_error(
            input.action,
            "bucket tags",
        ));
    }
    debug_assert!(
        !(bucket_tags_required && bucket_tags_unavailable),
        "bucket policy request for {:?} requires bucket tags",
        input.action
    );

    let request_tags_required = input
        .policy
        .requires_request_object_tags_for_action(input.action);
    if request_tags_required && input.request_tags.is_none() {
        return Err(required_policy_input_unavailable_error(
            input.action,
            "request tags",
        ));
    }
    debug_assert!(
        !(request_tags_required && input.request_tags.is_none()),
        "bucket policy request for {:?} requires request tags",
        input.action
    );
    let source_ip_required = input.policy.requires_source_ip_for_action(input.action);
    if source_ip_required && input.requester.source_ip().is_none() {
        return Err(required_policy_input_unavailable_error(
            input.action,
            "source IP",
        ));
    }
    debug_assert!(
        !(source_ip_required && input.requester.source_ip().is_none()),
        "bucket policy request for {:?} requires source IP",
        input.action
    );
    let current_time_required = input.policy.requires_current_time_for_action(input.action);
    if current_time_required && input.requester.request_epoch_seconds().is_none() {
        return Err(required_policy_input_unavailable_error(
            input.action,
            "request time",
        ));
    }
    debug_assert!(
        !(current_time_required && input.requester.request_epoch_seconds().is_none()),
        "bucket policy request for {:?} requires request time",
        input.action
    );

    let mut request = auth::PolicyRequest::for_bucket(
        input.action,
        input.bucket_name,
        input.requester.principal_opt(),
        input.requester.canonical_user_id(),
        input.bucket_tags,
    )
    .with_source_ip(input.requester.source_ip())
    .with_current_time_epoch_seconds(input.requester.request_epoch_seconds())
    .with_absent_request_headers()
    .with_absent_list_parameters();

    if let Some(request_tags) = input.request_tags {
        request = request.with_request_object_tags(request_tags);
    }

    if let Some(policy_context) = input.policy_context {
        request = request
            .with_canned_acl(policy_context.canned_acl)
            .with_grant_read(policy_context.grant_read)
            .with_grant_write(policy_context.grant_write)
            .with_grant_read_acp(policy_context.grant_read_acp)
            .with_grant_write_acp(policy_context.grant_write_acp)
            .with_grant_full_control(policy_context.grant_full_control)
            .with_prefix(policy_context.prefix)
            .with_delimiter(policy_context.delimiter)
            .with_max_keys(input.requested_max_keys)
            .with_object_ownership(policy_context.object_ownership);
    }

    Ok(request)
}

fn required_policy_input_unavailable_error(action: auth::PolicyAction, input: &str) -> ServerError {
    ServerError::InternalError {
        reason: format!(
            "bucket policy request builder missing required {input} input for {action:?}"
        ),
    }
}

pub(super) fn bucket_tags_for_policy_request(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    policy: &auth::BucketPolicy,
) -> Result<Vec<(String, String)>, ServerError> {
    if let Some(tags) = request.bucket_tags {
        return Ok(tags.to_vec());
    }

    Ok(
        load_bucket_tags_for_policy_action(coord, request.bucket, request.action, policy)?
            .unwrap_or_default(),
    )
}

pub(super) fn bucket_policy_decision_for_object(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    object: &StoredObject,
    existing_object_tags_mode: ExistingObjectTagsMode,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = request.policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };

    let bucket_tags = bucket_tags_for_policy_request(coord, request, policy)?;
    evaluate_bucket_policy_for_object_request(
        ObjectPolicyEvaluationContext {
            requester: request.requester,
            bucket_name: request.bucket.name.as_str(),
            bucket_abac_enabled: request.bucket.bucket_abac_enabled,
            action: request.action,
            policy_context: request.policy_context,
            policy,
        },
        object,
        existing_object_tags_mode,
        &bucket_tags,
    )
}

pub(super) fn evaluate_bucket_policy_for_object_request(
    context: ObjectPolicyEvaluationContext<'_>,
    object: &StoredObject,
    existing_object_tags_mode: ExistingObjectTagsMode,
    bucket_tags: &[(String, String)],
) -> Result<auth::PolicyEvaluation, ServerError> {
    let existing_tags_required = context
        .policy
        .requires_existing_object_tags_for_action(context.action);
    let existing_tags = if existing_tags_required
        && matches!(existing_object_tags_mode, ExistingObjectTagsMode::Available)
    {
        Some(Coordinator::parse_policy_existing_object_tags(object)?)
    } else {
        None
    };
    let existing_tags = existing_tags.unwrap_or_default();
    let existing_tags = policy_tags_from_pairs(&existing_tags);
    let request_object_tags = if context
        .policy
        .requires_request_object_tags_for_action(context.action)
    {
        match context.policy_context.request_object_tags_xml {
            Some(tags_xml) => Coordinator::parse_serialized_tag_set(tags_xml)?,
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let request_object_tags = policy_tags_from_pairs(&request_object_tags);
    let bucket_tags = policy_tags_from_pairs(bucket_tags);
    let version_id = version_id_policy_value(
        context.action,
        context.policy_context.version_id,
        Some(object.version_id()),
    );
    let existing_object_tags = if existing_tags_required
        && matches!(existing_object_tags_mode, ExistingObjectTagsMode::Available)
    {
        auth::bucket_policy::ExistingObjectTags::Available(&existing_tags)
    } else {
        auth::bucket_policy::ExistingObjectTags::Unavailable
    };
    let policy_request = object_policy_request(ObjectPolicyRequestInput {
        requester: context.requester,
        bucket_name: context.bucket_name,
        bucket_abac_enabled: context.bucket_abac_enabled,
        key: object.key().as_str(),
        action: context.action,
        policy_context: context.policy_context,
        policy: context.policy,
        existing_object_tags,
        existing_object_tags_not_evaluable: matches!(
            existing_object_tags_mode,
            ExistingObjectTagsMode::NotEvaluable
        ),
        bucket_tags: &bucket_tags,
        request_object_tags: &request_object_tags,
        version_id: version_id.as_deref(),
    })?;
    Ok(context.policy.evaluate(&policy_request))
}

pub(super) fn bucket_policy_decision_for_key(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    key: &str,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = request.policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };

    let bucket_tags = bucket_tags_for_policy_request(coord, request, policy)?;
    let bucket_tags = policy_tags_from_pairs(&bucket_tags);
    let version_id =
        version_id_policy_value(request.action, request.policy_context.version_id, None);
    let no_request_object_tags = [];
    let policy_request = object_policy_request(ObjectPolicyRequestInput {
        requester: request.requester,
        bucket_name: request.bucket.name.as_str(),
        bucket_abac_enabled: request.bucket.bucket_abac_enabled,
        key,
        action: request.action,
        policy_context: request.policy_context,
        policy,
        existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
        existing_object_tags_not_evaluable: true,
        bucket_tags: &bucket_tags,
        request_object_tags: &no_request_object_tags,
        version_id: version_id.as_deref(),
    })?;
    Ok(policy.evaluate(&policy_request))
}

pub(super) fn bucket_policy_decision_for_bucket_loaded_with_tags(
    coord: &Coordinator,
    requester: &Requester,
    bucket: &BucketSummary,
    bucket_tags: Option<&[(String, String)]>,
    action: auth::PolicyAction,
    policy: Option<&auth::BucketPolicy>,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };

    let bucket_tags_available =
        bucket.bucket_abac_enabled && policy.requires_bucket_tags_for_action(action);
    let bucket_tags = if let Some(bucket_tags) = bucket_tags {
        Some(bucket_tags.to_vec())
    } else if bucket_tags_available {
        load_bucket_tags_for_policy_action(coord, bucket, action, policy)?
    } else {
        None
    };
    let bucket_tags: Vec<auth::PolicyTag<'_>> = bucket_tags
        .iter()
        .flat_map(|tags| tags.iter())
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let request = bucket_policy_request(BucketPolicyRequestInput {
        requester,
        bucket_name: bucket.name.as_str(),
        bucket_abac_enabled: bucket.bucket_abac_enabled,
        action,
        policy,
        bucket_tags: if bucket_tags_available {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
        request_tags: None,
        policy_context: None,
        requested_max_keys: None,
    })?;
    Ok(policy.evaluate(&request))
}

pub(super) fn bucket_policy_decision_for_loaded_handle(
    coord: &Coordinator,
    requester: &Requester,
    bucket: &LoadedBucketHandle,
    action: auth::PolicyAction,
    policy: Option<&auth::BucketPolicy>,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let bucket_tags = Coordinator::loaded_bucket_tags_for_policy(bucket)?;
    bucket_policy_decision_for_bucket_loaded_with_tags(
        coord,
        requester,
        bucket.bucket(),
        bucket_tags.as_deref(),
        action,
        policy,
    )
}

pub(super) fn bucket_policy_decision_for_loaded_handle_with_context(
    _coord: &Coordinator,
    requester: &Requester,
    bucket: &LoadedBucketHandle,
    action: auth::PolicyAction,
    policy_context: PutObjectPolicyContext<'_>,
    policy: Option<&auth::BucketPolicy>,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };

    let bucket_tags = Coordinator::loaded_bucket_tags_for_policy(bucket)?;
    let bucket_tags_available = bucket_tags.is_some();
    let bucket_tags: Vec<auth::PolicyTag<'_>> = bucket_tags
        .iter()
        .flat_map(|tags| tags.iter())
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let request_tags: Vec<auth::PolicyTag<'_>> = policy_context
        .request_tags
        .unwrap_or(&[])
        .iter()
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let requested_max_keys = policy_context
        .requested_max_keys
        .map(|max_keys| max_keys.to_string());

    let request = bucket_policy_request(BucketPolicyRequestInput {
        requester,
        bucket_name: bucket.bucket().name.as_str(),
        bucket_abac_enabled: bucket.bucket().bucket_abac_enabled,
        action,
        policy,
        bucket_tags: if bucket_tags_available {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
        request_tags: Some(&request_tags),
        policy_context: Some(policy_context),
        requested_max_keys: requested_max_keys.as_deref(),
    })?;
    Ok(policy.evaluate(&request))
}

pub(super) fn bucket_policy_decision_for_put_object_action(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    key: &str,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = request.policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };

    let request_object_tags = if policy.requires_request_object_tags_for_action(request.action) {
        match request.policy_context.request_object_tags_xml {
            Some(tags_xml) => Coordinator::parse_serialized_tag_set(tags_xml)?,
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let request_object_tags = policy_tags_from_pairs(&request_object_tags);
    let bucket_tags = bucket_tags_for_policy_request(coord, request, policy)?;
    let bucket_tags = policy_tags_from_pairs(&bucket_tags);
    let version_id =
        version_id_policy_value(request.action, request.policy_context.version_id, None);
    let policy_request = object_policy_request(ObjectPolicyRequestInput {
        requester: request.requester,
        bucket_name: request.bucket.name.as_str(),
        bucket_abac_enabled: request.bucket.bucket_abac_enabled,
        key,
        action: request.action,
        policy_context: request.policy_context,
        policy,
        existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
        existing_object_tags_not_evaluable: true,
        bucket_tags: &bucket_tags,
        request_object_tags: &request_object_tags,
        version_id: version_id.as_deref(),
    })?;
    Ok(policy.evaluate(&policy_request))
}

fn version_id_policy_value(
    action: auth::PolicyAction,
    context_version_id: Option<VersionId>,
    object_version_id: Option<VersionId>,
) -> Option<String> {
    let version_id = context_version_id.or_else(|| {
        version_id_condition_applies_to_action(action)
            .then_some(object_version_id)
            .flatten()
    })?;
    Some(version_id.to_string())
}

fn version_id_condition_applies_to_action(action: auth::PolicyAction) -> bool {
    matches!(
        action,
        auth::PolicyAction::GetObjectVersion
            | auth::PolicyAction::GetObjectVersionAttributes
            | auth::PolicyAction::GetObjectVersionAcl
            | auth::PolicyAction::GetObjectVersionTagging
            | auth::PolicyAction::PutObjectVersionAcl
            | auth::PolicyAction::PutObjectVersionTagging
            | auth::PolicyAction::DeleteObjectVersion
            | auth::PolicyAction::DeleteObjectVersionTagging
    )
}

pub(super) fn requester_can_put_object_action_with_bucket_policy(
    coord: &Coordinator,
    authorization: BucketPolicyActionAuthorization<'_>,
    key: &str,
) -> Result<bool, ServerError> {
    let decision = bucket_policy_decision_for_put_object_action(coord, authorization.request, key)?;
    Ok(bucket_policy_allows_with_fallback(
        authorization.request.requester,
        authorization.request.bucket,
        decision,
        || authorization.default_allowed,
    ))
}

pub(super) fn bucket_policy_allows_with_fallback<F>(
    requester: &Requester,
    bucket: &BucketSummary,
    decision: auth::PolicyEvaluation,
    fallback: F,
) -> bool
where
    F: FnOnce() -> bool,
{
    match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => fallback(),
    }
}

pub(super) fn bucket_policy_allows_with_root_principal_bypass<F>(
    requester: &Requester,
    bucket: &BucketSummary,
    decision: auth::PolicyEvaluation,
    fallback: F,
) -> bool
where
    F: FnOnce() -> bool,
{
    match decision {
        auth::PolicyEvaluation::ExplicitDeny
            if Coordinator::requester_is_bucket_owner_account_root_principal(requester, bucket) =>
        {
            fallback()
        }
        _ => bucket_policy_allows_with_fallback(requester, bucket, decision, fallback),
    }
}

pub(super) fn requester_can_object_action_with_bucket_policy<F>(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    object: &StoredObject,
    fallback: F,
) -> Result<bool, ServerError>
where
    F: FnOnce() -> bool,
{
    let decision = filter_bucket_policy_allow_for_foreign_owned_object_action(
        request.bucket,
        object,
        request.action,
        bucket_policy_decision_for_object(
            coord,
            request,
            object,
            ExistingObjectTagsMode::Available,
        )?,
    );
    Ok(bucket_policy_allows_with_fallback(
        request.requester,
        request.bucket,
        decision,
        fallback,
    ))
}

pub(super) fn requester_can_object_action_with_unavailable_existing_tags_with_bucket_policy<F>(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    object: &StoredObject,
    fallback: F,
) -> Result<bool, ServerError>
where
    F: FnOnce() -> bool,
{
    let decision = filter_bucket_policy_allow_for_foreign_owned_object_action(
        request.bucket,
        object,
        request.action,
        bucket_policy_decision_for_object(
            coord,
            request,
            object,
            ExistingObjectTagsMode::Unavailable,
        )?,
    );
    Ok(bucket_policy_allows_with_fallback(
        request.requester,
        request.bucket,
        decision,
        fallback,
    ))
}

pub(super) fn requester_can_read_family_object_action_with_bucket_policy<F>(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    object: &StoredObject,
    existing_object_tags_mode: ExistingObjectTagsMode,
    fallback: F,
) -> Result<bool, ServerError>
where
    F: FnOnce() -> bool,
{
    let decision = filter_bucket_policy_allow_for_foreign_owned_object_action(
        request.bucket,
        object,
        request.action,
        bucket_policy_decision_for_object(coord, request, object, existing_object_tags_mode)?,
    );
    Ok(bucket_policy_allows_with_fallback(
        request.requester,
        request.bucket,
        decision,
        fallback,
    ))
}

/// Downgrade bucket-policy allows that AWS does not honor on objects owned
/// by another account (in non-BucketOwnerEnforced buckets): object reads and
/// both ACL directions. Tagging operations remain grantable. The ACL-write
/// entries were probed against AWS on 2026-07-07, when AWS stopped honoring
/// bucket-policy PutObjectAcl grants on foreign-owned objects ("no
/// resource-based policy allows the s3:PutObjectAcl action"), closing the
/// earlier anomaly where ACL reads were denied but ACL writes grantable.
pub(super) fn filter_bucket_policy_allow_for_foreign_owned_object_action(
    bucket: &BucketSummary,
    object: &StoredObject,
    action: auth::PolicyAction,
    decision: auth::PolicyEvaluation,
) -> auth::PolicyEvaluation {
    if decision != auth::PolicyEvaluation::ExplicitAllow {
        return decision;
    }

    if Coordinator::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
        return decision;
    }

    let owner_scoped = matches!(
        action,
        auth::PolicyAction::GetObject
            | auth::PolicyAction::GetObjectVersion
            | auth::PolicyAction::GetObjectAttributes
            | auth::PolicyAction::GetObjectVersionAttributes
            | auth::PolicyAction::GetObjectAcl
            | auth::PolicyAction::GetObjectVersionAcl
            | auth::PolicyAction::PutObjectAcl
            | auth::PolicyAction::PutObjectVersionAcl
    );
    if !owner_scoped {
        return decision;
    }

    if object_is_owned_by_bucket_owner_account(bucket, object) {
        return decision;
    }

    auth::PolicyEvaluation::NoMatch
}

pub(super) fn object_is_owned_by_bucket_owner_account(
    bucket: &BucketSummary,
    object: &StoredObject,
) -> bool {
    if object.owner().principal == bucket.owner_principal
        || object.owner().canonical_id == bucket.owner_canonical_id
    {
        return true;
    }

    let Some(object_owner_account_id) =
        aws_account_id_from_principal(object.owner().principal.as_str())
    else {
        return false;
    };
    let Some(bucket_owner_account_id) = aws_account_id_from_principal(&bucket.owner_principal)
    else {
        return false;
    };

    object_owner_account_id == bucket_owner_account_id
}

pub(super) fn requester_can_missing_object_action_with_bucket_policy<F>(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    key: &str,
    fallback: F,
) -> Result<bool, ServerError>
where
    F: FnOnce() -> bool,
{
    let decision = object_policy_decision(coord, request, ObjectPolicyTarget::MissingKey(key))?;
    Ok(bucket_policy_allows_with_fallback(
        request.requester,
        request.bucket,
        decision,
        fallback,
    ))
}

pub(super) fn object_policy_decision(
    coord: &Coordinator,
    request: BucketPolicyRequestContext<'_>,
    target: ObjectPolicyTarget<'_>,
) -> Result<auth::PolicyEvaluation, ServerError> {
    match target {
        ObjectPolicyTarget::Existing(object) => bucket_policy_decision_for_object(
            coord,
            request,
            object,
            ExistingObjectTagsMode::Available,
        ),
        ObjectPolicyTarget::MissingKey(key) => bucket_policy_decision_for_key(coord, request, key),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requester() -> Requester {
        Requester::authenticated(s3_types::AccountIdentity::from_principal(
            "arn:aws:iam::111122223333:user/requester",
        ))
    }

    fn parse_policy(body: &str) -> auth::BucketPolicy {
        auth::parse_bucket_policy(body).unwrap()
    }

    fn assert_required_input_error(error: ServerError, input: &str, action: auth::PolicyAction) {
        match error {
            ServerError::InternalError { reason } => {
                assert!(
                    reason.contains(input),
                    "reason {reason:?} should mention {input}"
                );
                assert!(
                    reason.contains(&format!("{action:?}")),
                    "reason {reason:?} should mention {action:?}"
                );
            }
            other => panic!("expected InternalError, got {other:?}"),
        }
    }

    #[test]
    fn object_policy_request_rejects_missing_required_existing_object_tags() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"private"}}}]}"#,
        );

        let error = object_policy_request(ObjectPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            key: "key",
            action: auth::PolicyAction::GetObject,
            policy_context: PutObjectPolicyContext::default(),
            policy: &policy,
            existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
            existing_object_tags_not_evaluable: false,
            bucket_tags: &[],
            request_object_tags: &[],
            version_id: None,
        })
        .unwrap_err();

        assert_required_input_error(error, "existing object tags", auth::PolicyAction::GetObject);
    }

    #[test]
    fn object_policy_request_allows_not_evaluable_required_existing_object_tags() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"private"}}}]}"#,
        );

        let request = object_policy_request(ObjectPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            key: "key",
            action: auth::PolicyAction::GetObject,
            policy_context: PutObjectPolicyContext::default(),
            policy: &policy,
            existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
            existing_object_tags_not_evaluable: true,
            bucket_tags: &[],
            request_object_tags: &[],
            version_id: None,
        })
        .unwrap();

        assert_eq!(policy.evaluate(&request), auth::PolicyEvaluation::NoMatch);
    }

    #[test]
    fn object_policy_request_allows_accepted_but_not_evaluable_existing_object_tags() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObjectAttributes","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"private"}}}]}"#,
        );

        let request = object_policy_request(ObjectPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            key: "key",
            action: auth::PolicyAction::GetObjectAttributes,
            policy_context: PutObjectPolicyContext::default(),
            policy: &policy,
            existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
            existing_object_tags_not_evaluable: false,
            bucket_tags: &[],
            request_object_tags: &[],
            version_id: None,
        })
        .unwrap();

        assert_eq!(policy.evaluate(&request), auth::PolicyEvaluation::NoMatch);
    }

    #[test]
    fn object_policy_request_rejects_missing_required_source_ip() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}}]}"#,
        );

        let error = object_policy_request(ObjectPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            key: "key",
            action: auth::PolicyAction::GetObject,
            policy_context: PutObjectPolicyContext::default(),
            policy: &policy,
            existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
            existing_object_tags_not_evaluable: false,
            bucket_tags: &[],
            request_object_tags: &[],
            version_id: None,
        })
        .unwrap_err();

        assert_required_input_error(error, "source IP", auth::PolicyAction::GetObject);
    }

    #[test]
    fn object_policy_request_propagates_source_ip() {
        let requester = requester().with_source_ip(Some("127.0.0.1".parse().unwrap()));
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}}]}"#,
        );

        let request = object_policy_request(ObjectPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            key: "key",
            action: auth::PolicyAction::GetObject,
            policy_context: PutObjectPolicyContext::default(),
            policy: &policy,
            existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
            existing_object_tags_not_evaluable: false,
            bucket_tags: &[],
            request_object_tags: &[],
            version_id: None,
        })
        .unwrap();

        assert_eq!(
            policy.evaluate(&request),
            auth::PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn object_policy_request_rejects_missing_required_current_time() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"DateEquals":{"aws:CurrentTime":"2024-01-01T00:00:00Z"}}}]}"#,
        );

        let error = object_policy_request(ObjectPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            key: "key",
            action: auth::PolicyAction::GetObject,
            policy_context: PutObjectPolicyContext::default(),
            policy: &policy,
            existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
            existing_object_tags_not_evaluable: false,
            bucket_tags: &[],
            request_object_tags: &[],
            version_id: None,
        })
        .unwrap_err();

        assert_required_input_error(error, "request time", auth::PolicyAction::GetObject);
    }

    #[test]
    fn object_policy_request_propagates_current_time() {
        let requester = requester().with_request_epoch_seconds(Some(1_704_067_200));
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"DateEquals":{"aws:CurrentTime":"2024-01-01T00:00:00Z"}}}]}"#,
        );

        let request = object_policy_request(ObjectPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            key: "key",
            action: auth::PolicyAction::GetObject,
            policy_context: PutObjectPolicyContext::default(),
            policy: &policy,
            existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
            existing_object_tags_not_evaluable: false,
            bucket_tags: &[],
            request_object_tags: &[],
            version_id: None,
        })
        .unwrap();

        assert_eq!(
            policy.evaluate(&request),
            auth::PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn bucket_policy_request_rejects_missing_required_bucket_tags_when_abac_enabled() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"private"}}}]}"#,
        );

        let error = bucket_policy_request(BucketPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: true,
            action: auth::PolicyAction::GetBucketTagging,
            policy: &policy,
            bucket_tags: auth::bucket_policy::BucketTags::Unavailable,
            request_tags: None,
            policy_context: None,
            requested_max_keys: None,
        })
        .unwrap_err();

        assert_required_input_error(error, "bucket tags", auth::PolicyAction::GetBucketTagging);
    }

    #[test]
    fn bucket_policy_request_allows_unavailable_bucket_tags_when_abac_disabled() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"private"}}}]}"#,
        );

        let request = bucket_policy_request(BucketPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            action: auth::PolicyAction::GetBucketTagging,
            policy: &policy,
            bucket_tags: auth::bucket_policy::BucketTags::Unavailable,
            request_tags: None,
            policy_context: None,
            requested_max_keys: None,
        })
        .unwrap();

        assert_eq!(policy.evaluate(&request), auth::PolicyEvaluation::NoMatch);
    }

    #[test]
    fn bucket_policy_request_rejects_missing_required_request_tags() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:TagResource","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"aws:RequestTag/security":"private"}}}]}"#,
        );

        let error = bucket_policy_request(BucketPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            action: auth::PolicyAction::TagResource,
            policy: &policy,
            bucket_tags: auth::bucket_policy::BucketTags::Unavailable,
            request_tags: None,
            policy_context: None,
            requested_max_keys: None,
        })
        .unwrap_err();

        assert_required_input_error(error, "request tags", auth::PolicyAction::TagResource);
    }

    #[test]
    fn bucket_policy_request_rejects_missing_required_source_ip() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}}]}"#,
        );

        let error = bucket_policy_request(BucketPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            action: auth::PolicyAction::ListBucket,
            policy: &policy,
            bucket_tags: auth::bucket_policy::BucketTags::Unavailable,
            request_tags: None,
            policy_context: None,
            requested_max_keys: None,
        })
        .unwrap_err();

        assert_required_input_error(error, "source IP", auth::PolicyAction::ListBucket);
    }

    #[test]
    fn bucket_policy_request_propagates_source_ip() {
        let requester = requester().with_source_ip(Some("127.0.0.1".parse().unwrap()));
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}}]}"#,
        );

        let request = bucket_policy_request(BucketPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            action: auth::PolicyAction::ListBucket,
            policy: &policy,
            bucket_tags: auth::bucket_policy::BucketTags::Unavailable,
            request_tags: None,
            policy_context: None,
            requested_max_keys: None,
        })
        .unwrap();

        assert_eq!(
            policy.evaluate(&request),
            auth::PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn bucket_policy_request_rejects_missing_required_current_time() {
        let requester = requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"DateEquals":{"aws:CurrentTime":"2024-01-01T00:00:00Z"}}}]}"#,
        );

        let error = bucket_policy_request(BucketPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            action: auth::PolicyAction::ListBucket,
            policy: &policy,
            bucket_tags: auth::bucket_policy::BucketTags::Unavailable,
            request_tags: None,
            policy_context: None,
            requested_max_keys: None,
        })
        .unwrap_err();

        assert_required_input_error(error, "request time", auth::PolicyAction::ListBucket);
    }

    #[test]
    fn bucket_policy_request_propagates_current_time() {
        let requester = requester().with_request_epoch_seconds(Some(1_704_067_200));
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"DateEquals":{"aws:CurrentTime":"2024-01-01T00:00:00Z"}}}]}"#,
        );

        let request = bucket_policy_request(BucketPolicyRequestInput {
            requester: &requester,
            bucket_name: "bucket",
            bucket_abac_enabled: false,
            action: auth::PolicyAction::ListBucket,
            policy: &policy,
            bucket_tags: auth::bucket_policy::BucketTags::Unavailable,
            request_tags: None,
            policy_context: None,
            requested_max_keys: None,
        })
        .unwrap();

        assert_eq!(
            policy.evaluate(&request),
            auth::PolicyEvaluation::ExplicitAllow
        );
    }
}
