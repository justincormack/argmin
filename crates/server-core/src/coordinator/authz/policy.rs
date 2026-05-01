use super::*;

pub(super) struct ObjectPolicyEvaluationContext<'a> {
    pub(super) requester: &'a Requester,
    pub(super) bucket_name: &'a str,
    pub(super) bucket_abac_enabled: bool,
    pub(super) action: auth::PolicyAction,
    pub(super) policy_context: PutObjectPolicyContext<'a>,
    pub(super) policy: &'a auth::BucketPolicy,
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
        .storage_node
        .get_bucket_subresource(&bucket.name, storage::BucketSubresourceKind::Tagging)
        .map_err(|error| match error {
            storage::BucketSnapshotLoadError::Store(error) => ServerError::Store(error),
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
    let existing_tags = if existing_tags_required {
        Some(Coordinator::parse_policy_existing_object_tags(object)?)
    } else {
        None
    };
    let existing_tags: Vec<auth::PolicyTag<'_>> = existing_tags
        .iter()
        .flat_map(|tags| tags.iter())
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
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
    let request_object_tags: Vec<auth::PolicyTag<'_>> = request_object_tags
        .iter()
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let bucket_tags: Vec<auth::PolicyTag<'_>> = bucket_tags
        .iter()
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let version_id = version_id_policy_value(
        context.action,
        context.policy_context.version_id,
        Some(object.version_id()),
    );
    let policy_request = auth::PolicyRequest::for_object(
        context.action,
        context.bucket_name,
        object.key().as_str(),
        context.requester.principal_opt(),
        context.requester.canonical_user_id(),
        if existing_tags_required
            && matches!(existing_object_tags_mode, ExistingObjectTagsMode::Available)
        {
            auth::bucket_policy::ExistingObjectTags::Available(&existing_tags)
        } else {
            auth::bucket_policy::ExistingObjectTags::Unavailable
        },
    )
    .with_bucket_tags(
        if context.bucket_abac_enabled
            && context
                .policy
                .requires_bucket_tags_for_action(context.action)
        {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
    );
    let policy_request = policy_request
        .with_request_object_tags(&request_object_tags)
        .with_copy_source(context.policy_context.copy_source)
        .with_metadata_directive(context.policy_context.metadata_directive)
        .with_canned_acl(context.policy_context.canned_acl)
        .with_server_side_encryption(
            context
                .policy_context
                .managed_encryption
                .map(ManagedEncryptionAlgorithm::as_str),
        )
        .with_sse_customer_algorithm(context.policy_context.sse_customer_algorithm)
        .with_grant_read(context.policy_context.grant_read)
        .with_grant_write(context.policy_context.grant_write)
        .with_grant_read_acp(context.policy_context.grant_read_acp)
        .with_grant_write_acp(context.policy_context.grant_write_acp)
        .with_grant_full_control(context.policy_context.grant_full_control)
        .with_if_match(context.policy_context.if_match)
        .with_if_none_match(context.policy_context.if_none_match)
        .with_object_creation_operation(context.policy_context.object_creation_operation)
        .with_version_id(version_id.as_deref());
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
    let bucket_tags: Vec<auth::PolicyTag<'_>> = bucket_tags
        .iter()
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let version_id =
        version_id_policy_value(request.action, request.policy_context.version_id, None);
    let policy_request = auth::PolicyRequest::for_object(
        request.action,
        request.bucket.name.as_str(),
        key,
        request.requester.principal_opt(),
        request.requester.canonical_user_id(),
        auth::bucket_policy::ExistingObjectTags::Unavailable,
    )
    .with_bucket_tags(
        if request.bucket.bucket_abac_enabled
            && policy.requires_bucket_tags_for_action(request.action)
        {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
    )
    .with_request_object_tags(&[])
    .with_copy_source(request.policy_context.copy_source)
    .with_metadata_directive(request.policy_context.metadata_directive)
    .with_canned_acl(request.policy_context.canned_acl)
    .with_server_side_encryption(
        request
            .policy_context
            .managed_encryption
            .map(ManagedEncryptionAlgorithm::as_str),
    )
    .with_sse_customer_algorithm(request.policy_context.sse_customer_algorithm)
    .with_grant_read(request.policy_context.grant_read)
    .with_grant_write(request.policy_context.grant_write)
    .with_grant_read_acp(request.policy_context.grant_read_acp)
    .with_grant_write_acp(request.policy_context.grant_write_acp)
    .with_grant_full_control(request.policy_context.grant_full_control)
    .with_if_match(request.policy_context.if_match)
    .with_if_none_match(request.policy_context.if_none_match)
    .with_object_creation_operation(request.policy_context.object_creation_operation)
    .with_version_id(version_id.as_deref());
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
    let request = auth::PolicyRequest::for_bucket(
        action,
        bucket.name.as_str(),
        requester.principal_opt(),
        requester.canonical_user_id(),
        if bucket_tags_available {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
    );
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

    let request = auth::PolicyRequest::for_bucket(
        action,
        bucket.bucket().name.as_str(),
        requester.principal_opt(),
        requester.canonical_user_id(),
        if bucket_tags_available {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
    )
    .with_request_object_tags(&request_tags)
    .with_canned_acl(policy_context.canned_acl)
    .with_grant_read(policy_context.grant_read)
    .with_grant_write(policy_context.grant_write)
    .with_grant_read_acp(policy_context.grant_read_acp)
    .with_grant_write_acp(policy_context.grant_write_acp)
    .with_grant_full_control(policy_context.grant_full_control)
    .with_prefix(policy_context.prefix)
    .with_delimiter(policy_context.delimiter)
    .with_max_keys(requested_max_keys.as_deref())
    .with_object_ownership(policy_context.object_ownership);
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
    let request_object_tags: Vec<auth::PolicyTag<'_>> = request_object_tags
        .iter()
        .map(|(tag_key, value)| auth::PolicyTag::new(tag_key, value))
        .collect();
    let bucket_tags = bucket_tags_for_policy_request(coord, request, policy)?;
    let bucket_tags: Vec<auth::PolicyTag<'_>> = bucket_tags
        .iter()
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let version_id =
        version_id_policy_value(request.action, request.policy_context.version_id, None);
    let policy_request = auth::PolicyRequest::for_object(
        request.action,
        request.bucket.name.as_str(),
        key,
        request.requester.principal_opt(),
        request.requester.canonical_user_id(),
        auth::bucket_policy::ExistingObjectTags::Unavailable,
    )
    .with_bucket_tags(
        if request.bucket.bucket_abac_enabled
            && policy.requires_bucket_tags_for_action(request.action)
        {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
    )
    .with_request_object_tags(&request_object_tags)
    .with_copy_source(request.policy_context.copy_source)
    .with_metadata_directive(request.policy_context.metadata_directive)
    .with_canned_acl(request.policy_context.canned_acl)
    .with_server_side_encryption(
        request
            .policy_context
            .managed_encryption
            .map(ManagedEncryptionAlgorithm::as_str),
    )
    .with_sse_customer_algorithm(request.policy_context.sse_customer_algorithm)
    .with_grant_read(request.policy_context.grant_read)
    .with_grant_write(request.policy_context.grant_write)
    .with_grant_read_acp(request.policy_context.grant_read_acp)
    .with_grant_write_acp(request.policy_context.grant_write_acp)
    .with_grant_full_control(request.policy_context.grant_full_control)
    .with_if_match(request.policy_context.if_match)
    .with_if_none_match(request.policy_context.if_none_match)
    .with_object_creation_operation(request.policy_context.object_creation_operation)
    .with_version_id(version_id.as_deref());
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
    let decision = bucket_policy_decision_for_object(
        coord,
        request,
        object,
        ExistingObjectTagsMode::Available,
    )?;
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
    let decision = bucket_policy_decision_for_object(
        coord,
        request,
        object,
        ExistingObjectTagsMode::Unavailable,
    )?;
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
    let decision = filter_bucket_policy_allow_for_foreign_owned_read_family_object(
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

pub(super) fn filter_bucket_policy_allow_for_foreign_owned_read_family_object(
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

    let is_read_family = matches!(
        action,
        auth::PolicyAction::GetObject
            | auth::PolicyAction::GetObjectVersion
            | auth::PolicyAction::GetObjectAttributes
            | auth::PolicyAction::GetObjectVersionAttributes
            | auth::PolicyAction::GetObjectAcl
            | auth::PolicyAction::GetObjectVersionAcl
    );
    if !is_read_family {
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
