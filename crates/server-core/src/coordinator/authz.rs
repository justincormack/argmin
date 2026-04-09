use super::*;

impl Coordinator {
    pub(super) fn requester_can_bucket_admin(requester: &Requester, owner_principal: &str) -> bool {
        requester.principal_opt() == Some(owner_principal)
    }

    pub(super) fn requester_has_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        permission: AclPermission,
    ) -> bool {
        requester.canonical_user_id().is_some_and(|id| {
            acl_grants.allows_canonical_user(id, permission)
                || acl_grants.allows_authenticated_users(permission)
        })
    }

    pub(super) fn requester_has_nonpublic_object_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        permission: AclPermission,
    ) -> bool {
        requester
            .canonical_user_id()
            .is_some_and(|id| acl_grants.allows_canonical_user(id, permission))
    }

    pub(super) fn requester_has_nonpublic_bucket_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        permission: AclPermission,
    ) -> bool {
        requester
            .canonical_user_id()
            .is_some_and(|id| acl_grants.allows_canonical_user(id, permission))
    }

    pub(super) fn acl_grants_public_read(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_all_users(AclPermission::Read)
    }

    pub(super) fn acl_grants_public_write(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_all_users(AclPermission::Write)
    }

    pub(super) fn acl_grants_grant_public_read(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_public_groups(AclPermission::Read)
    }

    pub(super) fn acl_grants_grant_public_write(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_public_groups(AclPermission::Write)
    }

    pub(super) fn requester_can_object_write(
        requester: &Requester,
        owner_principal: &str,
        acl_grants: &AclGrants,
        public_write: bool,
    ) -> bool {
        requester.principal_opt() == Some(owner_principal)
            || Self::requester_has_acl_permission(requester, acl_grants, AclPermission::Write)
            || public_write
    }

    pub(super) fn requester_can_read_bucket(
        requester: &Requester,
        bucket: &BucketSummary,
        owner_principal: &str,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> bool {
        let acl_allows_read = if Self::ignores_public_acls(bucket.public_access_block.as_deref()) {
            Self::requester_has_nonpublic_bucket_acl_permission(
                requester,
                acl_grants,
                AclPermission::Read,
            )
        } else {
            Self::requester_has_acl_permission(requester, acl_grants, AclPermission::Read)
        };

        requester.principal_opt() == Some(owner_principal) || acl_allows_read || public_read
    }

    pub(super) fn requester_can_read_object(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref()) {
            return Self::requester_is_bucket_owner_account(requester, bucket);
        }

        let acl_allows_read = object.acl_grants().is_some_and(|grants| {
            if Self::ignores_public_acls(bucket.public_access_block.as_deref()) {
                Self::requester_has_nonpublic_object_acl_permission(
                    requester,
                    grants,
                    AclPermission::Read,
                )
            } else {
                Self::requester_has_acl_permission(requester, grants, AclPermission::Read)
            }
        });

        requester.principal_opt() == Some(object.owner().principal.as_str())
            || acl_allows_read
            || (object.public_read()
                && !Self::ignores_public_acls(bucket.public_access_block.as_deref()))
    }

    pub(super) fn requester_can_read_bucket_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref()) {
            return Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
                || Self::requester_is_bucket_owner_account(requester, bucket);
        }

        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                &bucket.acl_grants,
                AclPermission::ReadAcp,
            )
    }

    pub(super) fn requester_can_write_bucket_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref()) {
            return Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
                || Self::requester_is_bucket_owner_account(requester, bucket);
        }

        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                &bucket.acl_grants,
                AclPermission::WriteAcp,
            )
    }

    pub(super) fn requester_can_read_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref())
            && Self::requester_is_bucket_owner_account(requester, bucket))
            || requester.principal_opt() == Some(object.owner().principal.as_str())
            || object.acl_grants().is_some_and(|grants| {
                Self::requester_has_acl_permission(requester, grants, AclPermission::ReadAcp)
            })
    }

    pub(super) fn requester_can_write_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref())
            && Self::requester_is_bucket_owner_account(requester, bucket))
            || requester.principal_opt() == Some(object.owner().principal.as_str())
            || object.acl_grants().is_some_and(|grants| {
                Self::requester_has_acl_permission(requester, grants, AclPermission::WriteAcp)
            })
    }

    pub(super) fn requester_matches_owner_identity(
        requester: &Requester,
        owner: &OwnerIdentity,
    ) -> bool {
        requester.account().is_some_and(|account| {
            account.canonical_user_id() == &owner.canonical_id
                || account.principal() == owner.principal
        })
    }

    pub(super) fn requester_is_bucket_owner_account(
        requester: &Requester,
        bucket: &BucketSummary,
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
        aws_account_id_from_principal(&bucket.owner_principal) == Some(requester_account_id)
    }

    pub(super) fn requester_can_discover_missing_object(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_read_bucket(
            requester,
            bucket,
            &bucket.owner_principal,
            &bucket.acl_grants,
            Self::effective_public_read(bucket),
        ) || (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref())
            && Self::requester_is_bucket_owner_account(requester, bucket))
    }

    pub(super) fn requester_can_discover_missing_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref())
                && Self::requester_is_bucket_owner_account(requester, bucket))
    }

    pub(super) fn requester_can_manage_multipart_upload(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &MultipartUploadRecord,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_matches_owner_identity(requester, &upload.owner)
            || upload.initiator.as_ref().is_some_and(|initiator| {
                Self::requester_matches_owner_identity(requester, initiator)
            })
    }

    pub(super) fn requester_can_manage_completed_multipart_upload(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &storage::CompletedMultipartUploadRecord,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_matches_owner_identity(requester, &upload.owner)
            || upload.initiator.as_ref().is_some_and(|initiator| {
                Self::requester_matches_owner_identity(requester, initiator)
            })
    }

    pub(super) fn requester_can_write_multipart_upload(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &MultipartUploadRecord,
    ) -> bool {
        Self::requester_can_object_write(
            requester,
            &bucket.owner_principal,
            &bucket.acl_grants,
            Self::effective_public_write(bucket),
        ) && Self::requester_can_manage_multipart_upload(requester, bucket, upload)
    }

    pub(super) fn requester_can_write_multipart_upload_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &MultipartUploadRecord,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Self::requester_can_put_object_action_with_bucket_policy(
            requester,
            bucket,
            upload.key.as_str(),
            auth::PolicyAction::PutObject,
            policy_context,
            policy,
            Self::requester_can_write_multipart_upload(requester, bucket, upload),
        )
    }

    pub(super) fn with_multipart_upload_managed_encryption_policy_context<'a>(
        policy_context: PutObjectPolicyContext<'a>,
        upload: &'a MultipartUploadRecord,
    ) -> PutObjectPolicyContext<'a> {
        if policy_context.managed_encryption.is_some() {
            return policy_context;
        }

        match upload.encryption.managed_encryption_algorithm() {
            Some(algorithm) => policy_context.with_managed_encryption(Some(algorithm)),
            None => policy_context,
        }
    }

    pub(super) fn requester_can_manage_object_tags(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        requester.principal_opt() == Some(bucket.owner_principal.as_str())
            || requester.principal_opt() == Some(object.owner().principal.as_str())
    }

    pub(super) fn is_bucket_owner_enforced(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| {
            xml.contains("<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>")
        })
    }

    pub(super) fn is_bucket_owner_preferred(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| {
            xml.contains("<ObjectOwnership>BucketOwnerPreferred</ObjectOwnership>")
        })
    }

    pub(super) fn ignores_public_acls(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| xml.contains("<IgnorePublicAcls>true</IgnorePublicAcls>"))
    }

    pub(super) fn blocks_public_acls(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| xml.contains("<BlockPublicAcls>true</BlockPublicAcls>"))
    }

    pub(super) fn blocks_public_policy(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| xml.contains("<BlockPublicPolicy>true</BlockPublicPolicy>"))
    }

    pub(super) fn restricts_public_buckets(config_xml: Option<&str>) -> bool {
        config_xml
            .is_some_and(|xml| xml.contains("<RestrictPublicBuckets>true</RestrictPublicBuckets>"))
    }

    pub(super) fn effective_public_read(bucket: &BucketSummary) -> bool {
        bucket.public_read && !Self::ignores_public_acls(bucket.public_access_block.as_deref())
    }

    pub(super) fn effective_public_write(bucket: &BucketSummary) -> bool {
        bucket.public_write && !Self::ignores_public_acls(bucket.public_access_block.as_deref())
    }

    pub(super) fn bucket_policy_allow_survives_restrict_public_buckets(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if !bucket.bucket_policy_public
            || !Self::restricts_public_buckets(bucket.public_access_block.as_deref())
        {
            return true;
        }

        Self::requester_is_bucket_owner_account(requester, bucket)
    }

    pub(super) fn get_object_policy_action(version_id: Option<VersionId>) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersion
        } else {
            auth::PolicyAction::GetObject
        }
    }

    pub(super) fn get_object_acl_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersionAcl
        } else {
            auth::PolicyAction::GetObjectAcl
        }
    }

    pub(super) fn put_object_acl_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::PutObjectVersionAcl
        } else {
            auth::PolicyAction::PutObjectAcl
        }
    }

    pub(super) fn get_object_tagging_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersionTagging
        } else {
            auth::PolicyAction::GetObjectTagging
        }
    }

    pub(super) fn put_object_tagging_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::PutObjectVersionTagging
        } else {
            auth::PolicyAction::PutObjectTagging
        }
    }

    pub(super) fn delete_object_tagging_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::DeleteObjectVersionTagging
        } else {
            auth::PolicyAction::DeleteObjectTagging
        }
    }

    pub(super) fn delete_object_policy_action(version_id: Option<VersionId>) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::DeleteObjectVersion
        } else {
            auth::PolicyAction::DeleteObject
        }
    }

    pub(super) fn cached_bucket_policy(
        &self,
        bucket: &BucketSummary,
    ) -> Result<Option<Arc<auth::BucketPolicy>>, ServerError> {
        if !bucket.bucket_policy_present {
            return Ok(None);
        }

        if let Some(cached) = read_rwlock_unpoisoned(&self.bucket_policy_cache)
            .get(&bucket.name)
            .cloned()
        {
            if cached.generation == bucket.bucket_policy_generation {
                return Ok(Some(cached.policy));
            }
        }

        let bucket_pg = self.get_bucket_pg(&bucket.name)?;
        let raw_policy = bucket_pg
            .get_bucket_policy(&bucket.name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })?;
        let parsed_policy = match raw_policy {
            Some(policy) => Arc::new(auth::parse_bucket_policy(&policy).map_err(|e| {
                ServerError::InternalError {
                    reason: format!(
                        "stored bucket policy for {} failed to parse at request time: {}",
                        bucket.name,
                        e.reason()
                    ),
                }
            })?),
            None => {
                self.clear_bucket_policy_cache(&bucket.name);
                return Ok(None);
            }
        };

        self.cache_bucket_policy(
            &bucket.name,
            bucket.bucket_policy_generation,
            Arc::clone(&parsed_policy),
        );
        Ok(Some(parsed_policy))
    }

    pub(super) fn cache_bucket_policy(
        &self,
        bucket: &str,
        generation: u64,
        policy: Arc<auth::BucketPolicy>,
    ) {
        write_rwlock_unpoisoned(&self.bucket_policy_cache).insert(
            bucket.to_string(),
            CachedBucketPolicy { generation, policy },
        );
    }

    pub(super) fn clear_bucket_policy_cache(&self, bucket: &str) {
        write_rwlock_unpoisoned(&self.bucket_policy_cache).remove(bucket);
    }

    pub(super) fn bucket_policy_decision_for_object(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        let Some(policy) = policy else {
            return Ok(auth::PolicyEvaluation::NoMatch);
        };

        let existing_tags = if policy.requires_existing_object_tags_for_action(action) {
            Self::parse_policy_existing_object_tags(object)?
        } else {
            Vec::new()
        };
        let existing_tags: Vec<auth::PolicyTag<'_>> = existing_tags
            .iter()
            .map(|(key, value)| auth::PolicyTag::new(key, value))
            .collect();
        let request_object_tags = if policy.requires_request_object_tags_for_action(action) {
            match request_object_tags_xml {
                Some(tags_xml) => Self::parse_serialized_tag_set(tags_xml)?,
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
        let request_object_tags: Vec<auth::PolicyTag<'_>> = request_object_tags
            .iter()
            .map(|(key, value)| auth::PolicyTag::new(key, value))
            .collect();
        let request = auth::PolicyRequest::new(
            action,
            &bucket.name,
            object.key().as_str(),
            requester.principal_opt(),
            requester.canonical_user_id(),
        );
        let request = request
            .with_existing_object_tags(&existing_tags)
            .with_request_object_tags(&request_object_tags);
        Ok(policy.evaluate(&request))
    }

    pub(super) fn bucket_policy_decision_for_key(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> auth::PolicyEvaluation {
        let Some(policy) = policy else {
            return auth::PolicyEvaluation::NoMatch;
        };

        let request = auth::PolicyRequest::new(
            action,
            &bucket.name,
            key,
            requester.principal_opt(),
            requester.canonical_user_id(),
        );
        policy.evaluate(&request)
    }

    pub(super) fn bucket_policy_decision_for_bucket(
        requester: &Requester,
        bucket: &BucketSummary,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> auth::PolicyEvaluation {
        let Some(policy) = policy else {
            return auth::PolicyEvaluation::NoMatch;
        };

        let request = auth::PolicyRequest::for_bucket(
            action,
            &bucket.name,
            requester.principal_opt(),
            requester.canonical_user_id(),
        );
        policy.evaluate(&request)
    }

    pub(super) fn bucket_policy_decision_for_put_object_action(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        let Some(policy) = policy else {
            return Ok(auth::PolicyEvaluation::NoMatch);
        };

        let request_object_tags = if policy.requires_request_object_tags_for_action(action) {
            match policy_context.request_object_tags_xml {
                Some(tags_xml) => Self::parse_serialized_tag_set(tags_xml)?,
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
        let request_object_tags: Vec<auth::PolicyTag<'_>> = request_object_tags
            .iter()
            .map(|(tag_key, value)| auth::PolicyTag::new(tag_key, value))
            .collect();
        let request = auth::PolicyRequest::new(
            action,
            &bucket.name,
            key,
            requester.principal_opt(),
            requester.canonical_user_id(),
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
        Ok(policy.evaluate(&request))
    }

    pub(super) fn requester_can_put_object_action_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
        default_allowed: bool,
    ) -> Result<bool, ServerError> {
        Ok(
            match Self::bucket_policy_decision_for_put_object_action(
                requester,
                bucket,
                key,
                action,
                policy_context,
                policy,
            )? {
                auth::PolicyEvaluation::ExplicitDeny => false,
                auth::PolicyEvaluation::ExplicitAllow
                    if Self::bucket_policy_allow_survives_restrict_public_buckets(
                        requester, bucket,
                    ) =>
                {
                    true
                }
                auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                    default_allowed
                }
            },
        )
    }

    pub(super) fn requester_can_read_object_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Ok(
            match Self::bucket_policy_decision_for_object(
                requester, bucket, object, action, None, policy,
            )? {
                auth::PolicyEvaluation::ExplicitDeny => false,
                auth::PolicyEvaluation::ExplicitAllow
                    if Self::bucket_policy_allow_survives_restrict_public_buckets(
                        requester, bucket,
                    ) =>
                {
                    true
                }
                auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                    Self::requester_can_read_object(requester, bucket, object)
                }
            },
        )
    }

    pub(super) fn requester_can_manage_object_tags_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Ok(
            match Self::bucket_policy_decision_for_object(
                requester,
                bucket,
                object,
                action,
                request_object_tags_xml,
                policy,
            )? {
                auth::PolicyEvaluation::ExplicitDeny => false,
                auth::PolicyEvaluation::ExplicitAllow
                    if Self::bucket_policy_allow_survives_restrict_public_buckets(
                        requester, bucket,
                    ) =>
                {
                    true
                }
                auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                    Self::requester_can_manage_object_tags(requester, bucket, object)
                }
            },
        )
    }

    pub(super) fn requester_can_manage_object_lock_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Ok(
            match Self::bucket_policy_decision_for_object(
                requester, bucket, object, action, None, policy,
            )? {
                auth::PolicyEvaluation::ExplicitDeny => false,
                auth::PolicyEvaluation::ExplicitAllow
                    if Self::bucket_policy_allow_survives_restrict_public_buckets(
                        requester, bucket,
                    ) =>
                {
                    true
                }
                auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                    Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
                }
            },
        )
    }

    pub(super) fn requester_can_delete_object_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        object: Option<&StoredObject>,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        let decision = match object {
            Some(object) => Self::bucket_policy_decision_for_object(
                requester, bucket, object, action, None, policy,
            )?,
            None => Self::bucket_policy_decision_for_key(requester, bucket, key, action, policy),
        };

        Ok(match decision {
            auth::PolicyEvaluation::ExplicitDeny => false,
            auth::PolicyEvaluation::ExplicitAllow
                if Self::bucket_policy_allow_survives_restrict_public_buckets(
                    requester, bucket,
                ) =>
            {
                true
            }
            auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                Self::requester_can_object_write(
                    requester,
                    &bucket.owner_principal,
                    &bucket.acl_grants,
                    Self::effective_public_write(bucket),
                )
            }
        })
    }

    pub(super) fn requester_can_read_object_acl_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Ok(
            match Self::bucket_policy_decision_for_object(
                requester, bucket, object, action, None, policy,
            )? {
                auth::PolicyEvaluation::ExplicitDeny => false,
                auth::PolicyEvaluation::ExplicitAllow
                    if Self::bucket_policy_allow_survives_restrict_public_buckets(
                        requester, bucket,
                    ) =>
                {
                    true
                }
                auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                    Self::requester_can_read_object_acl(requester, bucket, object)
                }
            },
        )
    }

    pub(super) fn requester_can_write_object_acl_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Ok(
            match Self::bucket_policy_decision_for_object(
                requester, bucket, object, action, None, policy,
            )? {
                auth::PolicyEvaluation::ExplicitDeny => false,
                auth::PolicyEvaluation::ExplicitAllow
                    if Self::bucket_policy_allow_survives_restrict_public_buckets(
                        requester, bucket,
                    ) =>
                {
                    true
                }
                auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                    Self::requester_can_write_object_acl(requester, bucket, object)
                }
            },
        )
    }

    pub(super) fn requester_can_put_object_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
        existing_object: Option<&StoredObject>,
    ) -> Result<bool, ServerError> {
        let default_allowed = if let Some(object) = existing_object {
            requester.principal_opt() == Some(bucket.owner_principal.as_str())
                || Self::requester_has_acl_permission(
                    requester,
                    &bucket.acl_grants,
                    AclPermission::Write,
                )
                || (Self::effective_public_write(bucket)
                    && Self::requester_matches_owner_identity(requester, object.owner()))
        } else {
            Self::requester_can_object_write(
                requester,
                &bucket.owner_principal,
                &bucket.acl_grants,
                Self::effective_public_write(bucket),
            )
        };
        let can_put_object = Self::requester_can_put_object_action_with_bucket_policy(
            requester,
            bucket,
            key,
            auth::PolicyAction::PutObject,
            policy_context,
            policy,
            default_allowed,
        )?;
        if !can_put_object {
            return Ok(false);
        }

        if policy_context.request_object_tags_xml.is_none() {
            return Ok(true);
        }

        Self::requester_can_put_object_action_with_bucket_policy(
            requester,
            bucket,
            key,
            auth::PolicyAction::PutObjectTagging,
            policy_context,
            policy,
            Self::requester_can_bucket_admin(requester, &bucket.owner_principal),
        )
    }

    pub(super) fn requester_can_get_bucket_public_access_block_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        policy: Option<&auth::BucketPolicy>,
    ) -> bool {
        Self::requester_can_bucket_action_with_bucket_policy(
            requester,
            bucket,
            auth::PolicyAction::GetBucketPublicAccessBlock,
            policy,
            Self::requester_can_bucket_admin(requester, &bucket.owner_principal),
        )
    }

    pub(super) fn requester_can_bucket_action_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
        default_allowed: bool,
    ) -> bool {
        match Self::bucket_policy_decision_for_bucket(requester, bucket, action, policy) {
            auth::PolicyEvaluation::ExplicitDeny => false,
            auth::PolicyEvaluation::ExplicitAllow
                if Self::bucket_policy_allow_survives_restrict_public_buckets(
                    requester, bucket,
                ) =>
            {
                true
            }
            auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                default_allowed
            }
        }
    }

    pub(super) fn requester_can_get_bucket_policy_status_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        policy: Option<&auth::BucketPolicy>,
    ) -> bool {
        Self::requester_can_bucket_action_with_bucket_policy(
            requester,
            bucket,
            auth::PolicyAction::GetBucketPolicyStatus,
            policy,
            Self::requester_can_bucket_admin(requester, &bucket.owner_principal),
        )
    }

    pub(super) fn requester_can_get_bucket_object_lock_configuration_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        policy: Option<&auth::BucketPolicy>,
    ) -> bool {
        Self::requester_can_bucket_action_with_bucket_policy(
            requester,
            bucket,
            auth::PolicyAction::GetBucketObjectLockConfiguration,
            policy,
            Self::requester_can_bucket_admin(requester, &bucket.owner_principal),
        )
    }

    pub(super) fn requester_can_list_bucket_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        policy: Option<&auth::BucketPolicy>,
    ) -> bool {
        Self::requester_can_bucket_action_with_bucket_policy(
            requester,
            bucket,
            auth::PolicyAction::ListBucket,
            policy,
            Self::requester_can_read_bucket(
                requester,
                bucket,
                &bucket.owner_principal,
                &bucket.acl_grants,
                Self::effective_public_read(bucket),
            ),
        )
    }

    pub(super) fn authorize_bucket_admin_or_bucket_policy_action(
        &self,
        requester: &Requester,
        bucket: &str,
        expected_bucket_owner: Option<&str>,
        action: auth::PolicyAction,
    ) -> Result<BucketSummary, ServerError> {
        let info = self.active_bucket_summary(bucket, expected_bucket_owner)?;
        let bucket_policy = self.cached_bucket_policy(&info)?;
        if Self::requester_can_bucket_action_with_bucket_policy(
            requester,
            &info,
            action,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_admin(requester, &info.owner_principal),
        ) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn parse_policy_existing_object_tags(
        object: &StoredObject,
    ) -> Result<Vec<(String, String)>, ServerError> {
        let Some(tags_xml) = object.as_live().and_then(|record| record.tags.as_deref()) else {
            return Ok(Vec::new());
        };
        Self::parse_serialized_tag_set(tags_xml)
    }

    pub(super) fn parse_serialized_tag_set(
        tags_xml: &str,
    ) -> Result<Vec<(String, String)>, ServerError> {
        let mut tags = Vec::new();
        let mut remaining = tags_xml;

        while let Some(tag_start) = remaining.find("<Tag>") {
            remaining = &remaining[tag_start + "<Tag>".len()..];
            let Some(tag_end) = remaining.find("</Tag>") else {
                return Err(ServerError::InternalError {
                    reason: "stored object tags missing </Tag> terminator".to_string(),
                });
            };
            let tag_xml = &remaining[..tag_end];
            let key = Self::xml_unescape(Self::extract_xml_text(
                tag_xml,
                "Key",
                "stored object tags missing <Key>",
            )?)?;
            let value = Self::xml_unescape(Self::extract_xml_text(
                tag_xml,
                "Value",
                "stored object tags missing <Value>",
            )?)?;
            tags.push((key, value));
            remaining = &remaining[tag_end + "</Tag>".len()..];
        }

        Ok(tags)
    }

    pub(super) fn extract_xml_text<'a>(
        xml: &'a str,
        tag: &str,
        missing_reason: &'static str,
    ) -> Result<&'a str, ServerError> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let Some(start) = xml.find(&open) else {
            return Err(ServerError::InternalError {
                reason: missing_reason.to_string(),
            });
        };
        let content = &xml[start + open.len()..];
        let Some(end) = content.find(&close) else {
            return Err(ServerError::InternalError {
                reason: format!("stored object tags missing closing </{tag}>"),
            });
        };
        Ok(&content[..end])
    }

    pub(super) fn xml_unescape(value: &str) -> Result<String, ServerError> {
        let mut out = String::with_capacity(value.len());
        let mut chars = value.chars();

        while let Some(ch) = chars.next() {
            if ch != '&' {
                out.push(ch);
                continue;
            }

            let mut entity = String::new();
            loop {
                let Some(next) = chars.next() else {
                    return Err(ServerError::InternalError {
                        reason: "stored object tags ended mid-entity".to_string(),
                    });
                };
                entity.push(next);
                if next == ';' {
                    break;
                }
            }

            match entity.as_str() {
                "amp;" => out.push('&'),
                "lt;" => out.push('<'),
                "gt;" => out.push('>'),
                "quot;" => out.push('"'),
                "apos;" => out.push('\''),
                _ => {
                    return Err(ServerError::InternalError {
                        reason: format!("stored object tags contain unsupported entity &{entity}"),
                    });
                }
            }
        }

        Ok(out)
    }

    pub(super) fn ensure_sse_c_allowed(
        bucket: &BucketSummary,
        uses_sse_c: bool,
    ) -> Result<(), ServerError> {
        if uses_sse_c && bucket.encryption.sse_c_blocked {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn ensure_expected_bucket_owner(
        bucket: &BucketSummary,
        expected_bucket_owner: Option<&str>,
    ) -> Result<(), ServerError> {
        if expected_bucket_owner.is_some_and(|expected| expected != bucket.owner_principal) {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn requester_principal_required(requester: &Requester) -> Result<&str, ServerError> {
        requester.principal_opt().ok_or(ServerError::AccessDenied)
    }

    pub(super) fn bucket_owner_identity(bucket: &BucketSummary) -> OwnerIdentity {
        OwnerIdentity::new(
            bucket.owner_principal.clone(),
            bucket.owner_canonical_id.clone(),
        )
    }

    pub(super) fn owner_full_control_grants(owner: &OwnerIdentity) -> AclGrants {
        AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(owner.canonical_id.clone()),
            AclPermission::FullControl,
        )])
    }

    pub(super) fn bucket_acl_grants_from_canned(
        bucket_owner: &OwnerIdentity,
        acl: BucketAcl,
    ) -> Result<AclGrants, ServerError> {
        let mut grants: Vec<AclGrant> = Self::owner_full_control_grants(bucket_owner)
            .iter()
            .cloned()
            .collect();
        match acl {
            BucketAcl::Private => {}
            BucketAcl::PublicRead => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
            }
            BucketAcl::PublicReadWrite => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Write));
            }
            BucketAcl::AuthenticatedRead => {
                grants.push(AclGrant::new(
                    AclGrantee::AuthenticatedUsers,
                    AclPermission::Read,
                ));
            }
        }
        Ok(AclGrants::new(grants))
    }

    #[cfg(test)]
    pub(super) fn bucket_acl_grants_from_flags(
        bucket_owner: &OwnerIdentity,
        public_read: bool,
        public_write: bool,
    ) -> AclGrants {
        let mut grants: Vec<AclGrant> = Self::owner_full_control_grants(bucket_owner)
            .iter()
            .cloned()
            .collect();
        if public_read {
            grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
        }
        if public_write {
            grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Write));
        }
        AclGrants::new(grants)
    }

    pub(super) fn ensure_put_bucket_acl_supported(
        bucket: &BucketSummary,
        acl: BucketAcl,
    ) -> Result<(), ServerError> {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref()) {
            return Err(ServerError::AccessControlListNotSupported);
        }
        if Self::blocks_public_acls(bucket.public_access_block.as_deref()) && acl.is_public() {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn requester_owner_identity(requester: &Requester) -> Option<OwnerIdentity> {
        requester.account().map(|account| {
            OwnerIdentity::new(
                account.principal().to_string(),
                account.canonical_user_id().clone(),
            )
        })
    }

    pub(super) fn effective_object_owner(
        bucket: &BucketSummary,
        requester: &Requester,
        acl: PutObjectAcl<'_>,
    ) -> OwnerIdentity {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref())
            || (Self::is_bucket_owner_preferred(bucket.ownership_controls.as_deref())
                && matches!(acl, PutObjectAcl::BucketOwnerFullControl))
        {
            return Self::bucket_owner_identity(bucket);
        }

        Self::requester_owner_identity(requester)
            .unwrap_or_else(|| Self::bucket_owner_identity(bucket))
    }

    pub(super) fn effective_put_object_owner(
        bucket: &BucketSummary,
        requester: &Requester,
        acl: &PutObjectWriteAcl<'_>,
    ) -> OwnerIdentity {
        let canned = match acl {
            PutObjectWriteAcl::None | PutObjectWriteAcl::Grants(_) => PutObjectAcl::None,
            PutObjectWriteAcl::Canned(acl) => *acl,
        };
        Self::effective_object_owner(bucket, requester, canned)
    }

    pub(super) fn ensure_put_object_acl_supported(
        bucket: &BucketSummary,
        acl: PutObjectAcl<'_>,
    ) -> Result<(), ServerError> {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref())
            && !acl.is_supported_with_bucket_owner_enforced()
        {
            return Err(ServerError::AccessControlListNotSupported);
        }
        if Self::blocks_public_acls(bucket.public_access_block.as_deref()) && acl.is_public() {
            return Err(ServerError::AccessDenied);
        }
        match acl {
            PutObjectAcl::Invalid(value) => {
                return Err(ServerError::InvalidArgument {
                    reason: format!("invalid x-amz-acl value: {value}"),
                });
            }
            PutObjectAcl::None
            | PutObjectAcl::Private
            | PutObjectAcl::PublicRead
            | PutObjectAcl::PublicReadWrite
            | PutObjectAcl::AuthenticatedRead
            | PutObjectAcl::AwsExecRead
            | PutObjectAcl::BucketOwnerRead
            | PutObjectAcl::BucketOwnerFullControl => {}
        }

        Ok(())
    }

    pub(super) fn ensure_put_object_write_acl_supported(
        bucket: &BucketSummary,
        acl: &PutObjectWriteAcl<'_>,
    ) -> Result<(), ServerError> {
        match acl {
            PutObjectWriteAcl::None => {
                Self::ensure_put_object_acl_supported(bucket, PutObjectAcl::None)
            }
            PutObjectWriteAcl::Canned(acl) => Self::ensure_put_object_acl_supported(bucket, *acl),
            PutObjectWriteAcl::Grants(acl_grants) => {
                if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_deref())
                    && !Self::acl_grants_owner_full_control_only(
                        &bucket.owner_canonical_id,
                        acl_grants,
                    )
                {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                Self::ensure_supported_object_acl_grants(acl_grants)?;
                if Self::blocks_public_acls(bucket.public_access_block.as_deref())
                    && (Self::acl_grants_grant_public_read(acl_grants)
                        || Self::acl_grants_grant_public_write(acl_grants))
                {
                    return Err(ServerError::AccessDenied);
                }
                Ok(())
            }
        }
    }

    pub(super) fn object_acl_grants_for_write(
        bucket: &BucketSummary,
        owner: &OwnerIdentity,
        acl: PutObjectAcl<'_>,
    ) -> AclGrants {
        let mut grants = vec![AclGrant::new(
            AclGrantee::CanonicalUser(owner.canonical_id.clone()),
            AclPermission::FullControl,
        )];
        match acl {
            PutObjectAcl::PublicRead => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
            }
            PutObjectAcl::PublicReadWrite => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Write));
            }
            PutObjectAcl::AuthenticatedRead => {
                grants.push(AclGrant::new(
                    AclGrantee::AuthenticatedUsers,
                    AclPermission::Read,
                ));
            }
            PutObjectAcl::AwsExecRead => {
                grants.push(AclGrant::new(
                    AclGrantee::CanonicalUser(CanonicalUserId::aws_exec_read()),
                    AclPermission::Read,
                ));
            }
            PutObjectAcl::BucketOwnerRead if bucket.owner_canonical_id != owner.canonical_id => {
                grants.push(AclGrant::new(
                    AclGrantee::CanonicalUser(bucket.owner_canonical_id.clone()),
                    AclPermission::Read,
                ));
            }
            PutObjectAcl::BucketOwnerFullControl
                if bucket.owner_canonical_id != owner.canonical_id =>
            {
                grants.push(AclGrant::new(
                    AclGrantee::CanonicalUser(bucket.owner_canonical_id.clone()),
                    AclPermission::FullControl,
                ));
            }
            PutObjectAcl::None
            | PutObjectAcl::Private
            | PutObjectAcl::BucketOwnerFullControl
            | PutObjectAcl::BucketOwnerRead
            | PutObjectAcl::Invalid(_) => {}
        }
        AclGrants::new(grants)
    }

    pub(super) fn object_acl_grants_for_put_object(
        bucket: &BucketSummary,
        owner: &OwnerIdentity,
        acl: &PutObjectWriteAcl<'_>,
    ) -> AclGrants {
        match acl {
            PutObjectWriteAcl::None => {
                Self::object_acl_grants_for_write(bucket, owner, PutObjectAcl::None)
            }
            PutObjectWriteAcl::Canned(acl) => {
                Self::object_acl_grants_for_write(bucket, owner, *acl)
            }
            PutObjectWriteAcl::Grants(acl_grants) => acl_grants.clone(),
        }
    }

    pub(super) fn ensure_supported_bucket_acl_grants(
        _acl_grants: &AclGrants,
    ) -> Result<(), ServerError> {
        Ok(())
    }

    pub(super) fn ensure_supported_object_acl_grants(
        acl_grants: &AclGrants,
    ) -> Result<(), ServerError> {
        for grant in acl_grants.iter() {
            if grant.permission() == AclPermission::Write {
                return Err(ServerError::InvalidArgument {
                    reason: "object ACLs do not support WRITE grants".to_string(),
                });
            }
        }
        Ok(())
    }

    pub(super) fn acl_grants_owner_full_control_only(
        owner_canonical_id: &CanonicalUserId,
        acl_grants: &AclGrants,
    ) -> bool {
        let expected = AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(owner_canonical_id.clone()),
            AclPermission::FullControl,
        )]);
        acl_grants == &expected
    }

    pub(super) fn lock_object_for_authorized_tagging<'a>(
        &'a self,
        object: &ObjectVersionRequest<'_>,
        policy_action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let bucket_info = self
            .active_bucket_summary(object.object.bucket_name(), object.expected_bucket_owner())?;
        let can_discover_missing = Self::requester_can_bucket_admin(
            object.object.requester(),
            &bucket_info.owner_principal,
        );
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        let locked = match self.lock_object_pgs_for_read(
            object.object.bucket_name(),
            object.object.key(),
            object.version_id,
        ) {
            Ok(locked) => locked,
            Err(ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. })
                if !can_discover_missing =>
            {
                return Err(ServerError::AccessDenied);
            }
            Err(other) => return Err(other),
        };

        if Self::requester_can_manage_object_tags_with_bucket_policy(
            object.object.requester(),
            &bucket_info,
            &locked.record,
            policy_action,
            request_object_tags_xml,
            bucket_policy.as_deref(),
        )? {
            Ok(locked)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn lock_object_for_authorized_acl<'a>(
        &'a self,
        requester: &Requester,
        bucket: &str,
        key: &str,
        version_id: Option<VersionId>,
        authorization: ObjectAclAuthorization,
        expected_bucket_owner: Option<&str>,
    ) -> Result<(BucketSummary, LockedReadObject<'a>), ServerError> {
        let bucket_info = self.active_bucket_summary(bucket, expected_bucket_owner)?;
        let can_discover_missing =
            Self::requester_can_discover_missing_object_acl(requester, &bucket_info);
        let bucket_policy = match authorization {
            ObjectAclAuthorization::ReadWithPolicy(_)
            | ObjectAclAuthorization::WriteWithPolicy(_) => {
                self.cached_bucket_policy(&bucket_info)?
            }
        };
        let locked = match self.lock_object_pgs_for_read(bucket, key, version_id) {
            Ok(locked) => locked,
            Err(ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. })
                if !can_discover_missing =>
            {
                return Err(ServerError::AccessDenied);
            }
            Err(other) => return Err(other),
        };
        let allowed = match authorization {
            ObjectAclAuthorization::WriteWithPolicy(policy_action) => {
                Self::requester_can_write_object_acl_with_bucket_policy(
                    requester,
                    &bucket_info,
                    &locked.record,
                    policy_action,
                    bucket_policy.as_deref(),
                )?
            }
            ObjectAclAuthorization::ReadWithPolicy(policy_action) => {
                Self::requester_can_read_object_acl_with_bucket_policy(
                    requester,
                    &bucket_info,
                    &locked.record,
                    policy_action,
                    bucket_policy.as_deref(),
                )?
            }
        };
        if allowed {
            Ok((bucket_info, locked))
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn authorize_bucket_read_requester(
        &self,
        requester: &Requester,
        bucket: &str,
        expected_bucket_owner: Option<&str>,
    ) -> Result<BucketSummary, ServerError> {
        let info = self.active_bucket_summary(bucket, expected_bucket_owner)?;
        if Self::requester_can_read_bucket(
            requester,
            &info,
            &info.owner_principal,
            &info.acl_grants,
            Self::effective_public_read(&info),
        ) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn authorize_bucket_admin_requester(
        &self,
        requester: &Requester,
        bucket: &str,
        expected_bucket_owner: Option<&str>,
    ) -> Result<BucketSummary, ServerError> {
        let info = self.active_bucket_summary(bucket, expected_bucket_owner)?;
        if Self::requester_can_bucket_admin(requester, &info.owner_principal) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn requester_can_bypass_governance_retention(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_is_bucket_owner_account(requester, bucket)
    }

    pub(super) fn requester_can_bypass_governance_retention_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Ok(
            match Self::bucket_policy_decision_for_object(
                requester,
                bucket,
                object,
                auth::PolicyAction::BypassGovernanceRetention,
                None,
                policy,
            )? {
                auth::PolicyEvaluation::ExplicitDeny => false,
                auth::PolicyEvaluation::ExplicitAllow
                    if Self::bucket_policy_allow_survives_restrict_public_buckets(
                        requester, bucket,
                    ) =>
                {
                    true
                }
                auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                    Self::requester_can_bypass_governance_retention(requester, bucket)
                }
            },
        )
    }

    pub(super) fn authorize_put_object_write(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let bucket = req.object.bucket_name();
        let key = req.object.key;
        self.with_bucket_write_reservation(bucket, |bucket_info| {
            Self::ensure_expected_bucket_owner(&bucket_info, req.object.expected_bucket_owner())?;
            let existing_object = self.put_target_existing_live_object(bucket, key)?;
            let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
            if !Self::requester_can_put_object_with_bucket_policy(
                req.object.requester(),
                &bucket_info,
                key,
                req.policy_context,
                bucket_policy.as_deref(),
                existing_object.as_ref(),
            )? {
                return Err(ServerError::AccessDenied);
            }
            let write_encryption = self.resolve_write_encryption(&bucket_info, req.encryption)?;
            Self::ensure_sse_c_allowed(&bucket_info, write_encryption.is_sse_customer())?;
            Self::ensure_put_object_write_acl_supported(&bucket_info, &req.acl)?;
            Self::validate_requested_object_lock_state(&bucket_info, req.object_lock)?;
            Ok(AuthorizedPutObjectWrite {
                bucket: bucket.to_string(),
                key: key.to_string(),
                requester: req.object.requester().clone(),
                expected_bucket_owner: req.object.expected_bucket_owner().map(str::to_string),
                acl: AuthorizedPutObjectWriteAcl::from_parsed(&req.acl),
                requested_object_lock: req.object_lock,
                tags: req.tags.map(str::to_string),
                write_encryption,
            })
        })
    }

    pub(super) fn lock_object_for_authorized_read<'a>(
        &'a self,
        requester: &Requester,
        bucket: &str,
        key: &str,
        version_id: Option<VersionId>,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let bucket_info = self.active_bucket_summary(bucket, expected_bucket_owner)?;
        let can_read_bucket = Self::requester_can_discover_missing_object(requester, &bucket_info);
        let locked = match self.lock_object_pgs_for_read(bucket, key, version_id) {
            Ok(locked) => locked,
            Err(ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. })
                if !can_read_bucket =>
            {
                return Err(ServerError::AccessDenied);
            }
            Err(other) => return Err(other),
        };

        if Self::requester_can_read_object(requester, &bucket_info, &locked.record) {
            Ok(locked)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn lock_object_for_authorized_read_with_policy<'a>(
        &'a self,
        requester: &Requester,
        bucket: &str,
        key: &str,
        version_id: Option<VersionId>,
        policy_action: auth::PolicyAction,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let bucket_info = self.active_bucket_summary(bucket, expected_bucket_owner)?;
        let can_read_bucket = Self::requester_can_discover_missing_object(requester, &bucket_info);
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        let locked = match self.lock_object_pgs_for_read(bucket, key, version_id) {
            Ok(locked) => locked,
            Err(ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. })
                if !can_read_bucket =>
            {
                return Err(ServerError::AccessDenied);
            }
            Err(other) => return Err(other),
        };

        if Self::requester_can_read_object_with_bucket_policy(
            requester,
            &bucket_info,
            &locked.record,
            policy_action,
            bucket_policy.as_deref(),
        )? {
            Ok(locked)
        } else {
            Err(ServerError::AccessDenied)
        }
    }
}
