use super::*;

#[derive(Clone, Copy)]
enum ObjectBucketPolicyRequirement {
    Required,
}

#[derive(Clone, Copy)]
enum MissingObjectDiscovery {
    ReadBucket,
    ReadObjectAttributes,
    BucketAdmin,
    ObjectAcl,
}

impl MissingObjectDiscovery {
    fn requester_can_discover_missing(
        self,
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        version_id: Option<VersionId>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        match self {
            Self::ReadBucket => Ok(Coordinator::requester_can_discover_missing_object(
                requester, bucket,
            ) || Coordinator::requester_can_list_bucket_with_bucket_policy(
                requester, bucket, policy,
            )),
            Self::ReadObjectAttributes => {
                Coordinator::requester_can_discover_missing_object_attrs_with_bucket_policy(
                    requester, bucket, key, version_id, policy,
                )
            }
            Self::BucketAdmin => Ok(Coordinator::requester_can_bucket_owner_account_admin(
                requester, bucket,
            )),
            Self::ObjectAcl => Ok(Coordinator::requester_can_discover_missing_object_acl(
                requester, bucket,
            )),
        }
    }
}

struct ObjectStateLoadRequest<'a> {
    requester: &'a Requester,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    version_id: Option<VersionId>,
    expected_bucket_owner: Option<&'a str>,
    policy_requirement: ObjectBucketPolicyRequirement,
    missing_discovery: MissingObjectDiscovery,
}

enum ObjectPolicyTarget<'a> {
    Existing(&'a StoredObject),
    MissingKey(&'a str),
}

impl Coordinator {
    pub(super) fn requester_can_bucket_admin(requester: &Requester, owner_principal: &str) -> bool {
        requester.principal_opt() == Some(owner_principal)
    }

    pub(super) fn requester_can_bucket_owner_account_admin(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || (requester.authorization_profile() == auth::AuthorizationProfile::OwnerAccountAdmin
                && Self::requester_is_bucket_owner_account(requester, bucket))
    }

    pub(super) fn requester_has_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        owner_canonical_id: &CanonicalUserId,
        permission: AclPermission,
    ) -> bool {
        requester.canonical_user_id().is_some_and(|id| {
            (acl_grants.allows_canonical_user(id, permission)
                && (id != owner_canonical_id
                    || requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin))
                || acl_grants.allows_authenticated_users(permission)
        })
    }

    pub(super) fn requester_has_nonpublic_object_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        owner_canonical_id: &CanonicalUserId,
        permission: AclPermission,
    ) -> bool {
        requester.canonical_user_id().is_some_and(|id| {
            acl_grants.allows_canonical_user(id, permission)
                && (id != owner_canonical_id
                    || requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin)
        })
    }

    pub(super) fn requester_has_nonpublic_bucket_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        owner_canonical_id: &CanonicalUserId,
        permission: AclPermission,
    ) -> bool {
        requester.canonical_user_id().is_some_and(|id| {
            acl_grants.allows_canonical_user(id, permission)
                && (id != owner_canonical_id
                    || requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin)
        })
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
        bucket: &BucketSummary,
        acl_grants: &AclGrants,
        public_write: bool,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
            || Self::requester_has_acl_permission(
                requester,
                acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::Write,
            )
            || public_write
    }

    pub(super) fn requester_can_read_bucket(
        requester: &Requester,
        bucket: &BucketSummary,
        owner_principal: &str,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> bool {
        let acl_allows_read = if Self::ignores_public_acls(bucket.public_access_block.as_ref()) {
            Self::requester_has_nonpublic_bucket_acl_permission(
                requester,
                acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::Read,
            )
        } else {
            Self::requester_has_acl_permission(
                requester,
                acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::Read,
            )
        };

        requester.principal_opt() == Some(owner_principal) || acl_allows_read || public_read
    }

    pub(super) fn requester_can_read_object(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_owner_account_admin(requester, bucket);
        }

        let acl_allows_read = object.acl_grants().is_some_and(|grants| {
            if Self::ignores_public_acls(bucket.public_access_block.as_ref()) {
                Self::requester_has_nonpublic_object_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::Read,
                )
            } else {
                Self::requester_has_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::Read,
                )
            }
        });

        Self::requester_matches_owner_identity(requester, object.owner())
            || acl_allows_read
            || (object.public_read()
                && !Self::ignores_public_acls(bucket.public_access_block.as_ref()))
    }

    pub(super) fn requester_can_read_object_attributes(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return requester
                .principal_opt()
                .is_some_and(|principal| principal == object.owner().principal.as_str());
        }

        Self::requester_can_read_object(requester, bucket, object)
    }

    pub(super) fn requester_can_read_bucket_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_owner_account_admin(requester, bucket);
        }

        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                &bucket.acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::ReadAcp,
            )
    }

    pub(super) fn requester_can_write_bucket_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_owner_account_admin(requester, bucket);
        }

        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                &bucket.acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::WriteAcp,
            )
    }

    pub(super) fn requester_can_read_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && Self::requester_can_bucket_owner_account_admin(requester, bucket))
            || Self::requester_matches_owner_identity(requester, object.owner())
            || object.acl_grants().is_some_and(|grants| {
                Self::requester_has_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::ReadAcp,
                )
            })
    }

    pub(super) fn requester_can_write_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && Self::requester_can_bucket_owner_account_admin(requester, bucket))
            || requester
                .account()
                .is_some_and(|_| Self::requester_matches_owner_identity(requester, object.owner()))
            || object.acl_grants().is_some_and(|grants| {
                Self::requester_has_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::WriteAcp,
                )
            })
    }

    pub(super) fn requester_matches_owner_identity(
        requester: &Requester,
        owner: &OwnerIdentity,
    ) -> bool {
        if requester.is_anonymous() {
            return owner.principal == OwnerIdentity::ANONYMOUS_UPLOAD_PRINCIPAL
                && owner.canonical_id == CanonicalUserId::anonymous_upload();
        }
        requester.account().is_some_and(|account| {
            account.principal() == owner.principal
                || (account.canonical_user_id() == &owner.canonical_id
                    && requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin)
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

    pub(super) fn requester_is_bucket_owner_account_root_principal(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if requester.authorization_profile() != auth::AuthorizationProfile::OwnerAccountAdmin {
            return false;
        }

        let Some(account) = requester.account() else {
            return false;
        };
        let Some(requester_account_id) = aws_account_id_from_principal(account.principal()) else {
            return false;
        };
        let Some(bucket_owner_account_id) = aws_account_id_from_principal(&bucket.owner_principal)
        else {
            return false;
        };
        if requester_account_id != bucket_owner_account_id {
            return false;
        }

        account.principal() == format!("arn:aws:iam::{requester_account_id}:root")
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
        ) || (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && Self::requester_can_bucket_owner_account_admin(requester, bucket))
    }

    pub(super) fn requester_can_discover_missing_object_attrs(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_admin(requester, &bucket.owner_principal);
        }

        Self::requester_can_read_bucket(
            requester,
            bucket,
            &bucket.owner_principal,
            &bucket.acl_grants,
            Self::effective_public_read(bucket),
        )
    }

    pub(super) fn requester_can_discover_missing_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
                && Self::requester_can_bucket_owner_account_admin(requester, bucket))
    }

    pub(super) fn requester_can_manage_multipart_upload(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &MultipartUploadRecord,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
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
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
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
            bucket,
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
        _object: &StoredObject,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
    }

    pub(super) fn is_bucket_owner_enforced(config: Option<&BucketOwnershipControls>) -> bool {
        config.is_some_and(|config| {
            config.object_ownership == BucketObjectOwnership::BucketOwnerEnforced
        })
    }

    pub(super) fn is_bucket_owner_preferred(config: Option<&BucketOwnershipControls>) -> bool {
        config.is_some_and(|config| {
            config.object_ownership == BucketObjectOwnership::BucketOwnerPreferred
        })
    }

    pub(super) fn ignores_public_acls(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.ignore_public_acls)
    }

    pub(super) fn blocks_public_acls(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.block_public_acls)
    }

    pub(super) fn blocks_public_policy(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.block_public_policy)
    }

    pub(super) fn restricts_public_buckets(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.restrict_public_buckets)
    }

    pub(super) fn effective_public_read(bucket: &BucketSummary) -> bool {
        bucket.public_read && !Self::ignores_public_acls(bucket.public_access_block.as_ref())
    }

    pub(super) fn effective_public_write(bucket: &BucketSummary) -> bool {
        bucket.public_write && !Self::ignores_public_acls(bucket.public_access_block.as_ref())
    }

    pub(super) fn bucket_policy_allow_survives_restrict_public_buckets(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if !bucket.bucket_policy_public
            || !Self::restricts_public_buckets(bucket.public_access_block.as_ref())
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

    pub(super) fn get_object_attributes_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersionAttributes
        } else {
            auth::PolicyAction::GetObjectAttributes
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

        if let Some(cached) = self.cached_bucket_policy_if_fresh(bucket) {
            return Ok(Some(cached));
        }

        let bucket_pg = self.get_bucket_pg_for(&bucket.name)?;
        self.cached_bucket_policy_with_locked_bucket_pg(bucket, &bucket_pg)
    }

    pub(super) fn cached_bucket_policy_if_fresh(
        &self,
        bucket: &BucketSummary,
    ) -> Option<Arc<auth::BucketPolicy>> {
        if !bucket.bucket_policy_present {
            return None;
        }

        let cached = read_rwlock_unpoisoned(&self.bucket_policy_cache)
            .get(&bucket.name)
            .cloned()?;
        (cached.generation == bucket.bucket_policy_generation).then_some(cached.policy)
    }

    pub(super) fn cached_bucket_policy_with_locked_bucket_pg(
        &self,
        bucket: &BucketSummary,
        bucket_pg: &storage::PgStore,
    ) -> Result<Option<Arc<auth::BucketPolicy>>, ServerError> {
        if !bucket.bucket_policy_present {
            return Ok(None);
        }

        if let Some(cached) = self.cached_bucket_policy_if_fresh(bucket) {
            return Ok(Some(cached));
        }

        let raw_policy = Self::load_bucket_subresource_from_pg(
            bucket_pg,
            &bucket.name,
            storage::BucketSubresourceKind::Policy,
        )?;
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
        bucket: &BucketName,
        generation: u64,
        policy: Arc<auth::BucketPolicy>,
    ) {
        write_rwlock_unpoisoned(&self.bucket_policy_cache)
            .insert(bucket.clone(), CachedBucketPolicy { generation, policy });
    }

    pub(super) fn clear_bucket_policy_cache(&self, bucket: &BucketName) {
        write_rwlock_unpoisoned(&self.bucket_policy_cache).remove(bucket);
    }

    pub(super) fn bucket_policy_decision_for_object(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
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
            match policy_context.request_object_tags_xml {
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
            bucket.name.as_str(),
            object.key().as_str(),
            requester.principal_opt(),
            requester.canonical_user_id(),
        );
        let request = request
            .with_existing_object_tags(&existing_tags)
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
            bucket.name.as_str(),
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
            bucket.name.as_str(),
            requester.principal_opt(),
            requester.canonical_user_id(),
        );
        policy.evaluate(&request)
    }

    pub(super) fn bucket_policy_decision_for_bucket_with_context(
        requester: &Requester,
        bucket: &BucketSummary,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> auth::PolicyEvaluation {
        let Some(policy) = policy else {
            return auth::PolicyEvaluation::NoMatch;
        };

        let request = auth::PolicyRequest::for_bucket(
            action,
            bucket.name.as_str(),
            requester.principal_opt(),
            requester.canonical_user_id(),
        )
        .with_canned_acl(policy_context.canned_acl)
        .with_grant_read(policy_context.grant_read)
        .with_grant_write(policy_context.grant_write)
        .with_grant_read_acp(policy_context.grant_read_acp)
        .with_grant_write_acp(policy_context.grant_write_acp)
        .with_grant_full_control(policy_context.grant_full_control);
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
            bucket.name.as_str(),
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
        let decision = Self::bucket_policy_decision_for_put_object_action(
            requester,
            bucket,
            key,
            action,
            policy_context,
            policy,
        )?;
        Ok(Self::bucket_policy_allows_with_fallback(
            requester,
            bucket,
            decision,
            || default_allowed,
        ))
    }

    fn bucket_policy_allows_with_fallback<F>(
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
                if Self::bucket_policy_allow_survives_restrict_public_buckets(
                    requester, bucket,
                ) =>
            {
                true
            }
            auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => fallback(),
        }
    }

    fn bucket_policy_allows_with_root_principal_bypass<F>(
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
                if Self::requester_is_bucket_owner_account_root_principal(requester, bucket) =>
            {
                fallback()
            }
            _ => Self::bucket_policy_allows_with_fallback(requester, bucket, decision, fallback),
        }
    }

    fn requester_can_object_action_with_bucket_policy<F>(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
        fallback: F,
    ) -> Result<bool, ServerError>
    where
        F: FnOnce() -> bool,
    {
        let decision = Self::bucket_policy_decision_for_object(
            requester,
            bucket,
            object,
            action,
            policy_context,
            policy,
        )?;
        Ok(Self::bucket_policy_allows_with_fallback(
            requester, bucket, decision, fallback,
        ))
    }

    fn requester_can_missing_object_action_with_bucket_policy<F>(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
        fallback: F,
    ) -> Result<bool, ServerError>
    where
        F: FnOnce() -> bool,
    {
        let decision = Self::object_policy_decision(
            requester,
            bucket,
            ObjectPolicyTarget::MissingKey(key),
            action,
            PutObjectPolicyContext::default(),
            policy,
        )?;
        Ok(Self::bucket_policy_allows_with_fallback(
            requester, bucket, decision, fallback,
        ))
    }

    fn object_policy_decision(
        requester: &Requester,
        bucket: &BucketSummary,
        target: ObjectPolicyTarget<'_>,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        match target {
            ObjectPolicyTarget::Existing(object) => Self::bucket_policy_decision_for_object(
                requester,
                bucket,
                object,
                action,
                policy_context,
                policy,
            ),
            ObjectPolicyTarget::MissingKey(key) => Ok(Self::bucket_policy_decision_for_key(
                requester, bucket, key, action, policy,
            )),
        }
    }

    pub(super) fn requester_can_read_object_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Self::requester_can_object_action_with_bucket_policy(
            requester,
            bucket,
            object,
            action,
            PutObjectPolicyContext::default(),
            policy,
            || Self::requester_can_read_object(requester, bucket, object),
        )
    }

    pub(super) fn requester_can_read_object_attributes_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        let read_action = match action {
            auth::PolicyAction::GetObjectVersionAttributes => auth::PolicyAction::GetObjectVersion,
            auth::PolicyAction::GetObjectAttributes => auth::PolicyAction::GetObject,
            _ => action,
        };
        Ok(Self::requester_can_read_object_with_bucket_policy(
            requester,
            bucket,
            object,
            read_action,
            policy,
        )? && Self::requester_can_object_action_with_bucket_policy(
            requester,
            bucket,
            object,
            action,
            PutObjectPolicyContext::default(),
            policy,
            || Self::requester_can_read_object_attributes(requester, bucket, object),
        )?)
    }

    pub(super) fn requester_can_discover_missing_object_attrs_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        version_id: Option<VersionId>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        let read_allowed = Self::requester_can_missing_object_action_with_bucket_policy(
            requester,
            bucket,
            key,
            Self::get_object_policy_action(version_id),
            policy,
            || Self::requester_can_discover_missing_object(requester, bucket),
        )?;
        let attrs_allowed = Self::requester_can_missing_object_action_with_bucket_policy(
            requester,
            bucket,
            key,
            Self::get_object_attributes_policy_action(version_id),
            policy,
            || Self::requester_can_discover_missing_object_attrs(requester, bucket),
        )?;

        Ok(read_allowed
            && attrs_allowed
            && Self::requester_can_list_bucket_with_bucket_policy(requester, bucket, policy))
    }

    pub(super) fn requester_can_manage_object_tags_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Self::requester_can_object_action_with_bucket_policy(
            requester,
            bucket,
            object,
            action,
            PutObjectPolicyContext::default().with_request_object_tags_xml(request_object_tags_xml),
            policy,
            || Self::requester_can_manage_object_tags(requester, bucket, object),
        )
    }

    pub(super) fn requester_can_manage_object_lock_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Self::requester_can_object_action_with_bucket_policy(
            requester,
            bucket,
            object,
            action,
            PutObjectPolicyContext::default(),
            policy,
            || Self::requester_can_bucket_owner_account_admin(requester, bucket),
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
        let decision = Self::object_policy_decision(
            requester,
            bucket,
            match object {
                Some(object) => ObjectPolicyTarget::Existing(object),
                None => ObjectPolicyTarget::MissingKey(key),
            },
            action,
            PutObjectPolicyContext::default(),
            policy,
        )?;

        Ok(Self::bucket_policy_allows_with_fallback(
            requester,
            bucket,
            decision,
            || {
                Self::requester_can_object_write(
                    requester,
                    bucket,
                    &bucket.acl_grants,
                    Self::effective_public_write(bucket),
                )
            },
        ))
    }

    pub(super) fn requester_can_read_object_acl_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Self::requester_can_object_action_with_bucket_policy(
            requester,
            bucket,
            object,
            action,
            PutObjectPolicyContext::default(),
            policy,
            || Self::requester_can_read_object_acl(requester, bucket, object),
        )
    }

    pub(super) fn requester_can_write_object_acl_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Self::requester_can_object_action_with_bucket_policy(
            requester,
            bucket,
            object,
            action,
            policy_context,
            policy,
            || Self::requester_can_write_object_acl(requester, bucket, object),
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
            Self::requester_can_bucket_owner_account_admin(requester, bucket)
                || Self::requester_has_acl_permission(
                    requester,
                    &bucket.acl_grants,
                    &bucket.owner_canonical_id,
                    AclPermission::Write,
                )
                || (Self::effective_public_write(bucket)
                    && Self::requester_matches_owner_identity(requester, object.owner()))
        } else {
            Self::requester_can_object_write(
                requester,
                bucket,
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
            Self::requester_can_bucket_owner_account_admin(requester, bucket),
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
            Self::requester_can_bucket_owner_account_admin(requester, bucket),
        )
    }

    pub(super) fn requester_can_bucket_action_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
        default_allowed: bool,
    ) -> bool {
        let decision = Self::bucket_policy_decision_for_bucket(requester, bucket, action, policy);
        Self::bucket_policy_allows_with_fallback(requester, bucket, decision, || default_allowed)
    }

    pub(super) fn requester_can_bucket_policy_action_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
        default_allowed: bool,
    ) -> bool {
        let decision = Self::bucket_policy_decision_for_bucket(requester, bucket, action, policy);
        Self::bucket_policy_allows_with_root_principal_bypass(requester, bucket, decision, || {
            default_allowed
        })
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
            Self::requester_can_bucket_owner_account_admin(requester, bucket),
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
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
        action: auth::PolicyAction,
    ) -> Result<ValidatedBucket, ServerError> {
        let info = self.checked_active_bucket_summary_for(bucket, expected_bucket_owner)?;
        let bucket_policy = self.cached_bucket_policy(&info)?;
        if Self::requester_can_bucket_action_with_bucket_policy(
            requester,
            &info,
            action,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(requester, &info),
        ) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn authorize_bucket_admin_or_bucket_policy_action_for<R>(
        &self,
        req: &R,
        action: auth::PolicyAction,
    ) -> Result<ValidatedBucket, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.authorize_bucket_admin_or_bucket_policy_action(
            req.requester(),
            req.bucket_name_typed(),
            req.expected_bucket_owner(),
            action,
        )
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

    pub(super) fn validate_expected_bucket_owner(
        bucket: BucketSummary,
        expected_bucket_owner: Option<&str>,
    ) -> Result<ValidatedBucket, ServerError> {
        Self::ensure_expected_bucket_owner(&bucket, expected_bucket_owner)?;
        Ok(ValidatedBucket(bucket))
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
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Err(ServerError::AccessControlListNotSupported);
        }
        if Self::blocks_public_acls(bucket.public_access_block.as_ref()) && acl.is_public() {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn requester_owner_identity(requester: &Requester) -> Option<OwnerIdentity> {
        if requester.is_anonymous() {
            return Some(OwnerIdentity::anonymous_upload());
        }
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
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            || (Self::is_bucket_owner_preferred(bucket.ownership_controls.as_ref())
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
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && !acl.is_supported_with_bucket_owner_enforced()
        {
            return Err(ServerError::AccessControlListNotSupported);
        }
        if Self::blocks_public_acls(bucket.public_access_block.as_ref()) && acl.is_public() {
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
                if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
                    && !Self::acl_grants_owner_full_control_only(
                        &bucket.owner_canonical_id,
                        acl_grants,
                    )
                {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                Self::ensure_supported_object_acl_grants(acl_grants)?;
                if Self::blocks_public_acls(bucket.public_access_block.as_ref())
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
        _acl_grants: &AclGrants,
    ) -> Result<(), ServerError> {
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

    pub(super) fn authorize_put_object_tags<'a>(
        &'a self,
        req: &PutObjectTagsRequest<'_>,
    ) -> Result<AuthorizedObjectTagsAccess<'a>, ServerError> {
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.authorize_object_tagging_access(
            &req.object,
            Self::put_object_tagging_policy_action(req.object.version_id),
            Some(req.tags),
        )?;
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        Ok(AuthorizedObjectTagsAccess {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
            pgs,
        })
    }

    pub(super) fn authorize_get_object_tags<'a>(
        &'a self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedObjectTagsAccess<'a>, ServerError> {
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.authorize_object_tagging_access(
            req,
            Self::get_object_tagging_policy_action(req.version_id),
            None,
        )?;
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        Ok(AuthorizedObjectTagsAccess {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
            pgs,
        })
    }

    pub(super) fn authorize_delete_object_tags<'a>(
        &'a self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedObjectTagsAccess<'a>, ServerError> {
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.authorize_object_tagging_access(
            req,
            Self::delete_object_tagging_policy_action(req.version_id),
            None,
        )?;
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        Ok(AuthorizedObjectTagsAccess {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
            pgs,
        })
    }

    pub(super) fn authorize_get_object_acl(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedGetObjectAcl, ServerError> {
        let LoadedObjectState {
            bucket_info,
            locked,
            ..
        } = self.authorize_object_acl_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.version_id,
            ObjectAclAuthorization::ReadWithPolicy(Self::get_object_acl_policy_action(
                req.version_id,
            )),
            req.expected_bucket_owner(),
        )?;
        let LockedReadObject { record: stored, .. } = locked;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        let result = if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
            let owner = Self::bucket_owner_identity(&bucket_info);
            GetObjectAclResult {
                owner_principal: owner.principal,
                owner_canonical_id: owner.canonical_id.clone(),
                acl_grants: AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(owner.canonical_id),
                    AclPermission::FullControl,
                )]),
                version_id: live.version_id,
            }
        } else {
            GetObjectAclResult {
                owner_principal: live.owner.principal.clone(),
                owner_canonical_id: live.owner.canonical_id.clone(),
                acl_grants: live.acl_grants.clone(),
                version_id: live.version_id,
            }
        };
        Ok(AuthorizedGetObjectAcl { result })
    }

    pub(super) fn authorize_put_object_acl<'a>(
        &'a self,
        req: &PutObjectAclRequest<'_>,
    ) -> Result<AuthorizedPutObjectAclUpdate<'a>, ServerError> {
        if req.object.requester().is_anonymous() {
            return Err(ServerError::AnonymousApiAccessDenied);
        }
        let LoadedObjectState {
            bucket_info,
            locked,
            ..
        } = self.authorize_object_acl_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            ObjectAclAuthorization::WriteWithPolicy {
                action: Self::put_object_acl_policy_action(req.object.version_id),
                policy_context: req.authorization_policy_context()?,
            },
            req.object.expected_bucket_owner(),
        )?;
        if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
            return Err(ServerError::AccessControlListNotSupported);
        }
        let LockedReadObject {
            record: stored,
            pgs,
        } = locked;
        let acl_grants = match &req.acl {
            PutObjectAclInput::Canned(acl) => {
                Self::ensure_put_object_acl_supported(&bucket_info, *acl)?;
                let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                Self::object_acl_grants_for_write(&bucket_info, &live.owner, *acl)
            }
            PutObjectAclInput::Grants(acl_grants) => {
                Self::ensure_supported_object_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        let public_read = Self::acl_grants_public_read(&acl_grants);
        if Self::blocks_public_acls(bucket_info.public_access_block.as_ref())
            && (Self::acl_grants_grant_public_read(&acl_grants)
                || Self::acl_grants_grant_public_write(&acl_grants))
        {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedPutObjectAclUpdate {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
            acl_grants,
            public_read,
            pgs,
        })
    }

    pub(super) fn authorize_put_object_retention<'a>(
        &'a self,
        req: &PutObjectRetentionRequest<'_>,
    ) -> Result<AuthorizedPutObjectRetention<'a>, ServerError> {
        let LoadedObjectState {
            bucket_info,
            bucket_policy,
            locked:
                LockedReadObject {
                    record: stored,
                    pgs,
                },
        } = self.authorize_object_lock_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            auth::PolicyAction::PutObjectRetention,
            req.object.expected_bucket_owner(),
        )?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        let can_bypass_governance =
            Self::requester_can_bypass_governance_retention_with_bucket_policy(
                req.object.requester(),
                &bucket_info,
                &stored,
                bucket_policy.as_deref(),
            )?;
        Self::validate_retention_update(
            live.object_lock.retention,
            req.retention,
            req.bypass_governance,
            can_bypass_governance,
        )?;
        Ok(AuthorizedPutObjectRetention {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: live.version_id,
            retention: req.retention,
            pgs,
        })
    }

    pub(super) fn authorize_get_object_retention(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedGetObjectRetention, ServerError> {
        let LoadedObjectState {
            locked: LockedReadObject { record: stored, .. },
            ..
        } = self.authorize_object_lock_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.version_id,
            auth::PolicyAction::GetObjectRetention,
            req.expected_bucket_owner(),
        )?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        Ok(AuthorizedGetObjectRetention {
            retention: live.object_lock.retention,
        })
    }

    pub(super) fn authorize_put_object_legal_hold<'a>(
        &'a self,
        req: &PutObjectLegalHoldRequest<'_>,
    ) -> Result<AuthorizedPutObjectLegalHold<'a>, ServerError> {
        let LoadedObjectState {
            locked:
                LockedReadObject {
                    record: stored,
                    pgs,
                },
            ..
        } = self.authorize_object_lock_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            auth::PolicyAction::PutObjectLegalHold,
            req.object.expected_bucket_owner(),
        )?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        Ok(AuthorizedPutObjectLegalHold {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: live.version_id,
            legal_hold: StoredLegalHoldStatus::from_legal_hold_status(Some(req.legal_hold)),
            pgs,
        })
    }

    pub(super) fn authorize_get_object_legal_hold(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedGetObjectLegalHold, ServerError> {
        let LoadedObjectState {
            locked: LockedReadObject { record: stored, .. },
            ..
        } = self.authorize_object_lock_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.version_id,
            auth::PolicyAction::GetObjectLegalHold,
            req.expected_bucket_owner(),
        )?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        Ok(AuthorizedGetObjectLegalHold {
            legal_hold: live.object_lock.legal_hold.as_legal_hold_status(),
        })
    }

    pub(super) fn authorize_bucket_read_requester(
        &self,
        requester: &Requester,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<ValidatedBucket, ServerError> {
        let info = self.checked_active_bucket_summary_for(bucket, expected_bucket_owner)?;
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

    pub(super) fn authorize_bucket_read_for<R>(
        &self,
        req: &R,
    ) -> Result<ValidatedBucket, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.authorize_bucket_read_requester(
            req.requester(),
            req.bucket_name_typed(),
            req.expected_bucket_owner(),
        )
    }

    pub(super) fn authorize_bucket_owner_account_admin_requester(
        &self,
        requester: &Requester,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<ValidatedBucket, ServerError> {
        let info = self.checked_active_bucket_summary_for(bucket, expected_bucket_owner)?;
        if Self::requester_can_bucket_owner_account_admin(requester, &info) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    pub(super) fn authorize_bucket_owner_account_admin_for<R>(
        &self,
        req: &R,
    ) -> Result<ValidatedBucket, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.authorize_bucket_owner_account_admin_requester(
            req.requester(),
            req.bucket_name_typed(),
            req.expected_bucket_owner(),
        )
    }

    pub(super) fn authorize_create_bucket(
        &self,
        req: &CreateBucketRequest,
    ) -> Result<AuthorizedCreateBucket, ServerError> {
        let owner_account = req.requester.account().ok_or(ServerError::AccessDenied)?;
        let locked_to_account_region =
            self.validate_create_bucket_namespace(&req.name, req.namespace, owner_account)?;
        if req.ownership == BucketObjectOwnership::BucketOwnerEnforced && req.acl.is_explicit() {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }
        let owner = OwnerIdentity::new(
            owner_account.principal(),
            owner_account.canonical_user_id().clone(),
        );
        let acl_grants = match &req.acl {
            CreateBucketAcl::DefaultPrivate => Self::owner_full_control_grants(&owner),
            CreateBucketAcl::Canned(acl) => Self::bucket_acl_grants_from_canned(&owner, *acl)?,
            CreateBucketAcl::Grants(acl_grants) => {
                Self::ensure_supported_bucket_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        if Self::acl_grants_grant_public_read(&acl_grants)
            || Self::acl_grants_grant_public_write(&acl_grants)
        {
            return Err(ServerError::InvalidBucketAclWithBlockPublicAccessError);
        }
        Ok(AuthorizedCreateBucket {
            name: req.name.clone(),
            requester: req.requester.clone(),
            owner,
            locked_to_account_region,
            acl: req.acl.clone(),
            ownership: req.ownership,
            object_lock_enabled: req.object_lock_enabled,
            acl_grants,
        })
    }

    pub(super) fn authorize_head_bucket(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedHeadBucket, ServerError> {
        let info = self.authorize_bucket_read_for(req)?;
        Ok(AuthorizedHeadBucket {
            bucket_info: info.into_inner(),
        })
    }

    pub(super) fn authorize_delete_bucket(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucket, ServerError> {
        let _bucket_info = self.authorize_bucket_owner_account_admin_for(req)?;
        Ok(AuthorizedDeleteBucket {
            name: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_cors(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourcePut, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketCors,
        )?;
        Ok(AuthorizedBucketSubresourcePut {
            bucket: req.bucket.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Cors,
            body: req.config.to_string(),
        })
    }

    pub(super) fn authorize_get_bucket_cors(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceGet, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetBucketCors,
        )?;
        Ok(AuthorizedBucketSubresourceGet {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Cors,
        })
    }

    /// Creates an internal authorization token for HTTP CORS evaluation.
    ///
    /// This intentionally bypasses normal bucket-config authorization because
    /// CORS preflight handling and actual-response header decoration need the
    /// stored CORS rules without turning those paths into authenticated bucket
    /// config reads.
    pub(super) fn authorize_load_bucket_cors_config_for(
        &self,
        name: &BucketName,
    ) -> AuthorizedBucketSubresourceGet {
        AuthorizedBucketSubresourceGet {
            bucket: name.clone(),
            kind: storage::BucketSubresourceKind::Cors,
        }
    }

    pub(super) fn authorize_delete_bucket_cors(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::PutBucketCors,
        )?;
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Cors,
        })
    }

    pub(super) fn authorize_put_bucket_tagging(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourcePut, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketTagging,
        )?;
        Ok(AuthorizedBucketSubresourcePut {
            bucket: req.bucket.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Tagging,
            body: req.config.to_string(),
        })
    }

    pub(super) fn authorize_get_bucket_tagging(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceGet, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetBucketTagging,
        )?;
        Ok(AuthorizedBucketSubresourceGet {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Tagging,
        })
    }

    pub(super) fn authorize_delete_bucket_tagging(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::PutBucketTagging,
        )?;
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Tagging,
        })
    }

    pub(super) fn authorize_put_bucket_policy(
        &self,
        req: &PutBucketPolicyRequest<'_>,
    ) -> Result<AuthorizedPutBucketPolicy, ServerError> {
        let bucket_info = self.checked_active_bucket_summary_for(
            req.bucket.name_typed(),
            req.bucket.expected_bucket_owner,
        )?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_bucket_policy_action_with_bucket_policy(
            &req.bucket.requester,
            &bucket_info,
            auth::PolicyAction::PutBucketPolicy,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(&req.bucket.requester, &bucket_info),
        ) {
            return Err(ServerError::AccessDenied);
        }
        let parsed_policy =
            auth::parse_bucket_policy(req.config).map_err(|e| ServerError::MalformedPolicy {
                reason: e.reason().to_string(),
            })?;
        parsed_policy
            .validate_evaluable_object_conditions()
            .map_err(|e| ServerError::MalformedPolicy {
                reason: e.reason().to_string(),
            })?;
        let normalized_policy = parsed_policy.normalized_json();
        if normalized_policy.len() > auth::bucket_policy::MAX_BUCKET_POLICY_BYTES {
            return Err(ServerError::MalformedPolicy {
                reason: format!(
                    "Normalized policy document exceeds the maximum allowed size of {} bytes",
                    auth::bucket_policy::MAX_BUCKET_POLICY_BYTES
                ),
            });
        }
        let policy_is_public = parsed_policy.is_public();
        if Self::blocks_public_policy(bucket_info.public_access_block.as_ref()) && policy_is_public
        {
            return Err(ServerError::BlockPublicPolicyAccessDenied {
                requester_principal: Self::requester_principal_required(&req.bucket.requester)?
                    .to_string(),
                bucket: req.bucket.name.to_string(),
            });
        }
        Ok(AuthorizedPutBucketPolicy {
            bucket: req.bucket.name_typed().clone(),
            body: normalized_policy,
            parsed_policy: Arc::new(parsed_policy),
            policy_is_public,
        })
    }

    pub(super) fn authorize_get_bucket_policy(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceGet, ServerError> {
        let bucket_info =
            self.checked_active_bucket_summary_for(req.name_typed(), req.expected_bucket_owner)?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_bucket_policy_action_with_bucket_policy(
            &req.requester,
            &bucket_info,
            auth::PolicyAction::GetBucketPolicy,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(&req.requester, &bucket_info),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedBucketSubresourceGet {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Policy,
        })
    }

    pub(super) fn authorize_delete_bucket_policy(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let bucket_info =
            self.checked_active_bucket_summary_for(req.name_typed(), req.expected_bucket_owner)?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_bucket_policy_action_with_bucket_policy(
            &req.requester,
            &bucket_info,
            auth::PolicyAction::DeleteBucketPolicy,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(&req.requester, &bucket_info),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Policy,
        })
    }

    pub(super) fn authorize_put_bucket_public_access_block(
        &self,
        req: &PutBucketPublicAccessBlockRequest<'_>,
    ) -> Result<AuthorizedPutBucketPublicAccessBlock, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketPublicAccessBlock,
        )?;
        Ok(AuthorizedPutBucketPublicAccessBlock {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    pub(super) fn authorize_get_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let bucket_info = self.active_bucket_summary_for(req)?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_get_bucket_public_access_block_with_bucket_policy(
            &req.requester,
            &bucket_info,
            bucket_policy.as_deref(),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_delete_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::PutBucketPublicAccessBlock,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_ownership_controls(
        &self,
        req: &PutBucketOwnershipControlsRequest<'_>,
    ) -> Result<AuthorizedPutBucketOwnershipControls, ServerError> {
        let bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketOwnershipControls,
        )?;
        if Self::is_bucket_owner_enforced(Some(&req.config))
            && !Self::acl_grants_owner_full_control_only(
                &bucket_info.owner_canonical_id,
                &bucket_info.acl_grants,
            )
        {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }
        Ok(AuthorizedPutBucketOwnershipControls {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    pub(super) fn authorize_get_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetBucketOwnershipControls,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_delete_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::PutBucketOwnershipControls,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_lifecycle(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedPutBucketLifecycle, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutLifecycleConfiguration,
        )?;
        let parsed_config = Arc::new(
            storage::parse_lifecycle_configuration_xml(req.config.as_bytes()).map_err(|error| {
                match error {
                    storage::LifecycleConfigError::MalformedXml { reason } => {
                        ServerError::MalformedXML { reason }
                    }
                    storage::LifecycleConfigError::InvalidRequest { reason } => {
                        ServerError::InvalidRequest { reason }
                    }
                    storage::LifecycleConfigError::InvalidArgument { reason } => {
                        ServerError::InvalidArgument { reason }
                    }
                    storage::LifecycleConfigError::NotImplemented { feature } => {
                        ServerError::NotImplemented { feature }
                    }
                }
            })?,
        );
        Ok(AuthorizedPutBucketLifecycle {
            bucket: req.bucket.name_typed().clone(),
            body: req.config.to_string(),
            parsed_config,
        })
    }

    pub(super) fn authorize_get_bucket_lifecycle(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceGet, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetLifecycleConfiguration,
        )?;
        Ok(AuthorizedBucketSubresourceGet {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Lifecycle,
        })
    }

    /// Creates an internal authorization token for lifecycle cache fills.
    ///
    /// This intentionally bypasses request auth because the coordinator is
    /// loading already-authoritative stored state for cache population.
    pub(super) fn authorize_load_bucket_lifecycle_for(
        &self,
        name: &BucketName,
    ) -> AuthorizedBucketSubresourceGet {
        AuthorizedBucketSubresourceGet {
            bucket: name.clone(),
            kind: storage::BucketSubresourceKind::Lifecycle,
        }
    }

    pub(super) fn authorize_delete_bucket_lifecycle(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::PutLifecycleConfiguration,
        )?;
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Lifecycle,
        })
    }

    pub(super) fn authorize_put_bucket_encryption(
        &self,
        req: &PutBucketEncryptionRequest<'_>,
    ) -> Result<AuthorizedPutBucketEncryption, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutEncryptionConfiguration,
        )?;
        Ok(AuthorizedPutBucketEncryption {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
            effective_config: req.config.effective(),
        })
    }

    pub(super) fn authorize_put_bucket_versioning(
        &self,
        req: &PutBucketVersioningRequest<'_>,
    ) -> Result<AuthorizedPutBucketVersioning, ServerError> {
        let bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketVersioning,
        )?;
        if bucket_info.object_lock.enabled && req.state != BucketVersioningState::Enabled {
            return Err(ServerError::InvalidBucketState);
        }
        Ok(AuthorizedPutBucketVersioning {
            bucket: req.bucket.name_typed().clone(),
            state: req.state,
        })
    }

    pub(super) fn authorize_get_bucket_versioning(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketVersioning, ServerError> {
        let info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetBucketVersioning,
        )?;
        Ok(AuthorizedGetBucketVersioning {
            state: info.versioning,
        })
    }

    pub(super) fn authorize_list_objects_v2(
        &self,
        req: &ListObjectsV2Request<'_>,
    ) -> Result<AuthorizedListObjectsV2, ServerError> {
        let bucket_info = self.checked_active_bucket_summary_for(
            req.bucket.name_typed(),
            req.bucket.expected_bucket_owner(),
        )?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_list_bucket_with_bucket_policy(
            &req.bucket.requester,
            &bucket_info,
            bucket_policy.as_deref(),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListObjectsV2 {
            bucket_info: bucket_info.into_inner(),
        })
    }

    pub(super) fn authorize_list_buckets(
        &self,
        req: &ListBucketsRequest,
    ) -> Result<AuthorizedListBuckets, ServerError> {
        let requester = req.requester.account().ok_or(ServerError::AccessDenied)?;
        Ok(AuthorizedListBuckets {
            owner_canonical_id: requester.canonical_user_id().clone(),
        })
    }

    pub(super) fn authorize_list_object_versions(
        &self,
        req: &ListObjectVersionsRequest<'_>,
    ) -> Result<AuthorizedListObjectVersions, ServerError> {
        let bucket_info = self.checked_active_bucket_summary_for(
            req.bucket.name_typed(),
            req.bucket.expected_bucket_owner(),
        )?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_bucket_action_with_bucket_policy(
            &req.bucket.requester,
            &bucket_info,
            auth::PolicyAction::ListBucketVersions,
            bucket_policy.as_deref(),
            Self::requester_can_read_bucket(
                &req.bucket.requester,
                &bucket_info,
                &bucket_info.owner_principal,
                &bucket_info.acl_grants,
                Self::effective_public_read(&bucket_info),
            ),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListObjectVersions {
            bucket_info: bucket_info.into_inner(),
        })
    }

    pub(super) fn authorize_list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsRequest<'_>,
    ) -> Result<AuthorizedListMultipartUploads, ServerError> {
        let bucket_info = self.checked_active_bucket_summary_for(
            req.bucket.name_typed(),
            req.bucket.expected_bucket_owner(),
        )?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_bucket_action_with_bucket_policy(
            &req.bucket.requester,
            &bucket_info,
            auth::PolicyAction::ListBucketMultipartUploads,
            bucket_policy.as_deref(),
            Self::requester_can_read_bucket(
                &req.bucket.requester,
                &bucket_info,
                &bucket_info.owner_principal,
                &bucket_info.acl_grants,
                Self::effective_public_read(&bucket_info),
            ),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListMultipartUploads {
            bucket: req.bucket.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_object_lock_configuration(
        &self,
        req: &PutBucketObjectLockConfigurationRequest<'_>,
    ) -> Result<AuthorizedPutBucketObjectLockConfiguration, ServerError> {
        let bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketObjectLockConfiguration,
        )?;
        if bucket_info.versioning != BucketVersioningState::Enabled {
            return Err(ServerError::InvalidBucketState);
        }

        let final_enabled =
            bucket_info.object_lock.enabled || req.config.object_lock_enabled.is_some();
        if !final_enabled {
            return Err(ServerError::InvalidRequest {
                reason: "Object Lock must be enabled before configuring this bucket".to_string(),
            });
        }

        Ok(AuthorizedPutBucketObjectLockConfiguration {
            bucket: req.bucket.name_typed().clone(),
            config: BucketObjectLockConfig {
                enabled: true,
                default_retention: req.config.default_retention,
            },
        })
    }

    pub(super) fn authorize_get_bucket_encryption(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketEncryption, ServerError> {
        let info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetEncryptionConfiguration,
        )?;
        Ok(AuthorizedGetBucketEncryption {
            config: info.encryption,
        })
    }

    pub(super) fn authorize_delete_bucket_encryption(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucketEncryption, ServerError> {
        let _bucket_info = self.authorize_bucket_admin_or_bucket_policy_action_for(
            req,
            auth::PolicyAction::PutEncryptionConfiguration,
        )?;
        Ok(AuthorizedDeleteBucketEncryption {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_get_bucket_object_lock_configuration(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketObjectLockConfiguration, ServerError> {
        let info = self.active_bucket_summary_for(req)?;
        let bucket_policy = self.cached_bucket_policy(&info)?;
        if !Self::requester_can_get_bucket_object_lock_configuration_with_bucket_policy(
            &req.requester,
            &info,
            bucket_policy.as_deref(),
        ) {
            return Err(ServerError::AccessDenied);
        }
        if !info.object_lock.enabled {
            return Err(ServerError::ObjectLockConfigurationNotFound {
                bucket: req.name.to_string(),
            });
        }
        Ok(AuthorizedGetBucketObjectLockConfiguration {
            config: info.object_lock,
        })
    }

    pub(super) fn authorize_get_bucket_policy_status(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketPolicyStatus, ServerError> {
        let bucket_info = self.active_bucket_summary_for(req)?;
        if !bucket_info.bucket_policy_present {
            if !Self::requester_can_bucket_admin(&req.requester, &bucket_info.owner_principal) {
                return Err(ServerError::AccessDenied);
            }
            return Err(ServerError::NoSuchBucketPolicy {
                bucket: req.name.to_string(),
            });
        }
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        if !Self::requester_can_get_bucket_policy_status_with_bucket_policy(
            &req.requester,
            &bucket_info,
            bucket_policy.as_deref(),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketPolicyStatus {
            is_public: bucket_info.bucket_policy_public,
        })
    }

    pub(super) fn authorize_get_bucket_acl(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketAcl, ServerError> {
        let bucket =
            self.checked_active_bucket_summary_for(req.name_typed(), req.expected_bucket_owner())?;
        let bucket_policy = self.cached_bucket_policy(&bucket)?;
        if !Self::requester_can_bucket_action_with_bucket_policy(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketAcl,
            bucket_policy.as_deref(),
            Self::requester_can_read_bucket_acl(&req.requester, &bucket),
        ) {
            return Err(ServerError::AccessDenied);
        }
        let result = if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            let owner = Self::bucket_owner_identity(&bucket);
            GetBucketAclResult {
                owner_principal: owner.principal,
                owner_canonical_id: owner.canonical_id.clone(),
                acl_grants: AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(owner.canonical_id),
                    AclPermission::FullControl,
                )]),
            }
        } else {
            let bucket = bucket.into_inner();
            GetBucketAclResult {
                owner_principal: bucket.owner_principal,
                owner_canonical_id: bucket.owner_canonical_id,
                acl_grants: bucket.acl_grants,
            }
        };
        Ok(AuthorizedGetBucketAcl { result })
    }

    pub(super) fn authorize_put_bucket_acl(
        &self,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<AuthorizedPutBucketAcl, ServerError> {
        let bucket_info = self.checked_active_bucket_summary_for(
            req.bucket.name_typed(),
            req.bucket.expected_bucket_owner(),
        )?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        let policy_decision = Self::bucket_policy_decision_for_bucket_with_context(
            &req.bucket.requester,
            &bucket_info,
            auth::PolicyAction::PutBucketAcl,
            req.authorization_policy_context()?,
            bucket_policy.as_deref(),
        );
        if !Self::bucket_policy_allows_with_fallback(
            &req.bucket.requester,
            &bucket_info,
            policy_decision,
            || Self::requester_can_write_bucket_acl(&req.bucket.requester, &bucket_info),
        ) {
            return Err(ServerError::AccessDenied);
        }
        let owner = Self::bucket_owner_identity(&bucket_info);
        let acl_grants = match &req.acl {
            PutBucketAclInput::Canned(acl) => {
                Self::ensure_put_bucket_acl_supported(&bucket_info, *acl)?;
                Self::bucket_acl_grants_from_canned(&owner, *acl)?
            }
            PutBucketAclInput::Grants(acl_grants) => {
                if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                Self::ensure_supported_bucket_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        let public_read = Self::acl_grants_public_read(&acl_grants);
        let public_write = Self::acl_grants_public_write(&acl_grants);
        if Self::blocks_public_acls(bucket_info.public_access_block.as_ref())
            && (Self::acl_grants_grant_public_read(&acl_grants)
                || Self::acl_grants_grant_public_write(&acl_grants))
        {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedPutBucketAcl {
            bucket: req.bucket.name_typed().clone(),
            acl_grants,
            public_read,
            public_write,
        })
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
        Self::requester_can_object_action_with_bucket_policy(
            requester,
            bucket,
            object,
            auth::PolicyAction::BypassGovernanceRetention,
            PutObjectPolicyContext::default(),
            policy,
            || Self::requester_can_bypass_governance_retention(requester, bucket),
        )
    }

    pub(super) fn requester_can_bypass_governance_retention_for_missing_version_with_bucket_policy(
        requester: &Requester,
        bucket: &BucketSummary,
        key: &str,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        Self::requester_can_put_object_action_with_bucket_policy(
            requester,
            bucket,
            key,
            auth::PolicyAction::BypassGovernanceRetention,
            PutObjectPolicyContext::default(),
            policy,
            Self::requester_can_bypass_governance_retention(requester, bucket),
        )
    }

    pub(super) fn authorize_put_object_write(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let key = req.object.key();
        self.with_bucket_write_reservation_for(&req.object, |bucket_info| {
            let existing_object = self.put_target_existing_live_object(
                req.object.bucket.name_typed(),
                req.object.key_typed(),
            )?;
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
                bucket: req.object.bucket.name_typed().clone(),
                key: req.object.key_typed().clone(),
                requester: req.object.requester().clone(),
                expected_bucket_owner: req.object.expected_bucket_owner().map(str::to_string),
                acl: AuthorizedPutObjectWriteAcl::from_parsed(&req.acl),
                requested_object_lock: req.object_lock,
                tags: req.tags.map(str::to_string),
                write_encryption,
            })
        })
    }

    fn authorize_delete_object_impl<'a>(
        &'a self,
        object: &ObjectVersionRequest<'_>,
        bypass_governance: bool,
    ) -> Result<AuthorizedDeleteObject<'a>, ServerError> {
        let bucket = object.bucket_name_typed();
        let key = object.key_typed();
        let key_str = key.as_str();
        let request_version_id = object.version_id();
        let requester = object.requester();
        let bucket_info =
            self.checked_active_bucket_summary_for(bucket, object.expected_bucket_owner())?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;

        match (bucket_info.versioning, request_version_id) {
            (BucketVersioningState::Disabled, _) => {
                let locked = match self.lock_object_pgs_for_read_typed(bucket, key, None) {
                    Ok(locked) => locked,
                    Err(ServerError::ObjectNotFound { .. }) => {
                        if !Self::requester_can_delete_object_with_bucket_policy(
                            requester,
                            &bucket_info,
                            key_str,
                            None,
                            Self::delete_object_policy_action(None),
                            bucket_policy.as_deref(),
                        )? {
                            return Err(ServerError::AccessDenied);
                        }
                        return Ok(AuthorizedDeleteObject::UnversionedMissing);
                    }
                    Err(other) => return Err(other),
                };

                if !Self::requester_can_delete_object_with_bucket_policy(
                    requester,
                    &bucket_info,
                    key_str,
                    Some(&locked.record),
                    Self::delete_object_policy_action(None),
                    bucket_policy.as_deref(),
                )? {
                    return Err(ServerError::AccessDenied);
                }

                let LockedReadObject {
                    record: stored,
                    pgs,
                } = locked;
                Ok(AuthorizedDeleteObject::UnversionedStored {
                    bucket: object.bucket_name_typed().clone(),
                    key: object.key_typed().clone(),
                    stored,
                    pgs,
                })
            }
            (_, Some(version_id)) => {
                let locked = match self.lock_object_pgs_for_read_typed(
                    bucket,
                    key,
                    Some(version_id),
                ) {
                    Ok(locked) => locked,
                    Err(
                        ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. },
                    ) => {
                        if !Self::requester_can_delete_object_with_bucket_policy(
                            requester,
                            &bucket_info,
                            key_str,
                            None,
                            Self::delete_object_policy_action(Some(version_id)),
                            bucket_policy.as_deref(),
                        )? {
                            return Err(ServerError::AccessDenied);
                        }
                        if bucket_info.object_lock.enabled
                            && bypass_governance
                            && !Self::requester_can_bypass_governance_retention_for_missing_version_with_bucket_policy(
                                requester,
                                &bucket_info,
                                key_str,
                                bucket_policy.as_deref(),
                            )?
                        {
                            return Err(ServerError::AccessDenied);
                        }
                        return Ok(AuthorizedDeleteObject::SpecificVersionMissing { version_id });
                    }
                    Err(other) => return Err(other),
                };

                if !Self::requester_can_delete_object_with_bucket_policy(
                    requester,
                    &bucket_info,
                    key_str,
                    Some(&locked.record),
                    Self::delete_object_policy_action(Some(version_id)),
                    bucket_policy.as_deref(),
                )? {
                    return Err(ServerError::AccessDenied);
                }

                if let StoredObject::Live(record) = &locked.record {
                    let can_bypass_governance =
                        Self::requester_can_bypass_governance_retention_with_bucket_policy(
                            requester,
                            &bucket_info,
                            &locked.record,
                            bucket_policy.as_deref(),
                        )?;
                    Self::validate_delete_against_object_lock(
                        record.object_lock,
                        bypass_governance,
                        can_bypass_governance,
                        Self::current_unix_seconds()?,
                    )?;
                }

                let LockedReadObject {
                    record: stored,
                    pgs,
                } = locked;
                Ok(AuthorizedDeleteObject::SpecificVersionStored {
                    bucket: object.bucket_name_typed().clone(),
                    key: object.key_typed().clone(),
                    version_id,
                    stored,
                    pgs,
                })
            }
            (_, None) => {
                let owner =
                    Self::effective_object_owner(&bucket_info, requester, PutObjectAcl::None);
                match self.lock_object_pgs_for_read_typed(bucket, key, None) {
                    Ok(locked) => {
                        if !Self::requester_can_delete_object_with_bucket_policy(
                            requester,
                            &bucket_info,
                            key_str,
                            Some(&locked.record),
                            Self::delete_object_policy_action(None),
                            bucket_policy.as_deref(),
                        )? {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                            bucket: object.bucket_name_typed().clone(),
                            key: object.key_typed().clone(),
                            owner,
                            current: Some(locked),
                        })
                    }
                    Err(ServerError::ObjectNotFound { .. }) => {
                        if !Self::requester_can_delete_object_with_bucket_policy(
                            requester,
                            &bucket_info,
                            key_str,
                            None,
                            Self::delete_object_policy_action(None),
                            bucket_policy.as_deref(),
                        )? {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                            bucket: object.bucket_name_typed().clone(),
                            key: object.key_typed().clone(),
                            owner,
                            current: None,
                        })
                    }
                    Err(other) => Err(other),
                }
            }
        }
    }

    pub(super) fn authorize_delete_object<'a>(
        &'a self,
        req: &DeleteObjectRequest<'_>,
    ) -> Result<AuthorizedDeleteObject<'a>, ServerError> {
        self.authorize_delete_object_impl(&req.object, req.bypass_governance)
    }

    pub(super) fn authorize_delete_objects_entry<'a>(
        &'a self,
        req: &DeleteObjectsRequest<'_>,
        entry: &DeleteEntry,
    ) -> Result<AuthorizedDeleteObject<'a>, ServerError> {
        let object = ObjectVersionRequest::from_object(
            ObjectRequest::new(
                req.bucket.name_typed().clone(),
                entry.key.clone(),
                req.bucket.requester.clone(),
                req.expected_bucket_owner(),
            ),
            entry.version_id,
        );
        self.authorize_delete_object_impl(&object, req.bypass_governance)
    }

    pub(super) fn authorize_copy_object<'a>(
        &'a self,
        req: &CopyObjectRequest<'_>,
    ) -> Result<AuthorizedCopyObject<'a>, ServerError> {
        let src_version_id = req.source.version_id;
        let requester = &req.destination.bucket.requester;
        let acl = req.acl.clone();
        let copy_source_policy_value = req.source.version_id.map_or_else(
            || format!("{}/{}", req.source.bucket, req.source.key),
            |version_id| {
                format!(
                    "{}/{}?versionId={}",
                    req.source.bucket, req.source.key, version_id
                )
            },
        );
        let metadata_directive = req.directive.policy_condition_value();
        let canned_acl = acl.policy_condition_value();
        let request_object_tags_xml = match &req.tagging {
            TaggingDirective::Copy => None,
            TaggingDirective::Replace(tags) => *tags,
        };
        let copy_policy_context = PutObjectPolicyContext::new(
            Some(copy_source_policy_value.as_str()),
            metadata_directive,
            canned_acl,
        )
        .with_acl_grant_headers(
            req.policy_context.grant_read,
            req.policy_context.grant_write,
            req.policy_context.grant_read_acp,
            req.policy_context.grant_write_acp,
            req.policy_context.grant_full_control,
        )
        .with_request_object_tags_xml(request_object_tags_xml);
        let dst_policy_context = req
            .destination_encryption
            .with_policy_context(copy_policy_context);
        let destination = self.authorize_put_object_write(&AuthorizePutObjectRequest {
            object: ObjectRequest::new(
                req.destination.bucket.name_typed().clone(),
                req.destination.key_typed().clone(),
                requester.clone(),
                req.expected_bucket_owner(),
            ),
            acl,
            policy_context: dst_policy_context,
            object_lock: req.object_lock,
            tags: request_object_tags_xml,
            encryption: req.destination_encryption,
        })?;
        let source = self.authorize_object_read(
            requester,
            &req.source.bucket,
            &req.source.key,
            src_version_id,
            req.source.expected_bucket_owner(),
            Self::get_object_policy_action(src_version_id),
        )?;

        Ok(AuthorizedCopyObject {
            source,
            destination,
        })
    }

    pub(super) fn authorize_create_multipart_upload(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        let key = req.object.key();
        let policy_context = req.effective_policy_context();
        self.with_bucket_write_reservation_for(&req.object, |bucket_info| {
            if req.object.requester().is_anonymous() {
                return Err(ServerError::AccessDenied);
            }
            let existing_object = self.put_target_existing_live_object(
                req.object.bucket.name_typed(),
                req.object.key_typed(),
            )?;
            let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
            if !Self::requester_can_put_object_with_bucket_policy(
                req.object.requester(),
                &bucket_info,
                key,
                policy_context,
                bucket_policy.as_deref(),
                existing_object.as_ref(),
            )? {
                return Err(ServerError::AccessDenied);
            }
            Self::ensure_sse_c_allowed(
                &bucket_info,
                req.encryption.sse_customer_request().is_some(),
            )?;
            let write_encryption = self.resolve_write_encryption(&bucket_info, req.encryption)?;
            Self::ensure_put_object_write_acl_supported(&bucket_info, &req.acl)?;
            let initiator = Self::requester_owner_identity(req.object.requester());
            let owner =
                Self::effective_put_object_owner(&bucket_info, req.object.requester(), &req.acl);
            let acl_grants = Self::object_acl_grants_for_put_object(&bucket_info, &owner, &req.acl);
            let public_read = Self::acl_grants_public_read(&acl_grants);
            Self::validate_requested_object_lock_state(&bucket_info, req.object_lock)?;
            Self::ensure_sse_c_allowed(&bucket_info, write_encryption.is_sse_customer())?;

            Ok(AuthorizedCreateMultipartUpload {
                bucket_info: bucket_info.into_inner(),
                bucket: req.object.bucket.name_typed().clone(),
                key: req.object.key_typed().clone(),
                tags: req.tags.map(str::to_string),
                checksum: req.checksum,
                initiator,
                owner,
                acl_grants,
                public_read,
                object_lock: req.object_lock,
                write_encryption,
            })
        })
    }

    pub(super) fn authorize_upload_part_copy<'a>(
        &'a self,
        req: &UploadPartCopyRequest<'_>,
    ) -> Result<AuthorizedUploadPartCopy<'a>, ServerError> {
        let src_version_id = req.source.version_id;
        let dst_bucket = req.upload.bucket_name_typed();
        let dst_key = req.upload.key_typed();
        let upload_id = req.upload.upload_id;
        let part_number = req.part_number;
        let requester = req.upload.requester();
        let copy_source_policy_value = req.source.version_id.map_or_else(
            || format!("{}/{}", req.source.bucket, req.source.key),
            |version_id| {
                format!(
                    "{}/{}?versionId={}",
                    req.source.bucket, req.source.key, version_id
                )
            },
        );
        let policy_context =
            PutObjectPolicyContext::new(Some(copy_source_policy_value.as_str()), None, None)
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm));

        let dst_bucket_info =
            self.checked_active_bucket_summary_for(dst_bucket, req.expected_bucket_owner())?;
        let dst_bucket_policy = self.cached_bucket_policy(&dst_bucket_info)?;
        {
            let dst_meta_pg = self
                .storage_node
                .get_pg(self.object_pg_id_for(dst_bucket, dst_key))?;
            let dst_upload = dst_meta_pg.get_multipart_upload(upload_id)?;
            if dst_upload.bucket != dst_bucket.as_str() || dst_upload.key != dst_key.as_str() {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            if dst_upload.state != UploadState::InProgress {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
                policy_context,
                &dst_upload,
            );
            if !Self::requester_can_write_multipart_upload_with_bucket_policy(
                requester,
                &dst_bucket_info,
                &dst_upload,
                policy_context,
                dst_bucket_policy.as_deref(),
            )? {
                return Err(ServerError::AccessDenied);
            }
            Self::ensure_sse_c_allowed(
                &dst_bucket_info,
                dst_upload.encryption.uses_sse_customer_headers(),
            )?;
            self.ensure_write_encryption_supported(&dst_upload.encryption)?;
            let sse_customer = self.prepare_existing_sse_customer_write_context(
                &dst_upload.encryption,
                req.sse_customer,
                SseCustomerSegmentScope::multipart_part(part_number)?,
                true,
            )?;
            drop(dst_meta_pg);

            let source = self.authorize_object_read(
                requester,
                &req.source.bucket,
                &req.source.key,
                src_version_id,
                req.source.expected_bucket_owner(),
                Self::get_object_policy_action(src_version_id),
            )?;
            Ok(AuthorizedUploadPartCopy {
                source,
                destination: AuthorizedMultipartPartWrite {
                    bucket: req.upload.bucket_name_typed().clone(),
                    key: req.upload.key_typed().clone(),
                    upload_id: upload_id.to_string(),
                    part_number,
                    upload: dst_upload,
                    sse_customer,
                },
            })
        }
    }

    pub(super) fn authorize_begin_stream_part<'a>(
        &'a self,
        req: &BeginStreamPartRequest<'_>,
    ) -> Result<AuthorizedBeginStreamPart<'a>, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id;
        let part_number = req.part_number;
        let policy_context = req.effective_policy_context();
        let bucket_info =
            self.checked_active_bucket_summary_for(bucket, req.expected_bucket_owner())?;
        let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
        let meta_pg = self
            .storage_node
            .get_pg(self.object_pg_id_for(bucket, key))?;
        let upload = meta_pg.get_multipart_upload(upload_id)?;
        if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        if upload.state != UploadState::InProgress {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        let policy_context =
            Self::with_multipart_upload_managed_encryption_policy_context(policy_context, &upload);
        if !Self::requester_can_write_multipart_upload_with_bucket_policy(
            req.upload.requester(),
            &bucket_info,
            &upload,
            policy_context,
            bucket_policy.as_deref(),
        )? {
            return Err(ServerError::AccessDenied);
        }
        Self::ensure_sse_c_allowed(&bucket_info, upload.encryption.uses_sse_customer_headers())?;
        self.ensure_write_encryption_supported(&upload.encryption)?;
        let sse_customer = self.prepare_existing_sse_customer_write_context(
            &upload.encryption,
            req.sse_customer,
            SseCustomerSegmentScope::multipart_part(part_number)?,
            true,
        )?;

        Ok(AuthorizedBeginStreamPart {
            bucket: req.upload.bucket_name_typed().clone(),
            key: req.upload.key_typed().clone(),
            upload_id: upload_id.to_string(),
            part_number,
            upload,
            sse_customer,
            meta_pg,
        })
    }

    pub(super) fn authorize_complete_multipart_upload(
        &self,
        req: &CompleteMultipartUploadRequest<'_>,
    ) -> Result<AuthorizedCompleteMultipartUpload, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id;
        self.with_bucket_write_reservation_for(&req.upload, |bucket_info| {
            let bucket_policy = self.cached_bucket_policy(&bucket_info)?;
            let meta_pg = self
                .storage_node
                .get_pg(self.object_pg_id_for(bucket, key))?;
            let upload = meta_pg.get_multipart_upload(upload_id)?;
            if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            if upload.state != UploadState::InProgress {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
                PutObjectPolicyContext::default().with_sse_customer_algorithm(
                    req.sse_customer.map(SseCustomerRequest::algorithm),
                ),
                &upload,
            );
            if !Self::requester_can_write_multipart_upload_with_bucket_policy(
                req.upload.requester(),
                &bucket_info,
                &upload,
                policy_context,
                bucket_policy.as_deref(),
            )? {
                return Err(ServerError::AccessDenied);
            }
            Self::ensure_sse_c_allowed(
                &bucket_info,
                upload.encryption.uses_sse_customer_headers(),
            )?;
            let multipart_write_encryption = self.resume_write_encryption(
                &upload.encryption,
                req.sse_customer,
                SseCustomerSegmentScope::object(),
                false,
            )?;
            drop(meta_pg);

            Ok(AuthorizedCompleteMultipartUpload {
                bucket_info: bucket_info.into_inner(),
                bucket: req.upload.bucket_name_typed().clone(),
                key: req.upload.key_typed().clone(),
                upload_id: upload_id.to_string(),
                upload,
                multipart_write_encryption,
            })
        })
    }

    pub(super) fn authorize_abort_multipart_upload(
        &self,
        req: &MultipartObjectRequest<'_>,
    ) -> Result<AuthorizedAbortMultipartUpload, ServerError> {
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let upload_id = req.upload_id;
        let bucket_info =
            self.checked_active_bucket_summary_for(bucket, req.expected_bucket_owner())?;
        let meta_pg = self
            .storage_node
            .get_pg(self.object_pg_id_for(bucket, key))?;
        let authorized = match meta_pg.get_multipart_upload(upload_id) {
            Ok(upload) => {
                if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
                    return Err(ServerError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                if !Self::requester_can_manage_multipart_upload(
                    req.object.requester(),
                    &bucket_info,
                    &upload,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                AuthorizedAbortMultipartUpload::InProgress {
                    bucket: req.bucket_name_typed().clone(),
                    key: req.key_typed().clone(),
                    upload_id: upload_id.to_string(),
                }
            }
            Err(storage::MetadataError::NoSuchUpload { .. }) => {
                let Some(completed) = meta_pg.get_completed_multipart_upload(upload_id)? else {
                    return Err(ServerError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                };
                if completed.bucket != bucket.as_str() || completed.key != key.as_str() {
                    return Err(ServerError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                if !Self::requester_can_manage_completed_multipart_upload(
                    req.object.requester(),
                    &bucket_info,
                    &completed,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                AuthorizedAbortMultipartUpload::Completed
            }
            Err(error) => return Err(ServerError::Metadata(error)),
        };
        drop(meta_pg);
        Ok(authorized)
    }

    pub(super) fn authorize_list_parts<'a>(
        &'a self,
        req: &ListPartsRequest<'_>,
    ) -> Result<AuthorizedListParts<'a>, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id;
        let bucket_info =
            self.checked_active_bucket_summary_for(bucket, req.expected_bucket_owner())?;
        let meta_pg = self
            .storage_node
            .get_pg(self.object_pg_id_for(bucket, key))?;
        let upload = meta_pg.get_multipart_upload(upload_id)?;
        if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        if upload.state != UploadState::InProgress {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        if !Self::requester_can_manage_multipart_upload(
            req.upload.requester(),
            &bucket_info,
            &upload,
        ) {
            return Err(ServerError::AccessDenied);
        }

        Ok(AuthorizedListParts {
            bucket_info: bucket_info.into_inner(),
            key: req.upload.key_typed().clone(),
            upload,
            meta_pg,
        })
    }

    fn load_locked_object_state<'a>(
        &'a self,
        req: ObjectStateLoadRequest<'_>,
    ) -> Result<LoadedObjectState<'a>, ServerError> {
        let fast_bucket_info = match self.storage_node.get_bucket_fast_path(req.bucket) {
            Some(info) if info.state == BucketState::Active => {
                Some(Self::validate_expected_bucket_owner(
                    Self::bucket_summary_fast(info),
                    req.expected_bucket_owner,
                )?)
            }
            Some(_) => {
                return Err(ServerError::BucketNotFound {
                    name: req.bucket.to_string(),
                });
            }
            None => None,
        };
        let fresh_bucket_policy = match req.policy_requirement {
            ObjectBucketPolicyRequirement::Required => fast_bucket_info
                .as_ref()
                .and_then(|bucket_info| self.cached_bucket_policy_if_fresh(bucket_info)),
        };
        let need_ordered_bucket_load = fast_bucket_info.is_none()
            || matches!(
                req.policy_requirement,
                ObjectBucketPolicyRequirement::Required
            ) && fast_bucket_info.as_ref().is_some_and(|bucket_info| {
                bucket_info.bucket_policy_present && fresh_bucket_policy.is_none()
            });

        if need_ordered_bucket_load {
            let guards = self.lock_bucket_and_object_pgs_for(req.bucket, req.key)?;
            let bucket_info = match fast_bucket_info {
                Some(bucket_info) => bucket_info,
                None => Self::validate_expected_bucket_owner(
                    self.load_active_bucket_summary_from_pg(guards.bucket(), req.bucket)?,
                    req.expected_bucket_owner,
                )?,
            };
            let bucket_policy = match req.policy_requirement {
                ObjectBucketPolicyRequirement::Required => {
                    self.cached_bucket_policy_with_locked_bucket_pg(&bucket_info, guards.bucket())?
                }
            };
            let can_discover_missing = req.missing_discovery.requester_can_discover_missing(
                req.requester,
                &bucket_info,
                req.key.as_str(),
                req.version_id,
                bucket_policy.as_deref(),
            )?;
            let record = match Self::lookup_object_record(
                guards.object(),
                req.bucket,
                req.key,
                req.version_id,
            ) {
                Ok(record) => record,
                Err(ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. })
                    if !can_discover_missing =>
                {
                    return Err(ServerError::AccessDenied);
                }
                Err(other) => return Err(other),
            };

            return Ok(LoadedObjectState {
                bucket_info,
                bucket_policy,
                locked: LockedReadObject {
                    record,
                    pgs: guards.into_object_guards(),
                },
            });
        }

        let bucket_info = fast_bucket_info.expect("ordered load handles missing bucket fast path");
        let bucket_policy = match req.policy_requirement {
            ObjectBucketPolicyRequirement::Required => fresh_bucket_policy,
        };
        let can_discover_missing = req.missing_discovery.requester_can_discover_missing(
            req.requester,
            &bucket_info,
            req.key.as_str(),
            req.version_id,
            bucket_policy.as_deref(),
        )?;
        let locked = match self.lock_object_pgs_for_read_typed(req.bucket, req.key, req.version_id)
        {
            Ok(locked) => locked,
            Err(ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. })
                if !can_discover_missing =>
            {
                return Err(ServerError::AccessDenied);
            }
            Err(other) => return Err(other),
        };

        Ok(LoadedObjectState {
            bucket_info,
            bucket_policy,
            locked,
        })
    }

    fn ensure_loaded_object_read_allowed(
        requester: &Requester,
        loaded: &LoadedObjectState<'_>,
        policy_action: auth::PolicyAction,
    ) -> Result<(), ServerError> {
        let allowed = Self::requester_can_read_object_with_bucket_policy(
            requester,
            &loaded.bucket_info,
            &loaded.locked.record,
            policy_action,
            loaded.bucket_policy.as_deref(),
        )?;
        if allowed {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn ensure_loaded_object_attributes_allowed(
        requester: &Requester,
        loaded: &LoadedObjectState<'_>,
        policy_action: auth::PolicyAction,
    ) -> Result<(), ServerError> {
        let allowed = Self::requester_can_read_object_attributes_with_bucket_policy(
            requester,
            &loaded.bucket_info,
            &loaded.locked.record,
            policy_action,
            loaded.bucket_policy.as_deref(),
        )?;

        if allowed {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn authorize_object_read<'a>(
        &'a self,
        requester: &Requester,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_bucket_owner: Option<&str>,
        policy_action: auth::PolicyAction,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let loaded = self.load_locked_object_state(ObjectStateLoadRequest {
            requester,
            bucket,
            key,
            version_id,
            expected_bucket_owner,
            policy_requirement: ObjectBucketPolicyRequirement::Required,
            missing_discovery: MissingObjectDiscovery::ReadBucket,
        })?;
        Self::ensure_loaded_object_read_allowed(requester, &loaded, policy_action)?;
        Ok(loaded.locked)
    }

    fn ensure_loaded_object_tagging_allowed(
        requester: &Requester,
        loaded: &LoadedObjectState<'_>,
        policy_action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
    ) -> Result<(), ServerError> {
        if Self::requester_can_manage_object_tags_with_bucket_policy(
            requester,
            &loaded.bucket_info,
            &loaded.locked.record,
            policy_action,
            request_object_tags_xml,
            loaded.bucket_policy.as_deref(),
        )? {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn authorize_object_tagging_access<'a>(
        &'a self,
        object: &ObjectVersionRequest<'_>,
        policy_action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let loaded = self.load_locked_object_state(ObjectStateLoadRequest {
            requester: object.object.requester(),
            bucket: object.object.bucket_name_typed(),
            key: object.object.key_typed(),
            version_id: object.version_id,
            expected_bucket_owner: object.expected_bucket_owner(),
            policy_requirement: ObjectBucketPolicyRequirement::Required,
            missing_discovery: MissingObjectDiscovery::BucketAdmin,
        })?;
        Self::ensure_loaded_object_tagging_allowed(
            object.object.requester(),
            &loaded,
            policy_action,
            request_object_tags_xml,
        )?;
        Ok(loaded.locked)
    }

    fn ensure_loaded_object_acl_allowed(
        requester: &Requester,
        loaded: &LoadedObjectState<'_>,
        authorization: ObjectAclAuthorization<'_>,
    ) -> Result<(), ServerError> {
        let allowed = match authorization {
            ObjectAclAuthorization::WriteWithPolicy {
                action: policy_action,
                policy_context,
            } => Self::requester_can_write_object_acl_with_bucket_policy(
                requester,
                &loaded.bucket_info,
                &loaded.locked.record,
                policy_action,
                policy_context,
                loaded.bucket_policy.as_deref(),
            )?,
            ObjectAclAuthorization::ReadWithPolicy(policy_action) => {
                Self::requester_can_read_object_acl_with_bucket_policy(
                    requester,
                    &loaded.bucket_info,
                    &loaded.locked.record,
                    policy_action,
                    loaded.bucket_policy.as_deref(),
                )?
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn authorize_object_acl_access<'a>(
        &'a self,
        requester: &Requester,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        authorization: ObjectAclAuthorization<'_>,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedObjectState<'a>, ServerError> {
        let loaded = self.load_locked_object_state(ObjectStateLoadRequest {
            requester,
            bucket,
            key,
            version_id,
            expected_bucket_owner,
            policy_requirement: ObjectBucketPolicyRequirement::Required,
            missing_discovery: MissingObjectDiscovery::ObjectAcl,
        })?;
        Self::ensure_loaded_object_acl_allowed(requester, &loaded, authorization)?;
        Ok(loaded)
    }

    fn ensure_loaded_object_lock_allowed(
        requester: &Requester,
        loaded: &LoadedObjectState<'_>,
        policy_action: auth::PolicyAction,
    ) -> Result<(), ServerError> {
        if Self::requester_can_manage_object_lock_with_bucket_policy(
            requester,
            &loaded.bucket_info,
            &loaded.locked.record,
            policy_action,
            loaded.bucket_policy.as_deref(),
        )? {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn authorize_object_lock_access<'a>(
        &'a self,
        requester: &Requester,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        policy_action: auth::PolicyAction,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedObjectState<'a>, ServerError> {
        let loaded = self.load_locked_object_state(ObjectStateLoadRequest {
            requester,
            bucket,
            key,
            version_id,
            expected_bucket_owner,
            policy_requirement: ObjectBucketPolicyRequirement::Required,
            missing_discovery: MissingObjectDiscovery::BucketAdmin,
        })?;
        Self::ensure_loaded_object_lock_allowed(requester, &loaded, policy_action)?;
        Self::ensure_object_lock_bucket(&loaded.bucket_info)?;
        Ok(loaded)
    }

    pub(super) fn authorize_get_object<'a>(
        &'a self,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead<'a>, ServerError> {
        let locked = self.authorize_object_read(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            req.expected_bucket_owner(),
            Self::get_object_policy_action(req.object.version_id),
        )?;
        Ok(AuthorizedObjectRead { locked })
    }

    pub(super) fn authorize_head_object<'a>(
        &'a self,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead<'a>, ServerError> {
        let locked = self.authorize_object_read(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            req.expected_bucket_owner(),
            Self::get_object_policy_action(req.object.version_id),
        )?;
        Ok(AuthorizedObjectRead { locked })
    }

    pub(super) fn authorize_get_object_attributes<'a>(
        &'a self,
        req: &GetObjectAttributesRequest<'_>,
    ) -> Result<AuthorizedObjectRead<'a>, ServerError> {
        let loaded = self.load_locked_object_state(ObjectStateLoadRequest {
            requester: req.object.requester(),
            bucket: req.object.bucket_name_typed(),
            key: req.object.key_typed(),
            version_id: req.object.version_id,
            expected_bucket_owner: req.expected_bucket_owner(),
            policy_requirement: ObjectBucketPolicyRequirement::Required,
            missing_discovery: MissingObjectDiscovery::ReadObjectAttributes,
        })?;
        Self::ensure_loaded_object_attributes_allowed(
            req.object.requester(),
            &loaded,
            Self::get_object_attributes_policy_action(req.object.version_id),
        )?;
        let locked = loaded.locked;
        Ok(AuthorizedObjectRead { locked })
    }
}
