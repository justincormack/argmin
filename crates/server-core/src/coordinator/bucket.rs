use std::sync::Arc;

use s3_types::{
    parse_account_regional_bucket_name, AccountIdentity, AclGrants, BucketNamespace,
    BucketVersioningState,
};
use storage::{
    BucketEncryptionConfig, BucketName, BucketObjectLockConfig, BucketOwnershipControls,
    BucketState, EffectiveBucketEncryptionConfig, OwnerIdentity, PublicAccessBlockConfig,
};

#[cfg(test)]
use super::{should_probe_bucket_mutation_write, trusted_bucket_name};
use super::{
    AuthorizedBucketSubresourceDelete, AuthorizedBucketSubresourceGet,
    AuthorizedBucketSubresourcePut, AuthorizedDeleteBucket, AuthorizedHeadBucket,
    AuthorizedListBuckets, AuthorizedPutBucketAcl, BucketCreateOutcome, BucketRequest,
    BucketSummary, BucketTagControlRequest, Coordinator, CreateBucketAcl, CreateBucketRequest,
    GetBucketAclResult, ListBucketsRequest, PutBucketAbacRequest, PutBucketAclInput,
    PutBucketAclRequest, PutBucketConfigRequest, PutBucketEncryptionRequest,
    PutBucketObjectLockConfigurationRequest, PutBucketOwnershipControlsRequest,
    PutBucketPolicyRequest, PutBucketPublicAccessBlockRequest, PutBucketTagControlRequest,
    PutBucketVersioningRequest, TRACE_TARGET,
};
use crate::error::ServerError;

impl Coordinator {
    fn map_bucket_snapshot_load_error(err: storage::BucketSnapshotLoadError) -> ServerError {
        match err {
            storage::BucketSnapshotLoadError::Store(other) => ServerError::Store(other),
            storage::BucketSnapshotLoadError::Metadata(storage::MetadataError::BucketNotEmpty) => {
                ServerError::BucketNotEmpty
            }
            storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { name },
            ) => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            storage::BucketSnapshotLoadError::Metadata(other) => ServerError::Metadata(other),
        }
    }

    fn clear_bucket_fast_path(&self, info: &storage::BucketInfo) {
        self.observe_bucket_fast_path_generation(&info.name, info.bucket_execution_generation);
    }

    fn map_bucket_write_drain_error(err: storage::BucketWriteDrainError) -> ServerError {
        match err {
            storage::BucketWriteDrainError::Store(other) => ServerError::Store(other),
            storage::BucketWriteDrainError::Metadata(storage::MetadataError::BucketNotEmpty) => {
                ServerError::BucketNotEmpty
            }
            storage::BucketWriteDrainError::Metadata(storage::MetadataError::BucketNotFound {
                name,
            }) => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            storage::BucketWriteDrainError::Metadata(other) => ServerError::Metadata(other),
        }
    }

    pub fn create_bucket(&self, req: &CreateBucketRequest) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::create_bucket",
            "bucket={:?} object_lock_enabled={}",
            req.name,
            req.object_lock_enabled
        );
        let authorized = self.authorize_create_bucket(req)?;
        let create_outcome = self.create_bucket_with_acl_grants_for(
            &authorized.owner,
            &authorized.name,
            authorized.acl_grants,
            authorized.object_lock_enabled,
        )?;
        match create_outcome {
            BucketCreateOutcome::Created => {
                self.put_bucket_ownership_controls(&PutBucketOwnershipControlsRequest {
                    bucket: BucketRequest {
                        name: authorized.name,
                        requester: authorized.requester.clone(),
                        expected_bucket_owner: None,
                    },
                    config: BucketOwnershipControls {
                        object_ownership: authorized.ownership,
                    },
                })
            }
            BucketCreateOutcome::AlreadyOwned => {
                if authorized.locked_to_account_region
                    || !s3_types::is_legacy_create_bucket_region(&self.region)
                {
                    return Err(ServerError::BucketAlreadyOwnedByYou);
                }
                let existing = self.unchecked_active_bucket_summary_for(&authorized.name)?;
                let authorized_acl = self.resolve_create_bucket_recreate_acl_update(
                    &existing,
                    &authorized.owner,
                    &authorized.acl,
                )?;
                self.apply_authorized_bucket_acl_update(&authorized_acl)
            }
        }
    }

    pub(super) fn validate_create_bucket_namespace(
        &self,
        bucket: &BucketName,
        namespace: BucketNamespace,
        owner_account: &AccountIdentity,
    ) -> Result<bool, ServerError> {
        let locked = parse_account_regional_bucket_name(bucket.as_str());
        if namespace == BucketNamespace::AccountRegional && locked.is_none() {
            let account_id =
                owner_account
                    .account_id()
                    .ok_or_else(|| ServerError::InvalidRequest {
                        reason:
                            "account-regional bucket namespace requires a 12-digit AWS account ID"
                                .to_string(),
                    })?;
            return Err(ServerError::InvalidBucketNamespace {
                reason: format!(
                    "The requested bucket is an account-regional namespace bucket, but the bucket name does not end with -{account_id}-{}-an. Specify the targeted account and region in the bucket name.",
                    self.region
                ),
                bucket_namespace: bucket.to_string(),
            });
        }

        let Some(locked) = locked else {
            return Ok(false);
        };

        let account_id = owner_account
            .account_id()
            .ok_or_else(|| ServerError::InvalidRequest {
                reason: "account-regional bucket namespace requires a 12-digit AWS account ID"
                    .to_string(),
            })?;
        if locked.account_id() != account_id || locked.region() != self.region {
            let reason = if locked.account_id() != account_id {
                format!(
                    "The requested bucket is an account-regional namespace bucket, but the requested AWS Account ID '{}' does not match the caller's AWS Account ID '{}'. Specify the caller's AWS Account ID in the bucket name.",
                    locked.account_id(),
                    account_id
                )
            } else {
                format!(
                    "The requested bucket is an account-regional namespace bucket, but the requested region '{}' does not match the current region '{}'. Specify the targeted region in the bucket name.",
                    locked.region(),
                    self.region
                )
            };
            return Err(ServerError::InvalidBucketNamespace {
                reason,
                bucket_namespace: bucket.to_string(),
            });
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn create_bucket_for_owner(
        &self,
        owner_principal: &str,
        name: &str,
        public_read: bool,
    ) -> Result<(), ServerError> {
        let owner = OwnerIdentity::from_principal(owner_principal);
        let acl_grants = Self::bucket_acl_grants_from_flags(&owner, public_read, false);
        self.create_bucket_with_acl_grants(&owner, name, acl_grants, false)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_bucket_abac_enabled_for_test(
        &self,
        name: &str,
        enabled: bool,
    ) -> Result<(), ServerError> {
        let bucket = trusted_bucket_name(name);
        let info = self
            .storage_node
            .put_bucket_abac_enabled_and_load_info(&bucket, enabled)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn create_bucket_with_acl_grants(
        &self,
        owner: &OwnerIdentity,
        name: &str,
        acl_grants: AclGrants,
        object_lock_enabled: bool,
    ) -> Result<BucketCreateOutcome, ServerError> {
        self.create_bucket_with_acl_grants_for(
            owner,
            &trusted_bucket_name(name),
            acl_grants,
            object_lock_enabled,
        )
    }

    fn create_bucket_with_acl_grants_for(
        &self,
        owner: &OwnerIdentity,
        name: &BucketName,
        acl_grants: AclGrants,
        object_lock_enabled: bool,
    ) -> Result<BucketCreateOutcome, ServerError> {
        let public_read = Self::acl_grants_public_read(&acl_grants);
        let public_write = Self::acl_grants_public_write(&acl_grants);
        let initial_versioning = if object_lock_enabled {
            BucketVersioningState::Enabled
        } else {
            BucketVersioningState::Disabled
        };
        let initial_object_lock = BucketObjectLockConfig {
            enabled: object_lock_enabled,
            default_retention: None,
        };
        match self
            .storage_node
            .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
                name: name.as_str(),
                owner_principal: owner.principal.as_str(),
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &acl_grants,
                public_read,
                public_write,
                versioning: initial_versioning,
                object_lock: initial_object_lock,
            })
            .map_err(Self::map_bucket_snapshot_load_error)?
        {
            storage::node::BucketCreateAttemptOutcome::Created(_info) => {
                Ok(BucketCreateOutcome::Created)
            }
            storage::node::BucketCreateAttemptOutcome::Exists(existing) => match existing.state {
                BucketState::Active
                    if existing.owner_principal == owner.principal
                        && existing.owner_canonical_id == owner.canonical_id =>
                {
                    Ok(BucketCreateOutcome::AlreadyOwned)
                }
                BucketState::Active | BucketState::Deleting => {
                    Err(ServerError::BucketAlreadyExists)
                }
            },
        }
    }

    fn resolve_create_bucket_recreate_acl_update(
        &self,
        bucket: &BucketSummary,
        owner: &OwnerIdentity,
        acl: &CreateBucketAcl,
    ) -> Result<AuthorizedPutBucketAcl, ServerError> {
        let acl_grants = match acl {
            CreateBucketAcl::DefaultPrivate => Self::owner_full_control_grants(owner),
            CreateBucketAcl::Canned(acl) => {
                Self::ensure_put_bucket_acl_supported(bucket, *acl)?;
                Self::bucket_acl_grants_from_canned(owner, *acl)?
            }
            CreateBucketAcl::Grants(acl_grants) => {
                if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                Self::ensure_supported_bucket_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        let public_read = Self::acl_grants_public_read(&acl_grants);
        let public_write = Self::acl_grants_public_write(&acl_grants);
        if Self::blocks_public_acls(bucket.public_access_block.as_ref())
            && (Self::acl_grants_grant_public_read(&acl_grants)
                || Self::acl_grants_grant_public_write(&acl_grants))
        {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedPutBucketAcl {
            bucket: bucket.name.clone(),
            acl_grants,
            public_read,
            public_write,
        })
    }

    pub fn delete_bucket(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket",
            "bucket={:?}",
            req.name
        );
        let AuthorizedDeleteBucket { name } = self.authorize_delete_bucket(req)?;
        self.storage_node
            .begin_bucket_delete(&name)
            .map_err(Self::map_bucket_write_drain_error)?;
        self.remove_bucket_fast_path(&name);
        self.clear_bucket_policy_cache(&name);
        self.clear_bucket_lifecycle_cache(&name);
        self.read_runtime()
            .enqueue_bucket_delete_finalize_for(&name);
        Ok(())
    }

    pub fn head_bucket(&self, req: &BucketRequest<'_>) -> Result<BucketSummary, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::head_bucket",
            "bucket={:?}",
            req.name
        );
        let AuthorizedHeadBucket { bucket_info } = self.authorize_head_bucket(req)?;
        Ok(bucket_info)
    }

    pub fn bucket_exists(&self, name: &BucketName) -> Result<bool, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::bucket_exists",
            "bucket={:?}",
            name
        );
        match self.unchecked_active_bucket_summary_for(name) {
            Ok(_) => Ok(true),
            Err(ServerError::BucketNotFound { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn list_buckets(
        &self,
        req: &ListBucketsRequest,
    ) -> Result<Vec<BucketSummary>, ServerError> {
        observability::trace_scope!(TRACE_TARGET, "Coordinator::list_buckets");
        let AuthorizedListBuckets { owner_canonical_id } = self.authorize_list_buckets(req)?;
        let buckets = self
            .storage_node
            .list_buckets_for_owner(owner_canonical_id.as_str())
            .map_err(Self::map_object_pg_action_error)?;
        Ok(buckets.into_iter().map(Self::bucket_summary).collect())
    }

    pub fn put_bucket_versioning(
        &self,
        req: &PutBucketVersioningRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_versioning",
            "bucket={:?} state={:?}",
            req.bucket.name,
            req.state
        );
        let authorized = self.authorize_put_bucket_versioning(req)?;
        let info = self
            .storage_node
            .put_bucket_versioning_and_load_info(&authorized.bucket, authorized.state)
            .map_err(|e| match e {
                storage::BucketSnapshotLoadError::Metadata(
                    storage::MetadataError::InvalidVersioningTransition { from, to },
                ) => ServerError::InvalidRequest {
                    reason: format!("invalid versioning transition from {from:?} to {to:?}"),
                },
                other => Self::map_bucket_snapshot_load_error(other),
            })?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_versioning(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<BucketVersioningState, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_versioning",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_versioning(req)?;
        Ok(authorized.state)
    }

    pub fn get_bucket_location(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_location",
            "bucket={:?}",
            req.name
        );
        let _authorized = self.authorize_get_bucket_location(req)?;
        Ok(())
    }

    pub fn put_bucket_object_lock_configuration(
        &self,
        req: &PutBucketObjectLockConfigurationRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_object_lock_configuration",
            "bucket={:?} enable_requested={} has_default_retention={}",
            req.bucket.name,
            req.config.object_lock_enabled.unwrap_or(false),
            req.config.default_retention.is_some()
        );
        let authorized = self.authorize_put_bucket_object_lock_configuration(req)?;
        let info = self
            .storage_node
            .put_bucket_object_lock_and_load_info(&authorized.bucket, authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_object_lock_configuration(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<BucketObjectLockConfig, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_object_lock_configuration",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_object_lock_configuration(req)?;
        Ok(authorized.config)
    }

    pub fn put_bucket_encryption(
        &self,
        req: &PutBucketEncryptionRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_encryption",
            "bucket={:?} sse_c_blocked={}",
            req.bucket.name,
            req.config.sse_c_blocked
        );
        let authorized = self.authorize_put_bucket_encryption(req)?;
        let info = self
            .storage_node
            .put_bucket_encryption_and_load_info(&authorized.bucket, authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_encryption(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<EffectiveBucketEncryptionConfig, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_encryption",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_encryption(req)?;
        Ok(authorized.config)
    }

    pub fn delete_bucket_encryption(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_encryption",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_encryption(req)?;
        let info = self
            .storage_node
            .put_bucket_encryption_and_load_info(
                &authorized.bucket,
                BucketEncryptionConfig::default(),
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn put_bucket_cors(&self, req: &PutBucketConfigRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_cors",
            "bucket={:?} bytes={}",
            req.bucket.name,
            req.config.len()
        );
        let authorized = self.authorize_put_bucket_cors(req)?;
        let info = self.store_authorized_bucket_subresource(&authorized)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_cors(&self, req: &BucketRequest<'_>) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_cors",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_cors(req)?;
        Ok(authorized.body)
    }

    pub fn load_bucket_cors_config(
        &self,
        name: &BucketName,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::load_bucket_cors_config",
            "bucket={:?}",
            name
        );
        let authorized = self.authorize_load_bucket_cors_config_for(name);
        self.load_authorized_bucket_subresource(&authorized)
    }

    pub fn delete_bucket_cors(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_cors",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_cors(req)?;
        let info = self.remove_authorized_bucket_subresource(&authorized)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn put_bucket_tags(&self, req: &PutBucketConfigRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_tags",
            "bucket={:?} bytes={}",
            req.bucket.name,
            req.config.len()
        );
        let authorized = self.authorize_put_bucket_tagging(req)?;
        let info = self.store_authorized_bucket_subresource(&authorized)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_tags(&self, req: &BucketRequest<'_>) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_tags",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_tagging(req)?;
        Ok(authorized.body)
    }

    pub fn delete_bucket_tags(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_tags",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_tagging(req)?;
        let info = self.remove_authorized_bucket_subresource(&authorized)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_tags_for_tag_resource(
        &self,
        req: &BucketTagControlRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_tags_for_tag_resource",
            "bucket={:?}",
            req.bucket.name
        );
        let authorized = self.authorize_bucket_tag_control(req)?;
        self.load_authorized_bucket_subresource(&AuthorizedBucketSubresourceGet {
            bucket: authorized.bucket,
            kind: storage::BucketSubresourceKind::Tagging,
        })
    }

    pub fn put_bucket_tags_for_tag_resource(
        &self,
        req: &PutBucketTagControlRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_tags_for_tag_resource",
            "bucket={:?} bytes={}",
            req.control.bucket.name,
            req.config.len()
        );
        let authorized = self.authorize_bucket_tag_control(&req.control)?;
        let info = self.store_bucket_subresource(
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Tagging,
                body: req.config,
                aux: storage::BucketSubresourceAux::None,
            },
        )?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn delete_bucket_tags_for_tag_resource(
        &self,
        req: &BucketTagControlRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_tags_for_tag_resource",
            "bucket={:?}",
            req.bucket.name
        );
        let authorized = self.authorize_bucket_tag_control(req)?;
        let info =
            self.remove_authorized_bucket_subresource(&AuthorizedBucketSubresourceDelete {
                bucket: authorized.bucket,
                kind: storage::BucketSubresourceKind::Tagging,
            })?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn put_bucket_abac(&self, req: &PutBucketAbacRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_abac",
            "bucket={:?} enabled={}",
            req.bucket.name,
            req.enabled
        );
        let authorized = self.authorize_put_bucket_abac(req)?;
        let info = self
            .storage_node
            .put_bucket_abac_enabled_and_load_info(&authorized.bucket, authorized.enabled)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_abac(&self, req: &BucketRequest<'_>) -> Result<bool, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_abac",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_abac(req)?;
        Ok(authorized.enabled)
    }

    pub fn put_bucket_policy(&self, req: &PutBucketPolicyRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_policy",
            "bucket={:?} bytes={}",
            req.bucket.name,
            req.config.len()
        );
        let authorized = self.authorize_put_bucket_policy(req)?;
        let info = self.store_bucket_subresource(
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Policy,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::policy(authorized.policy_is_public),
            },
        )?;
        self.clear_bucket_fast_path(&info);
        self.cache_bucket_policy(
            &authorized.bucket,
            info.bucket_policy_generation,
            Arc::clone(&authorized.parsed_policy),
        );
        Ok(())
    }

    pub fn get_bucket_policy(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_policy",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_policy(req)?;
        Ok(authorized.body)
    }

    pub fn get_bucket_policy_status(&self, req: &BucketRequest<'_>) -> Result<bool, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_policy_status",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_policy_status(req)?;
        Ok(authorized.is_public)
    }

    pub fn delete_bucket_policy(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_policy",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_policy(req)?;
        let info = self.remove_authorized_bucket_subresource(&authorized)?;
        self.clear_bucket_fast_path(&info);
        self.clear_bucket_policy_cache(&authorized.bucket);
        Ok(())
    }

    pub fn put_bucket_lifecycle(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_lifecycle",
            "bucket={:?} bytes={}",
            req.bucket.name,
            req.config.len()
        );
        let authorized = self.authorize_put_bucket_lifecycle(req)?;
        #[cfg(test)]
        if should_probe_bucket_mutation_write(authorized.bucket.as_str()) {
            let bucket_pg_ready = self
                .storage_node
                .try_probe_bucket_pg_available(&authorized.bucket)
                .map_err(Self::map_bucket_snapshot_load_error)?;
            if !bucket_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: bucket pg still locked before put_bucket_lifecycle write"
                        .to_string(),
                });
            }
        }
        let info = self.store_bucket_subresource(
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Lifecycle,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::None,
            },
        )?;
        self.clear_bucket_fast_path(&info);
        self.cache_bucket_lifecycle(
            &authorized.bucket,
            info.bucket_lifecycle_generation,
            Arc::new(authorized.parsed_config),
        );
        Ok(())
    }

    pub fn get_bucket_lifecycle(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_lifecycle",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_lifecycle(req)?;
        Ok(authorized.body)
    }

    pub fn delete_bucket_lifecycle(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_lifecycle",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_lifecycle(req)?;
        let info = self.remove_authorized_bucket_subresource(&authorized)?;
        self.clear_bucket_fast_path(&info);
        self.clear_bucket_lifecycle_cache(&authorized.bucket);
        Ok(())
    }

    pub fn put_bucket_public_access_block(
        &self,
        req: &PutBucketPublicAccessBlockRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_public_access_block",
            "bucket={:?} config={:?}",
            req.bucket.name,
            req.config
        );
        let authorized = self.authorize_put_bucket_public_access_block(req)?;
        let info = self
            .storage_node
            .put_bucket_public_access_block_and_load_info(&authorized.bucket, authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<PublicAccessBlockConfig>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_public_access_block",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_public_access_block(req)?;
        Ok(authorized.config)
    }

    pub fn delete_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_public_access_block",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_public_access_block(req)?;
        let info = self
            .storage_node
            .delete_bucket_public_access_block_and_load_info(&authorized.bucket)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn put_bucket_ownership_controls(
        &self,
        req: &PutBucketOwnershipControlsRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_ownership_controls",
            "bucket={:?} config={:?}",
            req.bucket.name,
            req.config
        );
        let authorized = self.authorize_put_bucket_ownership_controls(req)?;
        let info = self
            .storage_node
            .put_bucket_ownership_controls_and_load_info(&authorized.bucket, authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<BucketOwnershipControls>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_ownership_controls",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_ownership_controls(req)?;
        Ok(authorized.config)
    }

    pub fn delete_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_ownership_controls",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_ownership_controls(req)?;
        let info = self
            .storage_node
            .delete_bucket_ownership_controls_and_load_info(&authorized.bucket)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_acl(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<GetBucketAclResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_acl",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_acl(req)?;
        Ok(authorized.result)
    }

    pub fn put_bucket_acl(&self, req: &PutBucketAclRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_acl",
            "bucket={:?} acl_kind={}",
            req.bucket.name,
            match &req.acl {
                PutBucketAclInput::Canned(_) => "canned",
                PutBucketAclInput::Grants(_) => "grants",
            }
        );
        let authorized = self.authorize_put_bucket_acl(req)?;
        self.apply_authorized_bucket_acl_update(&authorized)
    }

    pub fn validate_put_bucket_acl_request(
        &self,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<(), ServerError> {
        self.authorize_put_bucket_acl(req).map(|_| ())
    }

    fn apply_authorized_bucket_acl_update(
        &self,
        authorized: &AuthorizedPutBucketAcl,
    ) -> Result<(), ServerError> {
        #[cfg(test)]
        if should_probe_bucket_mutation_write(authorized.bucket.as_str()) {
            let bucket_pg_ready = self
                .storage_node
                .try_probe_bucket_pg_available(&authorized.bucket)
                .map_err(Self::map_bucket_snapshot_load_error)?;
            if !bucket_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: bucket pg still locked before put_bucket_acl write"
                        .to_string(),
                });
            }
        }
        let info = self
            .storage_node
            .put_bucket_acl_and_load_info(
                &authorized.bucket,
                &authorized.acl_grants,
                authorized.public_read,
                authorized.public_write,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    fn store_authorized_bucket_subresource(
        &self,
        authorized: &AuthorizedBucketSubresourcePut,
    ) -> Result<storage::BucketInfo, ServerError> {
        self.store_bucket_subresource(
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: authorized.kind,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::None,
            },
        )
    }

    fn store_bucket_subresource(
        &self,
        name: &BucketName,
        req: storage::PutBucketSubresource<'_>,
    ) -> Result<storage::BucketInfo, ServerError> {
        self.storage_node
            .put_bucket_subresource_and_load_info(name, req)
            .map_err(Self::map_bucket_snapshot_load_error)
    }

    pub(super) fn load_authorized_bucket_subresource(
        &self,
        authorized: &AuthorizedBucketSubresourceGet,
    ) -> Result<Option<String>, ServerError> {
        self.storage_node
            .get_bucket_subresource(&authorized.bucket, authorized.kind)
            .map_err(Self::map_bucket_snapshot_load_error)
    }

    fn remove_authorized_bucket_subresource(
        &self,
        authorized: &AuthorizedBucketSubresourceDelete,
    ) -> Result<storage::BucketInfo, ServerError> {
        self.storage_node
            .delete_bucket_subresource_and_load_info(&authorized.bucket, authorized.kind)
            .map_err(Self::map_bucket_snapshot_load_error)
    }
}
