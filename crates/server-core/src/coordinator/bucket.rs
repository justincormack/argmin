use std::sync::Arc;

use s3_types::{
    parse_account_regional_bucket_name, AccountIdentity, AclGrants, BucketNamespace,
    BucketVersioningState,
};
use storage::traits::PgMetadataStore;
use storage::{
    BucketEncryptionConfig, BucketName, BucketObjectLockConfig, BucketOwnershipControls,
    BucketState, EffectiveBucketEncryptionConfig, ListMultipartUploadsReq, ListObjectVersionsReq,
    OwnerIdentity, PublicAccessBlockConfig,
};

#[cfg(test)]
use super::trusted_bucket_name;
use super::{
    AuthorizedBucketSubresourceDelete, AuthorizedBucketSubresourceGet,
    AuthorizedBucketSubresourcePut, AuthorizedDeleteBucket, AuthorizedHeadBucket,
    AuthorizedListBuckets, AuthorizedPutBucketAcl, BucketCreateOutcome, BucketRequest,
    BucketSummary, Coordinator, CreateBucketAcl, CreateBucketRequest, GetBucketAclResult,
    ListBucketsRequest, PutBucketAclInput, PutBucketAclRequest, PutBucketConfigRequest,
    PutBucketEncryptionRequest, PutBucketObjectLockConfigurationRequest,
    PutBucketOwnershipControlsRequest, PutBucketPolicyRequest, PutBucketPublicAccessBlockRequest,
    PutBucketVersioningRequest, TRACE_TARGET,
};
use crate::error::ServerError;

impl Coordinator {
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
        let _bucket_guard = self.storage_node.lock_bucket(name);
        let bucket_pg = self.get_bucket_pg_for(name)?;
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
        match bucket_pg.create_bucket_with_config(&storage::CreateBucketConfig {
            name: name.as_str(),
            owner_principal: owner.principal.as_str(),
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &acl_grants,
            public_read,
            public_write,
            versioning: initial_versioning,
            object_lock: initial_object_lock,
        }) {
            Ok(()) => {
                let info = storage::PgMetadataStore::head_bucket(&*bucket_pg, name).map_err(
                    |e| match e {
                        storage::MetadataError::BucketNotFound { name } => {
                            ServerError::BucketNotFound {
                                name: name.to_string(),
                            }
                        }
                        other => ServerError::Metadata(other),
                    },
                )?;
                self.storage_node.upsert_bucket_fast_path((&info).into());
                Ok(BucketCreateOutcome::Created)
            }
            Err(storage::MetadataError::BucketAlreadyExists) => {
                let existing = storage::PgMetadataStore::head_bucket_raw(&*bucket_pg, name)
                    .map_err(|e| match e {
                        storage::MetadataError::BucketNotFound { name } => {
                            ServerError::BucketNotFound {
                                name: name.to_string(),
                            }
                        }
                        other => ServerError::Metadata(other),
                    })?;
                match existing.state {
                    BucketState::Active
                        if existing.owner_principal == owner.principal
                            && existing.owner_canonical_id == owner.canonical_id =>
                    {
                        Ok(BucketCreateOutcome::AlreadyOwned)
                    }
                    BucketState::Active | BucketState::Deleting => {
                        Err(ServerError::BucketAlreadyExists)
                    }
                }
            }
            Err(other) => Err(ServerError::Metadata(other)),
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
        self.begin_bucket_write_drain_for(&name)?;

        let mut marked_deleting = false;
        let result = (|| {
            self.wait_for_bucket_write_reservations_to_drain_for(&name)?;

            self.pg_topology.for_each_pg(|pg_id| {
                let pg = self.storage_node.get_pg(pg_id)?;
                let resp = pg.list_object_versions(&ListObjectVersionsReq {
                    bucket: name.clone(),
                    prefix: None,
                    key_marker: None,
                    version_id_marker: None,
                    max_keys: 1,
                })?;
                if !resp.versions.is_empty() {
                    return Err(ServerError::BucketNotEmpty);
                }
                let mpu_resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                    bucket: name.clone(),
                    prefix: None,
                    key_marker: None,
                    upload_id_marker: None,
                    max_uploads: 1,
                })?;
                if !mpu_resp.uploads.is_empty() {
                    return Err(ServerError::BucketNotEmpty);
                }
                let sessions = pg
                    .list_all_stream_uploads()
                    .map_err(ServerError::Metadata)?;
                if sessions
                    .iter()
                    .any(|session| session.bucket == name.as_str())
                {
                    return Err(ServerError::BucketNotEmpty);
                }
                Ok::<(), ServerError>(())
            })?;

            let bucket_pg = self.get_bucket_pg_for(&name)?;
            storage::PgMetadataStore::mark_bucket_deleting(&*bucket_pg, &name).map_err(
                |e| match e {
                    storage::MetadataError::BucketNotFound { name } => {
                        ServerError::BucketNotFound {
                            name: name.to_string(),
                        }
                    }
                    other => ServerError::Metadata(other),
                },
            )?;
            self.storage_node.remove_bucket_fast_path(&name);
            self.clear_bucket_policy_cache(&name);
            self.clear_bucket_lifecycle_cache(&name);
            marked_deleting = true;
            self.read_runtime()
                .enqueue_bucket_delete_finalize_for(&name);
            Ok(())
        })();

        if !marked_deleting {
            let bucket_pg = self.get_bucket_pg_for(&name)?;
            let _ = storage::PgMetadataStore::end_bucket_write_drain(&*bucket_pg, &name);
        }

        result
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
        let mut out = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.storage_node.get_pg(pg_id)?;
            let mut buckets = pg.list_buckets(owner_canonical_id.as_str())?;
            out.extend(buckets.drain(..).map(Self::bucket_summary));
            Ok::<(), ServerError>(())
        })?;
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::put_bucket_versioning(
            &*bucket_pg,
            &authorized.bucket,
            authorized.state,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            storage::MetadataError::InvalidVersioningTransition { from, to } => {
                ServerError::InvalidRequest {
                    reason: format!("invalid versioning transition from {from:?} to {to:?}"),
                }
            }
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.versioning = authorized.state
            });
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::put_bucket_object_lock(
            &*bucket_pg,
            &authorized.bucket,
            authorized.config,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.object_lock = authorized.config
            });
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::put_bucket_encryption(
            &*bucket_pg,
            &authorized.bucket,
            authorized.config,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.encryption = authorized.effective_config
            });
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::put_bucket_encryption(
            &*bucket_pg,
            &authorized.bucket,
            BucketEncryptionConfig::default(),
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.encryption = EffectiveBucketEncryptionConfig::default()
            });
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
        self.store_authorized_bucket_subresource(&authorized)
    }

    pub fn get_bucket_cors(&self, req: &BucketRequest<'_>) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_cors",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_cors(req)?;
        self.load_authorized_bucket_subresource(&authorized)
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
        self.remove_authorized_bucket_subresource(&authorized)
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
        self.store_authorized_bucket_subresource(&authorized)
    }

    pub fn get_bucket_tags(&self, req: &BucketRequest<'_>) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_tags",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_tagging(req)?;
        self.load_authorized_bucket_subresource(&authorized)
    }

    pub fn delete_bucket_tags(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_tags",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_tagging(req)?;
        self.remove_authorized_bucket_subresource(&authorized)
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
        self.store_bucket_subresource(
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Policy,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::policy(authorized.policy_is_public),
            },
        )?;
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        let info = storage::PgMetadataStore::head_bucket_raw(&*bucket_pg, &authorized.bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })?;
        self.storage_node.upsert_bucket_fast_path((&info).into());
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
        self.load_authorized_bucket_subresource(&authorized)
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
        self.remove_authorized_bucket_subresource(&authorized)?;
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        let info = storage::PgMetadataStore::head_bucket_raw(&*bucket_pg, &authorized.bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })?;
        self.storage_node.upsert_bucket_fast_path((&info).into());
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
        self.store_bucket_subresource(
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Lifecycle,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::None,
            },
        )?;
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        let info = storage::PgMetadataStore::head_bucket_raw(&*bucket_pg, &authorized.bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })?;
        self.storage_node.upsert_bucket_fast_path((&info).into());
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
        self.load_authorized_bucket_subresource(&authorized)
    }

    pub fn delete_bucket_lifecycle(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_lifecycle",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_delete_bucket_lifecycle(req)?;
        self.remove_authorized_bucket_subresource(&authorized)?;
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        let info = storage::PgMetadataStore::head_bucket_raw(&*bucket_pg, &authorized.bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })?;
        self.storage_node.upsert_bucket_fast_path((&info).into());
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::put_bucket_public_access_block(
            &*bucket_pg,
            &authorized.bucket,
            authorized.config,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.public_access_block = Some(authorized.config);
            });
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::get_bucket_public_access_block(&*bucket_pg, &authorized.bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::delete_bucket_public_access_block(
            &*bucket_pg,
            &authorized.bucket,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.public_access_block = None
            });
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::put_bucket_ownership_controls(
            &*bucket_pg,
            &authorized.bucket,
            authorized.config,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.ownership_controls = Some(authorized.config);
            });
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::get_bucket_ownership_controls(&*bucket_pg, &authorized.bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::delete_bucket_ownership_controls(&*bucket_pg, &authorized.bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.ownership_controls = None
            });
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
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::put_bucket_acl(
            &*bucket_pg,
            &authorized.bucket,
            &authorized.acl_grants,
            authorized.public_read,
            authorized.public_write,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node
            .update_bucket_fast_path_if_present(&authorized.bucket, |info| {
                info.acl_grants = authorized.acl_grants.clone();
                info.public_read = authorized.public_read;
                info.public_write = authorized.public_write;
            });
        Ok(())
    }

    fn store_authorized_bucket_subresource(
        &self,
        authorized: &AuthorizedBucketSubresourcePut,
    ) -> Result<(), ServerError> {
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
    ) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg_for(name)?;
        storage::PgMetadataStore::put_bucket_subresource(&*bucket_pg, name, req).map_err(
            |e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            },
        )
    }

    pub(super) fn load_authorized_bucket_subresource(
        &self,
        authorized: &AuthorizedBucketSubresourceGet,
    ) -> Result<Option<String>, ServerError> {
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        Self::load_bucket_subresource_from_pg(&bucket_pg, &authorized.bucket, authorized.kind)
    }

    fn remove_authorized_bucket_subresource(
        &self,
        authorized: &AuthorizedBucketSubresourceDelete,
    ) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg_for(&authorized.bucket)?;
        storage::PgMetadataStore::delete_bucket_subresource(
            &*bucket_pg,
            &authorized.bucket,
            authorized.kind,
        )
        .map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub(super) fn load_bucket_subresource_from_pg(
        bucket_pg: &storage::PgStore,
        bucket: &BucketName,
        kind: storage::BucketSubresourceKind,
    ) -> Result<Option<String>, ServerError> {
        storage::PgMetadataStore::get_bucket_subresource(bucket_pg, bucket, kind)
            .map(|stored| stored.map(|stored| stored.body))
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }
}
