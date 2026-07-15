use s3_types::{
    parse_account_regional_bucket_name, AccountIdentity, AclGrants, BucketNamespace,
    BucketVersioningState,
};
use storage::{
    BucketEncryptionConfig, BucketName, BucketObjectLockConfig, BucketObjectOwnership,
    BucketOwnershipControls, BucketState, EffectiveBucketEncryptionConfig, OwnerIdentity,
    PublicAccessBlockConfig,
};

#[cfg(test)]
use super::trusted_bucket_name;
use super::{
    AuthorizedDeleteBucket, AuthorizedHeadBucket, AuthorizedListBuckets, AuthorizedPutBucketAcl,
    BucketCreateOutcome, BucketRequest, BucketSummary, BucketTagControlRequest, Coordinator,
    CreateBucketAcl, CreateBucketRequest, GetBucketAclResult, ListBucketsRequest,
    PutBucketAbacRequest, PutBucketAclInput, PutBucketAclRequest, PutBucketConfigRequest,
    PutBucketEncryptionRequest, PutBucketObjectLockConfigurationRequest,
    PutBucketOwnershipControlsRequest, PutBucketPolicyRequest, PutBucketPublicAccessBlockRequest,
    PutBucketTagControlRequest, PutBucketTagsForUntagResourceRequest, PutBucketVersioningRequest,
    UntagBucketTagControlRequest, TRACE_TARGET,
};
use crate::error::ServerError;

pub(super) fn map_bucket_write_drain_error(err: storage::BucketWriteDrainError) -> ServerError {
    match err {
        storage::BucketWriteDrainError::Store(
            storage::StoreError::MetadataCommandLogConflict { .. }
            | storage::StoreError::MetadataCommandLogGap { .. }
            | storage::StoreError::MetadataCommandPendingConflict { .. },
        ) => ServerError::OperationAborted,
        storage::BucketWriteDrainError::Metadata(ref error)
            if super::metadata_error_is_command_contention(error) =>
        {
            ServerError::OperationAborted
        }
        storage::BucketWriteDrainError::Store(other) => super::map_store_error(other),
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

impl Coordinator {
    pub(super) fn map_bucket_snapshot_load_error(
        err: storage::BucketSnapshotLoadError,
    ) -> ServerError {
        match err {
            storage::BucketSnapshotLoadError::Store(
                storage::StoreError::MetadataCommandLogConflict { .. }
                | storage::StoreError::MetadataCommandLogGap { .. }
                | storage::StoreError::MetadataCommandPendingConflict { .. },
            ) => ServerError::OperationAborted,
            storage::BucketSnapshotLoadError::Store(other) => super::map_store_error(other),
            storage::BucketSnapshotLoadError::Metadata(ref error)
                if super::metadata_error_is_command_contention(error) =>
            {
                ServerError::OperationAborted
            }
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

    pub(super) fn map_bucket_write_drain_error(err: storage::BucketWriteDrainError) -> ServerError {
        map_bucket_write_drain_error(err)
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
        let storage_node = self.storage_node();
        let create_outcome = self.create_bucket_with_acl_grants_for_storage_node(
            &storage_node,
            &authorized.owner,
            &authorized.name,
            authorized.acl_grants,
            authorized.ownership,
            authorized.object_lock_enabled,
        )?;
        let _ = observability::event(
            TRACE_TARGET,
            "bucket_create_storage_outcome",
            Some(format_args!(
                "bucket={:?} outcome={:?}",
                authorized.name, create_outcome
            )),
        );
        match create_outcome {
            BucketCreateOutcome::Created => Ok(()),
            BucketCreateOutcome::AlreadyOwned => {
                if authorized.locked_to_account_region
                    || !s3_types::is_legacy_create_bucket_region(&self.region)
                {
                    return Err(ServerError::BucketAlreadyOwnedByYou);
                }
                let existing = self.unchecked_active_bucket_summary_for_storage_node(
                    &storage_node,
                    &authorized.name,
                )?;
                if !Self::is_bucket_owner_enforced(existing.ownership_controls.as_ref()) {
                    return Err(ServerError::BucketAlreadyOwnedByYou);
                }
                let authorized_acl = self.resolve_create_bucket_recreate_acl_update(
                    &existing,
                    &authorized.owner,
                    &authorized.acl,
                )?;
                self.apply_authorized_bucket_acl_update(&storage_node, &authorized_acl)
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
            return Err(ServerError::AccountRegionalNamespaceHeaderRequiresSuffix {
                bucket: bucket.to_string(),
            });
        }

        let Some(locked) = locked else {
            return Ok(false);
        };

        if namespace == BucketNamespace::Global {
            return Err(ServerError::MissingNamespaceHeader);
        }

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
            .storage_node()
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
        self.create_bucket_with_acl_grants_for_storage_node(
            &self.storage_node(),
            owner,
            &trusted_bucket_name(name),
            acl_grants,
            BucketObjectOwnership::ObjectWriter,
            object_lock_enabled,
        )
    }

    fn create_bucket_with_acl_grants_for_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        owner: &OwnerIdentity,
        name: &BucketName,
        acl_grants: AclGrants,
        ownership: BucketObjectOwnership,
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
        loop {
            match storage_node
                .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
                    name: name.as_str(),
                    owner_principal: owner.principal.as_str(),
                    owner_canonical_id: &owner.canonical_id,
                    acl_grants: &acl_grants,
                    public_read,
                    public_write,
                    versioning: initial_versioning,
                    object_lock: initial_object_lock,
                    ownership_controls: BucketOwnershipControls {
                        object_ownership: ownership,
                    },
                })
                .map_err(Self::map_bucket_snapshot_load_error)?
            {
                storage::node::BucketCreateAttemptOutcome::Created(info) => {
                    self.clear_bucket_fast_path(&info);
                    return Ok(BucketCreateOutcome::Created);
                }
                storage::node::BucketCreateAttemptOutcome::Exists(existing) => {
                    match existing.state {
                        BucketState::Active
                            if existing.owner_principal == owner.principal
                                && existing.owner_canonical_id == owner.canonical_id =>
                        {
                            return Ok(BucketCreateOutcome::AlreadyOwned);
                        }
                        BucketState::Active => return Err(ServerError::BucketAlreadyExists),
                        BucketState::Deleting => {
                            let _ = observability::event(
                                TRACE_TARGET,
                                "bucket_create_finalize_deleting_start",
                                Some(format_args!("bucket={:?}", name)),
                            );
                            match storage_node.try_finalize_bucket_delete(name) {
                                Ok(
                                    storage::BucketDeleteFinalizeOutcome::Finalized
                                    | storage::BucketDeleteFinalizeOutcome::NotFound,
                                ) => {
                                    let _ = observability::event(
                                        TRACE_TARGET,
                                        "bucket_create_finalize_deleting_done",
                                        Some(format_args!("bucket={:?}", name)),
                                    );
                                    continue;
                                }
                                Ok(storage::BucketDeleteFinalizeOutcome::NotDeleting) => {
                                    let _ = observability::event(
                                        TRACE_TARGET,
                                        "bucket_create_finalize_deleting_changed",
                                        Some(format_args!("bucket={:?}", name)),
                                    );
                                    continue;
                                }
                                Ok(storage::BucketDeleteFinalizeOutcome::Pending) => {
                                    let _ = observability::event(
                                        TRACE_TARGET,
                                        "bucket_create_finalize_deleting_pending",
                                        Some(format_args!("bucket={:?}", name)),
                                    );
                                    return Err(ServerError::BucketAlreadyExists);
                                }
                                Err(err) => return Err(Self::map_bucket_write_drain_error(err)),
                            }
                        }
                    }
                }
            }
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
            summary: storage::BucketAclSummary {
                public_read,
                public_write,
            },
        })
    }

    pub fn delete_bucket(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let request_started = std::time::Instant::now();
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket",
            "bucket={:?}",
            req.name
        );
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_request_start",
            format!("bucket={:?}", req.name),
        );
        let authorize_started = std::time::Instant::now();
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_authorize_start",
            format!("bucket={:?}", req.name),
        );
        let storage_node = self.storage_node();
        let AuthorizedDeleteBucket {
            name,
            bucket_execution_generation,
            bucket_incarnation_generation,
        } = match self.authorize_delete_bucket_with_storage_node(&storage_node, req) {
            Ok(authorized) => {
                let _ = observability::emit_flight_event(
                    TRACE_TARGET,
                    "bucket_delete_authorize_done",
                    format!(
                        "bucket={:?} elapsed_us={} total_elapsed_us={}",
                        authorized.name,
                        authorize_started.elapsed().as_micros(),
                        request_started.elapsed().as_micros()
                    ),
                );
                authorized
            }
            Err(error) => {
                let _ = observability::emit_flight_event(
                    TRACE_TARGET,
                    "bucket_delete_authorize_failed",
                    format!(
                        "bucket={:?} elapsed_us={} total_elapsed_us={} error={:?}",
                        req.name,
                        authorize_started.elapsed().as_micros(),
                        request_started.elapsed().as_micros(),
                        error
                    ),
                );
                return Err(error);
            }
        };
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_authorized",
            format!(
                "bucket={:?} total_elapsed_us={}",
                name,
                request_started.elapsed().as_micros()
            ),
        );
        let begin_started = std::time::Instant::now();
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_begin_call_start",
            format!(
                "bucket={:?} total_elapsed_us={}",
                name,
                request_started.elapsed().as_micros()
            ),
        );
        if let Err(err) = storage_node.begin_bucket_delete_if_current(
            &name,
            storage::cluster::BucketIdentityGenerations {
                bucket_execution_generation,
                bucket_incarnation_generation,
            },
        ) {
            let _ = observability::emit_flight_event(
                TRACE_TARGET,
                "bucket_delete_begin_failed",
                format!(
                    "bucket={:?} elapsed_us={} total_elapsed_us={} error={:?}",
                    name,
                    begin_started.elapsed().as_micros(),
                    request_started.elapsed().as_micros(),
                    err
                ),
            );
            return Err(Self::map_bucket_write_drain_error(err));
        }
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_begin_call_done",
            format!(
                "bucket={:?} elapsed_us={} total_elapsed_us={}",
                name,
                begin_started.elapsed().as_micros(),
                request_started.elapsed().as_micros()
            ),
        );
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_marked",
            format!(
                "bucket={:?} total_elapsed_us={}",
                name,
                request_started.elapsed().as_micros()
            ),
        );
        self.remove_bucket_fast_path(&name);
        self.read_runtime_for_storage_node(storage_node)
            .enqueue_bucket_delete_finalize_for(&name);
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_finalize_enqueued",
            format!(
                "bucket={:?} total_elapsed_us={}",
                name,
                request_started.elapsed().as_micros()
            ),
        );
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
            .storage_node()
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_put_bucket_versioning_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized = self
            .authorize_put_bucket_object_lock_configuration_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_put_bucket_encryption_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_delete_bucket_encryption_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized = self.authorize_put_bucket_cors_with_storage_node(&storage_node, req)?;
        let info = self.store_bucket_subresource_with_storage_node(
            &storage_node,
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Cors,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::None,
            },
        )?;
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
        self.storage_node()
            .get_bucket_subresource(&authorized.bucket, storage::BucketSubresourceKind::Cors)
            .map_err(Self::map_bucket_snapshot_load_error)
    }

    pub fn delete_bucket_cors(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_cors",
            "bucket={:?}",
            req.name
        );
        let storage_node = self.storage_node();
        let authorized = self.authorize_delete_bucket_cors_with_storage_node(&storage_node, req)?;
        let info = storage_node
            .delete_bucket_subresource_and_load_info(
                &authorized.bucket,
                storage::BucketSubresourceKind::Cors,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
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
        let storage_node = self.storage_node();
        let authorized = self.authorize_put_bucket_tagging_with_storage_node(&storage_node, req)?;
        let info = self.store_bucket_subresource_with_storage_node(
            &storage_node,
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Tagging,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::None,
            },
        )?;
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_delete_bucket_tagging_with_storage_node(&storage_node, req)?;
        let info = storage_node
            .delete_bucket_subresource_and_load_info(
                &authorized.bucket,
                storage::BucketSubresourceKind::Tagging,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn get_bucket_tags_for_tag_resource(
        &self,
        req: &BucketTagControlRequest<'_>,
        request_tags: &[(String, String)],
        action: auth::PolicyAction,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_tags_for_tag_resource",
            "bucket={:?}",
            req.bucket.name
        );
        let authorized = self.authorize_bucket_tag_resource_action(req, request_tags, action)?;
        self.storage_node()
            .get_bucket_subresource(&authorized.bucket, storage::BucketSubresourceKind::Tagging)
            .map_err(Self::map_bucket_snapshot_load_error)
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_put_bucket_tag_control_with_storage_node(&storage_node, req)?;
        let info = self.store_bucket_subresource_with_storage_node(
            &storage_node,
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
        let storage_node = self.storage_node();
        let authorized = self.authorize_bucket_tag_control_with_storage_node(&storage_node, req)?;
        let info = storage_node
            .delete_bucket_subresource_and_load_info(
                &authorized.bucket,
                storage::BucketSubresourceKind::Tagging,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    pub fn put_bucket_tags_for_untag_resource(
        &self,
        req: &PutBucketTagsForUntagResourceRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_tags_for_untag_resource",
            "bucket={:?} bytes={}",
            req.control.bucket.name,
            req.config.len()
        );
        let storage_node = self.storage_node();
        let authorized = self
            .authorize_put_bucket_tags_for_untag_resource_with_storage_node(&storage_node, req)?;
        let info = self.store_bucket_subresource_with_storage_node(
            &storage_node,
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

    pub fn delete_bucket_tags_for_untag_resource(
        &self,
        req: &UntagBucketTagControlRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_tags_for_untag_resource",
            "bucket={:?}",
            req.control.bucket.name
        );
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_untag_bucket_tag_control_with_storage_node(&storage_node, req)?;
        let info = storage_node
            .delete_bucket_subresource_and_load_info(
                &authorized.bucket,
                storage::BucketSubresourceKind::Tagging,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
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
        let storage_node = self.storage_node();
        let authorized = self.authorize_put_bucket_abac_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized = self.authorize_put_bucket_policy_with_storage_node(&storage_node, req)?;
        let info = self.store_bucket_subresource_with_storage_node(
            &storage_node,
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Policy,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::policy(authorized.policy_is_public),
            },
        )?;
        self.clear_bucket_fast_path(&info);
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_delete_bucket_policy_with_storage_node(&storage_node, req)?;
        let info = storage_node
            .delete_bucket_subresource_and_load_info(
                &authorized.bucket,
                storage::BucketSubresourceKind::Policy,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
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
        let storage_node = self.storage_node();
        #[cfg(test)]
        self.maybe_run_bucket_mutation_storage_node_capture_hook(req.bucket.name.as_str());
        let authorized =
            self.authorize_put_bucket_lifecycle_with_storage_node(&storage_node, req)?;
        #[cfg(test)]
        if self.should_probe_bucket_mutation_write(authorized.bucket.as_str()) {
            let bucket_pg_ready = storage_node
                .try_probe_bucket_pg_available(&authorized.bucket)
                .map_err(Self::map_bucket_snapshot_load_error)?;
            if !bucket_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: bucket pg still locked before put_bucket_lifecycle write"
                        .to_string(),
                });
            }
        }
        let info = self.store_bucket_subresource_with_storage_node(
            &storage_node,
            &authorized.bucket,
            storage::PutBucketSubresource {
                kind: storage::BucketSubresourceKind::Lifecycle,
                body: &authorized.body,
                aux: storage::BucketSubresourceAux::None,
            },
        )?;
        self.clear_bucket_fast_path(&info);
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_delete_bucket_lifecycle_with_storage_node(&storage_node, req)?;
        let info = storage_node
            .delete_bucket_subresource_and_load_info(
                &authorized.bucket,
                storage::BucketSubresourceKind::Lifecycle,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_put_bucket_public_access_block_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_delete_bucket_public_access_block_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_put_bucket_ownership_controls_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized =
            self.authorize_delete_bucket_ownership_controls_with_storage_node(&storage_node, req)?;
        let info = storage_node
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
        let storage_node = self.storage_node();
        let authorized = self.authorize_put_bucket_acl_with_storage_node(&storage_node, req)?;
        self.apply_authorized_bucket_acl_update(&storage_node, &authorized)
    }

    pub fn validate_put_bucket_acl_request(
        &self,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<(), ServerError> {
        let storage_node = self.storage_node();
        self.authorize_put_bucket_acl_with_storage_node(&storage_node, req)
            .map(|_| ())
    }

    fn apply_authorized_bucket_acl_update(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        authorized: &AuthorizedPutBucketAcl,
    ) -> Result<(), ServerError> {
        #[cfg(test)]
        if self.should_probe_bucket_mutation_write(authorized.bucket.as_str()) {
            let bucket_pg_ready = storage_node
                .try_probe_bucket_pg_available(&authorized.bucket)
                .map_err(Self::map_bucket_snapshot_load_error)?;
            if !bucket_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: bucket pg still locked before put_bucket_acl write"
                        .to_string(),
                });
            }
        }
        let info = storage_node
            .put_bucket_acl_and_load_info(
                &authorized.bucket,
                &authorized.acl_grants,
                authorized.summary,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    fn store_bucket_subresource_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        name: &BucketName,
        req: storage::PutBucketSubresource<'_>,
    ) -> Result<storage::BucketInfo, ServerError> {
        storage_node
            .put_bucket_subresource_and_load_info(name, req)
            .map_err(Self::map_bucket_snapshot_load_error)
    }
}
