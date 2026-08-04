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
    BucketCreateOutcome, BucketRequest, BucketSummary, BucketTagControlAction,
    BucketTagControlRequest, Coordinator, CreateBucketAcl, CreateBucketRequest, GetBucketAclResult,
    ListBucketsRequest, PutBucketAbacRequest, PutBucketAclInput, PutBucketAclRequest,
    PutBucketConfigRequest, PutBucketEncryptionRequest, PutBucketObjectLockConfigurationRequest,
    PutBucketOwnershipControlsRequest, PutBucketPolicyRequest, PutBucketPublicAccessBlockRequest,
    PutBucketTagControlRequest, PutBucketTagsForUntagResourceRequest, PutBucketTagsRequest,
    PutBucketVersioningRequest, UntagBucketTagControlRequest, TRACE_TARGET,
};
use crate::error::ServerError;

pub(super) fn map_bucket_write_drain_error(err: storage::BucketWriteDrainError) -> ServerError {
    let _ = observability::event(
        TRACE_TARGET,
        "bucket_write_drain_error",
        Some(format_args!("error={err:?}")),
    );
    match err {
        storage::BucketWriteDrainError::Store(ref error)
            if super::store_error_is_metadata_command_contention(error) =>
        {
            ServerError::OperationAborted
        }
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
        Self::map_bucket_snapshot_load_error_with_metadata_contention(
            err,
            super::MetadataContentionResponse::SlowDown,
        )
    }

    fn map_bucket_snapshot_load_error_with_metadata_contention(
        err: storage::BucketSnapshotLoadError,
        metadata_contention: super::MetadataContentionResponse,
    ) -> ServerError {
        let _ = observability::event(
            TRACE_TARGET,
            "bucket_snapshot_load_error",
            Some(format_args!("error={err:?}")),
        );
        match err {
            storage::BucketSnapshotLoadError::Store(other) => {
                super::map_store_error_with_metadata_contention(other, metadata_contention)
            }
            storage::BucketSnapshotLoadError::Metadata(ref error)
                if super::metadata_error_is_command_contention(error) =>
            {
                metadata_contention.into_server_error()
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

    pub fn create_bucket_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &CreateBucketRequest,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::create_bucket",
            "bucket={:?} object_lock_enabled={}",
            req.name,
            req.object_lock_enabled
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_create_bucket(req)?;
        let route = admission
            .active_bucket_route(&authorized.name)
            .map_err(super::map_store_error)?;
        let create_outcome = self.create_bucket_with_acl_grants_on_admitted_route(
            &route,
            &authorized.owner,
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
                let existing = route
                    .head_bucket_info()
                    .map_err(Self::map_bucket_snapshot_load_error)?;
                if existing.state != BucketState::Active {
                    return Err(ServerError::BucketNotFound {
                        name: authorized.name.to_string(),
                    });
                }
                let existing = Self::bucket_summary(existing);
                if !Self::is_bucket_owner_enforced(existing.ownership_controls.as_ref()) {
                    return Err(ServerError::BucketAlreadyOwnedByYou);
                }
                let authorized_acl = self.resolve_create_bucket_recreate_acl_update(
                    &existing,
                    &authorized.owner,
                    &authorized.acl,
                )?;
                self.apply_authorized_bucket_acl_update_on_route(&route, &authorized_acl)
            }
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn create_bucket(&self, req: &CreateBucketRequest) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.create_bucket_on_admitted_route(&admission, req)
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
        let admission = self.admit_storage_route_for_request()?;
        let route = admission
            .active_bucket_route(&trusted_bucket_name(name))
            .map_err(super::map_store_error)?;
        self.create_bucket_with_acl_grants_on_admitted_route(
            &route,
            owner,
            acl_grants,
            BucketObjectOwnership::ObjectWriter,
            object_lock_enabled,
        )
    }

    fn create_bucket_with_acl_grants_on_admitted_route(
        &self,
        route: &storage::ActiveBucketRoute<'_>,
        owner: &OwnerIdentity,
        acl_grants: AclGrants,
        ownership: BucketObjectOwnership,
        object_lock_enabled: bool,
    ) -> Result<BucketCreateOutcome, ServerError> {
        let name = route.bucket();
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
            match route
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
                storage::BucketCreateAttemptOutcome::Created(info) => {
                    self.clear_bucket_fast_path(&info);
                    return Ok(BucketCreateOutcome::Created);
                }
                storage::BucketCreateAttemptOutcome::Exists(existing) => match existing.state {
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
                        match route.try_finalize_bucket_delete() {
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
                            Ok(
                                storage::BucketDeleteFinalizeOutcome::NotDeleting
                                | storage::BucketDeleteFinalizeOutcome::StaleIncarnation,
                            ) => {
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
                },
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

    pub fn delete_bucket_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
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
        self.require_storage_route_admission(admission)?;
        let authorize_started = std::time::Instant::now();
        let _ = observability::emit_flight_event(
            TRACE_TARGET,
            "bucket_delete_authorize_start",
            format!("bucket={:?}", req.name),
        );
        let AuthorizedDeleteBucket {
            name,
            bucket_execution_generation,
            bucket_incarnation_generation,
        } = match self.authorize_delete_bucket_on_admitted_route(admission, req) {
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
        let route = admission
            .active_bucket_route(&name)
            .map_err(super::map_store_error)?;
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
        if let Err(err) = route.begin_bucket_delete_if_current(storage::BucketIdentityGenerations {
            bucket_execution_generation,
            bucket_incarnation_generation,
        }) {
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
        route.enqueue_bucket_delete_finalize(bucket_incarnation_generation);
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_on_admitted_route(&admission, req)
    }

    pub fn head_bucket_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<BucketSummary, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::head_bucket_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let AuthorizedHeadBucket { bucket_info } =
            self.authorize_head_bucket_on_admitted_route(admission, req)?;
        Ok(bucket_info)
    }

    #[cfg(test)]
    pub(crate) fn head_bucket(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<BucketSummary, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.head_bucket_on_admitted_route(&admission, req)
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

    pub fn bucket_exists_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        name: &BucketName,
    ) -> Result<bool, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::bucket_exists_on_admitted_route",
            "bucket={:?}",
            name
        );
        self.require_storage_route_admission(admission)?;
        match self.unchecked_active_bucket_summary_for_admitted_route(admission, name) {
            Ok(_) => Ok(true),
            Err(ServerError::BucketNotFound { .. }) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn list_buckets_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &ListBucketsRequest,
    ) -> Result<Vec<BucketSummary>, ServerError> {
        observability::trace_scope!(TRACE_TARGET, "Coordinator::list_buckets_on_admitted_route");
        self.require_storage_route_admission(admission)?;
        let AuthorizedListBuckets { owner_canonical_id } = self.authorize_list_buckets(req)?;
        let buckets = admission
            .active_bucket_metadata_scan(&owner_canonical_id)
            .map_err(super::map_store_error)?
            .list_buckets_for_owner()
            .map_err(Self::map_object_pg_action_error)?;
        Ok(buckets.into_iter().map(Self::bucket_summary).collect())
    }

    #[cfg(test)]
    pub fn list_buckets(
        &self,
        req: &ListBucketsRequest,
    ) -> Result<Vec<BucketSummary>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.list_buckets_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_versioning_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketVersioningRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_versioning",
            "bucket={:?} state={:?}",
            req.bucket.name,
            req.state
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_versioning_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_versioning_and_load_info(authorized.state)
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_versioning(
        &self,
        req: &PutBucketVersioningRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_versioning_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_versioning_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<BucketVersioningState, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_versioning_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_versioning_on_admitted_route(admission, req)?;
        Ok(authorized.state)
    }

    #[cfg(test)]
    pub fn get_bucket_versioning(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<BucketVersioningState, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_versioning_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_location_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_location_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let _authorized = self.authorize_get_bucket_location_on_admitted_route(admission, req)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn get_bucket_location(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_location_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_object_lock_configuration_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
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
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_put_bucket_object_lock_configuration_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_object_lock_and_load_info(authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_object_lock_configuration(
        &self,
        req: &PutBucketObjectLockConfigurationRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_object_lock_configuration_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_object_lock_configuration_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<BucketObjectLockConfig, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_object_lock_configuration_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized =
            self.authorize_get_bucket_object_lock_configuration_on_admitted_route(admission, req)?;
        Ok(authorized.config)
    }

    #[cfg(test)]
    pub fn get_bucket_object_lock_configuration(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<BucketObjectLockConfig, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_object_lock_configuration_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_encryption_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketEncryptionRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_encryption",
            "bucket={:?} sse_c_blocked={}",
            req.bucket.name,
            req.config.sse_c_blocked
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_encryption_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_encryption_and_load_info(authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_encryption(
        &self,
        req: &PutBucketEncryptionRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_encryption_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_encryption_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<EffectiveBucketEncryptionConfig, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_encryption_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_encryption_on_admitted_route(admission, req)?;
        Ok(authorized.config)
    }

    #[cfg(test)]
    pub fn get_bucket_encryption(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<EffectiveBucketEncryptionConfig, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_encryption_on_admitted_route(&admission, req)
    }

    pub fn delete_bucket_encryption_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_encryption",
            "bucket={:?}",
            req.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_delete_bucket_encryption_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_encryption_and_load_info(BucketEncryptionConfig::default())
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_encryption(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_encryption_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_cors_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_cors",
            "bucket={:?} bytes={}",
            req.bucket.name,
            req.config.len()
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_cors_on_admitted_route(admission, req)?;
        let info = self.store_bucket_subresource_on_admitted_route(
            admission,
            &authorized.bucket,
            storage::PutBucketSubresource::cors(&authorized.body),
        )?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_cors(&self, req: &PutBucketConfigRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_cors_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_cors_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_cors_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_cors_on_admitted_route(admission, req)?;
        Ok(authorized.body)
    }

    #[cfg(test)]
    pub fn get_bucket_cors(&self, req: &BucketRequest<'_>) -> Result<Option<String>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_cors_on_admitted_route(&admission, req)
    }

    pub fn load_bucket_cors_config(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        name: &BucketName,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::load_bucket_cors_config",
            "bucket={:?}",
            name
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_load_bucket_cors_config_for(name);
        admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .get_bucket_subresource(storage::OpaqueBucketSubresourceKind::Cors)
            .map_err(Self::map_bucket_snapshot_load_error)
    }

    pub fn delete_bucket_cors_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_cors",
            "bucket={:?}",
            req.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_delete_bucket_cors_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .delete_bucket_subresource_and_load_info(storage::OpaqueBucketSubresourceKind::Cors)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_cors(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_cors_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_tags_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketTagsRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_tags",
            "bucket={:?} tag_count={}",
            req.bucket.name,
            req.tags.len()
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_tagging_on_admitted_route(admission, req)?;
        let stored_tags =
            storage::SerializedBucketTagSet::from_tag_set(authorized.tags).map_err(|error| {
                ServerError::InternalError {
                    reason: format!("authorized bucket tags exceed the stored limit: {error}"),
                }
            })?;
        let info = self.store_bucket_subresource_with_metadata_contention_on_admitted_route(
            admission,
            &authorized.bucket,
            storage::PutBucketSubresource::tagging(&stored_tags),
            super::MetadataContentionResponse::OperationAborted,
        )?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_tags(&self, req: &PutBucketTagsRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_tags_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_tags_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<Option<s3_types::TagSet>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_tags_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_tagging_on_admitted_route(admission, req)?;
        Ok(authorized.tags)
    }

    #[cfg(test)]
    pub fn get_bucket_tags(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<s3_types::TagSet>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_tags_on_admitted_route(&admission, req)
    }

    pub fn delete_bucket_tags_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_tags",
            "bucket={:?}",
            req.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_delete_bucket_tagging_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .delete_bucket_tags_and_load_info()
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_tags(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_tags_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_tags_for_control_action_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketTagControlRequest<'_>,
        request_tags: &[(String, String)],
        action: BucketTagControlAction,
    ) -> Result<Option<s3_types::TagSet>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_tags_for_control_action",
            "bucket={:?}",
            req.bucket.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_bucket_tag_resource_action_on_admitted_route(
            admission,
            req,
            request_tags,
            action,
        )?;
        admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .get_bucket_tags()
            .map(|tags| tags.map(|tags| tags.tag_set().clone()))
            .map_err(Self::map_bucket_snapshot_load_error)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn get_bucket_tags_for_control_action(
        &self,
        req: &BucketTagControlRequest<'_>,
        request_tags: &[(String, String)],
        action: BucketTagControlAction,
    ) -> Result<Option<s3_types::TagSet>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_tags_for_control_action_on_admitted_route(
            &admission,
            req,
            request_tags,
            action,
        )
    }

    pub fn put_bucket_tags_for_tag_resource_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketTagControlRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_tags_for_tag_resource",
            "bucket={:?} tag_count={}",
            req.control.bucket.name,
            req.tags.len()
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_tag_control_on_admitted_route(admission, req)?;
        let stored_tags =
            storage::SerializedBucketTagSet::from_tag_set(req.tags.clone()).map_err(|error| {
                ServerError::InternalError {
                    reason: format!("authorized bucket tags exceed the stored limit: {error}"),
                }
            })?;
        let info = self.store_bucket_subresource_on_admitted_route(
            admission,
            &authorized.bucket,
            storage::PutBucketSubresource::tagging(&stored_tags),
        )?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_tags_for_tag_resource(
        &self,
        req: &PutBucketTagControlRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_tags_for_tag_resource_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_tags_for_untag_resource_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketTagsForUntagResourceRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_tags_for_untag_resource",
            "bucket={:?} bytes={}",
            req.control.bucket.name,
            req.tags.len()
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_put_bucket_tags_for_untag_resource_on_admitted_route(admission, req)?;
        let stored_tags =
            storage::SerializedBucketTagSet::from_tag_set(req.tags.clone()).map_err(|error| {
                ServerError::InternalError {
                    reason: format!("authorized bucket tags exceed the stored limit: {error}"),
                }
            })?;
        let info = self.store_bucket_subresource_on_admitted_route(
            admission,
            &authorized.bucket,
            storage::PutBucketSubresource::tagging(&stored_tags),
        )?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_tags_for_untag_resource(
        &self,
        req: &PutBucketTagsForUntagResourceRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_tags_for_untag_resource_on_admitted_route(&admission, req)
    }

    pub fn delete_bucket_tags_for_untag_resource_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &UntagBucketTagControlRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_tags_for_untag_resource",
            "bucket={:?}",
            req.control.bucket.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_untag_bucket_tag_control_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .delete_bucket_tags_and_load_info()
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_tags_for_untag_resource(
        &self,
        req: &UntagBucketTagControlRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_tags_for_untag_resource_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_abac_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketAbacRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_abac",
            "bucket={:?} enabled={}",
            req.bucket.name,
            req.enabled
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_abac_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_abac_enabled_and_load_info(authorized.enabled)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_abac(&self, req: &PutBucketAbacRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_abac_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_abac_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<bool, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_abac_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_abac_on_admitted_route(admission, req)?;
        Ok(authorized.enabled)
    }

    #[cfg(test)]
    pub fn get_bucket_abac(&self, req: &BucketRequest<'_>) -> Result<bool, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_abac_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_policy_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketPolicyRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_policy",
            "bucket={:?} bytes={}",
            req.bucket.name,
            req.config.len()
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_policy_on_admitted_route(admission, req)?;
        let info = self.store_bucket_subresource_on_admitted_route(
            admission,
            &authorized.bucket,
            storage::PutBucketSubresource::policy(&authorized.body, authorized.policy_is_public),
        )?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_policy(&self, req: &PutBucketPolicyRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_policy_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_policy_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_policy_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_policy_on_admitted_route(admission, req)?;
        Ok(authorized.body)
    }

    #[cfg(test)]
    pub fn get_bucket_policy(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_policy_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_policy_status_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<bool, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_policy_status_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized =
            self.authorize_get_bucket_policy_status_on_admitted_route(admission, req)?;
        Ok(authorized.is_public)
    }

    #[cfg(test)]
    pub fn get_bucket_policy_status(&self, req: &BucketRequest<'_>) -> Result<bool, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_policy_status_on_admitted_route(&admission, req)
    }

    pub fn delete_bucket_policy_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_policy",
            "bucket={:?}",
            req.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_delete_bucket_policy_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .delete_bucket_subresource_and_load_info(storage::OpaqueBucketSubresourceKind::Policy)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_policy(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_policy_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_lifecycle_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_lifecycle",
            "bucket={:?} bytes={}",
            req.bucket.name,
            req.config.len()
        );
        self.require_storage_route_admission(admission)?;
        #[cfg(test)]
        self.maybe_run_bucket_mutation_storage_node_capture_hook(req.bucket.name.as_str());
        let authorized = self.authorize_put_bucket_lifecycle_on_admitted_route(admission, req)?;
        #[cfg(test)]
        if self.should_probe_bucket_mutation_write(authorized.bucket.as_str()) {
            let bucket_pg_ready = admission
                .active_bucket_route(&authorized.bucket)
                .map_err(super::map_store_error)?
                .try_probe_bucket_pg_available()
                .map_err(Self::map_bucket_snapshot_load_error)?;
            if !bucket_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: bucket pg still locked before put_bucket_lifecycle write"
                        .to_string(),
                });
            }
        }
        let info = self.store_bucket_subresource_on_admitted_route(
            admission,
            &authorized.bucket,
            storage::PutBucketSubresource::lifecycle(&authorized.body),
        )?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_lifecycle(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_lifecycle_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_lifecycle_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_lifecycle_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_lifecycle_on_admitted_route(admission, req)?;
        Ok(authorized.body)
    }

    #[cfg(test)]
    pub fn get_bucket_lifecycle(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<String>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_lifecycle_on_admitted_route(&admission, req)
    }

    pub fn delete_bucket_lifecycle_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_lifecycle",
            "bucket={:?}",
            req.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_delete_bucket_lifecycle_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .delete_bucket_subresource_and_load_info(
                storage::OpaqueBucketSubresourceKind::Lifecycle,
            )
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_lifecycle(&self, req: &BucketRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_lifecycle_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_public_access_block_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketPublicAccessBlockRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_public_access_block",
            "bucket={:?} config={:?}",
            req.bucket.name,
            req.config
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_put_bucket_public_access_block_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_public_access_block_and_load_info(authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_public_access_block(
        &self,
        req: &PutBucketPublicAccessBlockRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_public_access_block_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_public_access_block_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<Option<PublicAccessBlockConfig>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_public_access_block_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized =
            self.authorize_get_bucket_public_access_block_on_admitted_route(admission, req)?;
        Ok(authorized.config)
    }

    #[cfg(test)]
    pub fn get_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<PublicAccessBlockConfig>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_public_access_block_on_admitted_route(&admission, req)
    }

    pub fn delete_bucket_public_access_block_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_public_access_block",
            "bucket={:?}",
            req.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_delete_bucket_public_access_block_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .delete_bucket_public_access_block_and_load_info()
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_public_access_block_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_ownership_controls_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketOwnershipControlsRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_ownership_controls",
            "bucket={:?} config={:?}",
            req.bucket.name,
            req.config
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_put_bucket_ownership_controls_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_ownership_controls_and_load_info(authorized.config)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_ownership_controls(
        &self,
        req: &PutBucketOwnershipControlsRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_ownership_controls_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_ownership_controls_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<Option<BucketOwnershipControls>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_ownership_controls_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized =
            self.authorize_get_bucket_ownership_controls_on_admitted_route(admission, req)?;
        Ok(authorized.config)
    }

    #[cfg(test)]
    pub fn get_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<Option<BucketOwnershipControls>, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_ownership_controls_on_admitted_route(&admission, req)
    }

    pub fn delete_bucket_ownership_controls_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_ownership_controls",
            "bucket={:?}",
            req.name
        );
        self.require_storage_route_admission(admission)?;
        let authorized =
            self.authorize_delete_bucket_ownership_controls_on_admitted_route(admission, req)?;
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .delete_bucket_ownership_controls_and_load_info()
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn delete_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_bucket_ownership_controls_on_admitted_route(&admission, req)
    }

    pub fn get_bucket_acl_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<GetBucketAclResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_acl_on_admitted_route",
            "bucket={:?}",
            req.name
        );
        let authorized = self.authorize_get_bucket_acl_on_admitted_route(admission, req)?;
        Ok(authorized.result)
    }

    #[cfg(test)]
    pub fn get_bucket_acl(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<GetBucketAclResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.get_bucket_acl_on_admitted_route(&admission, req)
    }

    pub fn put_bucket_acl_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<(), ServerError> {
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
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_put_bucket_acl_on_admitted_route(admission, req)?;
        self.apply_authorized_bucket_acl_update_on_admitted_route(admission, &authorized)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn put_bucket_acl(&self, req: &PutBucketAclRequest<'_>) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_bucket_acl_on_admitted_route(&admission, req)
    }

    pub fn validate_put_bucket_acl_request_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<(), ServerError> {
        self.require_storage_route_admission(admission)?;
        self.authorize_put_bucket_acl_on_admitted_route(admission, req)
            .map(|_| ())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn validate_put_bucket_acl_request(
        &self,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.validate_put_bucket_acl_request_on_admitted_route(&admission, req)
    }

    fn apply_authorized_bucket_acl_update_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        authorized: &AuthorizedPutBucketAcl,
    ) -> Result<(), ServerError> {
        #[cfg(test)]
        if self.should_probe_bucket_mutation_write(authorized.bucket.as_str()) {
            let bucket_pg_ready = admission
                .active_bucket_route(&authorized.bucket)
                .map_err(super::map_store_error)?
                .try_probe_bucket_pg_available()
                .map_err(Self::map_bucket_snapshot_load_error)?;
            if !bucket_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: bucket pg still locked before put_bucket_acl write"
                        .to_string(),
                });
            }
        }
        let info = admission
            .active_bucket_route(&authorized.bucket)
            .map_err(super::map_store_error)?
            .put_bucket_acl_and_load_info(&authorized.acl_grants, authorized.summary)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    fn apply_authorized_bucket_acl_update_on_route(
        &self,
        route: &storage::ActiveBucketRoute<'_>,
        authorized: &AuthorizedPutBucketAcl,
    ) -> Result<(), ServerError> {
        let info = route
            .put_bucket_acl_and_load_info(&authorized.acl_grants, authorized.summary)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        self.clear_bucket_fast_path(&info);
        Ok(())
    }

    fn store_bucket_subresource_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        name: &BucketName,
        req: storage::PutBucketSubresource<'_>,
    ) -> Result<storage::BucketInfo, ServerError> {
        self.store_bucket_subresource_with_metadata_contention_on_admitted_route(
            admission,
            name,
            req,
            super::MetadataContentionResponse::SlowDown,
        )
    }

    fn store_bucket_subresource_with_metadata_contention_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        name: &BucketName,
        req: storage::PutBucketSubresource<'_>,
        metadata_contention: super::MetadataContentionResponse,
    ) -> Result<storage::BucketInfo, ServerError> {
        self.require_storage_route_admission(admission)?;
        admission
            .active_bucket_route(name)
            .map_err(super::map_store_error)?
            .put_bucket_subresource_and_load_info(req)
            .map_err(|error| {
                Self::map_bucket_snapshot_load_error_with_metadata_contention(
                    error,
                    metadata_contention,
                )
            })
    }
}
