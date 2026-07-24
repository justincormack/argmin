use super::*;

impl Coordinator {
    pub(in crate::coordinator) fn authorize_create_bucket(
        &self,
        req: &CreateBucketRequest,
    ) -> Result<AuthorizedCreateBucket, ServerError> {
        let owner_account = req.requester.account().ok_or(ServerError::AccessDenied)?;
        let owner_principal = req
            .requester
            .configured_principal()
            .ok_or(ServerError::AccessDenied)?;
        let locked_to_account_region =
            self.validate_create_bucket_namespace(&req.name, req.namespace, owner_account)?;
        if req.ownership == BucketObjectOwnership::BucketOwnerEnforced && req.acl.is_explicit() {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }
        let owner = OwnerIdentity::new(owner_principal, owner_account.canonical_user_id().clone());
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
            owner,
            locked_to_account_region,
            acl: req.acl.clone(),
            ownership: req.ownership,
            object_lock_enabled: req.object_lock_enabled,
            acl_grants,
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_head_bucket(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedHeadBucket, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        self.authorize_head_bucket_with_loaded_handle(req, bucket)
    }

    pub(in crate::coordinator) fn authorize_head_bucket_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedHeadBucket, ServerError> {
        let bucket =
            self.load_bucket_handle_for_bucket_policy_read_on_admitted_route(admission, req)?;
        self.authorize_head_bucket_with_loaded_handle(req, bucket)
    }

    fn authorize_head_bucket_with_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedHeadBucket, ServerError> {
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_read_fallback = || {
            Self::requester_can_read_bucket(
                req.requester(),
                bucket.bucket(),
                &bucket.bucket().owner_principal,
                &bucket.bucket().acl_grants,
                Self::effective_public_read(bucket.bucket()),
            )
        };
        let list_decision = self.bucket_policy_decision_for_loaded_handle(
            req.requester(),
            &bucket,
            auth::PolicyAction::ListBucket,
            bucket_policy.as_deref(),
        )?;
        let location_decision = self.bucket_policy_decision_for_loaded_handle(
            req.requester(),
            &bucket,
            auth::PolicyAction::GetBucketLocation,
            bucket_policy.as_deref(),
        )?;
        let list_allowed = Self::bucket_policy_allows_with_fallback(
            req.requester(),
            bucket.bucket(),
            list_decision,
            bucket_read_fallback,
        );
        let location_allowed = Self::bucket_policy_allows_with_fallback(
            req.requester(),
            bucket.bucket(),
            location_decision,
            bucket_read_fallback,
        );
        if !(list_allowed && location_allowed) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedHeadBucket {
            bucket_info: bucket.bucket().clone(),
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_delete_bucket(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucket, ServerError> {
        self.authorize_delete_bucket_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucket, ServerError> {
        let bucket = match self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::DeleteBucket,
            Self::requester_can_bucket_owner_account_admin,
        ) {
            Ok(bucket) => bucket,
            Err(ServerError::OperationAborted) => {
                if let Some(authorized) =
                    self.authorize_delete_bucket_from_active_attempt_snapshot(storage_node, req)?
                {
                    return Ok(authorized);
                }
                return Err(ServerError::OperationAborted);
            }
            Err(ServerError::BucketNotFound { name }) => {
                if let Some(authorized) =
                    self.authorize_delete_bucket_from_raw_snapshot(storage_node, req)?
                {
                    return Ok(authorized);
                }
                return Err(ServerError::BucketNotFound { name });
            }
            Err(error) => return Err(error),
        };
        Ok(AuthorizedDeleteBucket {
            name: bucket.bucket().name.clone(),
            bucket_execution_generation: bucket.bucket_execution_generation(),
            bucket_incarnation_generation: bucket.bucket_incarnation_generation(),
        })
    }

    fn authorize_delete_bucket_from_raw_snapshot(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<Option<AuthorizedDeleteBucket>, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        let snapshot = match storage_node.load_bucket_delete_authorization_snapshot(
            req.name_typed(),
            request.resolve_to_storage_request(),
        ) {
            Ok(snapshot) => snapshot,
            Err(_) => return Ok(None),
        };
        if snapshot.bucket.state != BucketState::Deleting {
            return Ok(None);
        }

        // DeleteBucket itself installs or observes the bucket write drain. Once
        // the drain has marked the bucket Deleting, normal bucket snapshots hide
        // it as not found, but an idempotent retry must still be able to reach
        // begin_bucket_delete where AlreadyDeleting is handled.
        self.authorize_delete_bucket_loaded_snapshot(snapshot, req, request)
            .map(Some)
    }

    fn authorize_delete_bucket_from_active_attempt_snapshot(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<Option<AuthorizedDeleteBucket>, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        let Some(snapshot) = storage_node
            .load_active_bucket_delete_attempt_authorization_snapshot(
                req.name_typed(),
                request.resolve_to_storage_request(),
            )
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
        else {
            return Ok(None);
        };

        // The storage proof only returns an Active snapshot after the preserved
        // DeleteBucket drain is live, same-generation, has drained older bucket
        // writes, and has no unrelated pending bucket command. That makes the
        // raw policy/tag view stable enough to authorize this retry before it
        // re-enters begin_bucket_delete_if_current.
        self.authorize_delete_bucket_loaded_snapshot(snapshot, req, request)
            .map(Some)
    }

    fn authorize_delete_bucket_loaded_snapshot(
        &self,
        snapshot: storage::BucketSnapshot,
        req: &BucketRequest<'_>,
        request: BucketHandleRequest,
    ) -> Result<AuthorizedDeleteBucket, ServerError> {
        let bucket = self
            .bucket_handle_loader()
            .load_bucket_handle_from_snapshot(snapshot, req.expected_bucket_owner(), request)?;
        let default_allowed =
            Self::requester_can_bucket_owner_account_admin(req.requester(), bucket.bucket());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let allowed = self.requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
            req.requester(),
            bucket.bucket(),
            Self::loaded_bucket_tags_for_policy(&bucket)?.as_deref(),
            auth::PolicyAction::DeleteBucket,
            bucket_policy.as_deref(),
            default_allowed,
        )?;
        if !allowed {
            return Err(ServerError::AccessDenied);
        }

        Ok(AuthorizedDeleteBucket {
            name: bucket.bucket().name.clone(),
            bucket_execution_generation: bucket.bucket_execution_generation(),
            bucket_incarnation_generation: bucket.bucket_incarnation_generation(),
        })
    }

    pub(in crate::coordinator) fn authorize_put_bucket_cors_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedPutBucketCors, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutBucketCors,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedPutBucketCors {
            bucket: req.bucket.name_typed().clone(),
            body: req.config.to_string(),
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_get_bucket_cors(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketCors, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read(
            req,
            BucketHandleRequest::new().requiring_cors_view(),
        )?;
        self.authorize_get_bucket_cors_with_loaded_handle(req, bucket)
    }

    pub(in crate::coordinator) fn authorize_get_bucket_cors_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketCors, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read_on_admitted_route(
            admission,
            req,
            BucketHandleRequest::new().requiring_cors_view(),
        )?;
        self.authorize_get_bucket_cors_with_loaded_handle(req, bucket)
    }

    fn authorize_get_bucket_cors_with_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketCors, ServerError> {
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let allowed = self.requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
            &req.requester,
            bucket.bucket(),
            Self::loaded_bucket_tags_for_policy(&bucket)?.as_deref(),
            auth::PolicyAction::GetBucketCors,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        )?;
        if !allowed {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketCors {
            body: Self::loaded_bucket_subresource_body(bucket.cors())?,
        })
    }

    pub(in crate::coordinator) fn authorize_get_bucket_tagging_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketTagging, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read_on_admitted_route(
            admission,
            req,
            BucketHandleRequest::new().requiring_bucket_tags(),
        )?;
        self.authorize_get_bucket_tagging_with_loaded_handle(req, bucket)
    }

    fn authorize_get_bucket_tagging_with_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketTagging, ServerError> {
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketTagging,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketTagging {
            body: Self::loaded_bucket_subresource_body(bucket.tags())?,
        })
    }

    /// Creates an internal authorization token for HTTP CORS evaluation.
    ///
    /// This intentionally bypasses normal bucket-config authorization because
    /// CORS preflight handling and actual-response header decoration need the
    /// stored CORS rules without turning those paths into authenticated bucket
    /// config reads.
    pub(in crate::coordinator) fn authorize_load_bucket_cors_config_for(
        &self,
        name: &BucketName,
    ) -> AuthorizedLoadBucketCorsConfig {
        AuthorizedLoadBucketCorsConfig {
            bucket: name.clone(),
        }
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_cors_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucketCors, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::PutBucketCors,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedDeleteBucketCors {
            bucket: req.name_typed().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_put_bucket_tagging_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedPutBucketTagging, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutBucketTagging,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().bucket_abac_enabled {
            return Err(ServerError::BadRequest {
                reason: "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To add tags to this bucket, initiate a TagResource request. To delete tags from this bucket, initiate an UntagResource request.".to_string(),
            });
        }
        Ok(AuthorizedPutBucketTagging {
            bucket: req.bucket.name_typed().clone(),
            body: req.config.to_string(),
        })
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_tagging_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucketTagging, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::PutBucketTagging,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().bucket_abac_enabled {
            return Err(ServerError::BadRequest {
                reason: "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To delete tags from this bucket, initiate an UntagResource request.".to_string(),
            });
        }
        Ok(AuthorizedDeleteBucketTagging {
            bucket: req.name_typed().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_bucket_tag_control_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketTagControlRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket = self.authorize_loaded_bucket_owner_account_admin_write_for_storage_node(
            storage_node,
            &req.bucket,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.bucket.name_typed().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_put_bucket_tag_control_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketTagControlRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        self.authorize_bucket_tag_resource_action_with_storage_node(
            storage_node,
            &req.control,
            req.request_tags,
            BucketTagControlAction::TagResource,
        )
    }

    pub(in crate::coordinator) fn authorize_put_bucket_tags_for_untag_resource_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketTagsForUntagResourceRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        self.authorize_bucket_tag_resource_action_with_storage_node(
            storage_node,
            &req.control,
            req.request_tags,
            BucketTagControlAction::UntagResource,
        )
    }

    pub(in crate::coordinator) fn authorize_untag_bucket_tag_control_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &UntagBucketTagControlRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        self.authorize_bucket_tag_resource_action_with_storage_node(
            storage_node,
            &req.control,
            req.request_tags,
            BucketTagControlAction::UntagResource,
        )
    }

    pub(in crate::coordinator) fn authorize_bucket_tag_resource_action(
        &self,
        control: &BucketTagControlRequest<'_>,
        request_tags: &[(String, String)],
        action: BucketTagControlAction,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        self.authorize_bucket_tag_resource_action_with_storage_node(
            &self.storage_node(),
            control,
            request_tags,
            action,
        )
    }

    pub(in crate::coordinator) fn authorize_bucket_tag_resource_action_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        control: &BucketTagControlRequest<'_>,
        request_tags: &[(String, String)],
        action: BucketTagControlAction,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        self.with_bucket_write_handle_for_storage_node(
            storage_node,
            &control.bucket,
            BucketHandleRequest::new()
                .requiring_policy_view()
                .requiring_bucket_tags_if_abac_enabled(),
            |bucket| {
                let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
                let policy_decision = self.bucket_policy_decision_for_loaded_handle_with_context(
                    &control.bucket.requester,
                    &bucket,
                    action.policy_action(),
                    PutObjectPolicyContext::default().with_request_tags(Some(request_tags)),
                    bucket_policy.as_deref(),
                )?;
                if !Self::bucket_policy_allows_with_fallback(
                    &control.bucket.requester,
                    bucket.bucket(),
                    policy_decision,
                    || {
                        Self::requester_can_bucket_owner_account_admin(
                            &control.bucket.requester,
                            bucket.bucket(),
                        )
                    },
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Ok(AuthorizedBucketConfigAccess {
                    bucket: control.bucket.name_typed().clone(),
                })
            },
        )
    }

    pub(in crate::coordinator) fn authorize_put_bucket_abac_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketAbacRequest<'_>,
    ) -> Result<AuthorizedPutBucketAbac, ServerError> {
        let _bucket = self.authorize_loaded_bucket_owner_account_admin_write_for_storage_node(
            storage_node,
            &req.bucket,
        )?;
        Ok(AuthorizedPutBucketAbac {
            bucket: req.bucket.name_typed().clone(),
            enabled: req.enabled,
        })
    }

    pub(in crate::coordinator) fn authorize_get_bucket_abac_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketAbac, ServerError> {
        let bucket = self.bucket_handle_loader().load_bucket_on_admitted_route(
            admission,
            req.name_typed(),
            req.expected_bucket_owner(),
            BucketHandleRequest::new(),
        )?;
        Self::authorize_get_bucket_abac_with_loaded_handle(req, bucket)
    }

    fn authorize_get_bucket_abac_with_loaded_handle(
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketAbac, ServerError> {
        if !Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketAbac {
            enabled: bucket.bucket().bucket_abac_enabled,
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_put_bucket_policy(
        &self,
        req: &PutBucketPolicyRequest<'_>,
    ) -> Result<AuthorizedPutBucketPolicy, ServerError> {
        self.authorize_put_bucket_policy_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_put_bucket_policy_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketPolicyRequest<'_>,
    ) -> Result<AuthorizedPutBucketPolicy, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_policy_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutBucketPolicy,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        let parsed_policy =
            auth::parse_bucket_policy(req.config).map_err(|e| ServerError::MalformedPolicy {
                reason: e.reason().to_string(),
                detail: e.detail().map(str::to_string),
            })?;
        if let Some(resource) =
            parsed_policy.first_resource_not_scoped_to_bucket(req.bucket.name.as_str())
        {
            return Err(ServerError::MalformedPolicy {
                reason: "Policy has invalid resource".to_string(),
                detail: Some(resource.to_string()),
            });
        }
        parsed_policy
            .validate_evaluable_object_conditions()
            .map_err(|e| ServerError::MalformedPolicy {
                reason: e.reason().to_string(),
                detail: e.detail().map(str::to_string),
            })?;
        let normalized_policy = parsed_policy.normalized_json();
        if normalized_policy.len() > auth::bucket_policy::MAX_BUCKET_POLICY_BYTES {
            return Err(ServerError::MalformedPolicy {
                reason: format!(
                    "Normalized policy document exceeds the maximum allowed size of {} bytes",
                    auth::bucket_policy::MAX_BUCKET_POLICY_BYTES
                ),
                detail: None,
            });
        }
        let policy_is_public = parsed_policy.is_public();
        if Self::blocks_public_policy(bucket.bucket().public_access_block.as_ref())
            && policy_is_public
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
            policy_is_public,
        })
    }

    pub(in crate::coordinator) fn authorize_get_bucket_policy(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketPolicy, ServerError> {
        let bucket = self.authorize_loaded_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetBucketPolicy,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedGetBucketPolicy {
            body: Self::loaded_bucket_subresource_body(bucket.policy())?,
        })
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_policy_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucketPolicy, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_policy_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::DeleteBucketPolicy,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedDeleteBucketPolicy {
            bucket: req.name_typed().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_put_bucket_public_access_block_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketPublicAccessBlockRequest<'_>,
    ) -> Result<AuthorizedPutBucketPublicAccessBlock, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutBucketPublicAccessBlock,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedPutBucketPublicAccessBlock {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    pub(in crate::coordinator) fn authorize_get_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketPublicAccessBlock, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetBucketPublicAccessBlock,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedGetBucketPublicAccessBlock {
            config: bucket.bucket().public_access_block,
        })
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_public_access_block_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::PutBucketPublicAccessBlock,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_put_bucket_ownership_controls(
        &self,
        req: &PutBucketOwnershipControlsRequest<'_>,
    ) -> Result<AuthorizedPutBucketOwnershipControls, ServerError> {
        self.authorize_put_bucket_ownership_controls_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_put_bucket_ownership_controls_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketOwnershipControlsRequest<'_>,
    ) -> Result<AuthorizedPutBucketOwnershipControls, ServerError> {
        let bucket = self.with_bucket_write_handle_for_storage_node(
            storage_node,
            &req.bucket,
            BucketHandleRequest::new()
                .requiring_policy_view()
                .requiring_bucket_tags_if_abac_enabled(),
            |bucket| {
                let default_allowed = Self::requester_can_bucket_owner_account_admin(
                    req.bucket.requester(),
                    bucket.bucket(),
                );
                let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
                let policy_context = PutObjectPolicyContext::default()
                    .with_object_ownership(Some(req.config.object_ownership.as_str()));
                let policy_decision = self.bucket_policy_decision_for_loaded_handle_with_context(
                    req.bucket.requester(),
                    &bucket,
                    auth::PolicyAction::PutBucketOwnershipControls,
                    policy_context,
                    bucket_policy.as_deref(),
                )?;
                if !Self::bucket_policy_allows_with_fallback(
                    req.bucket.requester(),
                    bucket.bucket(),
                    policy_decision,
                    || default_allowed,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Ok(bucket)
            },
        )?;
        if Self::is_bucket_owner_enforced(Some(&req.config))
            && !Self::acl_grants_owner_full_control_only(
                &bucket.bucket().owner_canonical_id,
                &bucket.bucket().acl_grants,
            )
        {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }
        Ok(AuthorizedPutBucketOwnershipControls {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    pub(in crate::coordinator) fn authorize_get_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketOwnershipControls, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetBucketOwnershipControls,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedGetBucketOwnershipControls {
            config: bucket.bucket().ownership_controls,
        })
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_ownership_controls_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::PutBucketOwnershipControls,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_put_bucket_lifecycle_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedPutBucketLifecycle, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutLifecycleConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        s3_types::parse_lifecycle_configuration_xml(req.config.as_bytes()).map_err(|error| {
            match error {
                LifecycleConfigError::MalformedXml { reason } => {
                    ServerError::MalformedXML { reason }
                }
                LifecycleConfigError::InvalidRequest { reason } => {
                    ServerError::InvalidRequest { reason }
                }
                LifecycleConfigError::LifecycleV2Required { reason } => {
                    ServerError::InvalidRequestHostId { reason }
                }
                LifecycleConfigError::InvalidArgument { reason } => {
                    ServerError::InvalidArgument { reason }
                }
                LifecycleConfigError::NotImplemented { feature } => {
                    ServerError::NotImplemented { feature }
                }
            }
        })?;
        Ok(AuthorizedPutBucketLifecycle {
            bucket: req.bucket.name_typed().clone(),
            body: req.config.to_string(),
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_get_bucket_lifecycle(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketLifecycle, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read(
            req,
            BucketHandleRequest::new().requiring_lifecycle_view(),
        )?;
        self.authorize_get_bucket_lifecycle_with_loaded_handle(req, bucket)
    }

    pub(in crate::coordinator) fn authorize_get_bucket_lifecycle_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketLifecycle, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read_on_admitted_route(
            admission,
            req,
            BucketHandleRequest::new().requiring_lifecycle_view(),
        )?;
        self.authorize_get_bucket_lifecycle_with_loaded_handle(req, bucket)
    }

    fn authorize_get_bucket_lifecycle_with_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketLifecycle, ServerError> {
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let allowed = self.requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
            &req.requester,
            bucket.bucket(),
            Self::loaded_bucket_tags_for_policy(&bucket)?.as_deref(),
            auth::PolicyAction::GetLifecycleConfiguration,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        )?;
        if !allowed {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketLifecycle {
            body: Self::loaded_bucket_subresource_body(bucket.lifecycle())?,
        })
    }

    /// Creates an internal authorization token for lifecycle state loads.
    ///
    /// This intentionally bypasses request auth because the coordinator is
    /// loading already-authoritative stored lifecycle state for internal
    /// lifecycle evaluation such as response-header computation.
    pub(in crate::coordinator) fn authorize_load_bucket_lifecycle_for(
        &self,
        name: &BucketName,
    ) -> AuthorizedLoadBucketLifecycleConfig {
        AuthorizedLoadBucketLifecycleConfig {
            bucket: name.clone(),
        }
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_lifecycle_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucketLifecycle, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::PutLifecycleConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedDeleteBucketLifecycle {
            bucket: req.name_typed().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_put_bucket_encryption_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketEncryptionRequest<'_>,
    ) -> Result<AuthorizedPutBucketEncryption, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutEncryptionConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedPutBucketEncryption {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_put_bucket_versioning(
        &self,
        req: &PutBucketVersioningRequest<'_>,
    ) -> Result<AuthorizedPutBucketVersioning, ServerError> {
        self.authorize_put_bucket_versioning_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_put_bucket_versioning_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketVersioningRequest<'_>,
    ) -> Result<AuthorizedPutBucketVersioning, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutBucketVersioning,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().object_lock.enabled && req.state != BucketVersioningState::Enabled {
            return Err(ServerError::InvalidBucketState);
        }
        Ok(AuthorizedPutBucketVersioning {
            bucket: req.bucket.name_typed().clone(),
            state: req.state,
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_get_bucket_versioning(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketVersioning, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        self.authorize_get_bucket_versioning_with_loaded_handle(req, bucket)
    }

    pub(in crate::coordinator) fn authorize_get_bucket_versioning_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketVersioning, ServerError> {
        let bucket =
            self.load_bucket_handle_for_bucket_policy_read_on_admitted_route(admission, req)?;
        self.authorize_get_bucket_versioning_with_loaded_handle(req, bucket)
    }

    fn authorize_get_bucket_versioning_with_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketVersioning, ServerError> {
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketVersioning,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketVersioning {
            state: bucket.bucket().versioning,
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_get_bucket_location(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketLocation, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        self.authorize_get_bucket_location_with_loaded_handle(req, bucket)
    }

    pub(in crate::coordinator) fn authorize_get_bucket_location_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketLocation, ServerError> {
        let bucket =
            self.load_bucket_handle_for_bucket_policy_read_on_admitted_route(admission, req)?;
        self.authorize_get_bucket_location_with_loaded_handle(req, bucket)
    }

    fn authorize_get_bucket_location_with_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketLocation, ServerError> {
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketLocation,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketLocation)
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_list_objects_v2(
        &self,
        req: &ListObjectsV2Request<'_>,
    ) -> Result<AuthorizedListObjectsV2, ServerError> {
        self.authorize_list_objects_v2_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_list_objects_v2_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &ListObjectsV2Request<'_>,
    ) -> Result<AuthorizedListObjectsV2, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read_with_storage_node(
            storage_node,
            &req.bucket,
        )?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle_with_context(
            &req.bucket.requester,
            &bucket,
            auth::PolicyAction::ListBucket,
            PutObjectPolicyContext::default()
                .with_prefix(req.prefix)
                .with_delimiter(req.delimiter)
                .with_requested_max_keys(req.requested_max_keys),
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.bucket.requester,
            bucket.bucket(),
            policy_decision,
            || {
                Self::requester_can_read_bucket(
                    &req.bucket.requester,
                    bucket.bucket(),
                    &bucket.bucket().owner_principal,
                    &bucket.bucket().acl_grants,
                    Self::effective_public_read(bucket.bucket()),
                )
            },
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListObjectsV2 {
            bucket_info: bucket.bucket().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_list_buckets(
        &self,
        req: &ListBucketsRequest,
    ) -> Result<AuthorizedListBuckets, ServerError> {
        req.requester
            .configured_principal()
            .ok_or(ServerError::AccessDenied)?;
        let requester = req.requester.account().ok_or(ServerError::AccessDenied)?;
        Ok(AuthorizedListBuckets {
            owner_canonical_id: requester.canonical_user_id().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_list_object_versions(
        &self,
        req: &ListObjectVersionsRequest<'_>,
    ) -> Result<AuthorizedListObjectVersions, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(&req.bucket)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle_with_context(
            &req.bucket.requester,
            &bucket,
            auth::PolicyAction::ListBucketVersions,
            PutObjectPolicyContext::default()
                .with_prefix(req.prefix)
                .with_delimiter(req.delimiter)
                .with_requested_max_keys(req.requested_max_keys),
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.bucket.requester,
            bucket.bucket(),
            policy_decision,
            || {
                Self::requester_can_read_bucket(
                    &req.bucket.requester,
                    bucket.bucket(),
                    &bucket.bucket().owner_principal,
                    &bucket.bucket().acl_grants,
                    Self::effective_public_read(bucket.bucket()),
                )
            },
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListObjectVersions {
            bucket_info: bucket.bucket().clone(),
        })
    }

    pub(in crate::coordinator) fn authorize_list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsRequest<'_>,
    ) -> Result<AuthorizedListMultipartUploads, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(&req.bucket)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.bucket.requester,
            &bucket,
            auth::PolicyAction::ListBucketMultipartUploads,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.bucket.requester,
            bucket.bucket(),
            policy_decision,
            || {
                Self::requester_can_read_bucket(
                    &req.bucket.requester,
                    bucket.bucket(),
                    &bucket.bucket().owner_principal,
                    &bucket.bucket().acl_grants,
                    Self::effective_public_read(bucket.bucket()),
                )
            },
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListMultipartUploads {
            bucket: bucket.bucket().name.clone(),
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_put_bucket_object_lock_configuration(
        &self,
        req: &PutBucketObjectLockConfigurationRequest<'_>,
    ) -> Result<AuthorizedPutBucketObjectLockConfiguration, ServerError> {
        self.authorize_put_bucket_object_lock_configuration_with_storage_node(
            &self.storage_node(),
            req,
        )
    }

    pub(in crate::coordinator) fn authorize_put_bucket_object_lock_configuration_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketObjectLockConfigurationRequest<'_>,
    ) -> Result<AuthorizedPutBucketObjectLockConfiguration, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            &req.bucket,
            auth::PolicyAction::PutBucketObjectLockConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().versioning != BucketVersioningState::Enabled {
            return Err(ServerError::InvalidBucketState);
        }

        let final_enabled =
            bucket.bucket().object_lock.enabled || req.config.object_lock_enabled.is_some();
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

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_get_bucket_encryption(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketEncryption, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetEncryptionConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Self::authorize_get_bucket_encryption_with_loaded_handle(bucket)
    }

    pub(in crate::coordinator) fn authorize_get_bucket_encryption_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketEncryption, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for_on_admitted_route(
            admission,
            req,
            auth::PolicyAction::GetEncryptionConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Self::authorize_get_bucket_encryption_with_loaded_handle(bucket)
    }

    fn authorize_get_bucket_encryption_with_loaded_handle(
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketEncryption, ServerError> {
        Ok(AuthorizedGetBucketEncryption {
            config: bucket.bucket().encryption,
        })
    }

    pub(in crate::coordinator) fn authorize_delete_bucket_encryption_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucketEncryption, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for_storage_node(
            storage_node,
            req,
            auth::PolicyAction::PutEncryptionConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedDeleteBucketEncryption {
            bucket: req.name_typed().clone(),
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_get_bucket_object_lock_configuration(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketObjectLockConfiguration, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetBucketObjectLockConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Self::authorize_get_bucket_object_lock_configuration_with_loaded_handle(req, bucket)
    }

    fn authorize_get_bucket_object_lock_configuration_with_loaded_handle(
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
    ) -> Result<AuthorizedGetBucketObjectLockConfiguration, ServerError> {
        if !bucket.bucket().object_lock.enabled {
            return Err(ServerError::ObjectLockConfigurationNotFound {
                bucket: req.name.to_string(),
            });
        }
        Ok(AuthorizedGetBucketObjectLockConfiguration {
            config: bucket.bucket().object_lock,
        })
    }

    pub(in crate::coordinator) fn authorize_get_bucket_object_lock_configuration_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketObjectLockConfiguration, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for_on_admitted_route(
            admission,
            req,
            auth::PolicyAction::GetBucketObjectLockConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Self::authorize_get_bucket_object_lock_configuration_with_loaded_handle(req, bucket)
    }

    pub(in crate::coordinator) fn authorize_get_bucket_policy_status(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketPolicyStatus, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        if !bucket.bucket().bucket_policy_present {
            if !Self::requester_can_bucket_admin(&req.requester, &bucket.bucket().owner_principal) {
                return Err(ServerError::AccessDenied);
            }
            return Err(ServerError::NoSuchBucketPolicy {
                bucket: req.name.to_string(),
            });
        }
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketPolicyStatus,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_admin(&req.requester, &bucket.bucket().owner_principal),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketPolicyStatus {
            is_public: bucket.bucket().bucket_policy_public,
        })
    }

    pub(in crate::coordinator) fn authorize_get_bucket_acl(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketAcl, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketAcl,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_read_bucket_acl(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        let result = if Self::is_bucket_owner_enforced(bucket.bucket().ownership_controls.as_ref())
        {
            let owner = Self::bucket_owner_identity(bucket.bucket());
            GetBucketAclResult {
                owner_principal: owner.principal,
                owner_canonical_id: owner.canonical_id.clone(),
                acl_grants: AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(owner.canonical_id),
                    AclPermission::FullControl,
                )]),
            }
        } else {
            let bucket = bucket.bucket();
            let acl_grants = Self::effective_acl_grants(bucket, &bucket.acl_grants).into_owned();
            GetBucketAclResult {
                owner_principal: bucket.owner_principal.clone(),
                owner_canonical_id: bucket.owner_canonical_id.clone(),
                acl_grants,
            }
        };
        Ok(AuthorizedGetBucketAcl { result })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_put_bucket_acl(
        &self,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<AuthorizedPutBucketAcl, ServerError> {
        self.authorize_put_bucket_acl_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_put_bucket_acl_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<AuthorizedPutBucketAcl, ServerError> {
        let bucket = self.with_bucket_write_handle_for_storage_node(
            storage_node,
            &req.bucket,
            BucketHandleRequest::new()
                .requiring_policy_view()
                .requiring_bucket_tags_if_abac_enabled(),
            |bucket| {
                let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
                let policy_decision = self.bucket_policy_decision_for_loaded_handle_with_context(
                    &req.bucket.requester,
                    &bucket,
                    auth::PolicyAction::PutBucketAcl,
                    req.authorization_policy_context()?,
                    bucket_policy.as_deref(),
                )?;
                if !Self::bucket_policy_allows_with_fallback(
                    &req.bucket.requester,
                    bucket.bucket(),
                    policy_decision,
                    || Self::requester_can_write_bucket_acl(&req.bucket.requester, bucket.bucket()),
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Ok(bucket)
            },
        )?;
        let owner = Self::bucket_owner_identity(bucket.bucket());
        let acl_grants = match &req.acl {
            PutBucketAclInput::Canned(acl) => {
                Self::ensure_put_bucket_acl_supported(bucket.bucket(), *acl)?;
                Self::bucket_acl_grants_from_canned(&owner, *acl)?
            }
            PutBucketAclInput::Grants(acl_grants) => {
                if Self::is_bucket_owner_enforced(bucket.bucket().ownership_controls.as_ref()) {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                Self::ensure_supported_bucket_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        let public_read = Self::acl_grants_public_read(&acl_grants);
        let public_write = Self::acl_grants_public_write(&acl_grants);
        if Self::blocks_public_acls(bucket.bucket().public_access_block.as_ref())
            && (Self::acl_grants_grant_public_read(&acl_grants)
                || Self::acl_grants_grant_public_write(&acl_grants))
        {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedPutBucketAcl {
            bucket: req.bucket.name_typed().clone(),
            acl_grants,
            summary: storage::BucketAclSummary {
                public_read,
                public_write,
            },
        })
    }
}
