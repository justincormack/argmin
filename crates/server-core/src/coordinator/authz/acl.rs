use super::*;
use std::ops::Deref;

#[derive(Clone, Copy)]
pub(in crate::coordinator) struct NonBoeLoadedBucketHandle<'a>(&'a LoadedBucketHandle);

impl<'a> NonBoeLoadedBucketHandle<'a> {
    pub(super) fn assume_non_boe(bucket: &'a LoadedBucketHandle) -> Self {
        debug_assert!(!Coordinator::is_bucket_owner_enforced(
            bucket.bucket().ownership_controls.as_ref()
        ));
        Self(bucket)
    }
}

impl Deref for NonBoeLoadedBucketHandle<'_> {
    type Target = LoadedBucketHandle;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl Coordinator {
    pub(in crate::coordinator) fn requester_can_bypass_governance_retention(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_is_bucket_owner_account(requester, bucket)
    }

    pub(in crate::coordinator) fn requester_can_bypass_governance_retention_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_object_action_with_unavailable_existing_tags_with_bucket_policy(
            BucketPolicyRequestContext {
                requester,
                bucket,
                bucket_tags,
                action: auth::PolicyAction::BypassGovernanceRetention,
                policy_context: PutObjectPolicyContext::default(),
                policy,
            },
            object,
            || Self::requester_can_bypass_governance_retention(requester, bucket),
        )
    }

    pub(in crate::coordinator) fn requester_can_bypass_governance_retention_for_missing_version_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        key: &str,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_put_object_action_with_bucket_policy(
            BucketPolicyActionAuthorization {
                request: BucketPolicyRequestContext {
                    requester,
                    bucket,
                    bucket_tags,
                    action: auth::PolicyAction::BypassGovernanceRetention,
                    policy_context: PutObjectPolicyContext::default(),
                    policy,
                },
                default_allowed: Self::requester_can_bypass_governance_retention(requester, bucket),
            },
            key,
        )
    }

    pub(in crate::coordinator) fn authorize_put_object_write(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        self.authorize_put_object_write_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_put_object_write_with_storage_node(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.with_bucket_write_handle_for_storage_node(
            storage_node,
            &req.object,
            request,
            |bucket| {
                let existing_object = storage_node
                    .load_existing_live_object(
                        req.object.bucket.name_typed(),
                        req.object.key_typed(),
                    )
                    .map_err(Self::map_object_pg_action_error)?;
                self.authorize_put_object_write_with_existing_object(
                    req,
                    &bucket,
                    existing_object.as_ref(),
                )
            },
        )
    }

    pub(in crate::coordinator) fn authorize_put_object_write_with_existing_object_non_boe(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
        bucket: NonBoeLoadedBucketHandle<'_>,
        existing_object: Option<&StoredObject>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let key = req.object.key();
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket)?;
        if !self.requester_can_put_object_with_bucket_policy(
            BucketPolicyAccess {
                requester: req.object.requester(),
                bucket: &bucket_info,
                bucket_tags: bucket_tags.as_deref(),
                policy: bucket_policy.as_deref(),
            },
            key,
            req.policy_context,
            existing_object,
        )? {
            return Err(ServerError::AccessDenied);
        }
        self.finalize_authorized_put_object_write_after_auth(req, &bucket_info)
    }

    pub(super) fn authorize_delete_object_impl_non_boe(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        object: &ObjectVersionRequest<'_>,
        bypass_governance: bool,
        bucket_handle: NonBoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        let bucket = object.bucket_name_typed();
        let key = object.key_typed();
        let key_str = key.as_str();
        let request_version_id = object.version_id();
        let requester = object.requester();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
        #[cfg(test)]
        if should_probe_delete_object_lookup(bucket.as_str()) {
            let object_pg_ready = storage_node
                .try_probe_object_pg_available(bucket, key)
                .map_err(Self::map_object_pg_action_error)?;
            if !object_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: object pg still locked before delete_object lookup"
                        .to_string(),
                });
            }
        }

        match (bucket_info.versioning, request_version_id) {
            (BucketVersioningState::Disabled, _) => {
                match storage_node.load_object_if(bucket, key, None, |stored| {
                    let allowed = self.requester_can_delete_object_with_bucket_policy(
                        BucketPolicyAccess {
                            requester,
                            bucket: &bucket_info,
                            bucket_tags: bucket_tags.as_deref(),
                            policy: bucket_policy.as_deref(),
                        },
                        key_str,
                        Some(stored),
                        Self::delete_object_policy_action(None),
                    )?;
                    if !allowed {
                        return Err(ServerError::AccessDenied);
                    }
                    Ok(())
                }) {
                    Ok(Ok(())) => Ok(AuthorizedDeleteObject::UnversionedDelete {
                        bucket: object.bucket_name_typed().clone(),
                        key: object.key_typed().clone(),
                        requester: requester.clone(),
                        bucket_info,
                        bucket_policy,
                        bucket_tags,
                    }),
                    Ok(Err(error)) => Err(error),
                    Err(storage::ObjectPgActionError::Metadata(
                        storage::MetadataError::ObjectNotFound,
                    )) => {
                        let allowed = self.requester_can_delete_object_with_bucket_policy(
                            BucketPolicyAccess {
                                requester,
                                bucket: &bucket_info,
                                bucket_tags: bucket_tags.as_deref(),
                                policy: bucket_policy.as_deref(),
                            },
                            key_str,
                            None,
                            Self::delete_object_policy_action(None),
                        )?;
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::UnversionedDelete {
                            bucket: object.bucket_name_typed().clone(),
                            key: object.key_typed().clone(),
                            requester: requester.clone(),
                            bucket_info,
                            bucket_policy,
                            bucket_tags,
                        })
                    }
                    Err(other) => Err(Self::map_object_pg_action_error(other)),
                }
            }
            (_, Some(version_id)) => {
                match storage_node.load_object_if(bucket, key, Some(version_id), |stored| {
                    let allowed = self.requester_can_delete_object_with_bucket_policy(
                        BucketPolicyAccess {
                            requester,
                            bucket: &bucket_info,
                            bucket_tags: bucket_tags.as_deref(),
                            policy: bucket_policy.as_deref(),
                        },
                        key_str,
                        Some(stored),
                        Self::delete_object_policy_action(Some(version_id)),
                    )?;
                    if !allowed {
                        return Err(ServerError::AccessDenied);
                    }

                    if let StoredObject::Live(record) = stored {
                        let can_bypass_governance = self
                            .requester_can_bypass_governance_retention_with_bucket_policy(
                                requester,
                                &bucket_info,
                                bucket_tags.as_deref(),
                                stored,
                                bucket_policy.as_deref(),
                            )?;
                        Self::validate_delete_against_object_lock(
                            record.object_lock,
                            bypass_governance,
                            can_bypass_governance,
                            Self::current_unix_seconds()?,
                        )?;
                    }
                    Ok(())
                }) {
                    Ok(Ok(())) => Ok(AuthorizedDeleteObject::SpecificVersion {
                        bucket: object.bucket_name_typed().clone(),
                        key: object.key_typed().clone(),
                        version_id,
                        requester: requester.clone(),
                        bucket_info,
                        bucket_policy,
                        bucket_tags,
                        bypass_governance,
                    }),
                    Ok(Err(error)) => Err(error),
                    Err(storage::ObjectPgActionError::Metadata(
                        storage::MetadataError::ObjectNotFound,
                    )) => {
                        let allowed = self.requester_can_delete_object_version_with_bucket_policy(
                            BucketPolicyAccess {
                                requester,
                                bucket: &bucket_info,
                                bucket_tags: bucket_tags.as_deref(),
                                policy: bucket_policy.as_deref(),
                            },
                            key_str,
                            None,
                            Self::delete_object_policy_action(Some(version_id)),
                            version_id,
                        )?;
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        if bucket_info.object_lock.enabled
                            && bypass_governance
                            && !self.requester_can_bypass_governance_retention_for_missing_version_with_bucket_policy(
                                requester,
                                &bucket_info,
                                bucket_tags.as_deref(),
                                key_str,
                                bucket_policy.as_deref(),
                            )?
                        {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::SpecificVersionMissing { version_id })
                    }
                    Err(other) => Err(Self::map_object_pg_action_error(other)),
                }
            }
            (_, None) => {
                let owner =
                    Self::effective_object_owner(&bucket_info, requester, PutObjectAcl::None);
                match storage_node.load_object_if(bucket, key, None, |stored| {
                    let allowed = self.requester_can_delete_object_with_bucket_policy(
                        BucketPolicyAccess {
                            requester,
                            bucket: &bucket_info,
                            bucket_tags: bucket_tags.as_deref(),
                            policy: bucket_policy.as_deref(),
                        },
                        key_str,
                        Some(stored),
                        Self::delete_object_policy_action(None),
                    )?;
                    if !allowed {
                        return Err(ServerError::AccessDenied);
                    }
                    Ok(())
                }) {
                    Ok(Ok(())) => Ok(AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                        bucket: object.bucket_name_typed().clone(),
                        key: object.key_typed().clone(),
                        owner,
                        requester: requester.clone(),
                        bucket_info,
                        bucket_policy,
                        bucket_tags,
                    }),
                    Ok(Err(error)) => Err(error),
                    Err(storage::ObjectPgActionError::Metadata(
                        storage::MetadataError::ObjectNotFound,
                    )) => {
                        let allowed = self.requester_can_delete_object_with_bucket_policy(
                            BucketPolicyAccess {
                                requester,
                                bucket: &bucket_info,
                                bucket_tags: bucket_tags.as_deref(),
                                policy: bucket_policy.as_deref(),
                            },
                            key_str,
                            None,
                            Self::delete_object_policy_action(None),
                        )?;
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                            bucket: object.bucket_name_typed().clone(),
                            key: object.key_typed().clone(),
                            owner,
                            requester: requester.clone(),
                            bucket_info,
                            bucket_policy,
                            bucket_tags,
                        })
                    }
                    Err(other) => Err(Self::map_object_pg_action_error(other)),
                }
            }
        }
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_delete_object(
        &self,
        req: &DeleteObjectRequest<'_>,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        self.authorize_delete_object_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_delete_object_with_storage_node(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &DeleteObjectRequest<'_>,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        self.authorize_delete_object_impl(storage_node, &req.object, req.bypass_governance)
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_delete_objects_entry(
        &self,
        req: &DeleteObjectsRequest<'_>,
        entry: &DeleteEntry,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        self.authorize_delete_objects_entry_with_storage_node(&self.storage_node(), req, entry)
    }

    pub(in crate::coordinator) fn authorize_delete_objects_entry_with_storage_node(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &DeleteObjectsRequest<'_>,
        entry: &DeleteEntry,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        let object = ObjectVersionRequest::from_object(
            ObjectRequest::new(
                req.bucket.name_typed().clone(),
                entry.key.clone(),
                req.bucket.requester.clone(),
                req.expected_bucket_owner(),
            ),
            entry.version_id,
        );
        self.authorize_delete_object_impl(storage_node, &object, req.bypass_governance)
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_copy_object(
        &self,
        req: &CopyObjectRequest<'_>,
    ) -> Result<AuthorizedCopyObject, ServerError> {
        self.authorize_copy_object_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_copy_object_with_storage_node(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &CopyObjectRequest<'_>,
    ) -> Result<AuthorizedCopyObject, ServerError> {
        let src_version_id = req.source.version_id;
        let requester = &req.destination.bucket.requester;
        let acl = req.acl.clone();
        let acl_policy_context = authorization_policy_context_for_put_object_write_acl(
            "CopyObject",
            &acl,
            req.policy_context,
        )?;
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
        let request_object_tags_xml = match &req.tagging {
            TaggingDirective::Copy => None,
            TaggingDirective::Replace(tags) => *tags,
        };
        let copy_policy_context = PutObjectPolicyContext::new(
            Some(copy_source_policy_value.as_str()),
            metadata_directive,
            acl_policy_context.canned_acl,
        )
        .with_acl_grant_headers(
            acl_policy_context.grant_read,
            acl_policy_context.grant_write,
            acl_policy_context.grant_read_acp,
            acl_policy_context.grant_write_acp,
            acl_policy_context.grant_full_control,
        )
        .with_request_object_tags_xml(request_object_tags_xml)
        .with_if_match(acl_policy_context.if_match)
        .with_if_none_match(acl_policy_context.if_none_match)
        .with_object_creation_operation(true);
        let dst_policy_context = req
            .destination_encryption
            .with_policy_context(copy_policy_context);
        let destination = self.authorize_put_object_write_with_storage_node(
            storage_node,
            &AuthorizePutObjectRequest {
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
            },
        )?;
        let source = self.authorize_copy_source_read_snapshot_with_storage_node(
            storage_node,
            CopySourceReadSnapshotRequest {
                requester,
                bucket: &req.source.bucket,
                key: &req.source.key,
                version_id: src_version_id,
                expected_bucket_owner: req.source.expected_bucket_owner(),
                policy_action: Self::get_object_policy_action(src_version_id),
                existing_object_tags_mode: ExistingObjectTagsMode::Unavailable,
            },
        )?;

        Ok(AuthorizedCopyObject {
            source,
            destination,
        })
    }

    pub(in crate::coordinator) fn authorize_create_multipart_upload_with_existing_object_non_boe(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
        bucket: NonBoeLoadedBucketHandle<'_>,
        existing_object: Option<&StoredObject>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        let key = req.object.key();
        let policy_context = req.effective_policy_context()?;
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        if req.object.requester().is_anonymous() {
            return Err(ServerError::AccessDenied);
        }
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket)?;
        if !self.requester_can_put_object_with_bucket_policy(
            BucketPolicyAccess {
                requester: req.object.requester(),
                bucket: &bucket_info,
                bucket_tags: bucket_tags.as_deref(),
                policy: bucket_policy.as_deref(),
            },
            key,
            policy_context,
            existing_object,
        )? {
            return Err(ServerError::AccessDenied);
        }
        self.finalize_authorized_create_multipart_upload_after_auth(req, &bucket_info)
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_create_multipart_upload(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.with_bucket_write_handle_for(&req.object, request, |bucket| {
            let existing_object = self
                .storage_node()
                .load_existing_live_object(req.object.bucket.name_typed(), req.object.key_typed())
                .map_err(Self::map_object_pg_action_error)?;
            self.authorize_create_multipart_upload_with_existing_object(
                req,
                &bucket,
                existing_object.as_ref(),
            )
        })
    }

    pub(in crate::coordinator) fn authorize_upload_part_copy_non_boe(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &UploadPartCopyRequest<'_>,
        dst_bucket_handle: NonBoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedUploadPartCopy, ServerError> {
        let src_version_id = req.source.version_id;
        let dst_bucket = req.upload.bucket_name_typed();
        let dst_key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
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
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm))
                .with_object_creation_operation(false);

        let dst_bucket_info = ValidatedBucket(dst_bucket_handle.bucket().clone());
        let dst_bucket_policy = self.cached_bucket_policy_for_loaded_handle(&dst_bucket_handle)?;
        let dst_bucket_tags = Self::loaded_bucket_tags_for_policy(&dst_bucket_handle)?;
        let dst_upload = storage_node
            .load_in_progress_multipart_upload(dst_bucket, dst_key, upload_id)
            .map_err(Self::map_object_pg_action_error)?;
        let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
            policy_context,
            &dst_upload,
        );
        if !self.requester_can_write_multipart_upload_with_bucket_policy(
            requester,
            &dst_bucket_info,
            dst_bucket_tags.as_deref(),
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

        let source = self.authorize_copy_source_read_snapshot_with_storage_node(
            storage_node,
            CopySourceReadSnapshotRequest {
                requester,
                bucket: &req.source.bucket,
                key: &req.source.key,
                version_id: src_version_id,
                expected_bucket_owner: req.source.expected_bucket_owner(),
                policy_action: Self::get_object_policy_action(src_version_id),
                existing_object_tags_mode: ExistingObjectTagsMode::Available,
            },
        )?;
        Ok(AuthorizedUploadPartCopy {
            source,
            destination: AuthorizedMultipartPartWrite {
                bucket: req.upload.bucket_name_typed().clone(),
                key: req.upload.key_typed().clone(),
                upload_id: dst_upload.upload_id.clone(),
                part_number,
                upload: storage::AuthorizedMultipartUploadRecord::assume_authorized(dst_upload),
                sse_customer,
            },
        })
    }

    pub(in crate::coordinator) fn authorize_begin_stream_part_with_upload_non_boe(
        &self,
        req: &BeginStreamPartRequest<'_>,
        bucket_handle: NonBoeLoadedBucketHandle<'_>,
        upload: &MultipartUploadRecord,
    ) -> Result<AuthorizedBeginStreamPart, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let part_number = req.part_number;
        let policy_context = req.effective_policy_context();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
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
            Self::with_multipart_upload_managed_encryption_policy_context(policy_context, upload);
        if !self.requester_can_write_multipart_upload_with_bucket_policy(
            req.upload.requester(),
            &bucket_info,
            bucket_tags.as_deref(),
            upload,
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
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload.upload_id.clone(),
            part_number,
            upload: storage::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
            sse_customer,
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_begin_stream_part(
        &self,
        req: &BeginStreamPartRequest<'_>,
    ) -> Result<AuthorizedBeginStreamPart, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.with_bucket_write_handle_for(&req.upload, request, |bucket_handle| {
            let upload = self
                .storage_node()
                .load_multipart_upload(bucket, key, upload_id)
                .map_err(BucketHandleLoader::map_bucket_snapshot_error)?;
            self.authorize_begin_stream_part_with_upload(req, &bucket_handle, &upload)
        })
    }

    pub(in crate::coordinator) fn authorize_complete_multipart_upload_non_boe_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: &CompleteMultipartUploadRequest<'_>,
        bucket_handle: NonBoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedCompleteMultipartUpload, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
        #[cfg(test)]
        let upload = if should_probe_multipart_complete_auth_lookup(bucket.as_str(), key.as_str()) {
            storage_node
                .try_load_in_progress_multipart_upload(bucket, key, upload_id)
                .map_err(Self::map_object_pg_action_error)
                .and_then(|upload| {
                    upload.ok_or_else(|| ServerError::InternalError {
                        reason: "multipart complete auth lookup would block".to_string(),
                    })
                })?
        } else {
            storage_node
                .load_in_progress_multipart_upload(bucket, key, upload_id)
                .map_err(Self::map_object_pg_action_error)?
        };
        #[cfg(not(test))]
        let upload = storage_node
            .load_in_progress_multipart_upload(bucket, key, upload_id)
            .map_err(Self::map_object_pg_action_error)?;
        let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
            PutObjectPolicyContext::default()
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm))
                .with_if_match(req.cond.if_match_policy_value())
                .with_if_none_match(req.cond.if_none_match_policy_value())
                .with_object_creation_operation(true),
            &upload,
        );
        if !self.requester_can_write_multipart_upload_with_bucket_policy(
            req.upload.requester(),
            &bucket_info,
            bucket_tags.as_deref(),
            &upload,
            policy_context,
            bucket_policy.as_deref(),
        )? {
            return Err(ServerError::AccessDenied);
        }
        Self::ensure_sse_c_allowed(&bucket_info, upload.encryption.uses_sse_customer_headers())?;
        let multipart_write_encryption = self.resume_write_encryption(
            &upload.encryption,
            req.sse_customer,
            SseCustomerSegmentScope::object(),
            false,
        )?;

        Ok(AuthorizedCompleteMultipartUpload {
            bucket_info: bucket_info.into_inner(),
            bucket: req.upload.bucket_name_typed().clone(),
            key: req.upload.key_typed().clone(),
            upload_id: upload.upload_id.clone(),
            upload: storage::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            multipart_write_encryption,
        })
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_abort_multipart_upload(
        &self,
        req: &MultipartObjectRequest<'_>,
    ) -> Result<AuthorizedAbortMultipartUpload, ServerError> {
        self.authorize_abort_multipart_upload_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_abort_multipart_upload_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: &MultipartObjectRequest<'_>,
    ) -> Result<AuthorizedAbortMultipartUpload, ServerError> {
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let upload_id = req.upload_id_typed();
        let bucket_info = self.checked_active_bucket_summary_for_storage_node(
            storage_node,
            bucket,
            req.expected_bucket_owner(),
        )?;
        #[cfg(test)]
        super::maybe_run_abort_multipart_bucket_summary_hook(bucket.as_str(), key.as_str());
        let authorized = match storage_node
            .lookup_multipart_upload_management(bucket, key, upload_id)
            .map_err(Self::map_object_pg_action_error)?
        {
            storage::MultipartUploadManagementLookup::InProgress(upload) => {
                let upload = *upload;
                if !Self::requester_can_manage_multipart_upload(
                    req.object.requester(),
                    &bucket_info,
                    &upload,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                AuthorizedAbortMultipartUpload::InProgress {
                    upload: Box::new(storage::AuthorizedMultipartUploadRecord::assume_authorized(
                        upload,
                    )),
                }
            }
            storage::MultipartUploadManagementLookup::NonInProgress(upload) => {
                if !Self::requester_can_manage_multipart_upload(
                    req.object.requester(),
                    &bucket_info,
                    &upload,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            storage::MultipartUploadManagementLookup::Completed(completed) => {
                if !Self::requester_can_manage_completed_multipart_upload(
                    req.object.requester(),
                    &bucket_info,
                    &completed,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                AuthorizedAbortMultipartUpload::Completed
            }
            storage::MultipartUploadManagementLookup::Missing => {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
        };
        #[cfg(test)]
        super::maybe_run_abort_multipart_auth_lookup_hook(bucket.as_str(), key.as_str());
        Ok(authorized)
    }

    pub(in crate::coordinator) fn authorize_list_parts(
        &self,
        req: &ListPartsRequest<'_>,
    ) -> Result<AuthorizedListParts, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let bucket_info =
            self.checked_active_bucket_summary_for(bucket, req.expected_bucket_owner())?;
        match self
            .storage_node()
            .lookup_multipart_upload_management(bucket, key, upload_id)
            .map_err(Self::map_object_pg_action_error)?
        {
            storage::MultipartUploadManagementLookup::InProgress(upload) => {
                if !Self::requester_can_manage_multipart_upload(
                    req.upload.requester(),
                    &bucket_info,
                    &upload,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Ok(AuthorizedListParts {
                    bucket_info: bucket_info.into_inner(),
                    upload: storage::AuthorizedMultipartUploadRecord::assume_authorized(*upload),
                })
            }
            storage::MultipartUploadManagementLookup::NonInProgress(upload) => {
                if !Self::requester_can_manage_multipart_upload(
                    req.upload.requester(),
                    &bucket_info,
                    &upload,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                })
            }
            storage::MultipartUploadManagementLookup::Completed(upload) => {
                if !Self::requester_can_manage_completed_multipart_upload(
                    req.upload.requester(),
                    &bucket_info,
                    &upload,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                })
            }
            storage::MultipartUploadManagementLookup::Missing => Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }),
        }
    }

    pub(super) fn authorize_object_read_snapshot_non_boe(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: AuthorizedObjectReadSnapshotRequest<'_>,
        bucket: NonBoeLoadedBucketHandle<'_>,
    ) -> Result<(BucketSummary, storage::ObjectReadSnapshot), ServerError> {
        let bucket_summary = bucket.bucket().clone();
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(&bucket)?
        } else {
            None
        };
        let can_discover_missing = req.missing_discovery.requester_can_discover_missing(
            self,
            BucketPolicyAccess {
                requester: req.requester,
                bucket: &bucket_info,
                bucket_tags: bucket_tags.as_deref(),
                policy: bucket_policy.as_deref(),
            },
            req.key.as_str(),
            req.version_id,
        )?;
        #[cfg(test)]
        if should_probe_object_read_snapshot(bucket.bucket().name.as_str()) {
            let object_pg_ready = storage_node
                .try_probe_object_pg_available(&bucket.bucket().name, req.key)
                .map_err(|error| {
                    Self::map_object_read_snapshot_error(
                        &bucket.bucket().name,
                        req.key,
                        req.version_id,
                        can_discover_missing,
                        error,
                    )
                })?;
            if !object_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: object pg still locked before object read snapshot"
                        .to_string(),
                });
            }
        }
        let outcome = storage_node
            .load_object_read_snapshot_if(
                &bucket.bucket().name,
                req.key,
                req.version_id,
                req.snapshot_mode,
                |stored| {
                    let allowed = match req.modern_action {
                        ModernReadAction::ReadCurrent | ModernReadAction::ReadVersion => self
                            .requester_can_read_object_with_bucket_policy(
                                req.requester,
                                &bucket_info,
                                bucket_tags.as_deref(),
                                stored,
                                req.modern_action.policy_action(),
                                bucket_policy.as_deref(),
                            )?,
                        ModernReadAction::AttributesCurrent
                        | ModernReadAction::AttributesVersion => {
                            let read_action = match req.modern_action {
                                ModernReadAction::AttributesCurrent => {
                                    auth::PolicyAction::GetObject
                                }
                                ModernReadAction::AttributesVersion => {
                                    auth::PolicyAction::GetObjectVersion
                                }
                                _ => unreachable!(),
                            };
                            self.requester_can_read_object_with_bucket_policy(
                                req.requester,
                                &bucket_info,
                                bucket_tags.as_deref(),
                                stored,
                                read_action,
                                bucket_policy.as_deref(),
                            )? && self
                                .requester_can_read_object_without_existing_tags_with_bucket_policy(
                                    req.requester,
                                    &bucket_info,
                                    bucket_tags.as_deref(),
                                    stored,
                                    req.modern_action.policy_action(),
                                    bucket_policy.as_deref(),
                                )?
                        }
                    };
                    if allowed {
                        Ok(())
                    } else {
                        Err(ServerError::AccessDenied)
                    }
                },
            )
            .map_err(|error| {
                Self::map_object_read_snapshot_error(
                    &bucket.bucket().name,
                    req.key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok((bucket_summary, outcome.snapshot))
    }

    pub(super) fn authorize_copy_source_read_snapshot_non_boe(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: CopySourceReadSnapshotRequest<'_>,
        bucket: NonBoeLoadedBucketHandle<'_>,
    ) -> Result<storage::ObjectReadSnapshot, ServerError> {
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(&bucket)?
        } else {
            None
        };
        let can_discover_missing = MissingObjectDiscovery::ReadBucket
            .requester_can_discover_missing(
                self,
                BucketPolicyAccess {
                    requester: req.requester,
                    bucket: &bucket_info,
                    bucket_tags: bucket_tags.as_deref(),
                    policy: bucket_policy.as_deref(),
                },
                req.key.as_str(),
                req.version_id,
            )?;
        let outcome = storage_node
            .load_object_read_snapshot_if(
                &bucket.bucket().name,
                req.key,
                req.version_id,
                ObjectReadSnapshotMode::FullPayloadLayout,
                |stored| {
                    let allowed = if matches!(
                        req.existing_object_tags_mode,
                        ExistingObjectTagsMode::Available
                    ) {
                        self.requester_can_read_object_with_bucket_policy(
                            req.requester,
                            &bucket_info,
                            bucket_tags.as_deref(),
                            stored,
                            req.policy_action,
                            bucket_policy.as_deref(),
                        )?
                    } else {
                        self.requester_can_read_object_without_existing_tags_with_bucket_policy(
                            req.requester,
                            &bucket_info,
                            bucket_tags.as_deref(),
                            stored,
                            req.policy_action,
                            bucket_policy.as_deref(),
                        )?
                    };
                    if allowed {
                        Ok(())
                    } else {
                        Err(ServerError::AccessDenied)
                    }
                },
            )
            .map_err(|error| {
                Self::map_object_read_snapshot_error(
                    &bucket.bucket().name,
                    req.key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok(outcome.snapshot)
    }

    pub(in crate::coordinator) fn map_object_read_snapshot_error(
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        can_discover_missing: bool,
        error: storage::ObjectPgActionError,
    ) -> ServerError {
        match error {
            storage::ObjectPgActionError::Metadata(storage::MetadataError::ObjectNotFound) => {
                if !can_discover_missing {
                    ServerError::AccessDenied
                } else if let Some(version_id) = version_id {
                    ServerError::VersionNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: version_id.to_string(),
                    }
                } else {
                    ServerError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    }
                }
            }
            other => Self::map_object_pg_action_error(other),
        }
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_get_object(
        &self,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        self.authorize_get_object_with_storage_node(&self.storage_node(), req)
    }

    pub(in crate::coordinator) fn authorize_get_object_with_storage_node(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) = self.authorize_object_read_snapshot_with_storage_node(
            storage_node,
            AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadBucket,
                modern_action: ModernReadAction::from_get_object_version(req.object.version_id),
                snapshot_mode: ObjectReadSnapshotMode::FullPayloadLayout,
            },
        )?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }

    pub(in crate::coordinator) fn authorize_head_object_with_storage_node(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) = self.authorize_object_read_snapshot_with_storage_node(
            storage_node,
            AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadBucket,
                modern_action: ModernReadAction::from_get_object_version(req.object.version_id),
                snapshot_mode: ObjectReadSnapshotMode::MetadataOnly,
            },
        )?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }

    pub(in crate::coordinator) fn authorize_get_object_attributes(
        &self,
        req: &GetObjectAttributesRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) =
            self.authorize_object_read_snapshot(AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadObjectAttributes,
                modern_action: ModernReadAction::from_get_object_attributes_version(
                    req.object.version_id,
                ),
                snapshot_mode: if req.want_parts {
                    ObjectReadSnapshotMode::MultipartParts
                } else {
                    ObjectReadSnapshotMode::MetadataOnly
                },
            })?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }

    pub(in crate::coordinator) fn authorize_head_object_for_part_with_storage_node(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) = self.authorize_object_read_snapshot_with_storage_node(
            storage_node,
            AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadBucket,
                modern_action: ModernReadAction::from_get_object_version(req.object.version_id),
                snapshot_mode: ObjectReadSnapshotMode::MultipartParts,
            },
        )?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }
}
