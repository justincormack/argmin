use super::*;
use std::ops::Deref;

#[derive(Clone, Copy)]
pub(in crate::coordinator) struct BoeLoadedBucketHandle<'a>(&'a LoadedBucketHandle);

impl<'a> BoeLoadedBucketHandle<'a> {
    pub(super) fn assume_boe(bucket: &'a LoadedBucketHandle) -> Self {
        debug_assert!(Coordinator::is_bucket_owner_enforced(
            bucket.bucket().ownership_controls.as_ref()
        ));
        Self(bucket)
    }
}

impl Deref for BoeLoadedBucketHandle<'_> {
    type Target = LoadedBucketHandle;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Clone, Copy)]
pub(in crate::coordinator) struct BoeBucketSummary<'a>(&'a ModernBucketSummary);

impl<'a> BoeBucketSummary<'a> {
    pub(in crate::coordinator) fn assume_boe(bucket: &'a ModernBucketSummary) -> Self {
        debug_assert!(Coordinator::is_bucket_owner_enforced(
            bucket.ownership_controls.as_ref()
        ));
        Self(bucket)
    }
}

impl Deref for BoeBucketSummary<'_> {
    type Target = ModernBucketSummary;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::coordinator) enum PreloadedBucketTags<'a> {
    Available(&'a [(String, String)]),
    Unavailable,
}

impl<'a> PreloadedBucketTags<'a> {
    pub(in crate::coordinator) fn new(tags: Option<&'a [(String, String)]>) -> Self {
        match tags {
            Some(tags) => Self::Available(tags),
            None => Self::Unavailable,
        }
    }

    fn for_policy_action(
        self,
        bucket: BoeBucketSummary<'_>,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<Option<&'a [(String, String)]>, ServerError> {
        let Some(policy) = policy else {
            return Ok(None);
        };
        if !(bucket.bucket_abac_enabled && policy.requires_bucket_tags_for_action(action)) {
            return Ok(None);
        }
        match self {
            Self::Available(tags) => Ok(Some(tags)),
            Self::Unavailable => Err(ServerError::InternalError {
                // This is a coordinator wiring bug, not an AWS-facing semantic branch.
                // Any BOE modern-auth path that evaluates a bucket-tag-conditioned policy
                // must have loaded the bucket tags before reaching the evaluator.
                reason: format!(
                    "BOE modern auth requires preloaded bucket tags for {action:?} when bucket ABAC is enabled"
                ),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernObjectReadAuthorization {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernObjectWriteAuthorization {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernWriteAction {
    PutObject,
    CreateMultipartUpload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::coordinator) enum ModernReadAction {
    ReadCurrent,
    ReadVersion,
    AttributesCurrent,
    AttributesVersion,
}

impl ModernReadAction {
    pub(in crate::coordinator) fn from_get_object_version(version_id: Option<VersionId>) -> Self {
        match version_id {
            Some(_) => Self::ReadVersion,
            None => Self::ReadCurrent,
        }
    }

    pub(in crate::coordinator) fn from_get_object_attributes_version(
        version_id: Option<VersionId>,
    ) -> Self {
        match version_id {
            Some(_) => Self::AttributesVersion,
            None => Self::AttributesCurrent,
        }
    }

    pub(in crate::coordinator) fn policy_action(self) -> auth::PolicyAction {
        match self {
            Self::ReadCurrent => auth::PolicyAction::GetObject,
            Self::ReadVersion => auth::PolicyAction::GetObjectVersion,
            Self::AttributesCurrent => auth::PolicyAction::GetObjectAttributes,
            Self::AttributesVersion => auth::PolicyAction::GetObjectVersionAttributes,
        }
    }
}

impl Coordinator {
    pub(super) fn authorize_put_object_write_with_existing_object_boe(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
        bucket: BoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let key = req.object.key();
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket)?;
        let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
        if put_object_authorization_with_bucket_policy(
            req.object.requester(),
            modern_bucket,
            modern_bucket_tags,
            key,
            ModernWriteAction::PutObject,
            &req.policy_context,
            bucket_policy.as_deref(),
        )? != ModernObjectWriteAuthorization::Allowed
        {
            return Err(ServerError::AccessDenied);
        }
        self.finalize_authorized_put_object_write_after_auth(req, &bucket_info)
    }

    pub(super) fn authorize_create_multipart_upload_with_existing_object_boe(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
        bucket: BoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        let key = req.object.key();
        let policy_context = req.effective_policy_context()?;
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        if req.object.requester().is_anonymous() {
            return Err(ServerError::AccessDenied);
        }
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket)?;
        let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
        if put_object_authorization_with_bucket_policy(
            req.object.requester(),
            modern_bucket,
            modern_bucket_tags,
            key,
            ModernWriteAction::CreateMultipartUpload,
            &policy_context,
            bucket_policy.as_deref(),
        )? != ModernObjectWriteAuthorization::Allowed
        {
            return Err(ServerError::AccessDenied);
        }
        self.finalize_authorized_create_multipart_upload_after_auth(req, &bucket_info)
    }

    pub(super) fn authorize_upload_part_copy_boe(
        &self,
        req: &UploadPartCopyRequest<'_>,
        dst_bucket_handle: BoeLoadedBucketHandle<'_>,
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
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm));
        let dst_bucket_info = ValidatedBucket(dst_bucket_handle.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*dst_bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let dst_bucket_policy = self.cached_bucket_policy_for_loaded_handle(&dst_bucket_handle)?;
        let dst_bucket_tags = Self::loaded_bucket_tags_for_policy(&dst_bucket_handle)?;
        let dst_upload = self
            .storage_node
            .load_in_progress_multipart_upload(dst_bucket, dst_key, upload_id)
            .map_err(Self::map_object_pg_action_error)?;
        let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
            policy_context,
            &dst_upload,
        );
        let modern_bucket_tags = PreloadedBucketTags::new(dst_bucket_tags.as_deref());
        if write_multipart_upload_with_bucket_policy(
            requester,
            modern_bucket,
            modern_bucket_tags,
            &dst_upload,
            &policy_context,
            dst_bucket_policy.as_deref(),
        )? != ModernObjectWriteAuthorization::Allowed
        {
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
        let source = self.authorize_copy_source_read_snapshot(CopySourceReadSnapshotRequest {
            requester,
            bucket: &req.source.bucket,
            key: &req.source.key,
            version_id: src_version_id,
            expected_bucket_owner: req.source.expected_bucket_owner(),
            policy_action: Self::get_object_policy_action(src_version_id),
            existing_object_tags_mode: ExistingObjectTagsMode::Available,
        })?;
        Ok(AuthorizedUploadPartCopy {
            source,
            destination: AuthorizedMultipartPartWrite {
                bucket: req.upload.bucket_name_typed().clone(),
                key: req.upload.key_typed().clone(),
                upload_id: dst_upload.upload_id.clone(),
                part_number,
                upload: dst_upload,
                sse_customer,
            },
        })
    }

    pub(super) fn authorize_begin_stream_part_with_upload_boe(
        &self,
        req: &BeginStreamPartRequest<'_>,
        bucket_handle: BoeLoadedBucketHandle<'_>,
        upload: &MultipartUploadRecord,
    ) -> Result<AuthorizedBeginStreamPart, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let part_number = req.part_number;
        let policy_context = req.effective_policy_context();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
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
        let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
        if write_multipart_upload_with_bucket_policy(
            req.upload.requester(),
            modern_bucket,
            modern_bucket_tags,
            upload,
            &policy_context,
            bucket_policy.as_deref(),
        )? != ModernObjectWriteAuthorization::Allowed
        {
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
            upload: upload.clone(),
            sse_customer,
        })
    }

    pub(super) fn authorize_complete_multipart_upload_boe(
        &self,
        req: &CompleteMultipartUploadRequest<'_>,
        bucket_handle: BoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedCompleteMultipartUpload, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
        #[cfg(test)]
        let upload = if should_probe_multipart_complete_auth_lookup(bucket.as_str(), key.as_str()) {
            self.storage_node
                .try_load_in_progress_multipart_upload(bucket, key, upload_id)
                .map_err(Self::map_object_pg_action_error)
                .and_then(|upload| {
                    upload.ok_or_else(|| ServerError::InternalError {
                        reason: "multipart complete auth lookup would block".to_string(),
                    })
                })?
        } else {
            self.storage_node
                .load_in_progress_multipart_upload(bucket, key, upload_id)
                .map_err(Self::map_object_pg_action_error)?
        };
        #[cfg(not(test))]
        let upload = self
            .storage_node
            .load_in_progress_multipart_upload(bucket, key, upload_id)
            .map_err(Self::map_object_pg_action_error)?;
        let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
            PutObjectPolicyContext::default()
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm)),
            &upload,
        );
        let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
        if write_multipart_upload_with_bucket_policy(
            req.upload.requester(),
            modern_bucket,
            modern_bucket_tags,
            &upload,
            &policy_context,
            bucket_policy.as_deref(),
        )? != ModernObjectWriteAuthorization::Allowed
        {
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
            upload,
            multipart_write_encryption,
        })
    }

    pub(super) fn authorize_copy_source_read_snapshot_boe(
        &self,
        req: CopySourceReadSnapshotRequest<'_>,
        bucket: BoeLoadedBucketHandle<'_>,
    ) -> Result<storage::ObjectReadSnapshot, ServerError> {
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(&bucket)?
        } else {
            None
        };
        let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
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
        let outcome = self
            .storage_node
            .load_object_read_snapshot_if(
                &bucket.bucket().name,
                req.key,
                req.version_id,
                ObjectReadSnapshotMode::FullPayloadLayout,
                |stored| {
                    let allowed = matches!(
                        copy_source_read_authorization_with_bucket_policy(
                            req.requester,
                            modern_bucket,
                            modern_bucket_tags,
                            stored,
                            req.policy_action,
                            req.existing_object_tags_mode,
                            bucket_policy.as_deref(),
                        )?,
                        ModernObjectReadAuthorization::Allowed
                    );
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

    pub(super) fn authorize_object_read_snapshot_boe(
        &self,
        req: AuthorizedObjectReadSnapshotRequest<'_>,
        bucket: BoeLoadedBucketHandle<'_>,
    ) -> Result<(BucketSummary, storage::ObjectReadSnapshot), ServerError> {
        let bucket_summary = bucket.bucket().clone();
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(&bucket)?
        } else {
            None
        };
        let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
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
            let object_pg_ready = self
                .storage_node
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
        let outcome = self
            .storage_node
            .load_object_read_snapshot_if(
                &bucket.bucket().name,
                req.key,
                req.version_id,
                req.snapshot_mode,
                |stored| {
                    let allowed = matches!(
                        read_object_authorization_with_bucket_policy(
                            req.requester,
                            modern_bucket,
                            modern_bucket_tags,
                            stored,
                            req.modern_action,
                            bucket_policy.as_deref(),
                        )?,
                        ModernObjectReadAuthorization::Allowed
                    );
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

    pub(super) fn authorize_delete_object_impl_boe(
        &self,
        object: &ObjectVersionRequest<'_>,
        bypass_governance: bool,
        bucket_handle: BoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        let bucket = object.bucket_name_typed();
        let key = object.key_typed();
        let key_str = key.as_str();
        let request_version_id = object.version_id();
        let requester = object.requester();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
        let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
        #[cfg(test)]
        if should_probe_delete_object_lookup(bucket.as_str()) {
            let object_pg_ready = self
                .storage_node
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
                match self
                    .storage_node
                    .load_object_if(bucket, key, None, |stored| {
                        let allowed = delete_object_authorization_with_bucket_policy(
                            requester,
                            modern_bucket,
                            modern_bucket_tags,
                            key_str,
                            Some(stored),
                            Self::delete_object_policy_action(None),
                            bucket_policy.as_deref(),
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
                        let allowed = delete_object_authorization_with_bucket_policy(
                            requester,
                            modern_bucket,
                            modern_bucket_tags,
                            key_str,
                            None,
                            Self::delete_object_policy_action(None),
                            bucket_policy.as_deref(),
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
                match self
                    .storage_node
                    .load_object_if(bucket, key, Some(version_id), |stored| {
                        let allowed = delete_object_authorization_with_bucket_policy(
                            requester,
                            modern_bucket,
                            modern_bucket_tags,
                            key_str,
                            Some(stored),
                            Self::delete_object_policy_action(Some(version_id)),
                            bucket_policy.as_deref(),
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
                        let allowed = delete_object_authorization_with_bucket_policy(
                            requester,
                            modern_bucket,
                            modern_bucket_tags,
                            key_str,
                            None,
                            Self::delete_object_policy_action(Some(version_id)),
                            bucket_policy.as_deref(),
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
                match self
                    .storage_node
                    .load_object_if(bucket, key, None, |stored| {
                        let allowed = delete_object_authorization_with_bucket_policy(
                            requester,
                            modern_bucket,
                            modern_bucket_tags,
                            key_str,
                            Some(stored),
                            Self::delete_object_policy_action(None),
                            bucket_policy.as_deref(),
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
                        let allowed = delete_object_authorization_with_bucket_policy(
                            requester,
                            modern_bucket,
                            modern_bucket_tags,
                            key_str,
                            None,
                            Self::delete_object_policy_action(None),
                            bucket_policy.as_deref(),
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
}

fn requester_is_modern_bucket_owner_account(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
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
    Coordinator::bucket_owner_account_id(&bucket.owner_principal) == Some(requester_account_id)
}

fn requester_can_modern_bucket_owner_account_admin(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
) -> bool {
    Coordinator::requester_can_bucket_admin(requester, &bucket.owner_principal)
        || (requester.authorization_profile() == auth::AuthorizationProfile::OwnerAccountAdmin
            && requester_is_modern_bucket_owner_account(requester, bucket))
}

fn modern_bucket_policy_allow_survives_restrict_public_buckets(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
) -> bool {
    if !bucket.bucket_policy_public
        || !Coordinator::restricts_public_buckets(bucket.public_access_block.as_ref())
    {
        return true;
    }

    requester_is_modern_bucket_owner_account(requester, bucket)
}

fn bucket_policy_decision_for_put_object_action_modern(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    action: auth::PolicyAction,
    policy_context: &PutObjectPolicyContext<'_>,
    policy: Option<&auth::BucketPolicy>,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };

    let request_object_tags = if policy.requires_request_object_tags_for_action(action) {
        match policy_context.request_object_tags_xml {
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
    let bucket_tags = bucket_tags.for_policy_action(bucket, action, Some(policy))?;
    let bucket_tags: Vec<auth::PolicyTag<'_>> = bucket_tags
        .into_iter()
        .flat_map(|tags| tags.iter())
        .map(|(key, value)| auth::PolicyTag::new(key, value))
        .collect();
    let policy_request = auth::PolicyRequest::for_object(
        action,
        bucket.name.as_str(),
        key,
        requester.principal_opt(),
        requester.canonical_user_id(),
        auth::bucket_policy::ExistingObjectTags::Unavailable,
    )
    .with_bucket_tags(
        if bucket.bucket_abac_enabled && policy.requires_bucket_tags_for_action(action) {
            auth::bucket_policy::BucketTags::Available(&bucket_tags)
        } else {
            auth::bucket_policy::BucketTags::Unavailable
        },
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
    Ok(policy.evaluate(&policy_request))
}

pub(super) fn write_multipart_upload_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    upload: &MultipartUploadRecord,
    policy_context: &PutObjectPolicyContext<'_>,
    policy: Option<&auth::BucketPolicy>,
) -> Result<ModernObjectWriteAuthorization, ServerError> {
    let decision = bucket_policy_decision_for_put_object_action_modern(
        requester,
        bucket,
        bucket_tags,
        upload.key.as_str(),
        auth::PolicyAction::PutObject,
        policy_context,
        policy,
    )?;
    let allowed = match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    };
    Ok(if allowed {
        ModernObjectWriteAuthorization::Allowed
    } else {
        ModernObjectWriteAuthorization::Denied
    })
}

pub(super) fn put_object_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    action: ModernWriteAction,
    policy_context: &PutObjectPolicyContext<'_>,
    policy: Option<&auth::BucketPolicy>,
) -> Result<ModernObjectWriteAuthorization, ServerError> {
    if action == ModernWriteAction::CreateMultipartUpload && requester.is_anonymous() {
        return Ok(ModernObjectWriteAuthorization::Denied);
    }
    let decision = bucket_policy_decision_for_put_object_action_modern(
        requester,
        bucket,
        bucket_tags,
        key,
        auth::PolicyAction::PutObject,
        policy_context,
        policy,
    )?;
    let allowed = match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    };
    if !allowed {
        return Ok(ModernObjectWriteAuthorization::Denied);
    }

    if policy_context.request_object_tags_xml.is_some() {
        let tagging_decision = bucket_policy_decision_for_put_object_action_modern(
            requester,
            bucket,
            bucket_tags,
            key,
            auth::PolicyAction::PutObjectTagging,
            policy_context,
            policy,
        )?;
        let tagging_allowed = match tagging_decision {
            auth::PolicyEvaluation::ExplicitDeny => false,
            auth::PolicyEvaluation::ExplicitAllow
                if modern_bucket_policy_allow_survives_restrict_public_buckets(
                    requester, bucket,
                ) =>
            {
                true
            }
            auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
                requester_can_modern_bucket_owner_account_admin(requester, bucket)
            }
        };
        if !tagging_allowed {
            return Ok(ModernObjectWriteAuthorization::Denied);
        }
    }

    Ok(ModernObjectWriteAuthorization::Allowed)
}

pub(super) fn delete_object_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    object: Option<&StoredObject>,
    action: auth::PolicyAction,
    policy: Option<&auth::BucketPolicy>,
) -> Result<bool, ServerError> {
    let decision = match object {
        Some(object) => bucket_policy_decision_for_object_with_preloaded_tags_modern(
            requester,
            bucket,
            bucket_tags,
            object,
            action,
            policy,
            ExistingObjectTagsMode::Available,
        )?,
        None => bucket_policy_decision_for_put_object_action_modern(
            requester,
            bucket,
            bucket_tags,
            key,
            action,
            &PutObjectPolicyContext::default(),
            policy,
        )?,
    };

    Ok(match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    })
}

fn bucket_policy_decision_for_object_with_preloaded_tags_modern(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    object: &StoredObject,
    action: auth::PolicyAction,
    policy: Option<&auth::BucketPolicy>,
    existing_object_tags_mode: ExistingObjectTagsMode,
) -> Result<auth::PolicyEvaluation, ServerError> {
    let Some(policy) = policy else {
        return Ok(auth::PolicyEvaluation::NoMatch);
    };
    let bucket_tags = bucket_tags.for_policy_action(bucket, action, Some(policy))?;

    Coordinator::evaluate_bucket_policy_for_object_request(
        super::policy::ObjectPolicyEvaluationContext {
            requester,
            bucket_name: bucket.name.as_str(),
            bucket_abac_enabled: bucket.bucket_abac_enabled,
            action,
            policy_context: PutObjectPolicyContext::default(),
            policy,
        },
        object,
        existing_object_tags_mode,
        bucket_tags.unwrap_or(&[]),
    )
}

fn modern_read_object_default_allowed(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    object: &StoredObject,
    action: ModernReadAction,
) -> bool {
    match action {
        ModernReadAction::ReadCurrent | ModernReadAction::ReadVersion => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
        ModernReadAction::AttributesCurrent | ModernReadAction::AttributesVersion => requester
            .principal_opt()
            .is_some_and(|principal| principal == object.owner().principal.as_str()),
    }
}

fn modern_read_object_authorization_for_single_action(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    object: &StoredObject,
    action: ModernReadAction,
    policy: Option<&auth::BucketPolicy>,
    existing_object_tags_mode: ExistingObjectTagsMode,
) -> Result<ModernObjectReadAuthorization, ServerError> {
    let decision = bucket_policy_decision_for_object_with_preloaded_tags_modern(
        requester,
        bucket,
        bucket_tags,
        object,
        action.policy_action(),
        policy,
        existing_object_tags_mode,
    )?;
    let modern_default_allowed =
        modern_read_object_default_allowed(requester, bucket, object, action);

    let outcome = match decision {
        auth::PolicyEvaluation::ExplicitDeny => ModernObjectReadAuthorization::Denied,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            ModernObjectReadAuthorization::Allowed
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            if modern_default_allowed {
                ModernObjectReadAuthorization::Allowed
            } else {
                ModernObjectReadAuthorization::Denied
            }
        }
    };
    Ok(outcome)
}

fn combine_modern_read_authorization(
    first: ModernObjectReadAuthorization,
    second: ModernObjectReadAuthorization,
) -> ModernObjectReadAuthorization {
    match (first, second) {
        (ModernObjectReadAuthorization::Denied, _) | (_, ModernObjectReadAuthorization::Denied) => {
            ModernObjectReadAuthorization::Denied
        }
        (ModernObjectReadAuthorization::Allowed, ModernObjectReadAuthorization::Allowed) => {
            ModernObjectReadAuthorization::Allowed
        }
    }
}

pub(super) fn read_object_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    object: &StoredObject,
    action: ModernReadAction,
    policy: Option<&auth::BucketPolicy>,
) -> Result<ModernObjectReadAuthorization, ServerError> {
    match action {
        ModernReadAction::AttributesCurrent | ModernReadAction::AttributesVersion => {
            let read_action = match action {
                ModernReadAction::AttributesVersion => ModernReadAction::ReadVersion,
                ModernReadAction::AttributesCurrent => ModernReadAction::ReadCurrent,
                _ => unreachable!(),
            };
            let read = modern_read_object_authorization_for_single_action(
                requester,
                bucket,
                bucket_tags,
                object,
                read_action,
                policy,
                ExistingObjectTagsMode::Available,
            )?;
            let attrs = modern_read_object_authorization_for_single_action(
                requester,
                bucket,
                bucket_tags,
                object,
                action,
                policy,
                ExistingObjectTagsMode::Unavailable,
            )?;
            Ok(combine_modern_read_authorization(read, attrs))
        }
        _ => modern_read_object_authorization_for_single_action(
            requester,
            bucket,
            bucket_tags,
            object,
            action,
            policy,
            ExistingObjectTagsMode::Available,
        ),
    }
}

pub(super) fn copy_source_read_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    object: &StoredObject,
    action: auth::PolicyAction,
    existing_object_tags_mode: ExistingObjectTagsMode,
    policy: Option<&auth::BucketPolicy>,
) -> Result<ModernObjectReadAuthorization, ServerError> {
    debug_assert!(matches!(
        action,
        auth::PolicyAction::GetObject | auth::PolicyAction::GetObjectVersion
    ));
    let decision = bucket_policy_decision_for_object_with_preloaded_tags_modern(
        requester,
        bucket,
        bucket_tags,
        object,
        action,
        policy,
        existing_object_tags_mode,
    )?;
    let allowed = match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    };
    Ok(if allowed {
        ModernObjectReadAuthorization::Allowed
    } else {
        ModernObjectReadAuthorization::Denied
    })
}
