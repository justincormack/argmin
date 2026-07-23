use super::policy::{object_policy_request, policy_tags_from_pairs, ObjectPolicyRequestInput};
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
    CompleteMultipartUploadReplay,
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

    pub(in crate::coordinator) fn discloses_optional_attributes(self) -> bool {
        match self {
            Self::ReadCurrent | Self::ReadVersion => true,
            Self::AttributesCurrent | Self::AttributesVersion => false,
        }
    }

    pub(in crate::coordinator) fn tagging_policy_action(self) -> auth::PolicyAction {
        match self {
            Self::ReadCurrent | Self::AttributesCurrent => auth::PolicyAction::GetObjectTagging,
            Self::ReadVersion | Self::AttributesVersion => {
                auth::PolicyAction::GetObjectVersionTagging
            }
        }
    }
}

impl Coordinator {
    pub(super) fn authorize_put_object_write_with_existing_object_boe(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
        bucket: BoeLoadedBucketHandle<'_>,
        existing_object: Option<&StoredObject>,
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
        if let (Some(_), Some(object)) = (req.policy_context.if_match, existing_object) {
            let can_read = read_object_authorization_with_bucket_policy(
                req.object.requester(),
                modern_bucket,
                modern_bucket_tags,
                object,
                ModernReadAction::ReadCurrent,
                bucket_policy.as_deref(),
            )? == ModernObjectReadAuthorization::Allowed;
            if !can_read {
                return Err(ServerError::AccessDenied);
            }
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
        storage_node: &Arc<storage::StorageCluster>,
        req: &UploadPartCopyRequest<'_>,
        dst_bucket_handle: BoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedUploadPartCopy, ServerError> {
        let src_version_id = req.source.version_id;
        let dst_bucket = req.upload.bucket_name_typed();
        let dst_key = req.upload.key_typed();
        let upload_id = req.upload.upload_id();
        let part_number = req.part_number;
        let requester = req.upload.requester();
        let policy_context = req
            .policy_context
            .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm))
            .with_object_creation_operation(false);
        let dst_bucket_info = ValidatedBucket(dst_bucket_handle.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*dst_bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let dst_bucket_policy = self.cached_bucket_policy_for_loaded_handle(&dst_bucket_handle)?;
        let dst_bucket_tags = Self::loaded_bucket_tags_for_policy(&dst_bucket_handle)?;
        let dst_upload = storage_node
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

    pub(super) fn authorize_begin_stream_part_with_upload_boe(
        &self,
        req: &BeginStreamPartRequest<'_>,
        bucket_handle: BoeLoadedBucketHandle<'_>,
        upload: &MultipartUploadRecord,
    ) -> Result<AuthorizedBeginStreamPart, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id();
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
            upload: storage::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
            sse_customer,
        })
    }

    pub(super) fn authorize_complete_multipart_upload_boe_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: &CompleteMultipartUploadRequest<'_>,
        bucket_handle: BoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedCompleteMultipartUpload, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::assume_boe(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
        #[cfg(test)]
        let lookup = if should_probe_multipart_complete_auth_lookup(bucket.as_str(), key.as_str()) {
            let upload = storage_node
                .try_load_in_progress_multipart_upload(bucket, key, upload_id)
                .map_err(Self::map_object_pg_action_error)?
                .ok_or_else(|| ServerError::InternalError {
                    reason: "multipart complete auth lookup would block".to_string(),
                })?;
            storage::MultipartUploadManagementLookup::InProgress(Box::new(upload))
        } else {
            storage_node
                .lookup_multipart_upload_management(bucket, key, upload_id)
                .map_err(Self::map_object_pg_action_error)?
        };
        #[cfg(not(test))]
        let lookup = storage_node
            .lookup_multipart_upload_management(bucket, key, upload_id)
            .map_err(Self::map_object_pg_action_error)?;
        let upload = match lookup {
            storage::MultipartUploadManagementLookup::InProgress(upload) => *upload,
            storage::MultipartUploadManagementLookup::Replay(replay) => {
                let replay = *replay;
                if !bucket_info
                    .multipart_upload_id_key
                    .authenticates(bucket, key, upload_id)
                {
                    return Err(ServerError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                let policy_context = PutObjectPolicyContext::default()
                    .with_sse_customer_algorithm(
                        req.sse_customer.map(SseCustomerRequest::algorithm),
                    )
                    .with_if_match(req.cond.if_match_policy_value())
                    .with_if_none_match(req.cond.if_none_match_policy_value())
                    .with_object_creation_operation(true)
                    .with_managed_encryption(replay.encryption.managed_encryption_algorithm());
                let modern_bucket_tags = PreloadedBucketTags::new(bucket_tags.as_deref());
                if put_object_authorization_with_bucket_policy(
                    req.upload.requester(),
                    modern_bucket,
                    modern_bucket_tags,
                    key.as_str(),
                    ModernWriteAction::CompleteMultipartUploadReplay,
                    &policy_context,
                    bucket_policy.as_deref(),
                )? != ModernObjectWriteAuthorization::Allowed
                {
                    return Err(ServerError::AccessDenied);
                }
                Self::ensure_sse_c_allowed(
                    &bucket_info,
                    replay.encryption.uses_sse_customer_headers(),
                )?;
                self.resume_write_encryption(
                    &replay.encryption,
                    req.sse_customer,
                    SseCustomerSegmentScope::object(),
                    false,
                )?;
                return Ok(AuthorizedCompleteMultipartUpload::Replay {
                    bucket_info: bucket_info.into_inner(),
                    key: key.clone(),
                    replay,
                });
            }
            storage::MultipartUploadManagementLookup::NonInProgress(_)
            | storage::MultipartUploadManagementLookup::Missing => {
                if bucket_info
                    .multipart_upload_id_key
                    .authenticates(bucket, key, upload_id)
                    && !Self::requester_can_manage_authenticated_multipart_upload_id(
                        req.upload.requester(),
                        &bucket_info,
                        upload_id,
                    )
                {
                    return Err(ServerError::AccessDenied);
                }
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
        };
        let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
            PutObjectPolicyContext::default()
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm))
                .with_if_match(req.cond.if_match_policy_value())
                .with_if_none_match(req.cond.if_none_match_policy_value())
                .with_object_creation_operation(true),
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

        Ok(AuthorizedCompleteMultipartUpload::InProgress {
            bucket_info: bucket_info.into_inner(),
            bucket: req.upload.bucket_name_typed().clone(),
            key: req.upload.key_typed().clone(),
            upload_id: upload.upload_id.clone(),
            upload: Box::new(storage::AuthorizedMultipartUploadRecord::assume_authorized(
                upload,
            )),
            multipart_write_encryption,
        })
    }

    pub(super) fn authorize_copy_source_read_snapshot_boe(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: CopySourceReadSnapshotRequest<'_>,
        bucket: BoeLoadedBucketHandle<'_>,
    ) -> Result<AuthorizedCopySourceRead, ServerError> {
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
        let outcome = storage_node
            .load_leased_object_read_snapshot_if(
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
        Ok(AuthorizedCopySourceRead {
            snapshot: outcome.snapshot,
            payload_lease: outcome.payload_lease,
        })
    }

    pub(super) fn authorize_object_read_snapshot_boe(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
        req: AuthorizedObjectReadSnapshotRequest<'_>,
        bucket: BoeLoadedBucketHandle<'_>,
    ) -> Result<
        (
            BucketSummary,
            storage::ObjectReadSnapshot,
            ObjectAttributePermissions,
        ),
        ServerError,
    > {
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
        if self.should_probe_object_read_snapshot(bucket.bucket().name.as_str()) {
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
                        self.object_attribute_permissions_with_bucket_policy(
                            req.requester,
                            &bucket_info,
                            bucket_tags.as_deref(),
                            stored,
                            req.modern_action,
                            bucket_policy.as_deref(),
                        )
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
        Ok((bucket_summary, outcome.snapshot, outcome.value))
    }

    pub(super) fn authorize_delete_object_impl_boe(
        &self,
        storage_node: &Arc<storage::StorageCluster>,
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
        if self.should_probe_delete_object_lookup(bucket.as_str()) {
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
                match storage_node.load_object_if(bucket, key, Some(version_id), |stored| {
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
                        let policy_context =
                            PutObjectPolicyContext::default().with_version_id(Some(version_id));
                        let allowed = delete_object_authorization_with_policy_context(
                            requester,
                            modern_bucket,
                            modern_bucket_tags,
                            key_str,
                            None,
                            Self::delete_object_policy_action(Some(version_id)),
                            (bucket_policy.as_deref(), &policy_context),
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
    if requester
        .configured_principal()
        .is_some_and(|principal| principal == bucket.owner_principal)
    {
        return true;
    }

    let Some(requester_account_id) = account.account_id() else {
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

fn put_object_policy_allows(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    decision: auth::PolicyEvaluation,
) -> bool {
    match decision {
        auth::PolicyEvaluation::ExplicitDeny => false,
        auth::PolicyEvaluation::ExplicitAllow
            if modern_bucket_policy_allow_survives_restrict_public_buckets(requester, bucket) =>
        {
            true
        }
        auth::PolicyEvaluation::ExplicitAllow | auth::PolicyEvaluation::NoMatch => {
            requester_can_modern_bucket_owner_account_admin(requester, bucket)
        }
    }
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
    let request_object_tags = policy_tags_from_pairs(&request_object_tags);
    let bucket_tags = bucket_tags.for_policy_action(bucket, action, Some(policy))?;
    let bucket_tags = bucket_tags.map_or_else(Vec::new, policy_tags_from_pairs);
    let version_id = policy_context
        .version_id
        .map(|version_id| version_id.to_string());
    let policy_request = object_policy_request(ObjectPolicyRequestInput {
        requester,
        bucket_name: bucket.name.as_str(),
        bucket_abac_enabled: bucket.bucket_abac_enabled,
        key,
        action,
        policy_context: *policy_context,
        policy,
        existing_object_tags: auth::bucket_policy::ExistingObjectTags::Unavailable,
        existing_object_tags_not_evaluable: true,
        bucket_tags: &bucket_tags,
        request_object_tags: &request_object_tags,
        version_id: version_id.as_deref(),
    })?;
    Ok(policy.evaluate(&policy_request))
}

pub(in crate::coordinator) fn write_multipart_upload_with_bucket_policy(
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
    let allowed = put_object_policy_allows(requester, bucket, decision);
    Ok(if allowed {
        ModernObjectWriteAuthorization::Allowed
    } else {
        ModernObjectWriteAuthorization::Denied
    })
}

pub(in crate::coordinator) fn put_object_authorization_with_bucket_policy(
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
    let allowed = put_object_policy_allows(requester, bucket, decision);
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
        let tagging_allowed = put_object_policy_allows(requester, bucket, tagging_decision);
        if !tagging_allowed {
            return Ok(ModernObjectWriteAuthorization::Denied);
        }
    }

    Ok(ModernObjectWriteAuthorization::Allowed)
}

pub(in crate::coordinator) fn delete_object_authorization_with_bucket_policy(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    object: Option<&StoredObject>,
    action: auth::PolicyAction,
    policy: Option<&auth::BucketPolicy>,
) -> Result<bool, ServerError> {
    delete_object_authorization_with_policy_context(
        requester,
        bucket,
        bucket_tags,
        key,
        object,
        action,
        (policy, &PutObjectPolicyContext::default()),
    )
}

fn delete_object_authorization_with_policy_context(
    requester: &Requester,
    bucket: BoeBucketSummary<'_>,
    bucket_tags: PreloadedBucketTags<'_>,
    key: &str,
    object: Option<&StoredObject>,
    action: auth::PolicyAction,
    policy_and_context: (Option<&auth::BucketPolicy>, &PutObjectPolicyContext<'_>),
) -> Result<bool, ServerError> {
    let (policy, policy_context) = policy_and_context;
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
            policy_context,
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
            .configured_principal()
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

pub(in crate::coordinator) fn read_object_authorization_with_bucket_policy(
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
                ExistingObjectTagsMode::NotEvaluable,
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

#[cfg(test)]
mod tests {
    use super::*;
    use s3_types::{AccountIdentity, ObjectLockState};
    use storage::{
        BucketName, BucketObjectLockConfig, BucketObjectOwnership, BucketOwnershipControls,
        BucketVersioningState, CanonicalUserId, EcShape, EffectiveBucketEncryptionConfig,
        GenerationId, ObjectEncryption, ObjectEtag, ObjectKey, ObjectLayout, OwnerIdentity,
        PublicAccessBlockConfig, StorageClass,
    };

    fn modern_bucket(ownership: Option<BucketObjectOwnership>) -> ModernBucketSummary {
        ModernBucketSummary {
            name: BucketName::try_from("test-bucket").unwrap(),
            owner_principal: "arn:aws:iam::111122223333:user/bucket-owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal(
                "arn:aws:iam::111122223333:user/bucket-owner",
            ),
            created_at: 0,
            versioning: BucketVersioningState::Suspended,
            object_lock: BucketObjectLockConfig::default(),
            public_access_block: None,
            ownership_controls: ownership
                .map(|object_ownership| BucketOwnershipControls { object_ownership }),
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            multipart_upload_id_key: storage::MultipartUploadIdKey::from_bytes([1; 32]),
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }
    }

    fn modern_bucket_with_abac(
        ownership: Option<BucketObjectOwnership>,
        bucket_abac_enabled: bool,
    ) -> ModernBucketSummary {
        let mut bucket = modern_bucket(ownership);
        bucket.bucket_abac_enabled = bucket_abac_enabled;
        bucket
    }

    fn stored_live_object(owner_principal: &str) -> StoredObject {
        StoredObject::Live(storage::LiveObjectRecord {
            bucket: BucketName::try_from("test-bucket").unwrap(),
            key: ObjectKey::try_from("key").unwrap(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal(owner_principal),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            size: 4,
            etag: ObjectEtag::single_part(1),
            last_modified: 0,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 1, m: 0 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        })
    }

    fn requester(principal: &str) -> Requester {
        Requester::authenticated(AccountIdentity::from_principal(principal))
    }

    fn owner_admin_requester() -> Requester {
        Requester::authenticated_owner_account_admin(AccountIdentity::from_principal(
            "arn:aws:iam::111122223333:user/owner-admin",
        ))
    }

    fn shared_canonical_owner_admin_requester() -> Requester {
        let owner_canonical =
            CanonicalUserId::from_principal("arn:aws:iam::111122223333:user/bucket-owner");
        Requester::authenticated_owner_account_admin(AccountIdentity::new(
            "arn:aws:iam::111122223333:user/shared-other",
            owner_canonical,
            "shared-other",
        ))
    }

    fn parse_policy(body: &str) -> auth::BucketPolicy {
        auth::parse_bucket_policy(body).unwrap()
    }

    fn preloaded_bucket_tags<'a>(tags: Option<&'a [(String, String)]>) -> PreloadedBucketTags<'a> {
        PreloadedBucketTags::new(tags)
    }

    #[test]
    fn modern_read_auth_allows_explicit_policy_allow_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Allowed);
    }

    #[test]
    fn modern_read_auth_denies_without_policy_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            None,
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_explicit_policy_deny_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_explicit_policy_deny_for_shared_canonical_owner_admin_on_boe_bucket()
    {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::111122223333:user/bucket-owner");
        let requester = shared_canonical_owner_admin_requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::111122223333:user/shared-other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_explicit_policy_deny_for_bucket_owner_root_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::111122223333:user/bucket-owner");
        let requester = Requester::authenticated_owner_account_admin(
            AccountIdentity::from_principal("arn:aws:iam::111122223333:root"),
        );
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::111122223333:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_non_owner_without_policy_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::444455556666:user/other");

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            None,
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_allows_bucket_owner_admin_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = owner_admin_requester();

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            None,
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Allowed);
    }

    #[test]
    fn modern_read_auth_ignores_public_policy_when_restrict_public_buckets_blocks_it() {
        let mut bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        bucket.bucket_policy_public = true;
        bucket.public_access_block = Some(PublicAccessBlockConfig {
            block_public_acls: false,
            ignore_public_acls: false,
            block_public_policy: false,
            restrict_public_buckets: true,
        });
        let object = stored_live_object("arn:aws:iam::111122223333:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::assume_boe(&bucket),
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_bucket_tag_condition_controls_boe_get_and_head_path() {
        let bucket =
            modern_bucket_with_abac(Some(BucketObjectOwnership::BucketOwnerEnforced), true);
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*","Condition":{"StringEquals":{"s3:BucketTag/environment":"prod"}}}]}"#,
        );
        let matching_tags = vec![("environment".to_string(), "prod".to_string())];
        let non_matching_tags = vec![("environment".to_string(), "dev".to_string())];
        let bucket = BoeBucketSummary::assume_boe(&bucket);

        let allowed = read_object_authorization_with_bucket_policy(
            &requester,
            bucket,
            preloaded_bucket_tags(Some(&matching_tags)),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();
        let denied = read_object_authorization_with_bucket_policy(
            &requester,
            bucket,
            preloaded_bucket_tags(Some(&non_matching_tags)),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(allowed, ModernObjectReadAuthorization::Allowed);
        assert_eq!(denied, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_write_auth_bucket_tag_condition_controls_boe_put_family() {
        let bucket =
            modern_bucket_with_abac(Some(BucketObjectOwnership::BucketOwnerEnforced), true);
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::test-bucket/*","Condition":{"StringEquals":{"s3:BucketTag/environment":"prod"}}}]}"#,
        );
        let matching_tags = vec![("environment".to_string(), "prod".to_string())];
        let non_matching_tags = vec![("environment".to_string(), "dev".to_string())];
        let bucket = BoeBucketSummary::assume_boe(&bucket);

        for action in [
            ModernWriteAction::PutObject,
            ModernWriteAction::CreateMultipartUpload,
        ] {
            let allowed = put_object_authorization_with_bucket_policy(
                &requester,
                bucket,
                preloaded_bucket_tags(Some(&matching_tags)),
                "key",
                action,
                &PutObjectPolicyContext::default(),
                Some(&policy),
            )
            .unwrap();
            let denied = put_object_authorization_with_bucket_policy(
                &requester,
                bucket,
                preloaded_bucket_tags(Some(&non_matching_tags)),
                "key",
                action,
                &PutObjectPolicyContext::default(),
                Some(&policy),
            )
            .unwrap();

            assert_eq!(allowed, ModernObjectWriteAuthorization::Allowed);
            assert_eq!(denied, ModernObjectWriteAuthorization::Denied);
        }
    }

    #[test]
    fn modern_delete_auth_bucket_tag_condition_controls_boe_delete() {
        let bucket =
            modern_bucket_with_abac(Some(BucketObjectOwnership::BucketOwnerEnforced), true);
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::test-bucket/*","Condition":{"StringEquals":{"s3:BucketTag/environment":"prod"}}}]}"#,
        );
        let matching_tags = vec![("environment".to_string(), "prod".to_string())];
        let non_matching_tags = vec![("environment".to_string(), "dev".to_string())];
        let bucket = BoeBucketSummary::assume_boe(&bucket);

        let allowed = delete_object_authorization_with_bucket_policy(
            &requester,
            bucket,
            preloaded_bucket_tags(Some(&matching_tags)),
            "key",
            Some(&object),
            auth::PolicyAction::DeleteObject,
            Some(&policy),
        )
        .unwrap();
        let denied = delete_object_authorization_with_bucket_policy(
            &requester,
            bucket,
            preloaded_bucket_tags(Some(&non_matching_tags)),
            "key",
            Some(&object),
            auth::PolicyAction::DeleteObject,
            Some(&policy),
        )
        .unwrap();

        assert!(allowed);
        assert!(!denied);
    }

    #[test]
    fn modern_bucket_tags_fail_closed_when_boe_abac_policy_needs_tags() {
        let bucket =
            modern_bucket_with_abac(Some(BucketObjectOwnership::BucketOwnerEnforced), true);
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*","Condition":{"StringEquals":{"s3:BucketTag/environment":"prod"}}}]}"#,
        );
        let bucket = BoeBucketSummary::assume_boe(&bucket);

        let error = read_object_authorization_with_bucket_policy(
            &requester,
            bucket,
            preloaded_bucket_tags(None),
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap_err();

        assert!(matches!(error, ServerError::InternalError { .. }));
    }
}
