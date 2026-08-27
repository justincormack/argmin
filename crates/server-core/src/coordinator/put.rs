// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use storage::{
    BucketName, CreateStreamUploadReq, ObjectKey, PreparedDirectPutObjectCommit, SessionId,
    StreamPutFinalizeSnapshot, StreamUploadTarget,
};

use super::bucket_handles::{BucketHandleLoader, BucketHandleRequest, LoadedBucketHandle};
use super::{
    ActiveWriteEncryption, AppendStreamPutRequest, AuthorizePutObjectRequest,
    AuthorizedFinalizeStreamPutRequest, AuthorizedPutObjectCommitRequest, AuthorizedPutObjectWrite,
    AuthorizedWriteTags, Coordinator, FinalizeStreamPutRequest, ObjectRequest, PreparedStreamPut,
    PutObjectPolicyContext, PutObjectRequest, PutObjectResult, INTERNAL_SEGMENT_SIZE, TRACE_TARGET,
};
use crate::error::ServerError;
use crate::etag::format_etag;
use crate::sse::SseCustomerRequest;
use crate::system_metadata::SystemMetadata;

trait StreamPutFinalizationRoute {
    fn finalize(
        &self,
        session_id: &SessionId,
        total_size: u64,
        action: &mut dyn FnMut(
            StreamPutFinalizeSnapshot,
        ) -> Result<
            storage::PreparedStreamPutCommit<SystemMetadata>,
            ServerError,
        >,
    ) -> Result<
        Result<storage::FinalizeStreamPutOutcome<SystemMetadata>, ServerError>,
        storage::StreamUploadFailure,
    >;

    fn enqueue_object_payload_reclaim(&self, generation_id: storage::GenerationId);

    #[cfg(test)]
    fn try_probe_object_pg_available(&self) -> Result<bool, storage::DirectPutFailure>;
}

impl StreamPutFinalizationRoute for storage::ActivePutObjectRoute<'_> {
    fn finalize(
        &self,
        session_id: &SessionId,
        total_size: u64,
        action: &mut dyn FnMut(
            StreamPutFinalizeSnapshot,
        ) -> Result<
            storage::PreparedStreamPutCommit<SystemMetadata>,
            ServerError,
        >,
    ) -> Result<
        Result<storage::FinalizeStreamPutOutcome<SystemMetadata>, ServerError>,
        storage::StreamUploadFailure,
    > {
        self.finalize_stream(session_id, total_size, action)
    }

    fn enqueue_object_payload_reclaim(&self, generation_id: storage::GenerationId) {
        storage::ActivePutObjectRoute::enqueue_object_payload_reclaim(self, generation_id);
    }

    #[cfg(test)]
    fn try_probe_object_pg_available(&self) -> Result<bool, storage::DirectPutFailure> {
        storage::ActivePutObjectRoute::try_probe_object_pg_available(self)
    }
}

impl Coordinator {
    pub(super) fn map_direct_put_failure(error: storage::DirectPutFailure) -> ServerError {
        let diagnostic_label = error.diagnostic_cause_label();
        let _ = observability::event(
            TRACE_TARGET,
            "direct_put_error",
            Some(format_args!("cause_label={diagnostic_label}")),
        );
        match error.kind() {
            storage::DirectPutFailureKind::SnapshotReinspectionConflict
            | storage::DirectPutFailureKind::ResourceExhausted
            | storage::DirectPutFailureKind::MetadataCommandContention
            | storage::DirectPutFailureKind::RetryableConvergence => ServerError::SlowDown,
            storage::DirectPutFailureKind::InternalError => ServerError::DirectPut(error),
        }
    }

    fn conditional_write_conflict(
        key: &str,
        condition: &crate::conditional::WriteCondition,
    ) -> ServerError {
        let condition = match condition {
            crate::conditional::WriteCondition::IfMatch(_) => "If-Match",
            crate::conditional::WriteCondition::IfNoneMatchStar => "If-None-Match",
            crate::conditional::WriteCondition::None => {
                return ServerError::InternalError {
                    reason: "unconditional PutObject reported a snapshot reinspection conflict"
                        .to_string(),
                };
            }
        };
        ServerError::ConditionalRequestConflict {
            key: key.to_string(),
            condition,
        }
    }

    pub(super) fn map_put_object_direct_failure(
        key: &str,
        condition: &crate::conditional::WriteCondition,
        error: storage::DirectPutFailure,
    ) -> ServerError {
        if !condition.is_empty()
            && error.kind() == storage::DirectPutFailureKind::SnapshotReinspectionConflict
        {
            return Self::conditional_write_conflict(key, condition);
        }
        Self::map_direct_put_failure(error)
    }

    pub(super) fn map_put_object_stream_failure(
        key: &str,
        condition: &crate::conditional::WriteCondition,
        error: storage::StreamUploadFailure,
    ) -> ServerError {
        if !condition.is_empty()
            && error.kind() == storage::StreamUploadFailureKind::SnapshotReinspectionConflict
        {
            return Self::conditional_write_conflict(key, condition);
        }
        Self::map_stream_upload_failure(error)
    }

    /// Put an object, using a direct single-segment commit when possible.
    pub fn put_object(&self, req: &PutObjectRequest<'_>) -> Result<PutObjectResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_object_on_admitted_route(&admission, req)
    }

    /// Put an object through one immutable admitted runtime-map generation.
    pub fn put_object_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &PutObjectRequest<'_>,
    ) -> Result<PutObjectResult, ServerError> {
        self.require_storage_route_admission(admission)?;
        let authorize_req = AuthorizePutObjectRequest {
            object: ObjectRequest::new(
                req.object.bucket.name_typed().clone(),
                req.object.key_typed().clone(),
                req.object.requester().clone(),
                req.expected_bucket_owner(),
            ),
            acl: req.acl.clone(),
            policy_context: req.effective_policy_context()?,
            object_lock: req.object_lock,
            tags: req.tags,
            encryption: req.encryption,
        };
        if req.data.len() > INTERNAL_SEGMENT_SIZE {
            let cleanup = self.retained_stream_upload_cleanup(
                admission,
                req.object.bucket.name_typed(),
                req.object.key_typed(),
            )?;
            let prepared = self.begin_stream_put_with_storage_admission_and_cleanup_deadline(
                admission,
                &authorize_req,
                admission.authority_valid_until_ms(),
            )?;
            let route = admission
                .active_put_object_route(
                    prepared.authorized_write.bucket_typed(),
                    prepared.authorized_write.key_typed(),
                )
                .map_err(super::map_store_failure)?;
            let result = self
                .put_large_object_from_authorized_write_with_session_on_admitted_route(
                    &route,
                    &AuthorizedPutObjectCommitRequest {
                        data: req.data,
                        metadata: req.metadata,
                        system_metadata: req.system_metadata,
                        cond: req.cond,
                    },
                    &prepared.authorized_write,
                    &prepared.session_id,
                );
            if result.is_err() {
                let _ = self.abort_stream_upload_with_retained_cleanup_retrying(
                    &cleanup,
                    &prepared.session_id,
                );
            }
            return result;
        }

        let authorized =
            self.authorize_put_object_write_on_admitted_route(admission, &authorize_req)?;
        self.put_object_from_authorized_write_on_admitted_route(
            admission,
            &AuthorizedPutObjectCommitRequest {
                data: req.data,
                metadata: req.metadata,
                system_metadata: req.system_metadata,
                cond: req.cond,
            },
            &authorized,
        )
    }

    pub fn commit_put_object_write(
        &self,
        req: &AuthorizedPutObjectCommitRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<PutObjectResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_object_from_authorized_write_on_admitted_route(&admission, req, authorized)
    }

    pub fn commit_put_object_write_with_storage_admission(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &AuthorizedPutObjectCommitRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<PutObjectResult, ServerError> {
        self.require_storage_route_admission(admission)?;
        self.put_object_from_authorized_write_on_admitted_route(admission, req, authorized)
    }

    pub fn put_object_from_authorized_write(
        &self,
        req: &AuthorizedPutObjectCommitRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<PutObjectResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.put_object_from_authorized_write_on_admitted_route(&admission, req, authorized)
    }

    fn put_object_from_authorized_write_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &AuthorizedPutObjectCommitRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<PutObjectResult, ServerError> {
        self.require_storage_route_admission(admission)?;
        let put_route = admission
            .active_put_object_route(authorized.bucket_typed(), authorized.key_typed())
            .map_err(super::map_store_failure)?;
        let object_crc64 = checksum::crc64::checksum(req.data);
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_object",
            "bucket={:?} key={:?} bytes={}",
            authorized.bucket(),
            authorized.key(),
            req.data.len()
        );

        if req.data.len() > INTERNAL_SEGMENT_SIZE {
            let cleanup = self.retained_stream_upload_cleanup(
                admission,
                authorized.bucket_typed(),
                authorized.key_typed(),
            )?;
            let session_id = self
                .begin_stream_put_session_with_storage_admission_and_cleanup_deadline(
                    admission,
                    authorized,
                    admission.authority_valid_until_ms(),
                )?;
            let result = self
                .put_large_object_from_authorized_write_with_session_on_admitted_route(
                    &put_route,
                    req,
                    authorized,
                    &session_id,
                );
            if result.is_err() {
                let _ =
                    self.abort_stream_upload_with_retained_cleanup_retrying(&cleanup, &session_id);
            }
            return result;
        }

        // Request-owned metadata cannot depend on the reserved bucket snapshot.
        // Serialize it before acquiring durable bucket-write authority so an
        // invalid metadata value cannot create even a transient reservation.
        let metadata_blob = storage::SerializedMetadataBlob::from(req.metadata.serialize()?);
        let request = BucketHandleRequest::new().requiring_lifecycle_view();
        let expected_bucket_owner = authorized.expected_bucket_owner();
        put_route
            .with_bucket_write_snapshot_for_command(
                request.resolve_to_storage_request(),
                |snapshot, proof| {
                    let bucket_handle = match self
                        .bucket_handle_loader()
                        .load_bucket_handle_from_snapshot(snapshot, expected_bucket_owner, request)
                    {
                        Ok(bucket) => bucket,
                        Err(error) => {
                            return storage::BucketWriteSnapshotAction::release(Err(error))
                        }
                    };
                    #[cfg(test)]
                    self.maybe_run_bucket_write_handle_loaded_hook(authorized.bucket());
                    let mut proof_transferred_to_command = false;
                    let result = (|| {
                        let bucket_info = bucket_handle.bucket().clone();
                        let lifecycle =
                            self.cached_bucket_lifecycle_for_loaded_handle(&bucket_handle)?;
                        let stored_tags = Self::stored_object_tags(authorized.tags())?;
                        let write_encryption = &authorized.write_encryption;
                        Self::ensure_sse_c_allowed(
                            &bucket_info,
                            write_encryption.is_sse_customer(),
                        )?;
                        let acl = authorized.acl();
                        Self::ensure_put_object_write_acl_supported(&bucket_info, &acl)?;
                        let resolved_object_lock = Self::resolve_new_object_lock_state(
                            &bucket_info,
                            authorized.requested_object_lock(),
                        )?;
                        let system_metadata = Self::object_system_metadata_with_default_checksum(
                            req.system_metadata,
                            write_encryption,
                            object_crc64,
                        );
                        let owner = Self::effective_put_object_owner(
                            &bucket_info,
                            authorized.requester(),
                            &acl,
                        );
                        let acl_grants =
                            Self::object_acl_grants_for_put_object(&bucket_info, &owner, &acl);
                        let public_read = Self::acl_grants_public_read(&acl_grants);
                        let (system_metadata_blob, encryption) =
                            Self::prepare_stored_system_metadata(
                                &system_metadata,
                                write_encryption,
                            )?;
                        let transient_segment_id =
                            Self::random_session_id("failed to generate direct put segment ID")?;

                        let storage_bytes =
                            write_encryption.encrypt_segment(0, req.data)?;
                        let generation_id = put_route
                            .reserve_generation(&transient_segment_id)
                            .map_err(Coordinator::map_direct_put_failure)?;

                        let written_payload = match put_route.write_direct_object_payload(
                            &transient_segment_id,
                            generation_id,
                            req.data.len() as u64,
                            &storage_bytes,
                        ) {
                            Ok(written_segment) => written_segment,
                            Err(error) => {
                                put_route.release_generation_reservation(&transient_segment_id);
                                return Err(Coordinator::map_direct_put_failure(error));
                            }
                        };
                        let prepared_commit = PreparedDirectPutObjectCommit {
                            versioning: bucket_info.versioning,
                            owner,
                            acl_grants,
                            public_read,
                            etag_crc64: object_crc64,
                            object_lock: resolved_object_lock,
                            encryption,
                            tags: stored_tags,
                            metadata_blob,
                            system_metadata_blob,
                            bucket_write_reservation: proof,
                        };
                        #[cfg(test)]
                        if self.should_probe_direct_put_commit(authorized.bucket()) {
                            let object_pg_ready = match put_route
                                .try_probe_object_pg_available()
                                .map_err(Coordinator::map_direct_put_failure)
                            {
                                Ok(object_pg_ready) => object_pg_ready,
                                Err(error) => {
                                    put_route
                                        .discard_direct_object_payload(written_payload)
                                        .map_err(Coordinator::map_direct_put_failure)?;
                                    return Err(error);
                                }
                            };
                            if !object_pg_ready {
                                put_route
                                    .discard_direct_object_payload(written_payload)
                                    .map_err(Coordinator::map_direct_put_failure)?;
                                return Err(ServerError::InternalError {
                                    reason: "test probe: object pg still locked before direct put commit"
                                        .to_string(),
                                });
                            }
                        }
                        proof_transferred_to_command = true;
                        let outcome = put_route
                            .commit_direct_object(
                                written_payload,
                                &prepared_commit,
                                |snapshot| {
                                    if matches!(
                                        req.cond,
                                        crate::conditional::WriteCondition::IfMatch(_)
                                    ) && snapshot.existing_etag.is_none()
                                    {
                                        return Err(ServerError::ObjectNotFound {
                                            bucket: authorized.bucket().to_string(),
                                            key: authorized.key().to_string(),
                                        });
                                    }
                                    crate::conditional::check_write_conditions(
                                        req.cond,
                                        snapshot.existing_etag.as_deref(),
                                    )?;
                                    Ok(())
                                },
                            )
                            .map_err(|error| {
                                Coordinator::map_put_object_direct_failure(
                                    authorized.key(),
                                    req.cond,
                                    error,
                                )
                            })??;
                        let lifecycle_expiration =
                            Self::current_object_write_lifecycle_expiration_for_config(
                                lifecycle.as_ref(),
                                authorized.key(),
                                outcome.live_tags.as_deref(),
                                outcome.live_size,
                                outcome.live_last_modified,
                            );
                        if let Some(generation_id) = outcome.stale_generation_id {
                            put_route.enqueue_object_payload_reclaim(generation_id);
                        }

                        Ok(PutObjectResult {
                            etag: format_etag(object_crc64),
                            last_modified: outcome.live_last_modified,
                            version_id: outcome.version_id,
                            bucket_versioning: bucket_info.versioning,
                            system_metadata,
                            managed_encryption: outcome.encryption.managed_encryption_algorithm(),
                            lifecycle_expiration,
                        })
                    })();
                    if proof_transferred_to_command {
                        storage::BucketWriteSnapshotAction::transferred_to_command(result)
                    } else {
                        storage::BucketWriteSnapshotAction::release(result)
                    }
                },
            )
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
    }

    fn put_large_object_from_authorized_write_with_session_on_admitted_route(
        &self,
        route: &storage::ActivePutObjectRoute<'_>,
        req: &AuthorizedPutObjectCommitRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        session_id: &SessionId,
    ) -> Result<PutObjectResult, ServerError> {
        let object_crc64 = checksum::crc64::checksum(req.data);
        let write_encryption = &authorized.write_encryption;
        for (idx, chunk) in req.data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
            let chunk_crc64 = checksum::crc64::checksum(chunk);
            let chunk_storage = write_encryption.encrypt_segment(idx as u32, chunk)?;
            self.append_stream_segment_on_admitted_put_route(
                route,
                authorized.bucket_typed(),
                authorized.key_typed(),
                session_id,
                idx as u32,
                super::StreamSegmentAppendPayload::new(&chunk_storage, chunk_crc64),
            )?;
        }
        self.finalize_stream_put_with_authorized_write_tags_on_admitted_route(
            route,
            &AuthorizedFinalizeStreamPutRequest {
                session_id,
                crc64: object_crc64,
                total_size: req.data.len() as u64,
                metadata_blob: req.metadata,
                system_metadata: req.system_metadata,
                write_encryption: write_encryption.as_ref(),
                cond: req.cond,
            },
            authorized,
            AuthorizedWriteTags::Bound,
        )
    }

    pub fn begin_stream_put(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<PreparedStreamPut, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.begin_stream_put_with_storage_admission_and_cleanup_deadline(
            &admission,
            req,
            admission.authority_valid_until_ms(),
        )
    }

    pub fn begin_stream_put_with_storage_admission_and_cleanup_deadline(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &AuthorizePutObjectRequest<'_>,
        cleanup_after: Option<u64>,
    ) -> Result<PreparedStreamPut, ServerError> {
        self.require_storage_route_admission(admission)?;
        let route = admission
            .active_put_object_route(req.object.bucket_name_typed(), req.object.key_typed())
            .map_err(super::map_store_failure)?;
        let session_id = Self::random_session_id("failed to generate session ID")?;
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        route
            .create_stream_session(
                request.resolve_to_storage_request(),
                cleanup_after,
                |snapshot, existing_object| {
                    let bucket = self
                        .bucket_handle_loader()
                        .load_bucket_handle_from_snapshot(
                            snapshot,
                            req.object.expected_bucket_owner(),
                            request,
                        )?;
                    #[cfg(test)]
                    self.maybe_run_bucket_write_handle_loaded_hook(
                        req.object.bucket.name_typed().as_str(),
                    );
                    let authorized_write = self.authorize_put_object_write_with_existing_object(
                        req,
                        &bucket,
                        existing_object.as_ref(),
                    )?;
                    let create = CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: authorized_write.bucket_typed().clone(),
                        key: authorized_write.key_typed().clone(),
                        target: StreamUploadTarget::PutObject,
                        encryption: authorized_write.write_encryption.object_encryption(),
                    };
                    Ok((
                        PreparedStreamPut {
                            authorized_write,
                            session_id: session_id.clone(),
                        },
                        create,
                    ))
                },
            )
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
    }

    #[cfg(test)]
    pub fn begin_stream_put_session(
        &self,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<SessionId, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.begin_stream_put_session_with_storage_admission_and_cleanup_deadline(
            &admission,
            authorized,
            admission.authority_valid_until_ms(),
        )
    }

    pub fn begin_stream_put_session_with_storage_admission_and_cleanup_deadline(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        authorized: &AuthorizedPutObjectWrite,
        cleanup_after: Option<u64>,
    ) -> Result<SessionId, ServerError> {
        self.require_storage_route_admission(admission)?;
        let route = admission
            .active_put_object_route(authorized.bucket_typed(), authorized.key_typed())
            .map_err(super::map_store_failure)?;
        let session_id = Self::random_session_id("failed to generate stream session ID")?;
        route
            .create_stream_session_record(
                &session_id,
                authorized.write_encryption.object_encryption(),
                cleanup_after,
            )
            .map_err(Self::map_stream_upload_failure)?;
        Ok(session_id)
    }

    #[cfg(test)]
    pub fn append_stream_put_data(
        &self,
        req: &AppendStreamPutRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.append_stream_put_data_with_storage_admission(&admission, req)
    }

    pub fn append_stream_put_data_with_storage_admission(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &AppendStreamPutRequest<'_>,
    ) -> Result<(), ServerError> {
        self.require_storage_route_admission(admission)?;
        let route = admission
            .active_put_object_route(req.bucket, req.key)
            .map_err(super::map_store_failure)?;
        let session = route
            .load_stream_session(req.session_id)
            .map_err(Self::map_stream_upload_failure)?;
        let write_encryption = self.resume_write_encryption(
            &session.encryption,
            req.sse_customer,
            crate::sse::SseCustomerSegmentScope::object(),
            false,
        )?;
        let payload_crc64 = checksum::crc64::checksum(req.data);
        let storage_data = write_encryption.encrypt_segment(req.segment_index, req.data)?;
        self.append_stream_segment_on_admitted_put_route(
            &route,
            req.bucket,
            req.key,
            req.session_id,
            req.segment_index,
            super::StreamSegmentAppendPayload::new(&storage_data, payload_crc64),
        )
    }

    /// Finalize a streaming PutObject session.
    ///
    /// Builds committed segment metadata from staging rows and atomically
    /// commits the object through the storage metadata-command finalizer.
    ///
    /// The caller passes the running CRC64 checksum, total size, and metadata
    /// blob computed during the append phase. No segment data is re-read.
    #[cfg(test)]
    pub fn finalize_stream_put_from_authorized_write(
        &self,
        req: &AuthorizedFinalizeStreamPutRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<PutObjectResult, ServerError> {
        self.finalize_stream_put_with_authorized_write_tags(
            req,
            authorized,
            AuthorizedWriteTags::Bound,
        )
    }

    #[cfg(test)]
    pub fn finalize_authorized_stream_put(
        &self,
        req: &AuthorizedFinalizeStreamPutRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<PutObjectResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.finalize_authorized_stream_put_with_storage_admission(
            &admission,
            req,
            authorized,
            sse_customer,
        )
    }

    pub fn finalize_authorized_stream_put_with_storage_admission(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &AuthorizedFinalizeStreamPutRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<PutObjectResult, ServerError> {
        self.require_storage_route_admission(admission)?;
        let route = admission
            .active_put_object_route(authorized.bucket_typed(), authorized.key_typed())
            .map_err(super::map_store_failure)?;
        let session = route
            .load_stream_session(req.session_id)
            .map_err(Self::map_stream_upload_failure)?;
        let write_encryption = self.resume_write_encryption(
            &session.encryption,
            sse_customer,
            crate::sse::SseCustomerSegmentScope::object(),
            false,
        )?;
        self.finalize_stream_put_with_authorized_write_tags_on_admitted_route(
            &route,
            &AuthorizedFinalizeStreamPutRequest {
                session_id: req.session_id,
                crc64: req.crc64,
                total_size: req.total_size,
                metadata_blob: req.metadata_blob,
                system_metadata: req.system_metadata,
                write_encryption: write_encryption.as_ref(),
                cond: req.cond,
            },
            authorized,
            AuthorizedWriteTags::Bound,
        )
    }

    #[cfg(test)]
    pub(super) fn finalize_stream_put_with_authorized_write_tags(
        &self,
        req: &AuthorizedFinalizeStreamPutRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        tags: AuthorizedWriteTags<'_>,
    ) -> Result<PutObjectResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        let route = admission
            .active_put_object_route(authorized.bucket_typed(), authorized.key_typed())
            .map_err(super::map_store_failure)?;
        self.finalize_stream_put_with_authorized_write_tags_on_admitted_route(
            &route, req, authorized, tags,
        )
    }

    pub(super) fn finalize_stream_put_with_authorized_write_tags_on_admitted_route(
        &self,
        route: &storage::ActivePutObjectRoute<'_>,
        req: &AuthorizedFinalizeStreamPutRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        tags: AuthorizedWriteTags<'_>,
    ) -> Result<PutObjectResult, ServerError> {
        let tags = match tags {
            AuthorizedWriteTags::Bound => authorized.tags(),
            AuthorizedWriteTags::TrustedDerived(tags) => tags,
        };
        let finalize = FinalizeStreamPutRequest {
            object: ObjectRequest::new(
                authorized.bucket_typed().clone(),
                authorized.key_typed().clone(),
                authorized.requester().clone(),
                authorized.expected_bucket_owner(),
            ),
            session_id: req.session_id,
            crc64: req.crc64,
            total_size: req.total_size,
            metadata_blob: req.metadata_blob,
            system_metadata: req.system_metadata,
            write_encryption: req.write_encryption,
            tags,
            cond: req.cond,
            acl: authorized.acl(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: authorized.requested_object_lock(),
        };
        let request = BucketHandleRequest::new().requiring_lifecycle_view();
        route
            .with_bucket_write_snapshot(request.resolve_to_storage_request(), |snapshot| {
                let bucket_handle = self
                    .bucket_handle_loader()
                    .load_bucket_handle_from_snapshot(
                        snapshot,
                        finalize.object.expected_bucket_owner(),
                        request,
                    )?;
                #[cfg(test)]
                self.maybe_run_bucket_write_handle_loaded_hook(
                    finalize.object.bucket_name_typed().as_str(),
                );
                self.finalize_stream_put_for_loaded_bucket(route, bucket_handle, &finalize)
            })
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
    }

    #[cfg(test)]
    pub(super) fn finalize_stream_put(
        &self,
        req: &FinalizeStreamPutRequest,
    ) -> Result<PutObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::finalize_stream_put",
            "bucket={:?} key={:?} session_id={:?} bytes={}",
            req.object.bucket_name(),
            req.object.key(),
            req.session_id,
            req.total_size
        );
        let admission = self.admit_storage_route_for_request()?;
        let route = admission
            .active_put_object_route(req.object.bucket_name_typed(), req.object.key_typed())
            .map_err(super::map_store_failure)?;
        let request = BucketHandleRequest::new().requiring_lifecycle_view();
        route
            .with_bucket_write_snapshot(request.resolve_to_storage_request(), |snapshot| {
                let bucket_handle = self
                    .bucket_handle_loader()
                    .load_bucket_handle_from_snapshot(
                        snapshot,
                        req.object.expected_bucket_owner(),
                        request,
                    )?;
                #[cfg(test)]
                self.maybe_run_bucket_write_handle_loaded_hook(
                    req.object.bucket_name_typed().as_str(),
                );
                self.finalize_stream_put_for_loaded_bucket(&route, bucket_handle, req)
            })
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
    }

    fn finalize_stream_put_for_loaded_bucket(
        &self,
        route: &impl StreamPutFinalizationRoute,
        bucket_handle: LoadedBucketHandle,
        req: &FinalizeStreamPutRequest,
    ) -> Result<PutObjectResult, ServerError> {
        let bucket_info = bucket_handle.bucket().clone();
        let lifecycle = self.cached_bucket_lifecycle_for_loaded_handle(&bucket_handle)?;
        let key = req.object.key();
        let session_id = req.session_id;
        let crc64 = req.crc64;
        let total_size = req.total_size;
        let metadata_blob = req.metadata_blob;
        let tags = req.tags;
        let cond = req.cond;
        let result = (|| {
            Self::ensure_put_object_write_acl_supported(&bucket_info, &req.acl)?;
            let resolved_object_lock =
                Self::resolve_new_object_lock_state(&bucket_info, req.requested_object_lock)?;
            #[cfg(test)]
            if self.should_probe_finalize_stream_put_commit(req.object.bucket_name()) {
                let object_pg_ready = route
                    .try_probe_object_pg_available()
                    .map_err(Coordinator::map_direct_put_failure)?;
                if !object_pg_ready {
                    return Err(ServerError::InternalError {
                        reason:
                            "test probe: object pg still locked before finalize_stream_put commit"
                                .to_string(),
                    });
                }
            }
            let mut prepare = |snapshot: StreamPutFinalizeSnapshot| {
                let write_encryption = ActiveWriteEncryption::from_stored_and_active(
                    &snapshot.session.encryption,
                    req.write_encryption,
                )?;
                let system_metadata = Self::object_system_metadata_with_default_checksum(
                    req.system_metadata,
                    &write_encryption,
                    crc64,
                );
                if !cond.is_empty() {
                    if matches!(cond, crate::conditional::WriteCondition::IfMatch(_))
                        && snapshot.existing_etag.is_none()
                    {
                        return Err(ServerError::ObjectNotFound {
                            bucket: req.object.bucket_name().to_string(),
                            key: req.object.key().to_string(),
                        });
                    }
                    crate::conditional::check_write_conditions(
                        cond,
                        snapshot.existing_etag.as_deref(),
                    )?;
                }
                let metadata_blob =
                    storage::SerializedMetadataBlob::from(metadata_blob.serialize()?);
                let (system_metadata_blob, encryption) =
                    Self::prepare_stored_system_metadata(&system_metadata, &write_encryption)?;
                let owner = Self::effective_put_object_owner(
                    &bucket_info,
                    req.object.requester(),
                    &req.acl,
                );
                let acl_grants =
                    Self::object_acl_grants_for_put_object(&bucket_info, &owner, &req.acl);

                Ok(storage::PreparedStreamPutCommit {
                    value: system_metadata,
                    versioning: bucket_info.versioning,
                    owner,
                    acl_grants: acl_grants.clone(),
                    public_read: Self::acl_grants_public_read(&acl_grants),
                    etag_crc64: crc64,
                    tags: Self::stored_object_tags(tags)?,
                    metadata_blob,
                    system_metadata_blob,
                    object_lock: resolved_object_lock,
                    encryption,
                })
            };
            let outcome = route
                .finalize(session_id, total_size, &mut prepare)
                .map_err(|error| Coordinator::map_put_object_stream_failure(key, cond, error))??;
            let lifecycle_expiration = Self::current_object_write_lifecycle_expiration_for_config(
                lifecycle.as_ref(),
                key,
                outcome.live_tags.as_deref(),
                outcome.live_size,
                outcome.live_last_modified,
            );
            if let Some(generation_id) = outcome.stale_generation_id {
                route.enqueue_object_payload_reclaim(generation_id);
            }

            Ok(PutObjectResult {
                etag: format_etag(crc64),
                last_modified: outcome.live_last_modified,
                version_id: outcome.version_id,
                bucket_versioning: bucket_info.versioning,
                system_metadata: outcome.value,
                managed_encryption: outcome.encryption.managed_encryption_algorithm(),
                lifecycle_expiration,
            })
        })();
        result
    }

    #[cfg(test)]
    pub fn abort_stream_put_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.require_storage_route_admission(&admission)?;
        admission
            .active_put_object_route(bucket, key)
            .map_err(super::map_store_failure)?
            .abort_stream_session(session_id)
            .map_err(Self::map_stream_upload_failure)
    }

    #[cfg(test)]
    pub fn heartbeat_stream_put_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.heartbeat_stream_put_session_with_storage_admission(
            &admission, bucket, key, session_id,
        )
    }

    pub fn heartbeat_stream_put_session_with_storage_admission(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.require_storage_route_admission(admission)?;
        admission
            .active_put_object_route(bucket, key)
            .map_err(super::map_store_failure)?
            .heartbeat_stream_session(session_id)
            .map_err(Self::map_stream_upload_failure)
    }
}
