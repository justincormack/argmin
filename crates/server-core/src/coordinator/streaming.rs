use std::time::{Duration, Instant};
use storage::{
    stream_segment_key_hash, BucketName, ManagedEncryptionAlgorithm, ObjectEncryption, ObjectKey,
    PrepareStreamUploadSegmentAppendReq, SessionId, ShardKey, StreamUploadTarget,
};

#[cfg(feature = "deep-tracing")]
use super::INTERNAL_SEGMENT_SIZE;
#[cfg(test)]
use super::{trusted_bucket_name, trusted_object_key};
use super::{
    ActiveWriteEncryption, BucketSummary, Coordinator, WriteEncryptionRequest, TRACE_TARGET,
};
use crate::error::ServerError;
use crate::sse::{
    prepare_managed_encryption_write, prepare_sse_customer_write, resume_managed_encryption_write,
    resume_sse_customer_write, validate_sse_customer_read, ManagedEncryptionWriteContext,
    SseCustomerRequest, SseCustomerResponseHeaders, SseCustomerSegmentScope,
    SseCustomerWriteContext, SSE_C_SEGMENT_TAG_LEN,
};

trait StreamSegmentMutationRoute {
    fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<storage::StreamUploadRecord, storage::ObjectPgActionError>;

    fn prepare_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<
        (StreamUploadTarget, storage::StreamUploadSegmentRecord),
        storage::ObjectPgActionError,
    >;

    fn write_payload(
        &self,
        session_id: &SessionId,
        segment: &storage::StreamUploadSegmentRecord,
        data: &[u8],
    ) -> Result<Vec<storage::WrittenShardAck>, storage::StoreError>;

    fn commit_append(
        &self,
        session_id: &SessionId,
        segment_index: u32,
        segment: &storage::StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, storage::WriteAck)],
    ) -> Result<(), storage::ObjectPgActionError>;
}

struct RawStreamSegmentMutationRoute<'a> {
    storage_node: &'a std::sync::Arc<storage::StorageCluster>,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
}

impl StreamSegmentMutationRoute for RawStreamSegmentMutationRoute<'_> {
    fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<storage::StreamUploadRecord, storage::ObjectPgActionError> {
        self.storage_node
            .load_stream_upload_session(self.bucket, self.key, session_id)
    }

    fn prepare_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<
        (StreamUploadTarget, storage::StreamUploadSegmentRecord),
        storage::ObjectPgActionError,
    > {
        self.storage_node
            .prepare_stream_segment_append(self.bucket, self.key, request)
    }

    fn write_payload(
        &self,
        _session_id: &SessionId,
        segment: &storage::StreamUploadSegmentRecord,
        data: &[u8],
    ) -> Result<Vec<storage::WrittenShardAck>, storage::StoreError> {
        self.storage_node
            .write_stream_segment_payload_shards(segment, data)
    }

    fn commit_append(
        &self,
        session_id: &SessionId,
        segment_index: u32,
        segment: &storage::StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, storage::WriteAck)],
    ) -> Result<(), storage::ObjectPgActionError> {
        self.storage_node.commit_stream_segment_append(
            self.bucket,
            self.key,
            session_id,
            segment_index,
            segment,
            shard_batch,
        )
    }
}

impl StreamSegmentMutationRoute for storage::ActivePutObjectRoute<'_> {
    fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<storage::StreamUploadRecord, storage::ObjectPgActionError> {
        self.load_stream_session(session_id)
    }

    fn prepare_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<
        (StreamUploadTarget, storage::StreamUploadSegmentRecord),
        storage::ObjectPgActionError,
    > {
        self.prepare_stream_segment_append(request)
    }

    fn write_payload(
        &self,
        session_id: &SessionId,
        segment: &storage::StreamUploadSegmentRecord,
        data: &[u8],
    ) -> Result<Vec<storage::WrittenShardAck>, storage::StoreError> {
        self.write_stream_segment_payload_shards(session_id, segment, data)
    }

    fn commit_append(
        &self,
        session_id: &SessionId,
        segment_index: u32,
        segment: &storage::StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, storage::WriteAck)],
    ) -> Result<(), storage::ObjectPgActionError> {
        self.commit_stream_segment_append(session_id, segment_index, segment, shard_batch)
    }
}

impl StreamSegmentMutationRoute for storage::ActiveMultipartObjectRoute<'_> {
    fn load_session(
        &self,
        session_id: &SessionId,
    ) -> Result<storage::StreamUploadRecord, storage::ObjectPgActionError> {
        self.load_stream_session(session_id)
    }

    fn prepare_append(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<
        (StreamUploadTarget, storage::StreamUploadSegmentRecord),
        storage::ObjectPgActionError,
    > {
        self.prepare_stream_segment_append(request)
    }

    fn write_payload(
        &self,
        session_id: &SessionId,
        segment: &storage::StreamUploadSegmentRecord,
        data: &[u8],
    ) -> Result<Vec<storage::WrittenShardAck>, storage::StoreError> {
        self.write_stream_segment_payload_shards(session_id, segment, data)
    }

    fn commit_append(
        &self,
        session_id: &SessionId,
        segment_index: u32,
        segment: &storage::StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, storage::WriteAck)],
    ) -> Result<(), storage::ObjectPgActionError> {
        self.commit_stream_segment_append(session_id, segment_index, segment, shard_batch)
    }
}

impl Coordinator {
    pub(super) fn require_admitted_storage_effect(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
    ) -> Result<(), ServerError> {
        self.require_storage_route_admission(admission)?;
        admission
            .require_valid_now_for(storage_node)
            .map_err(super::map_store_error)
    }

    pub fn retained_stream_upload_cleanup(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<storage::RetainedStreamUploadCleanup, ServerError> {
        self.require_storage_route_admission(admission)?;
        admission
            .retained_stream_upload_cleanup(bucket, key)
            .map_err(super::map_store_error)
    }

    pub fn abort_stream_upload_with_retained_cleanup(
        &self,
        cleanup: &storage::RetainedStreamUploadCleanup,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        cleanup
            .abort(session_id)
            .map_err(Self::map_object_pg_action_error)
    }

    pub(super) fn abort_stream_upload_with_retained_cleanup_retrying(
        &self,
        cleanup: &storage::RetainedStreamUploadCleanup,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_upload_with_retained_cleanup_until(
            cleanup,
            session_id,
            Instant::now() + Duration::from_secs(5),
            Duration::from_millis(5),
        )
    }

    fn abort_stream_upload_with_retained_cleanup_until(
        &self,
        cleanup: &storage::RetainedStreamUploadCleanup,
        session_id: &SessionId,
        deadline: Instant,
        retry_delay: Duration,
    ) -> Result<(), ServerError> {
        loop {
            match cleanup
                .abort(session_id)
                .map_err(Self::map_object_pg_action_error)
            {
                Ok(()) => return Ok(()),
                Err(error @ (ServerError::OperationAborted | ServerError::SlowDown)) => {
                    let now = Instant::now();
                    let Some(remaining) = deadline.checked_duration_since(now) else {
                        return Err(error);
                    };
                    if remaining.is_zero() {
                        return Err(error);
                    }
                    std::thread::sleep(retry_delay.min(remaining));
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(test)]
    pub(super) fn abort_stream_upload_with_retained_cleanup_for_test(
        &self,
        cleanup: &storage::RetainedStreamUploadCleanup,
        session_id: &SessionId,
        retry_timeout: Duration,
        retry_delay: Duration,
    ) -> Result<(), ServerError> {
        self.abort_stream_upload_with_retained_cleanup_until(
            cleanup,
            session_id,
            Instant::now() + retry_timeout,
            retry_delay,
        )
    }

    pub fn prepare_sse_customer_write_context(
        &self,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<Option<crate::sse::SseCustomerWriteContext>, ServerError> {
        match sse_customer {
            None => Ok(None),
            Some(request) => {
                let validator =
                    self.sse_c_validator
                        .as_ref()
                        .ok_or(ServerError::NotImplemented {
                            feature: "SSE-C requires ARGMIN_SSE_C_VALIDATOR_KEY".to_string(),
                        })?;
                Ok(Some(prepare_sse_customer_write(validator, request)?))
            }
        }
    }

    pub fn prepare_managed_write_context(
        &self,
    ) -> Result<ManagedEncryptionWriteContext, ServerError> {
        let provider = self
            .managed_key_provider
            .as_ref()
            .ok_or(ServerError::NotImplemented {
                feature: "SSE-S3 requires managed key provider configuration".to_string(),
            })?;
        prepare_managed_encryption_write(provider)
    }

    pub(super) fn resolve_write_encryption(
        &self,
        bucket: &BucketSummary,
        request_encryption: WriteEncryptionRequest<'_>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        if let Some(sse_customer_request) = request_encryption.sse_customer_request() {
            let sse_customer =
                self.prepare_sse_customer_write_context(Some(sse_customer_request))?;
            return Ok(ActiveWriteEncryption::sse_customer(
                sse_customer.expect("SSE-C request should produce write context"),
            ));
        }

        if request_encryption.explicit_managed_encryption().is_some()
            || bucket.encryption.default_encryption == ManagedEncryptionAlgorithm::Aes256
        {
            let managed_write = self.prepare_managed_write_context()?;
            return Ok(ActiveWriteEncryption::managed(
                ManagedEncryptionAlgorithm::Aes256,
                managed_write,
            ));
        }

        Ok(ActiveWriteEncryption::none())
    }

    pub(super) fn resume_write_encryption(
        &self,
        encryption: &ObjectEncryption,
        sse_customer: Option<&SseCustomerRequest>,
        segment_scope: SseCustomerSegmentScope,
        sse_customer_headers_required: bool,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        match encryption {
            ObjectEncryption::None => {
                let sse_customer = self.prepare_existing_sse_customer_write_context(
                    encryption,
                    sse_customer,
                    segment_scope,
                    sse_customer_headers_required,
                )?;
                Ok(match sse_customer {
                    Some(sse_customer) => ActiveWriteEncryption::sse_customer(sse_customer),
                    None => ActiveWriteEncryption::None,
                })
            }
            ObjectEncryption::SseCustomer(state) => {
                let sse_customer = self.prepare_existing_sse_customer_write_context(
                    encryption,
                    sse_customer,
                    segment_scope,
                    sse_customer_headers_required,
                )?;
                Ok(match sse_customer {
                    Some(sse_customer) => ActiveWriteEncryption::sse_customer(sse_customer),
                    None => ActiveWriteEncryption::SseCustomer {
                        encryption: ObjectEncryption::SseCustomer(state.clone()),
                        write: None,
                    },
                })
            }
            ObjectEncryption::SseS3(state) => {
                if sse_customer.is_some() {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-C headers may not be used for a non-SSE-C upload".to_string(),
                    });
                }
                let provider =
                    self.managed_key_provider
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-S3 key provider is not configured".to_string(),
                        })?;
                let managed_write =
                    resume_managed_encryption_write(provider, state, segment_scope)?;
                Ok(ActiveWriteEncryption::managed(
                    ManagedEncryptionAlgorithm::Aes256,
                    managed_write,
                ))
            }
        }
    }

    pub fn load_stream_put_write_encryption(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        self.load_stream_put_write_encryption_with_storage_node(
            &self.storage_node(),
            bucket,
            key,
            session_id,
            sse_customer,
        )
    }

    pub(super) fn load_stream_put_write_encryption_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        let session = storage_node
            .load_stream_upload_session(bucket, key, session_id)
            .map_err(Self::map_object_pg_action_error)?;
        self.resume_write_encryption(
            &session.encryption,
            sse_customer,
            SseCustomerSegmentScope::object(),
            false,
        )
    }

    pub fn load_stream_part_write_encryption(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        part_number: u32,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        self.load_stream_part_write_encryption_with_storage_node(
            &self.storage_node(),
            bucket,
            key,
            session_id,
            part_number,
            sse_customer,
        )
    }

    pub(super) fn load_stream_part_write_encryption_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        part_number: u32,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        let session = storage_node
            .load_stream_upload_session(bucket, key, session_id)
            .map_err(Self::map_object_pg_action_error)?;
        self.resume_write_encryption(
            &session.encryption,
            sse_customer,
            SseCustomerSegmentScope::multipart_part(part_number)?,
            false,
        )
    }

    pub(super) fn load_stream_part_write_encryption_on_admitted_multipart_route(
        &self,
        route: &storage::ActiveMultipartObjectRoute<'_>,
        session_id: &SessionId,
        part_number: u32,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        let session = route
            .load_stream_session(session_id)
            .map_err(Self::map_object_pg_action_error)?;
        self.resume_write_encryption(
            &session.encryption,
            sse_customer,
            SseCustomerSegmentScope::multipart_part(part_number)?,
            false,
        )
    }

    #[cfg(test)]
    fn load_stream_session_write_encryption(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        let target = self
            .storage_node()
            .load_stream_upload_session(bucket, key, session_id)
            .map_err(Self::map_object_pg_action_error)?
            .target;
        match target {
            StreamUploadTarget::PutObject => {
                self.load_stream_put_write_encryption(bucket, key, session_id, sse_customer)
            }
            StreamUploadTarget::UploadPart { part_number, .. } => self
                .load_stream_part_write_encryption(
                    bucket,
                    key,
                    session_id,
                    part_number,
                    sse_customer,
                ),
        }
    }

    #[cfg(test)]
    pub(crate) fn append_plaintext_stream_segment_for_test(
        &self,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let write_encryption = self.load_stream_session_write_encryption(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            session_id,
            None,
        )?;
        let payload_crc64 = checksum::crc64::checksum(data);
        let storage_data = write_encryption.encrypt_segment(segment_index, data)?;
        self.append_stream_segment_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            session_id,
            segment_index,
            super::StreamSegmentAppendPayload::maybe_encrypted(
                &storage_data,
                payload_crc64,
                matches!(
                    write_encryption.as_ref(),
                    super::ActiveWriteEncryptionRef::None
                ),
            ),
        )
    }

    pub(super) fn ensure_write_encryption_supported(
        &self,
        encryption: &ObjectEncryption,
    ) -> Result<(), ServerError> {
        match encryption {
            ObjectEncryption::None | ObjectEncryption::SseCustomer(_) => Ok(()),
            ObjectEncryption::SseS3(_) => {
                if self.managed_key_provider.is_some() {
                    Ok(())
                } else {
                    Err(ServerError::NotImplemented {
                        feature: "SSE-S3 requires managed key provider configuration".to_string(),
                    })
                }
            }
        }
    }

    pub(super) fn prepare_existing_sse_customer_write_context(
        &self,
        encryption: &ObjectEncryption,
        sse_customer: Option<&SseCustomerRequest>,
        segment_scope: SseCustomerSegmentScope,
        headers_required: bool,
    ) -> Result<Option<SseCustomerWriteContext>, ServerError> {
        match encryption {
            ObjectEncryption::None => {
                if sse_customer.is_some() {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-C headers may not be used for an unencrypted multipart upload"
                            .to_string(),
                    });
                }
                Ok(None)
            }
            ObjectEncryption::SseCustomer(state) => {
                let Some(request) = sse_customer else {
                    if headers_required {
                        return Err(ServerError::InvalidRequest {
                            reason: "SSE-C headers are required for this multipart upload"
                                .to_string(),
                        });
                    }
                    return Ok(None);
                };
                let validator =
                    self.sse_c_validator
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-C validator key is not configured".to_string(),
                        })?;
                Ok(Some(resume_sse_customer_write(
                    validator,
                    state,
                    request,
                    segment_scope,
                )?))
            }
            ObjectEncryption::SseS3(_) => {
                if sse_customer.is_some() {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-C headers may not be used for a non-SSE-C multipart upload"
                            .to_string(),
                    });
                }
                Ok(None)
            }
        }
    }

    pub(super) fn prepare_sse_customer_read_access(
        &self,
        encryption: &ObjectEncryption,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<Option<SseCustomerResponseHeaders>, ServerError> {
        match encryption {
            ObjectEncryption::None => {
                if sse_customer.is_some() {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-C headers may not be used for an unencrypted object"
                            .to_string(),
                    });
                }
                Ok(None)
            }
            ObjectEncryption::SseCustomer(state) => {
                let request = sse_customer.ok_or(ServerError::InvalidRequest {
                    reason: "SSE-C headers are required for this object".to_string(),
                })?;
                let validator =
                    self.sse_c_validator
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-C validator key is not configured".to_string(),
                        })?;
                Ok(Some(validate_sse_customer_read(validator, state, request)?))
            }
            ObjectEncryption::SseS3(_) => {
                if sse_customer.is_some() {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-C headers may not be used for a non-SSE-C object".to_string(),
                    });
                }
                Ok(None)
            }
        }
    }

    fn emit_stream_segment_layout(
        target: &StreamUploadTarget,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
        segment_index: u32,
        data_len: usize,
    ) {
        #[cfg(not(feature = "deep-tracing"))]
        {
            let _ = (target, bucket, key, session_id, segment_index, data_len);
        }

        #[cfg(feature = "deep-tracing")]
        let Some(trace) = observability::current_context() else {
            return;
        };
        #[cfg(feature = "deep-tracing")]
        let segment_offset_start = u64::from(segment_index) * INTERNAL_SEGMENT_SIZE as u64;
        #[cfg(feature = "deep-tracing")]
        let segment_offset_len = data_len as u64;
        #[cfg(feature = "deep-tracing")]
        let segment_offset_end_exclusive = segment_offset_start + segment_offset_len;
        #[cfg(feature = "deep-tracing")]
        match target {
            StreamUploadTarget::PutObject => {
                let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "stream_put_segment_layout",
                    Some(format_args!(
                        "bucket={:?} key={:?} session_id={:?} segment_index={} object_offset_start={} object_offset_len={} object_offset_end_exclusive={}",
                        bucket,
                        key,
                        session_id,
                        segment_index,
                        segment_offset_start,
                        segment_offset_len,
                        segment_offset_end_exclusive
                    )),
                );
            }
            StreamUploadTarget::UploadPart {
                upload_id,
                part_number,
            } => {
                let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "stream_part_segment_layout",
                    Some(format_args!(
                        "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} segment_index={} part_offset_start={} part_offset_len={} part_offset_end_exclusive={}",
                        bucket,
                        key,
                        upload_id,
                        part_number,
                        session_id,
                        segment_index,
                        segment_offset_start,
                        segment_offset_len,
                        segment_offset_end_exclusive
                    )),
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn append_stream_segment_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        payload: super::StreamSegmentAppendPayload<'_>,
    ) -> Result<(), ServerError> {
        self.append_stream_segment_for_storage_node(
            &self.storage_node(),
            bucket,
            key,
            session_id,
            segment_index,
            payload,
        )
    }

    pub(super) fn append_stream_segment_for_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        payload: super::StreamSegmentAppendPayload<'_>,
    ) -> Result<(), ServerError> {
        self.append_stream_segment_on_route(
            &RawStreamSegmentMutationRoute {
                storage_node,
                bucket,
                key,
            },
            bucket,
            key,
            session_id,
            segment_index,
            payload,
        )
    }

    pub(super) fn append_stream_segment_on_admitted_put_route(
        &self,
        route: &storage::ActivePutObjectRoute<'_>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        payload: super::StreamSegmentAppendPayload<'_>,
    ) -> Result<(), ServerError> {
        self.append_stream_segment_on_route(route, bucket, key, session_id, segment_index, payload)
    }

    pub(super) fn append_stream_segment_on_admitted_multipart_route(
        &self,
        route: &storage::ActiveMultipartObjectRoute<'_>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        payload: super::StreamSegmentAppendPayload<'_>,
    ) -> Result<(), ServerError> {
        self.append_stream_segment_on_route(route, bucket, key, session_id, segment_index, payload)
    }

    fn append_stream_segment_on_route(
        &self,
        route: &impl StreamSegmentMutationRoute,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        payload: super::StreamSegmentAppendPayload<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::append_stream_segment",
            "bucket={:?} key={:?} session_id={:?} segment_index={} bytes={}",
            bucket,
            key,
            session_id,
            segment_index,
            payload.storage_bytes.len()
        );
        let segment_okh = stream_segment_key_hash(session_id, segment_index);

        let logical_size = if payload.storage_bytes.is_empty() {
            0
        } else {
            match route
                .load_session(session_id)
                .map_err(Self::map_object_pg_action_error)?
                .encryption
            {
                ObjectEncryption::None => payload.storage_bytes.len() as u64,
                ObjectEncryption::SseCustomer(_) | ObjectEncryption::SseS3(_) => {
                    let ciphertext_len = payload.storage_bytes.len();
                    let logical_len = ciphertext_len.checked_sub(SSE_C_SEGMENT_TAG_LEN).ok_or(
                        ServerError::InvalidRequest {
                            reason: "encrypted stream segment shorter than authentication tag"
                                .to_string(),
                        },
                    )?;
                    logical_len as u64
                }
            }
        };

        let (target, segment_record) = route
            .prepare_append(&PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index,
                size: logical_size,
                segment_crc64: payload.segment_crc64,
                payload_crc64: payload.payload_crc64,
                segment_okh,
            })
            .map_err(Self::map_object_pg_action_error)?;

        Self::emit_stream_segment_layout(
            &target,
            bucket.as_str(),
            key.as_str(),
            session_id,
            segment_index,
            logical_size as usize,
        );

        #[cfg(test)]
        self.maybe_run_stream_append_prepare_hook(session_id, segment_index);

        let written_shards = route
            .write_payload(session_id, &segment_record, payload.storage_bytes)
            .map_err(super::map_store_error)?;

        let shard_batch: Vec<(&ShardKey, storage::WriteAck)> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();

        route
            .commit_append(session_id, segment_index, &segment_record, &shard_batch)
            .map_err(Self::map_object_pg_action_error)
    }

    #[cfg(test)]
    pub fn append_stream_segment(
        &self,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        self.append_stream_segment_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            session_id,
            segment_index,
            super::StreamSegmentAppendPayload::maybe_encrypted(
                data,
                checksum::crc64::checksum(data),
                true,
            ),
        )
    }

    #[cfg(test)]
    pub(super) fn abort_stream_put_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_put_for_storage_node(&self.storage_node(), bucket, key, session_id)
    }

    pub(super) fn abort_stream_put_for_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        storage_node
            .abort_stream_upload_session(bucket, key, session_id)
            .map_err(Self::map_object_pg_action_error)
    }

    #[cfg(test)]
    pub(super) fn abort_stream_put_for_cleanup(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_put_for_cleanup_with_storage_node(
            &self.storage_node(),
            bucket,
            key,
            session_id,
        )
    }

    pub(super) fn abort_stream_put_for_cleanup_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.abort_stream_put_for_storage_node(storage_node, bucket, key, session_id) {
                Ok(()) => return Ok(()),
                Err(ServerError::OperationAborted | ServerError::SlowDown)
                    if Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(test)]
    pub fn abort_stream_put(
        &self,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_put_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            session_id,
        )
    }

    pub fn scavenge_stale_sessions(&self, max_age_ms: u64) -> usize {
        self.storage_node()
            .scavenge_abandoned_stream_sessions(max_age_ms)
    }
}
