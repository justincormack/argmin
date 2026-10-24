// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};
#[cfg(any(test, feature = "test-utils"))]
use storage::SegmentPayloadCodec;
use storage::{
    BucketName, ManagedEncryptionAlgorithm, ObjectEncryption, ObjectKey, SessionId,
    StreamUploadTarget,
};

#[cfg(test)]
use storage::test_support::{
    ActiveMultipartStreamRouteTestSupport as _, ActiveStreamRouteTestSupport as _,
};

#[cfg(feature = "deep-tracing")]
use super::INTERNAL_SEGMENT_SIZE;
#[cfg(test)]
use super::{trusted_bucket_name, trusted_object_key};
use super::{
    ActiveWriteEncryption, BucketSummary, Coordinator, WriteEncryptionRequest, TRACE_TARGET,
};
use crate::error::ServerError;
#[cfg(not(any(test, feature = "test-utils")))]
use crate::sse::{prepare_managed_encryption_write, prepare_sse_customer_write};
use crate::sse::{
    resume_managed_encryption_write, resume_sse_customer_write,
    validate_sse_customer_completion_replay, validate_sse_customer_read,
    ManagedEncryptionWriteContext, SseCustomerRequest, SseCustomerResponseHeaders,
    SseCustomerSegmentScope, SseCustomerWriteContext,
};
#[cfg(any(test, feature = "test-utils"))]
use crate::sse::{
    test_prepare_managed_encryption_write_with_payload_codec,
    test_prepare_sse_customer_write_with_payload_codec,
};

impl Coordinator {
    #[cfg(any(test, feature = "test-utils"))]
    fn new_payload_codec(&self) -> SegmentPayloadCodec {
        self.test_new_payload_codec
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) fn test_select_new_payload_codec(&mut self, payload_codec: SegmentPayloadCodec) {
        self.test_new_payload_codec = payload_codec;
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
            .map_err(super::map_store_failure)
    }

    pub fn abort_stream_upload_with_retained_cleanup(
        &self,
        cleanup: &storage::RetainedStreamUploadCleanup,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        cleanup
            .abort(session_id)
            .map_err(Self::map_stream_upload_failure)
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
                .map_err(Self::map_stream_upload_failure)
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
                #[cfg(any(test, feature = "test-utils"))]
                let write = test_prepare_sse_customer_write_with_payload_codec(
                    validator,
                    request,
                    self.new_payload_codec(),
                )?;
                #[cfg(not(any(test, feature = "test-utils")))]
                let write = prepare_sse_customer_write(validator, request)?;
                Ok(Some(write))
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
        #[cfg(any(test, feature = "test-utils"))]
        {
            test_prepare_managed_encryption_write_with_payload_codec(
                provider,
                self.new_payload_codec(),
            )
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            prepare_managed_encryption_write(provider)
        }
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

        let algorithm = request_encryption
            .explicit_managed_encryption()
            .unwrap_or(bucket.encryption.default_encryption);
        let managed_write = self.prepare_managed_write_context()?;
        Ok(ActiveWriteEncryption::managed(algorithm, managed_write))
    }

    pub(super) fn resume_write_encryption(
        &self,
        encryption: &ObjectEncryption,
        sse_customer: Option<&SseCustomerRequest>,
        segment_scope: SseCustomerSegmentScope,
        sse_customer_headers_required: bool,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        match encryption {
            ObjectEncryption::SseCustomer(state) => {
                let sse_customer = self.prepare_existing_sse_customer_write_context(
                    encryption,
                    sse_customer,
                    segment_scope,
                    sse_customer_headers_required,
                )?;
                Ok(match sse_customer {
                    Some(sse_customer) => ActiveWriteEncryption::sse_customer(sse_customer),
                    None => {
                        ActiveWriteEncryption::sse_customer_without_write_context(state.clone())
                    }
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

    pub(super) fn load_stream_part_write_encryption_on_admitted_multipart_route(
        &self,
        authorization: &storage::AuthorizedStreamPartSegment<'_, '_>,
        part_number: u32,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<ActiveWriteEncryption, ServerError> {
        self.resume_write_encryption(
            authorization.encryption(),
            sse_customer,
            SseCustomerSegmentScope::multipart_part(part_number)?,
            false,
        )
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
        let bucket = trusted_bucket_name(bucket);
        let key = trusted_object_key(key);
        let admission = self.admit_storage_route_for_request()?;
        let put_route = admission
            .active_put_object_route(&bucket, &key)
            .map_err(super::map_store_failure)?;
        let session = put_route
            .load_stream_session(session_id)
            .map_err(Self::map_stream_upload_failure)?;
        let payload_crc64 = checksum::crc64::checksum(data);
        match session.target {
            StreamUploadTarget::PutObject => {
                let authorization = put_route
                    .authorize_stream_segment(session_id, segment_index, data.len() as u64)
                    .map_err(Self::map_stream_upload_failure)?;
                let write_encryption = self.resume_write_encryption(
                    authorization.encryption(),
                    None,
                    SseCustomerSegmentScope::object(),
                    false,
                )?;
                let storage_data = write_encryption.encrypt_standard_object_segment(
                    authorization.encryption_subject(),
                    segment_index,
                    data,
                )?;
                let session_id = authorization.session_id().clone();
                let outcome = put_route
                    .test_append_authorized_stream_segment_with_after_prepare(
                        authorization,
                        payload_crc64,
                        &storage_data,
                        || self.maybe_run_stream_append_prepare_hook(&session_id, segment_index),
                    )
                    .map_err(Self::map_stream_upload_failure)?;
                Self::emit_stream_segment_layout(
                    &outcome.target,
                    bucket.as_str(),
                    key.as_str(),
                    &session_id,
                    segment_index,
                    outcome.logical_size as usize,
                );
                Ok(())
            }
            StreamUploadTarget::UploadPart {
                upload_id,
                part_number,
            } => {
                let multipart_route = admission
                    .active_multipart_object_route(&bucket, &key)
                    .map_err(super::map_store_failure)?;
                let authorization = multipart_route
                    .authorize_stream_part_segment(
                        &upload_id,
                        session_id,
                        part_number,
                        segment_index,
                        data.len() as u64,
                    )
                    .map_err(|error| Self::map_upload_part_stream_error(&upload_id, error))?;
                let write_encryption = self
                    .load_stream_part_write_encryption_on_admitted_multipart_route(
                        &authorization,
                        part_number,
                        None,
                    )?;
                let storage_data = write_encryption.encrypt_multipart_part_segment(
                    &authorization,
                    segment_index,
                    data,
                )?;
                self.append_stream_segment_on_admitted_multipart_route(
                    &multipart_route,
                    &upload_id,
                    &bucket,
                    &key,
                    authorization,
                    super::StreamSegmentAppendPayload::new(&storage_data, payload_crc64),
                )
            }
        }
    }

    pub(super) fn ensure_write_encryption_supported(
        &self,
        encryption: &ObjectEncryption,
    ) -> Result<(), ServerError> {
        match encryption {
            ObjectEncryption::SseCustomer(_) => Ok(()),
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

    pub(super) fn validate_multipart_completion_replay_encryption(
        &self,
        encryption: &ObjectEncryption,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<(), ServerError> {
        match encryption {
            ObjectEncryption::SseCustomer(state) => {
                let Some(request) = sse_customer else {
                    return Ok(());
                };
                let validator =
                    self.sse_c_validator
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-C validator key is not configured".to_string(),
                        })?;
                validate_sse_customer_completion_replay(validator, state, request)
            }
            ObjectEncryption::SseS3(_) => {
                if sse_customer.is_some() {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-C headers may not be used for a non-SSE-C multipart upload"
                            .to_string(),
                    });
                }
                Ok(())
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

    pub(super) fn append_stream_segment_on_admitted_put_route(
        &self,
        route: &storage::ActivePutObjectRoute<'_>,
        bucket: &BucketName,
        key: &ObjectKey,
        authorization: storage::AuthorizedStreamPutSegment<'_, '_>,
        payload: super::StreamSegmentAppendPayload<'_>,
    ) -> Result<(), ServerError> {
        let session_id = authorization.session_id().clone();
        let segment_index = authorization.segment_index();
        let outcome = route
            .append_authorized_stream_segment(
                authorization,
                payload.payload_crc64,
                payload.storage_bytes,
            )
            .map_err(Self::map_stream_upload_failure)?;
        Self::emit_stream_segment_layout(
            &outcome.target,
            bucket.as_str(),
            key.as_str(),
            &session_id,
            segment_index,
            outcome.logical_size as usize,
        );
        Ok(())
    }

    pub(super) fn append_stream_segment_on_admitted_put_route_with_lease_maintenance(
        &self,
        route: &storage::ActivePutObjectRoute<'_>,
        bucket: &BucketName,
        key: &ObjectKey,
        authorization: storage::AuthorizedStreamPutSegment<'_, '_>,
        payload: super::StreamSegmentAppendPayload<'_>,
        maintain_lease: impl FnMut() -> Result<(), storage::StreamUploadFailure>,
    ) -> Result<(), ServerError> {
        let session_id = authorization.session_id().clone();
        let segment_index = authorization.segment_index();
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::append_stream_segment",
            "bucket={:?} key={:?} session_id={:?} segment_index={} bytes={}",
            bucket,
            key,
            &session_id,
            segment_index,
            payload.storage_bytes.len()
        );
        let outcome = route
            .append_authorized_stream_segment_with_lease_maintenance(
                authorization,
                payload.payload_crc64,
                payload.storage_bytes,
                maintain_lease,
            )
            .map_err(Self::map_stream_upload_failure)?;

        Self::emit_stream_segment_layout(
            &outcome.target,
            bucket.as_str(),
            key.as_str(),
            &session_id,
            segment_index,
            outcome.logical_size as usize,
        );

        Ok(())
    }

    pub(super) fn append_stream_segment_on_admitted_multipart_route(
        &self,
        route: &storage::ActiveMultipartObjectRoute<'_>,
        upload_id: &storage::UploadId,
        bucket: &BucketName,
        key: &ObjectKey,
        authorization: storage::AuthorizedStreamPartSegment<'_, '_>,
        payload: super::StreamSegmentAppendPayload<'_>,
    ) -> Result<(), ServerError> {
        let session_id = authorization.session_id().clone();
        let segment_index = authorization.segment_index();
        #[cfg(test)]
        let outcome = route
            .test_append_authorized_stream_part_segment_with_after_prepare(
                authorization,
                payload.payload_crc64,
                payload.storage_bytes,
                || self.maybe_run_stream_append_prepare_hook(&session_id, segment_index),
            )
            .map_err(|error| Self::map_upload_part_stream_error(upload_id, error))?;
        #[cfg(not(test))]
        let outcome = route
            .append_authorized_stream_part_segment(
                authorization,
                payload.payload_crc64,
                payload.storage_bytes,
            )
            .map_err(|error| Self::map_upload_part_stream_error(upload_id, error))?;
        Self::emit_stream_segment_layout(
            &outcome.target,
            bucket.as_str(),
            key.as_str(),
            &session_id,
            segment_index,
            outcome.logical_size as usize,
        );
        Ok(())
    }

    #[cfg(test)]
    pub fn abort_stream_put(
        &self,
        bucket: &str,
        key: &str,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_put_session(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            session_id,
        )
    }

    #[cfg(test)]
    pub fn scavenge_stale_sessions(&self, max_age_ms: u64) -> usize {
        storage::test_support::sweep_abandoned_stream_upload_sessions(
            &self.storage_node(),
            max_age_ms,
        )
    }
}
