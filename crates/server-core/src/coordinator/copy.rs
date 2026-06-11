use checksum::MultipartChecksumConfig;
use storage::{
    BucketName, ObjectEncryption, ObjectKey, ObjectLayout, SerializedTagSet, StoredObject,
};

#[cfg(test)]
use super::maybe_run_multipart_snapshot_hook;
use super::read_core::{
    segment_payloads_from_object_segments, snapshotted_multipart_parts_from_storage,
};
use super::{
    AuthorizedCopyObject, AuthorizedFinalizeStreamPutRequest, AuthorizedMultipartPartWrite,
    AuthorizedUploadPartCopy, AuthorizedWriteTags, BeginStreamPartResult, ChecksumClaim,
    Coordinator, CopyObjectRequest, CopyObjectResult, FinalizeStreamPartRequest, MetadataDirective,
    MultipartObjectRequest, ReadHandle, ReadObjectContext, StreamingChecksumAccumulator,
    TaggingDirective, UploadPartCopyRequest, UploadPartCopyResult, WriteEncryptionRequest,
    INTERNAL_SEGMENT_SIZE, MAX_OBJECT_SIZE, TRACE_TARGET,
};
use crate::conditional::check_copy_source_conditions;
use crate::error::ServerError;

impl Coordinator {
    fn copy_source_snapshot_to_read_handle(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        snapshot: storage::ObjectReadSnapshot,
        source_sse_customer: Option<&crate::sse::SseCustomerRequest>,
    ) -> Result<ReadHandle, ServerError> {
        let storage::ObjectReadSnapshot {
            stored,
            object_segments,
            multipart_parts,
            multipart_part_segments,
        } = snapshot;
        let src_record = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        match src_record.layout {
            ObjectLayout::MultipartManifest { .. } => {
                if src_record.size == 0 {
                    #[cfg(test)]
                    maybe_run_multipart_snapshot_hook(bucket.as_str(), key.as_str());
                    Ok(ReadHandle::from_buffered_bytes(Vec::new()))
                } else {
                    let obj_parts = snapshotted_multipart_parts_from_storage(
                        multipart_parts,
                        multipart_part_segments,
                        &src_record.encryption,
                    );
                    let body = ReadHandle::from_multipart(
                        self.read_runtime(),
                        bucket,
                        key,
                        src_record.generation_id,
                        obj_parts,
                        src_record.size as usize,
                        source_sse_customer.cloned(),
                    )?;
                    #[cfg(test)]
                    maybe_run_multipart_snapshot_hook(bucket.as_str(), key.as_str());
                    Ok(body)
                }
            }
            ObjectLayout::Standard => {
                if src_record.size == 0 {
                    Ok(ReadHandle::from_buffered_bytes(Vec::new()))
                } else {
                    ReadHandle::from_segments(
                        ReadObjectContext {
                            runtime: self.read_runtime(),
                            bucket,
                            key,
                            generation_id: src_record.generation_id,
                            sse_customer_request: source_sse_customer.cloned(),
                        },
                        segment_payloads_from_object_segments(
                            object_segments,
                            src_record.encryption.clone(),
                        ),
                        src_record.size as usize,
                        Some(src_record.etag.crc64()),
                    )
                }
            }
        }
    }

    fn copy_source_snapshot_to_range_read_handle(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        snapshot: storage::ObjectReadSnapshot,
        read_start: usize,
        read_end: usize,
        source_sse_customer: Option<&crate::sse::SseCustomerRequest>,
    ) -> Result<ReadHandle, ServerError> {
        let storage::ObjectReadSnapshot {
            stored,
            object_segments,
            multipart_parts,
            multipart_part_segments,
        } = snapshot;
        let src_record = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        match src_record.layout {
            ObjectLayout::MultipartManifest { .. } => {
                if src_record.size == 0 {
                    Ok(ReadHandle::from_buffered_bytes(Vec::new()))
                } else {
                    let obj_parts = snapshotted_multipart_parts_from_storage(
                        multipart_parts,
                        multipart_part_segments,
                        &src_record.encryption,
                    );
                    let body = ReadHandle::from_multipart_range(
                        self.read_runtime(),
                        bucket,
                        key,
                        src_record.generation_id,
                        obj_parts,
                        (read_start, read_end),
                        source_sse_customer.cloned(),
                    )?;
                    #[cfg(test)]
                    maybe_run_multipart_snapshot_hook(bucket.as_str(), key.as_str());
                    Ok(body)
                }
            }
            ObjectLayout::Standard => {
                if src_record.size == 0 {
                    Ok(ReadHandle::from_buffered_bytes(Vec::new()))
                } else {
                    ReadHandle::from_segments_range(
                        ReadObjectContext {
                            runtime: self.read_runtime(),
                            bucket,
                            key,
                            generation_id: src_record.generation_id,
                            sse_customer_request: source_sse_customer.cloned(),
                        },
                        segment_payloads_from_object_segments(
                            object_segments,
                            src_record.encryption.clone(),
                        ),
                        read_start,
                        read_end,
                    )
                }
            }
        }
    }

    /// Copy an object from one location to another.
    ///
    /// Supports conditional headers on both source and destination,
    /// and metadata directive (COPY preserves source metadata, REPLACE
    /// uses new headers).
    pub fn copy_object(&self, req: &CopyObjectRequest) -> Result<CopyObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::copy_object",
            "src_bucket={:?} src_key={:?} dst_bucket={:?} dst_key={:?}",
            req.source.bucket,
            req.source.key,
            req.destination.bucket.name(),
            req.destination.key()
        );
        let src_bucket = req.source.bucket.as_str();
        let src_key = req.source.key.as_str();
        let src_version_id = req.source.version_id;
        let dst_bucket = req.destination.bucket.name();
        let dst_key = req.destination.key();
        let src_cond = req.source.condition;
        let dst_cond = req.dst_condition;
        let directive = &req.directive;
        let source_sse_customer = req.source_sse_customer;
        let dst_explicit_sse_customer = self.prepare_sse_customer_write_context(
            req.destination_encryption.sse_customer_request(),
        )?;
        let dst_response_sse_customer = dst_explicit_sse_customer
            .as_ref()
            .map(|ctx| ctx.request().response_headers());
        let AuthorizedCopyObject {
            source: source_snapshot,
            destination: dst_authorized,
        } = self.authorize_copy_object(req)?;

        let (src_metadata, src_system_metadata, src_tags, mut source_body) = {
            let src_stored = source_snapshot.stored.clone();
            let src_record = match &src_stored {
                StoredObject::Live(r) => r,
                StoredObject::DeleteMarker(_) => {
                    return if src_version_id.is_some() {
                        Err(ServerError::InvalidRequest {
                            reason: "The source of a copy request may not specifically refer to a delete marker by version id.".to_string(),
                        })
                    } else {
                        Err(ServerError::ObjectNotFound {
                            bucket: src_bucket.to_string(),
                            key: src_key.to_string(),
                        })
                    };
                }
            };

            let src_etag = src_record.etag.format();
            check_copy_source_conditions(src_cond, &src_etag, src_record.last_modified)?;
            let _src_sse_customer =
                self.prepare_sse_customer_read_access(&src_record.encryption, source_sse_customer)?;

            let same_key_same_bucket = src_bucket == dst_bucket && src_key == dst_key;
            let encryption_attrs_unchanged =
                match (&src_record.encryption, req.destination_encryption) {
                    (ObjectEncryption::None, WriteEncryptionRequest::None) => true,
                    (ObjectEncryption::None, WriteEncryptionRequest::Managed(_)) => false,
                    (ObjectEncryption::None, WriteEncryptionRequest::SseCustomer(_)) => false,
                    (ObjectEncryption::SseCustomer(_), WriteEncryptionRequest::None) => false,
                    (ObjectEncryption::SseCustomer(_), WriteEncryptionRequest::Managed(_)) => false,
                    (
                        ObjectEncryption::SseCustomer(_),
                        WriteEncryptionRequest::SseCustomer(dst_request),
                    ) => source_sse_customer.is_some_and(|src_request| {
                        src_request.customer_key() == dst_request.customer_key()
                    }),
                    (ObjectEncryption::SseS3(_), WriteEncryptionRequest::None) => true,
                    (ObjectEncryption::SseS3(_), WriteEncryptionRequest::Managed(_)) => true,
                    (ObjectEncryption::SseS3(_), WriteEncryptionRequest::SseCustomer(_)) => false,
                };
            if matches!(
                directive,
                MetadataDirective::Copy | MetadataDirective::CopyExplicit
            ) && req.website_redirect_location.is_none()
                && same_key_same_bucket
                && encryption_attrs_unchanged
            {
                return Err(ServerError::InvalidRequest {
                    reason: "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.".to_string(),
                });
            }

            if src_record.size > MAX_OBJECT_SIZE {
                return Err(ServerError::ObjectTooLarge {
                    size: src_record.size,
                    max: MAX_OBJECT_SIZE,
                });
            }

            let body = self.copy_source_snapshot_to_read_handle(
                &req.source.bucket,
                &req.source.key,
                source_snapshot,
                source_sse_customer,
            )?;

            let src_metadata = Self::deserialize_user_metadata(src_record.metadata_blob.as_ref())?;
            let src_system_metadata = self.deserialize_visible_system_metadata(
                src_record.system_metadata_blob.as_ref(),
                &src_record.encryption,
                source_sse_customer,
            )?;

            (
                src_metadata,
                src_system_metadata,
                src_record.tags.clone(),
                body,
            )
        };

        let metadata_blob = match directive {
            MetadataDirective::Copy | MetadataDirective::CopyExplicit => src_metadata,
            MetadataDirective::Replace {
                metadata: new_metadata,
                ..
            } => (*new_metadata).clone(),
        };
        let mut system_metadata = match directive {
            MetadataDirective::Copy | MetadataDirective::CopyExplicit => src_system_metadata,
            MetadataDirective::Replace {
                system_metadata: new_system_metadata,
                ..
            } => (*new_system_metadata).clone(),
        };
        if matches!(
            directive,
            MetadataDirective::Copy | MetadataDirective::CopyExplicit
        ) {
            system_metadata.clear_website_redirect_location();
            if let Some(redirect) = &req.website_redirect_location {
                system_metadata.set_website_redirect_location(redirect.clone());
            }
        }
        let committed_tags = match &req.tagging {
            TaggingDirective::Copy => src_tags,
            TaggingDirective::Replace(tags) => tags.map(SerializedTagSet::from),
        };
        let mut replacement_checksum = match directive {
            MetadataDirective::Replace {
                checksum_algorithm: Some(algo),
                ..
            } => Some(StreamingChecksumAccumulator::new(*algo)),
            _ => None,
        };
        let session_id = self.create_stream_put_session_for_authorized_write(&dst_authorized)?;
        let dst_write_encryption = &dst_authorized.write_encryption;
        let not_found = |e: ServerError| match e {
            ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                bucket: src_bucket.to_string(),
                key: src_key.to_string(),
            },
            other => other,
        };
        let copy_result = (|| {
            let mut crc64 = checksum::crc64::Hasher::new();
            let mut total_size = 0u64;
            let mut segment_index = 0u32;

            while let Some(chunk) = source_body
                .next_chunk(INTERNAL_SEGMENT_SIZE)
                .map_err(not_found)?
            {
                total_size = total_size.checked_add(chunk.len() as u64).ok_or_else(|| {
                    ServerError::InternalError {
                        reason: "copy size overflow".to_string(),
                    }
                })?;
                crc64.update(&chunk);
                if let Some(checksum) = replacement_checksum.as_mut() {
                    checksum.update(&chunk);
                }
                let storage_chunk = dst_write_encryption.encrypt_segment(segment_index, &chunk)?;
                self.append_stream_segment_for(
                    req.destination.bucket.name_typed(),
                    req.destination.key_typed(),
                    &session_id,
                    segment_index,
                    &storage_chunk,
                )?;
                segment_index =
                    segment_index
                        .checked_add(1)
                        .ok_or_else(|| ServerError::InternalError {
                            reason: "too many copy segments".to_string(),
                        })?;
            }

            if let Some(checksum) = replacement_checksum.take() {
                use base64::Engine;

                let algo = checksum.algorithm();
                let finalized = checksum.finalize();
                let b64 = base64::engine::general_purpose::STANDARD.encode(finalized.bytes());
                system_metadata.set_checksum(algo, None, b64);
            }

            let put_result = self.finalize_stream_put_with_authorized_write_tags(
                &AuthorizedFinalizeStreamPutRequest {
                    session_id: &session_id,
                    crc64: crc64.finalize(),
                    total_size,
                    metadata_blob: &metadata_blob,
                    system_metadata: &system_metadata,
                    write_encryption: dst_write_encryption.as_ref(),
                    cond: dst_cond,
                },
                &dst_authorized,
                AuthorizedWriteTags::TrustedDerived(committed_tags.as_deref()),
            )?;

            Ok(CopyObjectResult {
                etag: put_result.etag,
                last_modified: put_result.last_modified,
                system_metadata: put_result.system_metadata.clone(),
                version_id: put_result.version_id,
                managed_encryption: put_result.managed_encryption,
                sse_customer: dst_response_sse_customer,
                lifecycle_expiration: put_result.lifecycle_expiration,
            })
        })();
        if copy_result.is_err() {
            let _ = self.abort_stream_put_for_cleanup(
                req.destination.bucket.name_typed(),
                req.destination.key_typed(),
                &session_id,
            );
        }
        copy_result
    }

    /// Copy a byte range from an existing object as a multipart upload part.
    pub fn upload_part_copy(
        &self,
        req: &UploadPartCopyRequest,
    ) -> Result<UploadPartCopyResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::upload_part_copy",
            "src_bucket={:?} src_key={:?} dst_bucket={:?} dst_key={:?} upload_id={:?} part_number={}",
            req.source.bucket,
            req.source.key,
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.part_number
        );
        Self::validate_upload_part_number(req.part_number)?;
        let src_bucket = req.source.bucket.as_str();
        let src_key = req.source.key.as_str();
        let src_version_id = req.source.version_id;
        let src_cond = req.source.condition;
        let copy_source_range = req.copy_source_range;
        let source_sse_customer = req.source_sse_customer;
        let AuthorizedUploadPartCopy {
            source,
            destination,
        } = self.authorize_upload_part_copy(req)?;
        let not_found = |e: ServerError| match e {
            ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                bucket: src_bucket.to_string(),
                key: src_key.to_string(),
            },
            other => other,
        };

        let mut source_body = {
            let src_stored = source.stored.clone();

            let src_record = match &src_stored {
                StoredObject::Live(r) => r,
                StoredObject::DeleteMarker(_) => {
                    return if src_version_id.is_some() {
                        Err(ServerError::InvalidRequest {
                            reason: "The source of a copy request may not specifically refer to a delete marker by version id.".to_string(),
                        })
                    } else {
                        Err(ServerError::ObjectNotFound {
                            bucket: src_bucket.to_string(),
                            key: src_key.to_string(),
                        })
                    };
                }
            };

            let src_etag = src_record.etag.format();
            check_copy_source_conditions(src_cond, &src_etag, src_record.last_modified)?;
            let _src_sse_customer =
                self.prepare_sse_customer_read_access(&src_record.encryption, source_sse_customer)?;

            let source_size = src_record.size;

            if let Some((start, end)) = copy_source_range {
                if end >= source_size {
                    return Err(ServerError::UploadPartCopyInvalidRange {
                        range_header: format!("bytes={start}-{end}"),
                        source_size,
                    });
                }
            }

            let (read_start, read_end) =
                copy_source_range.unwrap_or((0, source_size.saturating_sub(1)));
            let copy_size = if source_size == 0 {
                0
            } else {
                read_end - read_start + 1
            };
            if copy_size > MAX_OBJECT_SIZE {
                return Err(ServerError::ObjectTooLarge {
                    size: copy_size,
                    max: MAX_OBJECT_SIZE,
                });
            }

            self.copy_source_snapshot_to_range_read_handle(
                &req.source.bucket,
                &req.source.key,
                source,
                read_start as usize,
                read_end as usize,
                source_sse_customer,
            )?
        };

        let AuthorizedMultipartPartWrite {
            bucket,
            key,
            upload_id,
            part_number,
            upload,
            sse_customer,
        } = destination;
        let session_id = Self::random_session_id("failed to generate session ID")?;
        let session_id = self
            .storage_node
            .create_upload_part_stream_session(&upload, part_number, &session_id)
            .map_err(Self::map_object_pg_action_error)?;
        let session = BeginStreamPartResult {
            session_id,
            checksum_algorithm: upload.checksum.map(MultipartChecksumConfig::algorithm),
            sse_customer,
        };
        let session_id = &session.session_id;
        let sse_customer_headers = session
            .sse_customer
            .as_ref()
            .map(|ctx| ctx.request().response_headers());
        let result = (|| {
            let write_encryption = self.load_stream_part_write_encryption(
                &bucket,
                &key,
                session_id,
                part_number,
                req.sse_customer,
            )?;
            let mut crc64 = checksum::crc64::Hasher::new();
            let mut total_size = 0u64;
            let mut segment_index = 0u32;
            let mut computed_checksum = session
                .checksum_algorithm
                .map(StreamingChecksumAccumulator::new);

            while let Some(chunk) = source_body
                .next_chunk(INTERNAL_SEGMENT_SIZE)
                .map_err(not_found)?
            {
                total_size = total_size.checked_add(chunk.len() as u64).ok_or_else(|| {
                    ServerError::InternalError {
                        reason: "upload part copy size overflow".to_string(),
                    }
                })?;
                crc64.update(&chunk);
                if let Some(checksum) = computed_checksum.as_mut() {
                    checksum.update(&chunk);
                }
                let storage_chunk = write_encryption.encrypt_segment(segment_index, &chunk)?;
                self.append_stream_segment_for(
                    &bucket,
                    &key,
                    session_id,
                    segment_index,
                    &storage_chunk,
                )?;
                segment_index =
                    segment_index
                        .checked_add(1)
                        .ok_or_else(|| ServerError::InternalError {
                            reason: "too many upload part copy segments".to_string(),
                        })?;
            }

            let computed_checksum = computed_checksum.map(StreamingChecksumAccumulator::finalize);
            let claimed_checksum = computed_checksum
                .as_ref()
                .map(|checksum| ChecksumClaim::from_raw(checksum.clone()));

            self.finalize_stream_part(FinalizeStreamPartRequest {
                upload: MultipartObjectRequest::new_typed(
                    bucket.clone(),
                    key.clone(),
                    upload_id.clone(),
                    req.upload.requester().clone(),
                    req.expected_bucket_owner(),
                ),
                session_id,
                part_number,
                crc64: crc64.finalize(),
                total_size,
                claimed_checksum: claimed_checksum.as_ref(),
                computed_checksum,
            })
        })();
        if result.is_err() {
            let _ = self.abort_stream_put_for_cleanup(&bucket, &key, session_id);
        }
        let inner = result?;
        Ok(UploadPartCopyResult {
            etag: inner.etag,
            last_modified: inner.last_modified,
            checksum: inner.checksum,
            managed_encryption: inner.managed_encryption,
            sse_customer: sse_customer_headers,
        })
    }
}
