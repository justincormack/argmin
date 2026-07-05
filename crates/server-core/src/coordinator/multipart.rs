use checksum::{ChecksumAlgorithm, ChecksumBytes, ChecksumType, MultipartChecksumConfig};
use storage::{
    BucketName, CreateMultipartUploadOutcome, CreateMultipartUploadReq, FinalizeStreamPartOutcome,
    GenerationId, MultipartPartRecord, MultipartPartSegmentRecord, ObjectKey,
    PreparedStreamPartCommit, SerializedMetadataBlob, SerializedSystemMetadataBlob,
    SerializedTagSet, SessionId, StreamUploadPartSnapshot, StreamUploadState, StreamUploadTarget,
    UploadId, UploadState, UPLOAD_ID_ALPHABET, UPLOAD_ID_LEN,
};

use super::authz_results::{
    AuthorizedAbortMultipartUpload, AuthorizedCompleteMultipartUpload,
    AuthorizedCreateMultipartUpload, AuthorizedListMultipartUploads, AuthorizedListParts,
};
use super::bucket_handles::{BucketHandleLoader, BucketHandleRequest};
use super::object_state::StaleObjectPayload;
use super::request_types::{
    AppendStreamPartRequest, BeginStreamPartRequest, CompleteMultipartUploadRequest,
    CreateMultipartUploadRequest, FinalizeStreamPartRequest, ListMultipartUploadsRequest,
    ListPartsRequest, MultipartObjectRequest,
};
use super::response_types::{
    BeginStreamPartResult, CompleteMultipartUploadResult, CreateMultipartUploadResult,
    ListMultipartUploadsResult, ListPartsResult, MultipartUploadEntry, PartEntry, UploadPartResult,
};
use super::{
    compute_checksum, optional_list_object_key, Coordinator,
    COMPLETED_MULTIPART_UPLOADS_PER_BUCKET_LIMIT, MAX_LIST_RECORDS, MAX_PARTS, MIN_PART_SIZE,
    TRACE_TARGET,
};
#[cfg(test)]
use super::{
    maybe_run_multipart_complete_pre_commit_hook, maybe_run_multipart_complete_snapshot_hook,
};
use crate::checksum_claim::ChecksumClaim;
use crate::conditional::{check_write_conditions, WriteCondition};
use crate::error::ServerError;
use crate::etag::{compute_multipart_etag, crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::system_metadata::SystemMetadata;

const COMPLETE_MULTIPART_STALE_COMMIT_RETRIES: usize = 1;

fn complete_multipart_part_checksum(
    part: &MultipartPartRecord,
    checksum_type: ChecksumType,
) -> Result<&ChecksumBytes, ServerError> {
    part.checksum
        .as_ref()
        .ok_or_else(|| ServerError::InvalidRequest {
            reason: format!(
                "{} checksum requires all parts to have checksums",
                checksum_type.as_str()
            ),
        })
}

fn complete_multipart_checksum_value(
    config: MultipartChecksumConfig,
    part_records: &[MultipartPartRecord],
) -> Result<String, ServerError> {
    use base64::Engine;

    let b64 = base64::engine::general_purpose::STANDARD;
    let algorithm = config.algorithm();
    match config.checksum_type() {
        ChecksumType::Composite => {
            let mut concat = Vec::new();
            for part in part_records {
                concat.extend_from_slice(
                    complete_multipart_part_checksum(part, ChecksumType::Composite)?.as_slice(),
                );
            }
            let hash = compute_checksum(algorithm, &concat);
            Ok(format!(
                "{}-{}",
                b64.encode(hash.bytes()),
                part_records.len()
            ))
        }
        ChecksumType::FullObject => match algorithm {
            ChecksumAlgorithm::Crc32 => {
                let mut combined: u32 = 0;
                for part in part_records {
                    let bytes = complete_multipart_part_checksum(part, ChecksumType::FullObject)?;
                    let part_crc =
                        u32::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                            ServerError::InvalidRequest {
                                reason: "invalid CRC32 checksum length".to_string(),
                            }
                        })?);
                    combined = checksum::crc32::combine(combined, part_crc, part.size);
                }
                Ok(b64.encode(combined.to_be_bytes()))
            }
            ChecksumAlgorithm::Crc32c => {
                let mut combined: u32 = 0;
                for part in part_records {
                    let bytes = complete_multipart_part_checksum(part, ChecksumType::FullObject)?;
                    let part_crc =
                        u32::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                            ServerError::InvalidRequest {
                                reason: "invalid CRC32C checksum length".to_string(),
                            }
                        })?);
                    combined = checksum::crc32c::combine(combined, part_crc, part.size);
                }
                Ok(b64.encode(combined.to_be_bytes()))
            }
            ChecksumAlgorithm::Crc64nvme => {
                let mut combined: u64 = 0;
                for part in part_records {
                    let part_crc = if let Some(bytes) = part.checksum.as_ref() {
                        u64::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                            ServerError::InvalidRequest {
                                reason: "invalid CRC64NVME checksum length".to_string(),
                            }
                        })?)
                    } else {
                        part.payload_crc64
                    };
                    combined = checksum::crc64::combine(combined, part_crc, part.size);
                }
                Ok(b64.encode(combined.to_be_bytes()))
            }
            ChecksumAlgorithm::Sha1
            | ChecksumAlgorithm::Sha256
            | ChecksumAlgorithm::Md5
            | ChecksumAlgorithm::XxHash64
            | ChecksumAlgorithm::XxHash3
            | ChecksumAlgorithm::XxHash128
            | ChecksumAlgorithm::Sha512 => {
                unreachable!("MultipartChecksumConfig rejects non-CRC FULL_OBJECT checksums")
            }
        },
    }
}

impl Coordinator {
    pub(super) fn map_object_pg_action_error(error: storage::ObjectPgActionError) -> ServerError {
        match error {
            storage::ObjectPgActionError::Store(
                storage::StoreError::MetadataCommandLogConflict { .. }
                | storage::StoreError::MetadataCommandLogGap { .. }
                | storage::StoreError::MetadataCommandPendingConflict { .. },
            ) => ServerError::OperationAborted,
            storage::ObjectPgActionError::Metadata(ref error)
                if super::metadata_error_is_command_contention(error) =>
            {
                ServerError::OperationAborted
            }
            storage::ObjectPgActionError::Store(error) => super::map_store_error(error),
            storage::ObjectPgActionError::InvalidRequest { reason } => {
                ServerError::InvalidRequest { reason }
            }
            storage::ObjectPgActionError::StaleObjectReadSubject => ServerError::InternalError {
                reason: "stale object read subject escaped storage retry loop".to_string(),
            },
            storage::ObjectPgActionError::StaleDirectPutCommitSnapshot => {
                ServerError::InternalError {
                    reason: "stale direct PUT commit snapshot escaped storage retry loop"
                        .to_string(),
                }
            }
            storage::ObjectPgActionError::StaleStreamFinalizeSnapshot => {
                ServerError::InternalError {
                    reason: "stale stream finalize snapshot escaped storage retry loop".to_string(),
                }
            }
            storage::ObjectPgActionError::StaleMultipartCompletionSnapshot => {
                ServerError::InternalError {
                    reason: "stale multipart completion snapshot escaped storage retry loop"
                        .to_string(),
                }
            }
            storage::ObjectPgActionError::Metadata(error) => match error {
                storage::MetadataError::NoSuchUpload { upload_id } => {
                    ServerError::NoSuchUpload { upload_id }
                }
                storage::MetadataError::StreamSegmentConflict { .. } => {
                    ServerError::InvalidRequest {
                        reason: "stream segment index already exists".to_string(),
                    }
                }
                other => ServerError::Metadata(other),
            },
        }
    }

    fn map_upload_part_stream_error(
        upload_id: &UploadId,
        error: storage::ObjectPgActionError,
    ) -> ServerError {
        match error {
            storage::ObjectPgActionError::Metadata(
                storage::MetadataError::StreamSessionNotFound { .. }
                | storage::MetadataError::StreamSessionNotInProgress { .. },
            ) => ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            },
            other => Self::map_object_pg_action_error(other),
        }
    }

    #[cfg(test)]
    pub fn append_stream_part_data(
        &self,
        req: &AppendStreamPartRequest<'_>,
    ) -> Result<(), ServerError> {
        self.append_stream_part_data_with_storage_node(&self.storage_node(), req)
    }

    pub fn append_stream_part_data_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: &AppendStreamPartRequest<'_>,
    ) -> Result<(), ServerError> {
        let write_encryption = self
            .load_stream_part_write_encryption_with_storage_node(
                storage_node,
                &req.bucket,
                &req.key,
                req.session_id,
                req.part_number,
                req.sse_customer,
            )
            .map_err(|error| match error {
                ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
                | ServerError::Metadata(storage::MetadataError::StreamSessionNotInProgress {
                    ..
                }) => ServerError::NoSuchUpload {
                    upload_id: req.upload_id.to_string(),
                },
                other => other,
            })?;
        let payload_crc64 = checksum::crc64::checksum(req.data);
        let storage_data = write_encryption.encrypt_segment(req.segment_index, req.data)?;
        self.append_stream_segment_for_storage_node(
            storage_node,
            &req.bucket,
            &req.key,
            req.session_id,
            req.segment_index,
            super::StreamSegmentAppendPayload::maybe_encrypted(
                &storage_data,
                payload_crc64,
                matches!(
                    write_encryption.as_ref(),
                    super::ActiveWriteEncryptionRef::None
                ),
            ),
        )
        .map_err(|error| match error {
            ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
            | ServerError::Metadata(storage::MetadataError::StreamSessionNotInProgress {
                ..
            }) => ServerError::NoSuchUpload {
                upload_id: req.upload_id.to_string(),
            },
            other => other,
        })
    }

    /// Begin a streaming UploadPart session.
    ///
    /// Creates a `StreamUploadKind::UploadPart` session tied to the given
    /// multipart upload. Validates that the upload exists and is InProgress.
    #[cfg(test)]
    pub fn begin_stream_part(
        &self,
        req: &BeginStreamPartRequest<'_>,
    ) -> Result<BeginStreamPartResult, ServerError> {
        self.begin_stream_part_with_storage_node(&self.storage_node(), req)
    }

    pub fn begin_stream_part_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: &BeginStreamPartRequest<'_>,
    ) -> Result<BeginStreamPartResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::begin_stream_part",
            "bucket={:?} key={:?} upload_id={:?} part_number={}",
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.part_number
        );
        Self::validate_upload_part_number(req.part_number)?;
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        let session_id = Self::random_session_id("failed to generate session ID")?;
        self.with_bucket_write_handle_for_command_with_storage_node(
            storage_node,
            &req.upload,
            request,
            |bucket_handle, proof| {
            let mut proof_transferred_to_command = false;
            let result = (|| {
                #[cfg(test)]
                if self.should_probe_begin_stream_part_session(req.upload.bucket_name()) {
                    let object_pg_ready = storage_node
                        .try_probe_object_pg_available(
                            req.upload.bucket_name_typed(),
                            req.upload.key_typed(),
                        )
                        .map_err(Coordinator::map_object_pg_action_error)?;
                    if !object_pg_ready {
                        return Err(ServerError::InternalError {
                            reason:
                                "test probe: object pg still locked before begin_stream_part session"
                                    .to_string(),
                        });
                    }
                }
                proof_transferred_to_command = true;
                storage_node
                    .begin_upload_part_stream_session(
                        storage::BeginUploadPartStreamSessionReq {
                            bucket: req.upload.bucket_name_typed().clone(),
                            key: req.upload.key_typed().clone(),
                            upload_id: req.upload.upload_id().clone(),
                            part_number: req.part_number,
                            session_id: session_id.clone(),
                            bucket_write_reservation: proof.clone(),
                        },
                        |upload| {
                            let authorized =
                                self.authorize_begin_stream_part_with_upload(
                                    req,
                                    &bucket_handle,
                                    upload,
                                )?;
                            let checksum_algorithm = authorized
                                .upload
                                .checksum
                                .map(MultipartChecksumConfig::algorithm);
                            let authorized_upload = authorized.upload;
                            Ok::<_, ServerError>((
                                authorized_upload,
                                BeginStreamPartResult {
                                    session_id: session_id.clone(),
                                    checksum_algorithm,
                                    sse_customer: authorized.sse_customer,
                                },
                            ))
                        },
                    )
                    .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
            })();
            if proof_transferred_to_command {
                storage::BucketWriteSnapshotAction::transferred_to_command(result)
            } else {
                storage::BucketWriteSnapshotAction::release(result)
            }
            },
        )
    }

    pub(super) fn validate_upload_part_number(part_number: u32) -> Result<(), ServerError> {
        if !(1..=MAX_PARTS as u32).contains(&part_number) {
            return Err(ServerError::InvalidArgument {
                reason: format!("part number must be between 1 and {MAX_PARTS}, got {part_number}"),
            });
        }
        Ok(())
    }

    /// Initiate a multipart upload.
    ///
    /// Generates a random upload ID, serializes the metadata blob, and
    /// inserts a new multipart upload record in the metadata PG for (bucket, key).
    pub fn create_multipart_upload(
        &self,
        req: &CreateMultipartUploadRequest,
    ) -> Result<CreateMultipartUploadResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::create_multipart_upload",
            "bucket={:?} key={:?}",
            req.object.bucket_name(),
            req.object.key
        );
        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; UPLOAD_ID_LEN];
        ring::rand::SecureRandom::fill(&rng, &mut id_bytes).map_err(|_| {
            ServerError::InternalError {
                reason: "failed to generate upload ID".to_string(),
            }
        })?;
        let upload_id: String = id_bytes
            .iter()
            .map(|byte| UPLOAD_ID_ALPHABET[(byte & 0x3f) as usize] as char)
            .collect();
        let typed_upload_id =
            UploadId::try_from(upload_id).expect("generated multipart upload ID is valid");

        let metadata_blob = req.metadata.serialize()?;
        let system_metadata_blob = req.system_metadata.serialize()?;
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        let expected_bucket_owner = req.object.expected_bucket_owner();
        let CreateMultipartUploadOutcome {
            value: authorized,
            initiated_at,
        } = self
            .storage_node()
            .create_multipart_upload(
                req.object.bucket.name_typed(),
                req.object.key_typed(),
                request.resolve_to_storage_request(),
                |snapshot, existing_object| {
                    let bucket_handle = self
                        .bucket_handle_loader()
                        .load_bucket_handle_from_snapshot(
                            snapshot,
                            expected_bucket_owner,
                            request,
                        )?;
                    #[cfg(test)]
                    self.maybe_run_bucket_write_handle_loaded_hook(
                        req.object.bucket.name_typed().as_str(),
                    );
                    let authorized = self.authorize_create_multipart_upload_with_existing_object(
                        req,
                        &bucket_handle,
                        existing_object.as_ref(),
                    )?;
                    let create = CreateMultipartUploadReq {
                        upload_id: typed_upload_id.clone(),
                        bucket: authorized.bucket.clone(),
                        key: authorized.key.clone(),
                        tags: authorized.tags.as_deref().map(SerializedTagSet::from),
                        metadata_blob: SerializedMetadataBlob::from(metadata_blob.clone()),
                        system_metadata_blob: SerializedSystemMetadataBlob::from(
                            system_metadata_blob.clone(),
                        ),
                        initiator: authorized.initiator.clone(),
                        owner: authorized.owner.clone(),
                        acl_grants: authorized.acl_grants.clone(),
                        public_read: authorized.public_read,
                        object_lock: authorized.object_lock,
                        checksum: authorized.checksum,
                        encryption: authorized.write_encryption.object_encryption(),
                    };
                    Ok::<_, ServerError>((authorized, create))
                },
            )
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)??;
        let AuthorizedCreateMultipartUpload {
            bucket_info,
            key,
            write_encryption,
            ..
        } = authorized;
        let lifecycle_abort =
            self.multipart_lifecycle_abort_headers(&bucket_info, key.as_str(), initiated_at)?;

        Ok(CreateMultipartUploadResult {
            upload_id: typed_upload_id,
            managed_encryption: write_encryption
                .object_encryption()
                .managed_encryption_algorithm(),
            lifecycle_abort,
        })
    }

    /// Complete a multipart upload, committing a manifest object.
    ///
    /// Validates the part list, checks ETags and sizes, writes the final
    /// object metadata row with `MultipartManifest` layout, commits
    /// manifest rows into `object_parts`, and deletes in-progress state.
    pub fn complete_multipart_upload(
        &self,
        req: &CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadResult, ServerError> {
        let storage_node = self.storage_node();
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::complete_multipart_upload",
            "bucket={:?} key={:?} upload_id={:?} parts={}",
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.parts.len()
        );
        let mut stale_commit_retries = 0usize;
        'retry_stale_commit_snapshot: loop {
            let AuthorizedCompleteMultipartUpload {
                bucket_info,
                bucket,
                key,
                upload_id,
                upload,
                multipart_write_encryption,
            } = self.authorize_complete_multipart_upload_with_storage_node(&storage_node, req)?;
            let parts = req.parts;
            let claimed_checksum = req.claimed_checksum;
            let expected_object_size = req.expected_object_size;
            if parts.is_empty() {
                return Err(ServerError::InvalidRequest {
                    reason: "part list must not be empty".to_string(),
                });
            }
            if parts.len() > MAX_PARTS {
                return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "part list exceeds maximum of {MAX_PARTS} parts, got {}",
                        parts.len()
                    ),
                });
            }
            for window in parts.windows(2) {
                if window[0].part_number >= window[1].part_number {
                    return Err(ServerError::InvalidPartOrder);
                }
            }

            let requested_part_numbers: Vec<u32> =
                parts.iter().map(|part| part.part_number).collect();
            #[cfg(test)]
            maybe_run_multipart_complete_snapshot_hook(bucket.as_str(), key.as_str());
            let completion_snapshot = storage_node
                .load_multipart_completion_snapshot(&upload, &requested_part_numbers)
                .map_err(|error| match error {
                    storage::ObjectPgActionError::Metadata(
                        storage::MetadataError::PartNotFound { part_number, .. },
                    ) => ServerError::InvalidPart { part_number },
                    other => Coordinator::map_object_pg_action_error(other),
                })?;

            if !req.cond.is_empty() {
                let existing_etag = completion_snapshot.existing_etag.as_deref();
                if matches!(req.cond, WriteCondition::IfMatch(_)) && existing_etag.is_none() {
                    return Err(ServerError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    });
                }
                check_write_conditions(req.cond, existing_etag)?;
            }

            let checksum_config = upload.checksum;
            let stores_unconfigured_crc64nvme_checksum = checksum_config.is_none()
                && claimed_checksum.is_some_and(|claimed| {
                    claimed
                        .algorithm()
                        .stores_unconfigured_complete_multipart_header()
                });
            let effective_checksum_config = if stores_unconfigured_crc64nvme_checksum {
                Some(MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Crc64nvme,
                    Some(ChecksumType::FullObject),
                )?)
            } else {
                checksum_config
            };

            let mut part_records: Vec<MultipartPartRecord> =
                Vec::with_capacity(completion_snapshot.part_records.len());
            for (cp, part) in parts.iter().zip(completion_snapshot.part_records) {
                if let Some(config) = checksum_config {
                    if config.checksum_type() == ChecksumType::Composite && cp.checksum.is_none() {
                        return Err(ServerError::CompleteMultipartMissingPartChecksum {
                            algorithm: config.algorithm().as_str().to_ascii_lowercase(),
                            part_number: cp.part_number,
                        });
                    }
                }

                let stored_etag = etag_bytes_to_crc64(&part.etag)
                    .map(format_etag)
                    .unwrap_or_default();
                if stored_etag != cp.etag {
                    return Err(ServerError::InvalidPart {
                        part_number: cp.part_number,
                    });
                }

                if let Some(ref claim) = cp.checksum {
                    if let Some(config) = checksum_config {
                        let upload_algo = config.algorithm();
                        if claim.algorithm() != upload_algo {
                            return Err(ServerError::InvalidRequest {
                                reason: format!(
                                    "checksum element type {} does not match upload algorithm {}",
                                    claim.algorithm().as_str(),
                                    upload_algo.as_str()
                                ),
                            });
                        }
                    }
                    match &part.checksum {
                        Some(stored_bytes) => {
                            if claim.expected_bytes() != stored_bytes.as_slice() {
                                return Err(ServerError::InvalidPart {
                                    part_number: cp.part_number,
                                });
                            }
                        }
                        None => {
                            return Err(ServerError::InvalidPart {
                                part_number: cp.part_number,
                            });
                        }
                    }
                }

                part_records.push(part);
            }

            if part_records.len() > 1 {
                for part in &part_records[..part_records.len() - 1] {
                    if part.size < MIN_PART_SIZE {
                        return Err(ServerError::EntityTooSmall {
                            part_number: part.part_number,
                            size: part.size,
                            min: MIN_PART_SIZE,
                        });
                    }
                }
            }

            let part_etags: Vec<&[u8]> = part_records.iter().map(|p| p.etag.as_slice()).collect();
            let (etag_bytes_vec, etag_str) = compute_multipart_etag(&part_etags);
            let mut etag_crc64 = [0u8; 8];
            etag_crc64.copy_from_slice(&etag_bytes_vec);

            let total_size: u64 = part_records.iter().map(|p| p.size).sum();
            if let Some(expected) = expected_object_size {
                if expected != total_size {
                    return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "x-amz-mp-object-size {expected} does not match actual object size {total_size}"
                    ),
                });
                }
            }
            #[cfg(feature = "deep-tracing")]
            if let Some(trace) = observability::current_context() {
                let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "complete_multipart_layout",
                    Some(format_args!(
                        "bucket={:?} key={:?} upload_id={:?} parts={} total_size={}",
                        bucket,
                        key,
                        upload_id,
                        part_records.len(),
                        total_size
                    )),
                );
                let mut object_offset_start = 0u64;
                for (part_order, (requested_part, stored_part)) in
                    parts.iter().zip(part_records.iter()).enumerate()
                {
                    let object_offset_len = stored_part.size;
                    let object_offset_end_exclusive = object_offset_start + object_offset_len;
                    let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "complete_multipart_part_layout",
                    Some(format_args!(
                        "bucket={:?} key={:?} upload_id={:?} part_order={} part_number={} part_size={} object_offset_start={} object_offset_len={} object_offset_end_exclusive={} etag={}",
                        bucket,
                        key,
                        upload_id,
                        part_order,
                        requested_part.part_number,
                        stored_part.size,
                        object_offset_start,
                        object_offset_len,
                        object_offset_end_exclusive,
                        requested_part.etag
                    )),
                );
                    object_offset_start = object_offset_end_exclusive;
                }
            }

            let checksum_value = effective_checksum_config
                .map(|config| complete_multipart_checksum_value(config, &part_records))
                .transpose()?;

            if let Some(claimed) = claimed_checksum {
                match checksum_config.map(MultipartChecksumConfig::algorithm) {
                    Some(upload_algo) if claimed.algorithm() != upload_algo => {
                        return Err(ServerError::InvalidRequest {
                            reason: format!(
                                "checksum header algorithm {} does not match upload algorithm {}",
                                claimed.algorithm().as_str(),
                                upload_algo.as_str()
                            ),
                        });
                    }
                    None if claimed
                        .algorithm()
                        .accepts_unconfigured_complete_multipart_header() => {}
                    None if claimed
                        .algorithm()
                        .stores_unconfigured_complete_multipart_header() => {}
                    None => {
                        return Err(ServerError::InvalidRequestHostId {
                            reason: format!(
                                "Checksum Type mismatch occurred, expected checksum Type: null, actual checksum Type: {}",
                                claimed.algorithm().as_str().to_ascii_lowercase(),
                            ),
                        });
                    }
                    _ => {}
                }
                claimed.validate_complete_multipart_header_value()?;
                if !stores_unconfigured_crc64nvme_checksum {
                    if let Some(ref computed) = checksum_value {
                        if computed != claimed.encoded_value() {
                            return Err(ServerError::ChecksumDigestMismatch {
                                algorithm: claimed.algorithm().as_str().to_ascii_lowercase(),
                            });
                        }
                    }
                }
            }

            let mut system_metadata =
                SystemMetadata::deserialize(upload.system_metadata_blob.as_slice())?;
            if let (Some(config), Some(ref val)) = (effective_checksum_config, &checksum_value) {
                system_metadata.set_checksum(
                    config.algorithm(),
                    Some(config.checksum_type()),
                    val.clone(),
                );
            }
            self.ensure_write_encryption_supported(&upload.encryption)?;
            let (system_metadata_bytes, final_encryption) = Self::prepare_stored_system_metadata(
                &system_metadata,
                &multipart_write_encryption,
            )?;
            let managed_encryption = final_encryption.managed_encryption_algorithm();

            #[cfg(test)]
            maybe_run_multipart_complete_pre_commit_hook(bucket.as_str(), key.as_str());

            let completion_outcome = match storage_node.complete_multipart_upload_commit_serialized(
                storage::CompleteMultipartCommitRequest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id: upload_id.clone(),
                    versioning: bucket_info.versioning,
                    owner: upload.owner.clone(),
                    acl_grants: upload.acl_grants.clone(),
                    public_read: upload.public_read,
                    generation_id: upload.object_generation_id,
                    size: total_size,
                    etag_crc64,
                    tags: upload.tags.clone(),
                    metadata_blob: Some(upload.metadata_blob.clone()),
                    system_metadata_blob: Some(system_metadata_bytes),
                    object_lock: Self::resolve_new_object_lock_state(
                        &bucket_info,
                        upload.object_lock,
                    )?,
                    encryption: final_encryption,
                    expected_stale_payload_source: completion_snapshot.stale_payload_source,
                    part_records: part_records.clone(),
                    selected_streaming_segments: completion_snapshot.selected_streaming_segments,
                    expected_cleanup: completion_snapshot.cleanup,
                },
                COMPLETED_MULTIPART_UPLOADS_PER_BUCKET_LIMIT,
            ) {
                Ok(outcome) => outcome,
                Err(storage::ObjectPgActionError::StaleMultipartCompletionSnapshot)
                    if stale_commit_retries < COMPLETE_MULTIPART_STALE_COMMIT_RETRIES =>
                {
                    stale_commit_retries += 1;
                    continue 'retry_stale_commit_snapshot;
                }
                Err(error) => return Err(Coordinator::map_object_pg_action_error(error)),
            };
            let version_id = completion_outcome.version_id;
            let stale_payload = completion_outcome
                .stale_payload
                .map(StaleObjectPayload::from);
            let lifecycle_tags = completion_outcome.live_tags;
            let lifecycle_size = completion_outcome.live_size;
            let lifecycle_last_modified = completion_outcome.live_last_modified;

            let lifecycle_expiration = self.current_object_write_lifecycle_expiration(
                &bucket_info,
                key.as_str(),
                lifecycle_tags.as_deref(),
                lifecycle_size,
                lifecycle_last_modified,
            )?;
            if let Some(ref payload) = stale_payload {
                self.read_runtime_for_storage_node(std::sync::Arc::clone(&storage_node))
                    .enqueue_object_payload_reclaim_for(&bucket, &key, payload.generation_id);
            }

            return Ok(CompleteMultipartUploadResult {
                etag: etag_str,
                version_id,
                managed_encryption,
                checksum_algorithm: effective_checksum_config
                    .map(MultipartChecksumConfig::algorithm),
                checksum_type: effective_checksum_config
                    .map(MultipartChecksumConfig::checksum_type),
                checksum_value,
                lifecycle_expiration,
            });
        }
    }

    /// Abort an in-progress multipart upload.
    ///
    /// Publishes an abort metadata command, best-effort deletes all part shard
    /// sets, then deletes the upload and part metadata rows.
    pub fn abort_multipart_upload(&self, req: &MultipartObjectRequest) -> Result<(), ServerError> {
        let storage_node = self.storage_node();
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::abort_multipart_upload",
            "bucket={:?} key={:?} upload_id={:?}",
            req.object.bucket_name(),
            req.object.key,
            req.upload_id()
        );
        match self.authorize_abort_multipart_upload_with_storage_node(&storage_node, req)? {
            AuthorizedAbortMultipartUpload::Completed => Ok(()),
            AuthorizedAbortMultipartUpload::InProgress { upload } => {
                if self
                    .read_runtime_for_storage_node(std::sync::Arc::clone(&storage_node))
                    .abort_authorized_multipart_upload_internal(&upload)?
                {
                    Ok(())
                } else {
                    Err(ServerError::NoSuchUpload {
                        upload_id: upload.upload_id.to_string(),
                    })
                }
            }
        }
    }

    /// List parts of an in-progress multipart upload.
    pub fn list_parts(&self, req: &ListPartsRequest) -> Result<ListPartsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_parts",
            "bucket={:?} key={:?} upload_id={:?} max_parts={}",
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.max_parts
        );
        let AuthorizedListParts {
            bucket_info,
            upload: authorized_upload,
        } = self.authorize_list_parts(req)?;
        let listed = self
            .storage_node()
            .list_multipart_parts_for_authorized_upload(
                &authorized_upload,
                req.part_number_marker,
                req.max_parts,
            )
            .map_err(Self::map_object_pg_action_error)?;
        let upload = listed.upload;
        let resp = listed.response;
        let upload_initiated_at = upload.initiated_at;
        let checksum_algorithm = upload.checksum.map(MultipartChecksumConfig::algorithm);
        let checksum_type = upload.checksum.map(MultipartChecksumConfig::checksum_type);

        let parts = resp
            .parts
            .iter()
            .map(|p| {
                use base64::Engine;
                let etag_crc = etag_bytes_to_crc64(&p.etag).unwrap_or(0);
                let checksum = p
                    .checksum
                    .as_ref()
                    .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes));
                PartEntry {
                    part_number: p.part_number,
                    size: p.size,
                    etag: format_etag(etag_crc),
                    last_modified: p.last_modified,
                    checksum,
                }
            })
            .collect();

        Ok(ListPartsResult {
            parts,
            is_truncated: resp.is_truncated,
            next_part_number_marker: resp.next_part_number_marker,
            checksum_algorithm,
            checksum_type,
            lifecycle_abort: self.multipart_lifecycle_abort_headers(
                &bucket_info,
                upload.key.as_str(),
                upload_initiated_at,
            )?,
        })
    }

    /// List in-progress multipart uploads for a bucket.
    ///
    /// Fans out across all PGs, merges results sorted by (key, upload_id),
    /// and applies pagination.
    pub fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_multipart_uploads",
            "bucket={:?} max_uploads={}",
            req.bucket.name,
            req.max_uploads
        );
        let prefix = req.prefix;
        let key_marker = req.key_marker;
        let upload_id_marker = req.upload_id_marker.as_ref();
        let max_uploads = req.max_uploads;
        let AuthorizedListMultipartUploads { bucket } =
            self.authorize_list_multipart_uploads(req)?;

        if max_uploads == 0 {
            return Ok(ListMultipartUploadsResult {
                uploads: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_upload_id_marker: None,
            });
        }

        let storage::ListedBucketMultipartUploads {
            uploads: mut all_uploads,
            hit_record_cap,
        } = self
            .storage_node()
            .list_multipart_uploads_for_bucket(
                &bucket,
                optional_list_object_key(prefix)?.as_ref(),
                optional_list_object_key(key_marker)?.as_ref(),
                upload_id_marker,
                MAX_LIST_RECORDS,
                max_uploads,
            )
            .map_err(Self::map_object_pg_action_error)?;

        all_uploads.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(a.initiated_at.cmp(&b.initiated_at))
                .then(a.upload_id.cmp(&b.upload_id))
        });

        let max = max_uploads as usize;
        let is_truncated = hit_record_cap || all_uploads.len() > max;
        all_uploads.truncate(max);

        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            if let Some(last) = all_uploads.last() {
                (Some(last.key.to_string()), Some(last.upload_id.clone()))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        let uploads = all_uploads
            .into_iter()
            .map(|u| MultipartUploadEntry {
                key: u.key.to_string(),
                upload_id: u.upload_id,
                initiated: u.initiated_at,
                owner: u.owner,
                initiator: u.initiator,
                checksum_algorithm: u.checksum.map(MultipartChecksumConfig::algorithm),
                checksum_type: u.checksum.map(MultipartChecksumConfig::checksum_type),
            })
            .collect();

        Ok(ListMultipartUploadsResult {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    /// Finalize a streaming UploadPart session.
    ///
    /// Locks the metadata PG, builds committed object segments from staging
    /// rows, and atomically commits the part via `commit_stream_part`.
    /// `computed_checksum` is the actual checksum bytes computed incrementally
    /// during streaming. If `None`, the checksum is derived from `claimed_checksum`.
    #[cfg(test)]
    pub fn finalize_stream_part(
        &self,
        req: FinalizeStreamPartRequest,
    ) -> Result<UploadPartResult, ServerError> {
        self.finalize_stream_part_with_storage_node(&self.storage_node(), req)
    }

    pub fn finalize_stream_part_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: FinalizeStreamPartRequest,
    ) -> Result<UploadPartResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::finalize_stream_part",
            "bucket={:?} key={:?} upload_id={:?} part_number={} session_id={:?} bytes={}",
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.part_number,
            req.session_id,
            req.total_size
        );
        let bucket = req.upload.bucket_name();
        let key = req.upload.key();
        let session_id = req.session_id;
        let upload_id = req.upload.upload_id();
        let part_number = req.part_number;
        let crc64 = req.crc64;
        let total_size = req.total_size;
        let claimed_checksum = req.claimed_checksum;
        let computed_checksum = req.computed_checksum;
        #[cfg(test)]
        if self.should_probe_finalize_stream_part_commit(req.upload.bucket_name()) {
            let object_pg_ready = storage_node
                .try_probe_object_pg_available(
                    req.upload.bucket_name_typed(),
                    req.upload.key_typed(),
                )
                .map_err(Coordinator::map_object_pg_action_error)?;
            if !object_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: object pg still locked before finalize_stream_part commit"
                        .to_string(),
                });
            }
        }
        let FinalizeStreamPartOutcome {
            value: mut result,
            last_modified,
        } = storage_node
            .finalize_upload_part_stream(
                req.upload.bucket_name_typed(),
                req.upload.key_typed(),
                upload_id,
                session_id,
                part_number,
                |snapshot: StreamUploadPartSnapshot| {
                    let StreamUploadPartSnapshot {
                        session,
                        upload,
                        existing_part_generation,
                        staging_segments,
                    } = snapshot;
                    if session.state != StreamUploadState::InProgress {
                        return Err(ServerError::InvalidRequest {
                            reason: "stream session is not in progress".to_string(),
                        });
                    }
                    if session.bucket != *req.upload.bucket_name_typed()
                        || session.key != *req.upload.key_typed()
                    {
                        return Err(ServerError::InvalidRequest {
                            reason: "session bucket/key mismatch".to_string(),
                        });
                    }
                    match &session.target {
                        StreamUploadTarget::UploadPart {
                            upload_id: sess_upload_id,
                            part_number: sess_part_number,
                        } if sess_upload_id == upload_id && *sess_part_number == part_number => {}
                        StreamUploadTarget::UploadPart { .. } => {
                            return Err(ServerError::InvalidRequest {
                                reason: "session upload_id/part_number mismatch".to_string(),
                            });
                        }
                        StreamUploadTarget::PutObject => {
                            return Err(ServerError::InvalidRequest {
                                reason: "session is not an UploadPart session".to_string(),
                            });
                        }
                    }

                    if upload.bucket != bucket || upload.key != key {
                        return Err(ServerError::NoSuchUpload {
                            upload_id: upload_id.to_string(),
                        });
                    }
                    if upload.state != UploadState::InProgress {
                        return Err(ServerError::NoSuchUpload {
                            upload_id: upload_id.to_string(),
                        });
                    }

                    let claimed_algo = claimed_checksum.map(ChecksumClaim::algorithm);
                    let upload_checksum_algo = upload.checksum.map(MultipartChecksumConfig::algorithm);
                    let effective_algo = match (upload_checksum_algo, claimed_algo) {
                        (Some(upload_algo), Some(part_algo)) if upload_algo != part_algo => {
                            return Err(ServerError::InvalidRequest {
                                reason: format!(
                                    "checksum algorithm mismatch: upload configured with {} but part sent {}",
                                    upload_algo.as_str(),
                                    part_algo.as_str()
                                ),
                            });
                        }
                        (Some(algo), Some(_)) => Some(algo),
                        (Some(upload_algo), None) => Some(upload_algo),
                        (None, Some(part_algo)) => Some(part_algo),
                        (None, None) => None,
                    };

                    let checksum = if let Some(cksum) = computed_checksum.clone() {
                        let algo = cksum.algorithm();
                        let bytes = cksum.bytes();
                        if let Some(ea) = effective_algo {
                            if ea != algo {
                                return Err(ServerError::InvalidRequest {
                                    reason: format!(
                                        "computed checksum algorithm {} doesn't match effective {}",
                                        algo.as_str(),
                                        ea.as_str()
                                    ),
                                });
                            }
                        }
                        if let Some(claim) = &claimed_checksum {
                            if claim.algorithm() != algo {
                                return Err(ServerError::InvalidRequest {
                                    reason: format!(
                                        "claimed checksum algorithm {} doesn't match computed {}",
                                        claim.algorithm().as_str(),
                                        algo.as_str()
                                    ),
                                });
                            }
                            if claim.expected_bytes() != bytes {
                                return Err(ServerError::BadDigest);
                            }
                        }
                        Some(cksum)
                    } else if effective_algo.is_some() || claimed_checksum.is_some() {
                        return Err(ServerError::InvalidRequest {
                            reason: "missing computed checksum for streaming upload part".to_string(),
                        });
                    } else {
                        None
                    };

                    let generation = existing_part_generation.map_or(0, |generation| generation + 1);
                    let segments_total: u64 =
                        staging_segments.iter().map(|segment| segment.size).sum();
                    if segments_total != total_size {
                        return Err(ServerError::InvalidRequest {
                            reason: format!(
                                "total_size mismatch: caller passed {total_size} but staged segments sum to {segments_total}"
                            ),
                        });
                    }
                    let staged_crc64 = staging_segments.iter().fold(
                        checksum::crc64::checksum(&[]),
                        |crc64, segment| {
                            checksum::crc64::combine(crc64, segment.payload_crc64, segment.size)
                        },
                    );
                    if staged_crc64 != crc64 {
                        return Err(ServerError::InvalidRequest {
                            reason: format!(
                                "stream UploadPart etag CRC64 mismatch: caller passed {crc64} but staged payload segments combine to {staged_crc64}"
                            ),
                        });
                    }

                    let committed_segments: Vec<MultipartPartSegmentRecord> = staging_segments
                        .iter()
                        .map(|segment| MultipartPartSegmentRecord {
                            bucket: upload.bucket.clone(),
                            key: upload.key.clone(),
                            upload_id: upload_id.clone(),
                            version_id: u64::MAX,
                            part_number,
                            segment_index: segment.segment_index,
                            size: segment.size,
                            segment_crc64: segment.segment_crc64,
                            segment_okh: segment.segment_okh,
                            segment_vid: segment.segment_vid,
                            data_pg_id: segment.data_pg_id,
                            placement_cluster_epoch: segment.placement_cluster_epoch,
                            ec_k: segment.ec_k,
                            ec_m: segment.ec_m,
                        })
                        .collect();

                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let ec = staging_segments.first().map_or(
                        storage_node.default_payload_ec_shape(),
                        |segment| storage::EcShape {
                            k: segment.ec_k,
                            m: segment.ec_m,
                        },
                    );

                    let stored_checksum = if upload_checksum_algo.is_some() {
                        checksum.as_ref().map(ChecksumBytes::from)
                    } else {
                        None
                    };

                    let part_record = MultipartPartRecord {
                        upload_id: upload_id.clone(),
                        part_number,
                        generation,
                        size: total_size,
                        payload_crc64: crc64,
                        etag: crc64_to_etag_bytes(crc64),
                        etag_kind: storage::EtagKind::Crc64,
                        part_okh: [0u8; 16],
                        part_vid: GenerationId::new(u64::from(generation) + 1)
                            .expect("multipart part generation must be nonzero"),
                        placement_cluster_epoch: staging_segments
                            .first()
                            .map_or(storage_node.operation_epoch(), |segment| {
                                segment.placement_cluster_epoch
                            }),
                        ec_k: ec.k,
                        ec_m: ec.m,
                        last_modified: now,
                        checksum: stored_checksum,
                    };

                    Ok::<_, ServerError>(PreparedStreamPartCommit {
                        value: UploadPartResult {
                            etag: format_etag(crc64),
                            last_modified: now,
                            checksum,
                            managed_encryption: upload.encryption.managed_encryption_algorithm(),
                        },
                        part: part_record,
                        segments: committed_segments,
                    })
                },
            )
            .map_err(|error| Self::map_upload_part_stream_error(upload_id, error))??;

        result.last_modified = last_modified;
        Ok(result)
    }

    #[cfg(test)]
    pub fn abort_stream_part_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_part_session_with_storage_node(
            &self.storage_node(),
            bucket,
            key,
            session_id,
        )
    }

    pub fn abort_stream_part_session_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_put_for_storage_node(storage_node, bucket, key, session_id)
    }
}
