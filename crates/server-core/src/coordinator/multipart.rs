use checksum::{ChecksumAlgorithm, ChecksumBytes, ChecksumType, MultipartChecksumConfig};
use std::time::{Duration, Instant};
use storage::{
    BucketName, CreateMultipartUploadOutcome, CreateMultipartUploadReq, FinalizeStreamPartOutcome,
    GenerationId, MultipartPartRecord, MultipartPartSegmentRecord, ObjectKey,
    PreparedStreamPartCommit, SerializedMetadataBlob, SerializedSystemMetadataBlob, SessionId,
    StreamUploadPartSnapshot, StreamUploadState, StreamUploadTarget, UploadId, UploadState,
};

fn multipart_completion_fingerprint(
    parts: &[CompletePart],
) -> storage::MultipartCompletionFingerprint {
    fn update_bytes(context: &mut ring::digest::Context, bytes: &[u8]) {
        context.update(&(bytes.len() as u64).to_be_bytes());
        context.update(bytes);
    }

    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    context.update(b"argmin complete multipart manifest v2\0");
    context.update(&(parts.len() as u64).to_be_bytes());
    for part in parts {
        context.update(&part.part_number.to_be_bytes());
        update_bytes(&mut context, part.etag.as_bytes());
        match &part.checksum {
            None => context.update(&[0]),
            Some(checksum) => {
                context.update(&[1]);
                update_bytes(&mut context, checksum.algorithm().as_str().as_bytes());
                update_bytes(&mut context, checksum.expected_bytes());
            }
        }
    }
    let digest = context.finish();
    let mut fingerprint = [0u8; 32];
    fingerprint.copy_from_slice(digest.as_ref());
    storage::MultipartCompletionFingerprint::from_bytes(fingerprint)
}

use super::authz_results::{
    AuthorizedAbortMultipartUpload, AuthorizedCompleteMultipartUpload,
    AuthorizedCreateMultipartUpload, AuthorizedListMultipartUploads, AuthorizedListParts,
};
use super::bucket_handles::{BucketHandleLoader, BucketHandleRequest};
use super::object_state::StaleObjectPayload;
use super::request_types::{
    AppendStreamPartRequest, BeginStreamPartRequest, CompleteMultipartUploadRequest, CompletePart,
    CreateMultipartUploadRequest, FinalizeStreamPartRequest, ListMultipartUploadsRequest,
    ListPartsRequest, MultipartObjectRequest,
};
use super::response_types::{
    BeginStreamPartResult, CompleteMultipartUploadResult, CreateMultipartUploadResult,
    ListMultipartUploadsNextMarker, ListMultipartUploadsResult, ListPartsResult,
    MultipartUploadEntry, PartEntry, UploadPartResult,
};
use super::{
    compute_checksum, optional_list_object_key, Coordinator, MAX_MULTIPART_PARTS, MIN_PART_SIZE,
    TRACE_TARGET,
};
#[cfg(test)]
use super::{
    maybe_run_multipart_complete_commit_hook, maybe_run_multipart_complete_pre_commit_hook,
    maybe_run_multipart_complete_snapshot_hook,
};
use crate::checksum_claim::ChecksumClaim;
use crate::conditional::{check_write_conditions, WriteCondition};
use crate::error::ServerError;
use crate::etag::{compute_multipart_etag, crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::system_metadata::SystemMetadata;

const COMPLETE_MULTIPART_TERMINAL_RACE_RETRIES: usize = 1;
pub(super) const COMPLETE_MULTIPART_STALE_SNAPSHOT_RETRY_BUDGET: Duration = Duration::from_secs(2);

enum StreamPartFinalizeRoute<'a> {
    #[cfg(any(test, feature = "test-utils"))]
    Raw(&'a std::sync::Arc<storage::StorageCluster>),
    Admitted(&'a storage::ActiveMultipartObjectRoute<'a>),
}

impl StreamPartFinalizeRoute<'_> {
    fn default_payload_ec_shape(&self) -> storage::EcShape {
        match self {
            #[cfg(any(test, feature = "test-utils"))]
            Self::Raw(storage_node) => storage_node.default_payload_ec_shape(),
            Self::Admitted(route) => route.default_payload_ec_shape(),
        }
    }

    fn operation_epoch(&self) -> storage::ClusterEpoch {
        match self {
            #[cfg(any(test, feature = "test-utils"))]
            Self::Raw(storage_node) => storage_node.operation_epoch(),
            Self::Admitted(route) => route.operation_epoch(),
        }
    }

    fn finalize<T, E>(
        &self,
        _bucket: &BucketName,
        _key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        action: impl FnMut(StreamUploadPartSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, storage::ObjectPgActionError> {
        match self {
            #[cfg(any(test, feature = "test-utils"))]
            Self::Raw(storage_node) => storage_node.finalize_upload_part_stream(
                _bucket,
                _key,
                upload_id,
                session_id,
                part_number,
                action,
            ),
            Self::Admitted(route) => {
                route.finalize_stream_part(upload_id, session_id, part_number, action)
            }
        }
    }
}

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

fn composite_checksum_value_from_complete_parts(
    algorithm: ChecksumAlgorithm,
    parts: &[CompletePart],
) -> Option<String> {
    use base64::Engine;

    let mut concatenated = Vec::new();
    for part in parts {
        let claim = part.checksum.as_ref()?;
        if claim.algorithm() != algorithm {
            return None;
        }
        concatenated.extend_from_slice(claim.expected_bytes());
    }
    let checksum = compute_checksum(algorithm, &concatenated);
    Some(format!(
        "{}-{}",
        base64::engine::general_purpose::STANDARD.encode(checksum.bytes()),
        parts.len()
    ))
}

impl Coordinator {
    pub(super) fn map_object_pg_action_error(error: storage::ObjectPgActionError) -> ServerError {
        if super::object_pg_action_error_is_metadata_command_contention(&error) {
            return ServerError::OperationAborted;
        }
        match error {
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
            storage::ObjectPgActionError::MultipartConditionalRequestConflict => {
                ServerError::InternalError {
                    reason: "multipart conditional conflict escaped completion-specific mapping"
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

    #[cfg(any(test, feature = "test-utils"))]
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

    pub fn append_stream_part_data_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &AppendStreamPartRequest<'_>,
    ) -> Result<(), ServerError> {
        self.require_storage_route_admission(admission)?;
        let route = admission
            .active_multipart_object_route(&req.bucket, &req.key)
            .map_err(super::map_store_error)?;
        let write_encryption = self
            .load_stream_part_write_encryption_on_admitted_multipart_route(
                &route,
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
        self.append_stream_segment_on_admitted_multipart_route(
            &route,
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn begin_stream_part_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: &BeginStreamPartRequest<'_>,
    ) -> Result<BeginStreamPartResult, ServerError> {
        self.begin_stream_part_with_storage_node_and_cleanup_deadline(storage_node, req, None)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn begin_stream_part_with_storage_node_and_cleanup_deadline(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: &BeginStreamPartRequest<'_>,
        cleanup_after: Option<u64>,
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
                    .begin_upload_part_stream_session_with_cleanup_deadline(
                        storage::BeginUploadPartStreamSessionReq {
                            bucket: req.upload.bucket_name_typed().clone(),
                            key: req.upload.key_typed().clone(),
                            upload_id: req.upload.upload_id().clone(),
                            part_number: req.part_number,
                            session_id: session_id.clone(),
                            bucket_write_reservation: proof.clone(),
                        },
                        cleanup_after,
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

    pub fn begin_stream_part_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
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
        self.require_storage_route_admission(admission)?;
        let route = admission
            .active_multipart_object_route(req.upload.bucket_name_typed(), req.upload.key_typed())
            .map_err(super::map_store_error)?;
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        let authorized = self.with_bucket_write_handle_on_admitted_route(
            admission,
            &req.upload,
            request,
            |bucket_handle| {
                let upload = route
                    .load_in_progress_multipart_upload(req.upload.upload_id())
                    .map_err(Self::map_object_pg_action_error)?;
                self.authorize_begin_stream_part_with_upload(req, &bucket_handle, &upload)
            },
        )?;
        let checksum_algorithm = authorized
            .upload
            .record()
            .checksum
            .map(MultipartChecksumConfig::algorithm);
        let session_id = Self::random_session_id("failed to generate session ID")?;
        let session_id = route
            .create_upload_part_stream_session(
                &authorized.upload,
                authorized.part_number,
                &session_id,
            )
            .map_err(|error| Self::map_upload_part_stream_error(&authorized.upload_id, error))?;
        Ok(BeginStreamPartResult {
            session_id,
            checksum_algorithm,
            sse_customer: authorized.sse_customer,
        })
    }

    pub(super) fn validate_upload_part_number(part_number: u32) -> Result<(), ServerError> {
        if !(1..=MAX_MULTIPART_PARTS as u32).contains(&part_number) {
            return Err(ServerError::InvalidArgument {
                reason: format!(
                    "part number must be between 1 and {MAX_MULTIPART_PARTS}, got {part_number}"
                ),
            });
        }
        Ok(())
    }

    /// Initiate a multipart upload.
    ///
    /// Generates a random upload ID, serializes the metadata blob, and
    /// inserts a new multipart upload record in the metadata PG for (bucket, key).
    pub fn create_multipart_upload_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &CreateMultipartUploadRequest,
    ) -> Result<CreateMultipartUploadResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::create_multipart_upload",
            "bucket={:?} key={:?}",
            req.object.bucket_name(),
            req.object.key
        );
        self.require_storage_route_admission(admission)?;
        let multipart_route = admission
            .active_multipart_object_route(req.object.bucket.name_typed(), req.object.key_typed())
            .map_err(super::map_store_error)?;
        let metadata_blob = req.metadata.serialize()?;
        let system_metadata_blob = req.system_metadata.serialize()?;
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_lifecycle_view()
            .requiring_bucket_tags_if_abac_enabled();
        let expected_bucket_owner = req.object.expected_bucket_owner();
        let CreateMultipartUploadOutcome {
            value: authorized,
            upload_id: typed_upload_id,
            initiated_at,
        } = multipart_route
            .create_multipart_upload_with_ordered_id(
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
                    let typed_upload_id = authorized
                        .bucket_info
                        .multipart_upload_id_key
                        .issue(
                            &authorized.bucket,
                            &authorized.key,
                            &authorized.initiator.principal,
                        )
                        .map_err(|reason| ServerError::InternalError { reason })?;
                    let create = CreateMultipartUploadReq {
                        upload_id: typed_upload_id.clone(),
                        bucket: authorized.bucket.clone(),
                        key: authorized.key.clone(),
                        tags: Self::stored_object_tags(authorized.tags.as_ref())?,
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
                    let upload_id_key = authorized.bucket_info.multipart_upload_id_key.clone();
                    Ok::<_, ServerError>((authorized, create, upload_id_key))
                },
            )
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)??;
        #[cfg(test)]
        self.maybe_run_multipart_create_committed_hook(req.object.bucket_name());
        let AuthorizedCreateMultipartUpload {
            lifecycle,
            key,
            write_encryption,
            ..
        } = authorized;
        let lifecycle_abort = lifecycle.as_ref().and_then(|config| {
            Self::evaluate_multipart_lifecycle_abort_headers(config, key.as_str(), initiated_at)
        });

        Ok(CreateMultipartUploadResult {
            upload_id: typed_upload_id,
            managed_encryption: write_encryption
                .object_encryption()
                .managed_encryption_algorithm(),
            lifecycle_abort,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn create_multipart_upload(
        &self,
        req: &CreateMultipartUploadRequest,
    ) -> Result<CreateMultipartUploadResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.create_multipart_upload_on_admitted_route(&admission, req)
    }

    /// Complete a multipart upload, committing a manifest object.
    ///
    /// Validates the part list, checks ETags and sizes, writes the final
    /// object metadata row with `MultipartManifest` layout, commits
    /// manifest rows into `object_parts`, and deletes in-progress state.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn complete_multipart_upload(
        &self,
        req: &CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.complete_multipart_upload_on_admitted_route(&admission, req)
    }

    pub fn complete_multipart_upload_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadResult, ServerError> {
        self.require_storage_route_admission(admission)?;
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::complete_multipart_upload",
            "bucket={:?} key={:?} upload_id={:?} parts={}",
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.parts.len()
        );
        let mut terminal_race_retries = 0usize;
        let mut stale_snapshot_retry_deadline = None;
        'retry_stale_commit_snapshot: loop {
            let authorized =
                self.authorize_complete_multipart_upload_on_admitted_route(admission, req)?;
            let (
                bucket_info,
                lifecycle,
                bucket,
                key,
                upload_id,
                upload,
                multipart_write_encryption,
            ) = match authorized {
                AuthorizedCompleteMultipartUpload::InProgress {
                    bucket_info,
                    lifecycle,
                    bucket,
                    key,
                    upload_id,
                    upload,
                    multipart_write_encryption,
                } => (
                    bucket_info,
                    lifecycle,
                    bucket,
                    key,
                    upload_id,
                    upload,
                    multipart_write_encryption,
                ),
                AuthorizedCompleteMultipartUpload::Replay {
                    lifecycle,
                    key,
                    replay,
                } => {
                    if replay.fingerprint != multipart_completion_fingerprint(req.parts) {
                        return Err(ServerError::NoSuchUpload {
                            upload_id: replay.upload_id.to_string(),
                        });
                    }
                    let lifecycle_expiration =
                        Self::current_object_write_lifecycle_expiration_for_config(
                            lifecycle.as_deref(),
                            key.as_str(),
                            replay.tags.as_deref(),
                            replay.size,
                            replay.last_modified,
                        )?;
                    return Ok(CompleteMultipartUploadResult {
                        etag: replay.etag.format(),
                        version_id: replay.version_id,
                        managed_encryption: replay.encryption.managed_encryption_algorithm(),
                        checksum_algorithm: None,
                        checksum_type: None,
                        checksum_value: None,
                        lifecycle_expiration,
                    });
                }
            };
            let parts = req.parts;
            let claimed_checksum = req.claimed_checksum;
            let expected_object_size = req.expected_object_size;
            if parts.is_empty() {
                return Err(ServerError::InvalidRequest {
                    reason: "part list must not be empty".to_string(),
                });
            }
            if parts.len() > MAX_MULTIPART_PARTS {
                return Err(ServerError::CompleteMultipartTooManyParts);
            }
            for window in parts.windows(2) {
                if window[0].part_number >= window[1].part_number {
                    return Err(ServerError::InvalidPartOrder);
                }
            }

            let requested_part_numbers: Vec<u32> =
                parts.iter().map(|part| part.part_number).collect();
            let checksum_config = upload.checksum;
            #[cfg(test)]
            maybe_run_multipart_complete_snapshot_hook(bucket.as_str(), key.as_str());
            let multipart_route = admission
                .active_multipart_object_route(&bucket, &key)
                .map_err(super::map_store_error)?;
            let completion_snapshot = match multipart_route
                .load_multipart_completion_snapshot(&upload, &requested_part_numbers)
            {
                Ok(snapshot) => snapshot,
                Err(storage::ObjectPgActionError::Metadata(
                    storage::MetadataError::NoSuchUpload { .. },
                )) if terminal_race_retries < COMPLETE_MULTIPART_TERMINAL_RACE_RETRIES => {
                    // Another completion can publish after authorization but
                    // before this snapshot lookup. Restart authorization so an
                    // identical completion resolves through terminal replay,
                    // while abort or a different manifest still resolves to
                    // NoSuchUpload.
                    terminal_race_retries += 1;
                    continue 'retry_stale_commit_snapshot;
                }
                Err(storage::ObjectPgActionError::Metadata(
                    storage::MetadataError::PartNotFound { part_number, .. },
                )) => {
                    // For composite-checksum uploads AWS can reject an object-level
                    // checksum using only the completion XML, before reporting that
                    // a requested part does not exist.
                    if let (Some(config), Some(claimed)) = (checksum_config, claimed_checksum) {
                        if config.checksum_type() == ChecksumType::Composite
                            && claimed.algorithm() == config.algorithm()
                        {
                            claimed.validate_complete_multipart_header_value()?;
                            if composite_checksum_value_from_complete_parts(
                                config.algorithm(),
                                parts,
                            )
                            .is_some_and(|computed| computed != claimed.encoded_value())
                            {
                                return Err(ServerError::ChecksumDigestMismatch {
                                    algorithm: claimed.algorithm().as_str().to_ascii_lowercase(),
                                });
                            }
                        }
                    }
                    return Err(ServerError::InvalidPart { part_number });
                }
                Err(other) => return Err(Coordinator::map_object_pg_action_error(other)),
            };

            let stores_unconfigured_crc64nvme_checksum_claim = checksum_config.is_none()
                && claimed_checksum.is_some_and(|claimed| {
                    claimed
                        .algorithm()
                        .stores_unconfigured_complete_multipart_header()
                });
            let stores_default_crc64nvme_checksum = checksum_config.is_none()
                && multipart_write_encryption.can_store_checksum_metadata();
            let effective_checksum_config = if stores_unconfigured_crc64nvme_checksum_claim
                || stores_default_crc64nvme_checksum
            {
                Some(MultipartChecksumConfig::new(
                    ChecksumAlgorithm::Crc64nvme,
                    Some(ChecksumType::FullObject),
                )?)
            } else {
                checksum_config
            };

            let part_records = completion_snapshot.part_records;
            for (cp, part) in parts.iter().zip(&part_records) {
                let stored_etag = etag_bytes_to_crc64(&part.etag)
                    .map(format_etag)
                    .unwrap_or_default();
                if stored_etag != cp.etag {
                    return Err(ServerError::InvalidPart {
                        part_number: cp.part_number,
                    });
                }
            }

            let checksum_value = effective_checksum_config
                .map(|config| complete_multipart_checksum_value(config, &part_records))
                .transpose()?;

            if let Some(claimed) = claimed_checksum {
                claimed.validate_complete_multipart_header_value()?;
                if checksum_config.is_some_and(|config| config.algorithm() == claimed.algorithm())
                    && checksum_value
                        .as_ref()
                        .is_some_and(|computed| computed != claimed.encoded_value())
                {
                    return Err(ServerError::ChecksumDigestMismatch {
                        algorithm: claimed.algorithm().as_str().to_ascii_lowercase(),
                    });
                }
            }

            for (cp, part) in parts.iter().zip(&part_records) {
                if let Some(config) = checksum_config {
                    if config.checksum_type() == ChecksumType::Composite && cp.checksum.is_none() {
                        return Err(ServerError::CompleteMultipartMissingPartChecksum {
                            algorithm: config.algorithm().as_str().to_ascii_lowercase(),
                            part_number: cp.part_number,
                        });
                    }
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
            }

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
            if checksum_config.is_some()
                && parts
                    .iter()
                    .enumerate()
                    .any(|(index, part)| part.part_number != (index + 1) as u32)
            {
                return Err(ServerError::InvalidRequest {
                    reason:
                        "Part numbers must be consecutive and begin with 1 when a checksum is used."
                            .to_string(),
                });
            }
            if let Some(expected) = expected_object_size {
                if expected != total_size {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "The provided 'x-amz-mp-object-size' header value {expected} does not match what was computed: {total_size}"
                        ),
                    });
                }
            }
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

            let completion_outcome = match multipart_route
                .complete_multipart_upload_commit_serialized(
                    storage::CompleteMultipartCommitRequest {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        upload_id: upload_id.clone(),
                        completion_fingerprint: multipart_completion_fingerprint(req.parts),
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
                        expected_current_object_identity: completion_snapshot
                            .current_object_identity,
                        conditional_completion: !req.cond.is_empty(),
                        part_records: part_records.clone(),
                        selected_streaming_segments: completion_snapshot
                            .selected_streaming_segments,
                        expected_cleanup: completion_snapshot.cleanup,
                    },
                ) {
                Ok(outcome) => outcome,
                Err(storage::ObjectPgActionError::StaleMultipartCompletionSnapshot) => {
                    let deadline = stale_snapshot_retry_deadline.get_or_insert_with(|| {
                        Instant::now() + COMPLETE_MULTIPART_STALE_SNAPSHOT_RETRY_BUDGET
                    });
                    if Instant::now() < *deadline {
                        continue 'retry_stale_commit_snapshot;
                    }
                    return Err(ServerError::OperationAborted);
                }
                Err(storage::ObjectPgActionError::Metadata(
                    storage::MetadataError::NoSuchUpload { .. },
                )) if terminal_race_retries < COMPLETE_MULTIPART_TERMINAL_RACE_RETRIES => {
                    // The upload can disappear after the validated snapshot if
                    // another completion or abort publishes first. Reauthorize
                    // to distinguish an identical terminal replay from a
                    // genuinely unavailable upload.
                    terminal_race_retries += 1;
                    continue 'retry_stale_commit_snapshot;
                }
                Err(storage::ObjectPgActionError::MultipartConditionalRequestConflict) => {
                    let condition = match req.cond {
                        WriteCondition::IfMatch(_) => "If-Match",
                        WriteCondition::IfNoneMatchStar => "If-None-Match",
                        WriteCondition::None => {
                            return Err(ServerError::InternalError {
                                reason: "unconditional multipart completion reported a conditional conflict"
                                    .to_string(),
                            });
                        }
                    };
                    return Err(ServerError::ConditionalRequestConflict {
                        key: key.as_str().to_string(),
                        condition,
                    });
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

            #[cfg(test)]
            maybe_run_multipart_complete_commit_hook(bucket.as_str(), key.as_str());

            let lifecycle_expiration = Self::current_object_write_lifecycle_expiration_for_config(
                lifecycle.as_deref(),
                key.as_str(),
                lifecycle_tags.as_deref(),
                lifecycle_size,
                lifecycle_last_modified,
            )?;
            if let Some(ref payload) = stale_payload {
                multipart_route.enqueue_object_payload_reclaim(payload.generation_id);
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

    /// Validate the upload target before parsing a completion body.
    ///
    /// AWS resolves a missing or wrong-key upload ID before reporting malformed
    /// completion XML, while authorization still follows XML validation. The
    /// full completion path repeats this lookup so a concurrent terminal state
    /// transition cannot use a stale existence check.
    pub fn validate_complete_multipart_upload_target_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        upload: &MultipartObjectRequest<'_>,
    ) -> Result<(), ServerError> {
        self.require_storage_route_admission(admission)?;
        let multipart_route = admission
            .active_multipart_object_route(upload.bucket_name_typed(), upload.key_typed())
            .map_err(super::map_store_error)?;
        match multipart_route.load_in_progress_multipart_upload(upload.upload_id()) {
            Ok(_) => Ok(()),
            Err(storage::ObjectPgActionError::Metadata(storage::MetadataError::NoSuchUpload {
                ..
            })) => {
                let bucket_info = self.checked_active_bucket_summary_for_admitted_route(
                    admission,
                    upload.bucket_name_typed(),
                    upload.expected_bucket_owner(),
                )?;
                if bucket_info.multipart_upload_id_key.authenticates(
                    upload.bucket_name_typed(),
                    upload.key_typed(),
                    upload.upload_id(),
                ) {
                    Ok(())
                } else {
                    Err(ServerError::NoSuchUpload {
                        upload_id: upload.upload_id().to_string(),
                    })
                }
            }
            Err(error) => Err(Self::map_object_pg_action_error(error)),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn validate_complete_multipart_upload_target(
        &self,
        upload: &MultipartObjectRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.validate_complete_multipart_upload_target_on_admitted_route(&admission, upload)
    }

    /// Resolve an operation that is valid only for an active multipart upload.
    ///
    /// AWS performs this lookup before operation-specific part-number, checksum,
    /// and copy-range validation, but before current-policy authorization.
    pub fn validate_in_progress_multipart_upload_target_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        upload: &MultipartObjectRequest<'_>,
    ) -> Result<(), ServerError> {
        self.require_storage_route_admission(admission)?;
        admission
            .active_multipart_object_route(upload.bucket_name_typed(), upload.key_typed())
            .map_err(super::map_store_error)?
            .load_in_progress_multipart_upload(upload.upload_id())
            .map(|_| ())
            .map_err(Self::map_object_pg_action_error)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn validate_in_progress_multipart_upload_target(
        &self,
        upload: &MultipartObjectRequest<'_>,
    ) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.validate_in_progress_multipart_upload_target_on_admitted_route(&admission, upload)
    }

    /// Abort an in-progress multipart upload.
    ///
    /// Publishes an abort metadata command, best-effort deletes all part shard
    /// sets, then deletes the upload and part metadata rows.
    pub fn abort_multipart_upload_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &MultipartObjectRequest,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::abort_multipart_upload",
            "bucket={:?} key={:?} upload_id={:?}",
            req.object.bucket_name(),
            req.object.key,
            req.upload_id()
        );
        self.require_storage_route_admission(admission)?;
        let multipart_route = admission
            .active_multipart_object_route(req.bucket_name_typed(), req.key_typed())
            .map_err(super::map_store_error)?;
        match self.authorize_abort_multipart_upload_on_admitted_route(admission, req)? {
            AuthorizedAbortMultipartUpload::Terminal => Ok(()),
            AuthorizedAbortMultipartUpload::InProgress { upload } => {
                if multipart_route
                    .abort_authorized_multipart_upload(&upload)
                    .map_err(Self::map_object_pg_action_error)?
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn abort_multipart_upload(&self, req: &MultipartObjectRequest) -> Result<(), ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.abort_multipart_upload_on_admitted_route(&admission, req)
    }

    /// List parts of an in-progress multipart upload.
    pub fn list_parts_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &ListPartsRequest,
    ) -> Result<ListPartsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_parts",
            "bucket={:?} key={:?} upload_id={:?} max_parts={}",
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.max_parts
        );
        self.require_storage_route_admission(admission)?;
        let multipart_route = admission
            .active_multipart_object_route(req.upload.bucket_name_typed(), req.upload.key_typed())
            .map_err(super::map_store_error)?;
        let AuthorizedListParts {
            bucket_info,
            upload: authorized_upload,
        } = self.authorize_list_parts_on_admitted_route(admission, req)?;
        #[cfg(test)]
        super::maybe_run_list_parts_authorized_hook(req.upload.bucket_name(), req.upload.key());
        let listed = multipart_route
            .list_multipart_parts_for_authorized_upload(
                &authorized_upload,
                req.part_number_marker,
                req.max_parts,
            )
            .map_err(Self::map_object_pg_action_error)?;
        #[cfg(test)]
        super::maybe_run_list_parts_storage_list_hook(req.upload.bucket_name(), req.upload.key());
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
            owner: upload.owner.clone(),
            initiator: upload.initiator.clone(),
            checksum_algorithm,
            checksum_type,
            lifecycle_abort: self.multipart_lifecycle_abort_headers_on_admitted_route(
                admission,
                &bucket_info,
                upload.key.as_str(),
                upload_initiated_at,
            )?,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn list_parts(&self, req: &ListPartsRequest) -> Result<ListPartsResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.list_parts_on_admitted_route(&admission, req)
    }

    /// List in-progress multipart uploads for a bucket.
    ///
    /// Fans out across all PGs, merges results sorted by
    /// (key, initiated_at, upload_id), and applies pagination.
    pub fn list_multipart_uploads_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
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
        let delimiter = req.delimiter;
        let key_marker = req.key_marker;
        let upload_id_marker = req.upload_id_marker.as_ref();
        let max_uploads = req.max_uploads;
        let AuthorizedListMultipartUploads { bucket } =
            self.authorize_list_multipart_uploads_on_admitted_route(admission, req)?;

        if max_uploads == 0 {
            return Ok(ListMultipartUploadsResult {
                uploads: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_marker: None,
            });
        }

        let listed = admission
            .active_object_metadata_scan(&bucket)
            .map_err(super::map_store_error)?
            .list_multipart_uploads(
                optional_list_object_key(prefix)?.as_ref(),
                delimiter,
                optional_list_object_key(key_marker)?.as_ref(),
                upload_id_marker,
                max_uploads,
            )
            .map_err(Self::map_object_pg_action_error)?;

        let next_marker = match listed.next_marker {
            Some(storage::MultipartUploadListMarker::Upload { key, upload_id }) => {
                Some(ListMultipartUploadsNextMarker::Upload {
                    key: key.to_string(),
                    upload_id,
                })
            }
            Some(storage::MultipartUploadListMarker::CommonPrefix(_)) => {
                Some(ListMultipartUploadsNextMarker::CommonPrefix)
            }
            None => None,
        };

        let uploads = listed
            .uploads
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
        let common_prefixes = listed
            .common_prefixes
            .into_iter()
            .map(|prefix| prefix.to_string())
            .collect();

        Ok(ListMultipartUploadsResult {
            uploads,
            common_prefixes,
            is_truncated: listed.is_truncated,
            next_marker,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.list_multipart_uploads_on_admitted_route(&admission, req)
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

    #[cfg(any(test, feature = "test-utils"))]
    pub fn finalize_stream_part_with_storage_node(
        &self,
        storage_node: &std::sync::Arc<storage::StorageCluster>,
        req: FinalizeStreamPartRequest,
    ) -> Result<UploadPartResult, ServerError> {
        self.finalize_stream_part_on_route(StreamPartFinalizeRoute::Raw(storage_node), req)
    }

    fn finalize_stream_part_on_route(
        &self,
        storage_route: StreamPartFinalizeRoute<'_>,
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
            let object_pg_ready = match &storage_route {
                #[cfg(any(test, feature = "test-utils"))]
                StreamPartFinalizeRoute::Raw(storage_node) => storage_node
                    .try_probe_object_pg_available(
                        req.upload.bucket_name_typed(),
                        req.upload.key_typed(),
                    ),
                StreamPartFinalizeRoute::Admitted(route) => route.try_probe_object_pg_available(),
            }
            .map_err(Coordinator::map_object_pg_action_error)?;
            if !object_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: object pg still locked before finalize_stream_part commit"
                        .to_string(),
                });
            }
        }
        let default_payload_ec = storage_route.default_payload_ec_shape();
        let operation_epoch = storage_route.operation_epoch();
        let FinalizeStreamPartOutcome {
            value: mut result,
            last_modified,
        } = storage_route
            .finalize(
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
                    let ec = staging_segments.first().map_or(default_payload_ec, |segment| {
                        storage::EcShape {
                            k: segment.ec_k,
                            m: segment.ec_m,
                        }
                    });

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
                        part_vid: GenerationId::new(u64::from(generation) + 1)
                            .expect("multipart part generation must be nonzero"),
                        placement_cluster_epoch: staging_segments
                            .first()
                            .map_or(operation_epoch, |segment| segment.placement_cluster_epoch),
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

    pub(super) fn finalize_stream_part_on_admitted_multipart_route(
        &self,
        route: &storage::ActiveMultipartObjectRoute<'_>,
        req: FinalizeStreamPartRequest,
    ) -> Result<UploadPartResult, ServerError> {
        self.finalize_stream_part_on_route(StreamPartFinalizeRoute::Admitted(route), req)
    }

    pub fn finalize_stream_part_with_storage_admission(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: FinalizeStreamPartRequest,
    ) -> Result<UploadPartResult, ServerError> {
        self.require_storage_route_admission(admission)?;
        let route = admission
            .active_multipart_object_route(req.upload.bucket_name_typed(), req.upload.key_typed())
            .map_err(super::map_store_error)?;
        self.finalize_stream_part_on_route(StreamPartFinalizeRoute::Admitted(&route), req)
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

#[cfg(test)]
mod completion_fingerprint_tests {
    use checksum::ChecksumAlgorithm;

    use super::{multipart_completion_fingerprint, ChecksumClaim, CompletePart};

    #[test]
    fn fingerprint_identifies_only_the_completion_manifest() {
        let part = CompletePart {
            part_number: 1,
            etag: "\"etag-one\"".to_string(),
            checksum: None,
        };
        let original = multipart_completion_fingerprint(std::slice::from_ref(&part));

        assert_eq!(
            original,
            multipart_completion_fingerprint(std::slice::from_ref(&part))
        );
        assert_ne!(
            original,
            multipart_completion_fingerprint(&[CompletePart {
                part_number: 2,
                ..part.clone()
            }])
        );
        assert_ne!(
            original,
            multipart_completion_fingerprint(&[CompletePart {
                etag: "\"etag-two\"".to_string(),
                ..part.clone()
            }])
        );
        assert_ne!(
            original,
            multipart_completion_fingerprint(&[CompletePart {
                checksum: Some(
                    ChecksumClaim::from_base64(
                        ChecksumAlgorithm::Sha256,
                        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                    )
                    .unwrap(),
                ),
                ..part
            }])
        );
    }
}
