use storage::traits::PgMetadataStore;
use storage::{
    BucketName, CreateStreamUploadReq, EcShape, GenerationId, ObjectKey, ObjectLayout,
    ObjectSegmentRecord, PutLiveObjectReq, SessionId, ShardKey, StreamPutFinalizeSnapshot,
    StreamUploadTarget,
};

use super::bucket_handles::{BucketHandleLoader, BucketHandleRequest};
use super::{
    ActiveWriteEncryption, AuthorizePutObjectRequest, AuthorizedFinalizeStreamPutRequest,
    AuthorizedPutObjectCommitRequest, AuthorizedPutObjectWrite, AuthorizedWriteTags, Coordinator,
    FinalizeStreamPutRequest, ObjectRequest, PreparedStreamPut, PutCommitRequest,
    PutObjectPolicyContext, PutObjectRequest, PutObjectResult, INTERNAL_SEGMENT_SIZE, TRACE_TARGET,
};
use crate::error::ServerError;
use crate::etag::format_etag;
use crate::pg::stream_segment_key_hash;
use crate::sse::SseCustomerRequest;

impl Coordinator {
    /// Put an object, using a direct single-segment commit when possible.
    pub fn put_object(&self, req: &PutObjectRequest<'_>) -> Result<PutObjectResult, ServerError> {
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
            let prepared = self.begin_stream_put(&authorize_req)?;
            let result = self.put_large_object_from_authorized_write_with_session(
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
                let _ = self.abort_stream_put_for(
                    prepared.authorized_write.bucket_typed(),
                    prepared.authorized_write.key_typed(),
                    &prepared.session_id,
                );
            }
            return result;
        }

        let authorized = self.authorize_put_object_write(&authorize_req)?;
        self.put_object_from_authorized_write(
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
        self.put_object_from_authorized_write(req, authorized)
    }

    pub fn put_object_from_authorized_write(
        &self,
        req: &AuthorizedPutObjectCommitRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<PutObjectResult, ServerError> {
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
            let session_id = self.create_stream_put_session_for_authorized_write(authorized)?;
            let result = self.put_large_object_from_authorized_write_with_session(
                req,
                authorized,
                &session_id,
            );
            if result.is_err() {
                let _ = self.abort_stream_put_for(
                    authorized.bucket_typed(),
                    authorized.key_typed(),
                    &session_id,
                );
            }
            return result;
        }

        let request = BucketHandleRequest::new().requiring_lifecycle_view();
        self.with_bucket_write_handle_for(authorized, request, |bucket_handle| {
            let bucket_info = bucket_handle.bucket().clone();
            let write_encryption = &authorized.write_encryption;
            Self::ensure_sse_c_allowed(&bucket_info, write_encryption.is_sse_customer())?;
            let acl = authorized.acl();
            Self::ensure_put_object_write_acl_supported(&bucket_info, &acl)?;
            let resolved_object_lock = Self::resolve_new_object_lock_state(
                &bucket_info,
                authorized.requested_object_lock(),
            )?;

            let transient_segment_id =
                Self::random_session_id("failed to generate direct put segment ID")?;

            let segment_index = 0;
            let segment_okh = stream_segment_key_hash(&transient_segment_id, segment_index);
            let segment_vid = GenerationId::MIN;
            let shard_pg_id = self.shard_pg_id_raw(
                &format!("segment/{}", transient_segment_id.as_str()),
                &segment_index.to_string(),
                segment_vid.get(),
            );
            let storage_bytes = write_encryption.encrypt_segment(segment_index, req.data)?;

            let written_shards =
                self.write_segment_shards(shard_pg_id, &segment_okh, segment_vid, &storage_bytes)?;

            let meta_pg_id =
                self.object_pg_id_for(authorized.bucket_typed(), authorized.key_typed());
            let pgs = match self.lock_object_pgs_for_write_ids(meta_pg_id, shard_pg_id) {
                Ok(pgs) => pgs,
                Err(err) => {
                    self.best_effort_delete_written_shards(shard_pg_id, &written_shards);
                    return Err(err);
                }
            };
            let meta_pg = pgs.meta();
            let shard_pg = pgs.shard();
            let shard_batch: Vec<(&ShardKey, storage::WriteAck)> = written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            let system_metadata = Self::object_system_metadata_with_default_checksum(
                req.system_metadata,
                write_encryption,
                object_crc64,
            );

            let prepared = match self.prepare_put_commit_locked(
                meta_pg,
                &bucket_info,
                &PutCommitRequest {
                    bucket: authorized.bucket_typed(),
                    key: authorized.key_typed(),
                    metadata_blob: req.metadata,
                    system_metadata: &system_metadata,
                    write_encryption,
                    tags: authorized.tags(),
                    cond: req.cond,
                },
            ) {
                Ok(prepared) => prepared,
                Err(err) => {
                    Self::cleanup_written_shards_locked(shard_pg, &written_shards);
                    return Err(err);
                }
            };
            let owner =
                Self::effective_put_object_owner(&bucket_info, authorized.requester(), &acl);
            let acl_grants = Self::object_acl_grants_for_put_object(&bucket_info, &owner, &acl);

            let segment_record = ObjectSegmentRecord {
                bucket: authorized.bucket_typed().clone(),
                key: authorized.key_typed().clone(),
                version_id: prepared.version_id,
                segment_index,
                size: req.data.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(&storage_bytes)),
                segment_okh,
                segment_vid,
                shard_pg_id,
                ec_k: self.ec_config.data_shards,
                ec_m: self.ec_config.parity_shards,
            };
            let live_req = PutLiveObjectReq {
                bucket: authorized.bucket_typed().clone(),
                key: authorized.key_typed().clone(),
                version_id: prepared.version_id,
                owner,
                acl_grants: acl_grants.clone(),
                public_read: Self::acl_grants_public_read(&acl_grants),
                generation_id: prepared.generation_id,
                size: req.data.len() as u64,
                etag: storage::ObjectEtag::single_part(object_crc64),
                ec: EcShape {
                    k: self.ec_config.data_shards,
                    m: self.ec_config.parity_shards,
                },
                object_lock: resolved_object_lock,
                encryption: prepared.encryption.clone(),
                layout: ObjectLayout::Standard,
                tags: prepared.tags.clone(),
                metadata_blob: Some(prepared.metadata_blob.clone()),
                system_metadata_blob: Some(prepared.system_metadata_blob.clone()),
            };

            if let Err(err) = shard_pg.register_written_shards_batch(&shard_batch) {
                Self::cleanup_written_shards_locked(shard_pg, &written_shards);
                return Err(ServerError::Store(err));
            }
            if let Err(err) = meta_pg.put_object_with_segments(&live_req, &[segment_record]) {
                Self::cleanup_written_shards_locked(shard_pg, &written_shards);
                return Err(ServerError::Metadata(err));
            }
            Self::finalize_put_commit_metadata_locked(
                meta_pg,
                authorized.bucket_typed(),
                authorized.key_typed(),
                prepared.version_id,
                prepared.stale_payload.as_ref(),
            )?;
            let stored = storage::PgMetadataStore::get_object_meta(
                meta_pg,
                authorized.bucket_typed(),
                authorized.key_typed(),
            )
            .map_err(ServerError::Metadata)?;
            let live_record = stored.as_live().ok_or_else(|| ServerError::InternalError {
                reason: format!(
                    "stored object {} / {} is not live immediately after PutObject",
                    authorized.bucket(),
                    authorized.key()
                ),
            })?;
            let lifecycle_tags = live_record.tags.clone();
            let lifecycle_size = live_record.size;
            let lifecycle_last_modified = live_record.last_modified;

            drop(pgs);
            let lifecycle_expiration = self
                .current_object_write_lifecycle_expiration_for_loaded_bucket(
                    &bucket_handle,
                    authorized.key(),
                    lifecycle_tags.as_deref(),
                    lifecycle_size,
                    lifecycle_last_modified,
                )?;
            if let Some(ref payload) = prepared.stale_payload {
                self.delete_stale_object_payload(
                    authorized.bucket_typed(),
                    authorized.key_typed(),
                    payload,
                );
            }

            Ok(PutObjectResult {
                etag: format_etag(object_crc64),
                version_id: prepared.version_id,
                system_metadata,
                managed_encryption: prepared.encryption.managed_encryption_algorithm(),
                lifecycle_expiration,
            })
        })
    }

    fn put_large_object_from_authorized_write_with_session(
        &self,
        req: &AuthorizedPutObjectCommitRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        session_id: &SessionId,
    ) -> Result<PutObjectResult, ServerError> {
        let object_crc64 = checksum::crc64::checksum(req.data);
        let write_encryption = &authorized.write_encryption;
        for (idx, chunk) in req.data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
            let chunk_storage = write_encryption.encrypt_segment(idx as u32, chunk)?;
            self.append_stream_segment_for(
                authorized.bucket_typed(),
                authorized.key_typed(),
                session_id,
                idx as u32,
                &chunk_storage,
            )?;
        }
        self.finalize_stream_put_from_authorized_write(
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
        )
    }

    pub fn begin_stream_put(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<PreparedStreamPut, ServerError> {
        let session_id = Self::random_session_id("failed to generate session ID")?;
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.storage_node
            .create_put_object_stream_session(
                req.object.bucket.name_typed(),
                req.object.key_typed(),
                request.resolve_to_storage_request(),
                |snapshot, existing_object| {
                    let bucket = self
                        .bucket_handle_loader()
                        .load_bucket_handle_from_snapshot(
                            snapshot,
                            req.object.expected_bucket_owner(),
                            request,
                        )?;
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

    pub fn begin_stream_put_session(
        &self,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<SessionId, ServerError> {
        self.create_stream_put_session_for_authorized_write(authorized)
    }

    pub fn append_stream_put_data(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        data: &[u8],
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<(), ServerError> {
        let write_encryption =
            self.load_stream_put_write_encryption(bucket, key, session_id, sse_customer)?;
        let storage_data = write_encryption.encrypt_segment(segment_index, data)?;
        self.append_stream_segment_for(bucket, key, session_id, segment_index, &storage_data)
    }

    /// Finalize a streaming PutObject session.
    ///
    /// Locks the metadata PG, allocates a version_id, builds committed segment
    /// metadata from staging rows, and atomically commits the object via
    /// `commit_stream_put`.
    ///
    /// The caller passes the running CRC64 checksum, total size, and metadata
    /// blob computed during the append phase. No segment data is re-read.
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

    pub fn finalize_authorized_stream_put(
        &self,
        req: &AuthorizedFinalizeStreamPutRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<PutObjectResult, ServerError> {
        let write_encryption = self.load_stream_put_write_encryption(
            authorized.bucket_typed(),
            authorized.key_typed(),
            req.session_id,
            sse_customer,
        )?;
        self.finalize_stream_put_from_authorized_write(
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
        )
    }

    pub(super) fn finalize_stream_put_with_authorized_write_tags(
        &self,
        req: &AuthorizedFinalizeStreamPutRequest<'_>,
        authorized: &AuthorizedPutObjectWrite,
        tags: AuthorizedWriteTags<'_>,
    ) -> Result<PutObjectResult, ServerError> {
        let tags = match tags {
            AuthorizedWriteTags::Bound => authorized.tags(),
            AuthorizedWriteTags::TrustedDerived(tags) => tags,
        };
        self.finalize_stream_put(&FinalizeStreamPutRequest {
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
        })
    }

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
        let key = req.object.key();
        let session_id = req.session_id;
        let crc64 = req.crc64;
        let total_size = req.total_size;
        let metadata_blob = req.metadata_blob;
        let tags = req.tags;
        let cond = req.cond;
        let request = BucketHandleRequest::new().requiring_lifecycle_view();
        self.with_bucket_write_handle_for(&req.object, request, |bucket_handle| {
            let bucket_info = bucket_handle.bucket().clone();
            Self::ensure_put_object_write_acl_supported(&bucket_info, &req.acl)?;
            let resolved_object_lock =
                Self::resolve_new_object_lock_state(&bucket_info, req.requested_object_lock)?;
            let outcome = self
                .storage_node
                .finalize_put_object_stream(
                    req.object.bucket_name_typed(),
                    req.object.key_typed(),
                    session_id,
                    total_size,
                    |snapshot: StreamPutFinalizeSnapshot| {
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
                            Self::prepare_stored_system_metadata(
                                &system_metadata,
                                &write_encryption,
                            )?;
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
                            ec: EcShape {
                                k: self.ec_config.data_shards,
                                m: self.ec_config.parity_shards,
                            },
                            owner,
                            acl_grants: acl_grants.clone(),
                            public_read: Self::acl_grants_public_read(&acl_grants),
                            size: total_size,
                            etag_crc64: crc64,
                            tags: tags.map(storage::SerializedTagSet::from),
                            metadata_blob,
                            system_metadata_blob,
                            object_lock: resolved_object_lock,
                            encryption,
                        })
                    },
                )
                .map_err(|error| match error {
                    storage::ObjectPgActionError::Store(error) => ServerError::Store(error),
                    storage::ObjectPgActionError::InvalidRequest { reason } => {
                        ServerError::InvalidRequest { reason }
                    }
                    storage::ObjectPgActionError::Metadata(error) => ServerError::Metadata(error),
                })??;
            let lifecycle_expiration = self
                .current_object_write_lifecycle_expiration_for_loaded_bucket(
                    &bucket_handle,
                    key,
                    outcome.live_tags.as_deref(),
                    outcome.live_size,
                    outcome.live_last_modified,
                )?;
            if let Some(generation_id) = outcome.stale_generation_id {
                self.read_runtime().enqueue_object_payload_reclaim_for(
                    req.object.bucket_name_typed(),
                    req.object.key_typed(),
                    generation_id,
                );
            }

            Ok(PutObjectResult {
                etag: format_etag(crc64),
                version_id: outcome.version_id,
                system_metadata: outcome.value,
                managed_encryption: outcome.encryption.managed_encryption_algorithm(),
                lifecycle_expiration,
            })
        })
    }

    pub fn abort_stream_put_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ServerError> {
        self.abort_stream_put_for(bucket, key, session_id)
    }
}
