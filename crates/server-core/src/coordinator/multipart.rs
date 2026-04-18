use super::*;

impl Coordinator {
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
        let AuthorizedCreateMultipartUpload {
            bucket_info,
            bucket,
            key,
            tags,
            checksum,
            initiator,
            owner,
            acl_grants,
            public_read,
            object_lock,
            write_encryption,
        } = self.authorize_create_multipart_upload(req)?;

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
        let meta_pg_id = self.object_pg_id_for(&bucket, &key);
        let pg = self.storage_node.get_pg(meta_pg_id)?;
        pg.create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: typed_upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: tags.as_deref().map(SerializedTagSet::from),
            metadata_blob: SerializedMetadataBlob::from(metadata_blob),
            system_metadata_blob: SerializedSystemMetadataBlob::from(system_metadata_blob),
            initiator,
            owner,
            acl_grants,
            public_read,
            object_lock,
            checksum,
            encryption: write_encryption.object_encryption(),
        })?;
        let upload = pg.get_multipart_upload(&typed_upload_id)?;
        let initiated_at = upload.initiated_at;
        drop(pg);
        let lifecycle_abort =
            self.multipart_lifecycle_abort_headers(&bucket_info, key.as_str(), initiated_at)?;

        Ok(CreateMultipartUploadResult {
            upload_id: typed_upload_id,
            managed_encryption: upload.encryption.managed_encryption_algorithm(),
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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::complete_multipart_upload",
            "bucket={:?} key={:?} upload_id={:?} parts={}",
            req.upload.bucket_name(),
            req.upload.key(),
            req.upload.upload_id(),
            req.parts.len()
        );
        let AuthorizedCompleteMultipartUpload {
            bucket_info,
            bucket,
            key,
            upload_id,
            upload,
            multipart_write_encryption,
        } = self.authorize_complete_multipart_upload(req)?;
        let parts = req.parts;
        let claimed_checksum = req.claimed_checksum;
        let expected_object_size = req.expected_object_size;
        let _completion_guard = self.storage_node.lock_multipart_completion_bucket(&bucket);
        let completion_order =
            self.next_completed_multipart_upload_order_for_bucket_name(&bucket)?;
        let meta_pg_id = self.object_pg_id_for(&bucket, &key);
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
        let current_upload = meta_pg.get_multipart_upload(&upload_id)?;
        if current_upload.bucket != bucket.as_str() || current_upload.key != key.as_str() {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        if current_upload.state != UploadState::InProgress {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }

        if !req.cond.is_empty() {
            let existing_etag =
                match storage::PgMetadataStore::get_object_meta(&*meta_pg, &bucket, &key) {
                    Ok(stored) => stored.as_live().map(|record| record.etag.format()),
                    Err(storage::MetadataError::ObjectNotFound) => None,
                    Err(e) => return Err(ServerError::Metadata(e)),
                };
            if matches!(req.cond, WriteCondition::IfMatch(_)) && existing_etag.is_none() {
                return Err(ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
            check_write_conditions(req.cond, existing_etag.as_deref())?;
        }
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

        let checksum_algo = upload.checksum.map(MultipartChecksumConfig::algorithm);
        let checksum_type = upload.checksum.map(MultipartChecksumConfig::checksum_type);

        let mut part_records: Vec<MultipartPartRecord> = Vec::with_capacity(parts.len());
        for cp in parts {
            if let (Some(upload_algo), Some(ChecksumType::Composite), None) =
                (checksum_algo, checksum_type, cp.checksum.as_ref())
            {
                return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "The upload was created using a {} checksum. The complete request must include the checksum for each part. It was missing for part {} in the request.",
                        upload_algo.as_str(),
                        cp.part_number
                    ),
                });
            }
            let part = match meta_pg.get_multipart_part(&upload_id, cp.part_number) {
                Ok(p) => p,
                Err(storage::MetadataError::PartNotFound { .. }) => {
                    return Err(ServerError::InvalidPart {
                        part_number: cp.part_number,
                    });
                }
                Err(e) => return Err(ServerError::Metadata(e)),
            };

            let stored_etag = etag_bytes_to_crc64(&part.etag)
                .map(format_etag)
                .unwrap_or_default();
            if stored_etag != cp.etag {
                return Err(ServerError::InvalidPart {
                    part_number: cp.part_number,
                });
            }

            if let Some(ref claim) = cp.checksum {
                if let Some(upload_algo) = checksum_algo {
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
                            return Err(ServerError::InvalidRequest {
                                reason: "part checksum mismatch".to_string(),
                            });
                        }
                    }
                    None => {
                        return Err(ServerError::InvalidRequest {
                            reason: "part checksum mismatch".to_string(),
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

        let version_id = if bucket_info.versioning == BucketVersioningState::Enabled {
            storage::PgMetadataStore::next_version_id(&*meta_pg, &bucket, &key)?
        } else {
            VersionId::Null
        };
        let generation_id = storage::PgMetadataStore::next_generation_id(&*meta_pg, &bucket, &key)?;
        let stale_payload = if version_id.is_null() {
            Self::snapshot_overwritten_null_version_payload(&meta_pg, &bucket, &key)?
        } else {
            None
        };

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

        let checksum_value = if let (Some(algo), Some(ctype)) = (checksum_algo, checksum_type) {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD;
            match ctype {
                ChecksumType::Composite => {
                    let mut concat = Vec::new();
                    for part in &part_records {
                        match &part.checksum {
                            Some(bytes) => concat.extend_from_slice(bytes.as_slice()),
                            None => {
                                return Err(ServerError::InvalidRequest {
                                    reason:
                                        "COMPOSITE checksum requires all parts to have checksums"
                                            .to_string(),
                                });
                            }
                        }
                    }
                    let hash = compute_checksum(algo, &concat);
                    Some(format!(
                        "{}-{}",
                        b64.encode(hash.bytes()),
                        part_records.len()
                    ))
                }
                ChecksumType::FullObject => {
                    match algo {
                        ChecksumAlgorithm::Crc32 => {
                            let mut combined: u32 = 0;
                            for part in &part_records {
                                let bytes = part.checksum.as_ref().ok_or_else(|| {
                                ServerError::InvalidRequest {
                                    reason: "FULL_OBJECT checksum requires all parts to have checksums"
                                        .to_string(),
                                }
                            })?;
                                let part_crc =
                                    u32::from_be_bytes(bytes.as_slice().try_into().map_err(
                                        |_| ServerError::InvalidRequest {
                                            reason: "invalid CRC32 checksum length".to_string(),
                                        },
                                    )?);
                                combined = checksum::crc32::combine(combined, part_crc, part.size);
                            }
                            Some(b64.encode(combined.to_be_bytes()))
                        }
                        ChecksumAlgorithm::Crc32c => {
                            let mut combined: u32 = 0;
                            for part in &part_records {
                                let bytes = part.checksum.as_ref().ok_or_else(|| {
                                ServerError::InvalidRequest {
                                    reason: "FULL_OBJECT checksum requires all parts to have checksums"
                                        .to_string(),
                                }
                            })?;
                                let part_crc =
                                    u32::from_be_bytes(bytes.as_slice().try_into().map_err(
                                        |_| ServerError::InvalidRequest {
                                            reason: "invalid CRC32C checksum length".to_string(),
                                        },
                                    )?);
                                combined = checksum::crc32c::combine(combined, part_crc, part.size);
                            }
                            Some(b64.encode(combined.to_be_bytes()))
                        }
                        ChecksumAlgorithm::Crc64nvme => {
                            let mut combined: u64 = 0;
                            for part in &part_records {
                                let bytes = part.checksum.as_ref().ok_or_else(|| {
                                ServerError::InvalidRequest {
                                    reason: "FULL_OBJECT checksum requires all parts to have checksums"
                                        .to_string(),
                                }
                            })?;
                                let part_crc =
                                    u64::from_be_bytes(bytes.as_slice().try_into().map_err(
                                        |_| ServerError::InvalidRequest {
                                            reason: "invalid CRC64NVME checksum length".to_string(),
                                        },
                                    )?);
                                combined = checksum::crc64::combine(combined, part_crc, part.size);
                            }
                            Some(b64.encode(combined.to_be_bytes()))
                        }
                        ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => {
                            return Err(ServerError::InternalError {
                                reason: format!(
                                    "FULL_OBJECT checksum type is not supported for {}",
                                    algo.as_str()
                                ),
                            });
                        }
                    }
                }
            }
        } else {
            None
        };

        if let Some(claimed) = claimed_checksum {
            match checksum_algo {
                Some(upload_algo) if claimed.algorithm() != upload_algo => {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "checksum header algorithm {} does not match upload algorithm {}",
                            claimed.algorithm().as_str(),
                            upload_algo.as_str()
                        ),
                    });
                }
                None => {
                    return Err(ServerError::InvalidRequest {
                        reason: "checksum header sent but upload has no checksum algorithm"
                            .to_string(),
                    });
                }
                _ => {}
            }
            if let Some(ref computed) = checksum_value {
                if computed != claimed.encoded_value() {
                    return Err(ServerError::InvalidRequest {
                        reason: "checksum mismatch".to_string(),
                    });
                }
            }
        }

        let mut system_metadata =
            SystemMetadata::deserialize(upload.system_metadata_blob.as_slice())?;
        if let (Some(algo), Some(ref val)) = (checksum_algo, &checksum_value) {
            system_metadata.set_checksum(algo, checksum_type, val.clone());
        }
        self.ensure_write_encryption_supported(&upload.encryption)?;
        let (system_metadata_bytes, final_encryption) =
            Self::prepare_stored_system_metadata(&system_metadata, &multipart_write_encryption)?;
        let managed_encryption = final_encryption.managed_encryption_algorithm();

        let obj_req = CommitMultipartReq {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            owner: upload.owner.clone(),
            acl_grants: upload.acl_grants.clone(),
            public_read: upload.public_read,
            generation_id,
            size: total_size,
            etag_crc64,
            ec: EcShape { k: 0, m: 0 },
            tags: upload.tags.clone(),
            metadata_blob: Some(upload.metadata_blob.clone()),
            system_metadata_blob: Some(system_metadata_bytes),
            object_lock: Self::resolve_new_object_lock_state(&bucket_info, upload.object_lock)?,
            encryption: final_encryption,
        };

        let object_parts: Vec<ObjectPartRecord> = part_records
            .iter()
            .map(|p| {
                let shard_pg_id = self.shard_pg_id_raw(
                    &format!("mpu/{}", p.upload_id),
                    &format!("{}/{}", p.part_number, p.generation),
                    p.part_vid.get(),
                );
                ObjectPartRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id,
                    part_number: p.part_number,
                    size: p.size,
                    etag: p.etag.clone(),
                    etag_kind: p.etag_kind,
                    part_okh: p.part_okh,
                    part_vid: p.part_vid,
                    ec_k: p.ec_k,
                    ec_m: p.ec_m,
                    shard_pg_id,
                    checksum: p.checksum.clone(),
                }
            })
            .collect();

        drop(meta_pg);

        #[cfg(test)]
        maybe_run_multipart_complete_pre_commit_hook(bucket.as_str(), key.as_str());

        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
        meta_pg
            .complete_multipart_commit(&upload_id, completion_order, &obj_req, &object_parts)
            .map_err(ServerError::Metadata)?;
        let stored = storage::PgMetadataStore::get_object_meta(&*meta_pg, &bucket, &key)
            .map_err(ServerError::Metadata)?;
        let live_record = stored.as_live().ok_or_else(|| ServerError::InternalError {
            reason: format!(
                "stored object {} / {} is not live immediately after CompleteMultipartUpload",
                bucket, key
            ),
        })?;
        let lifecycle_tags = live_record.tags.clone();
        let lifecycle_size = live_record.size;
        let lifecycle_last_modified = live_record.last_modified;

        if let Some(ref payload) = stale_payload {
            match payload {
                StaleObjectPayload::Multipart {
                    generation_id,
                    parts,
                    streaming_segments,
                } => {
                    Self::enqueue_multipart_reclaim(
                        &meta_pg,
                        &bucket,
                        &key,
                        *generation_id,
                        parts,
                        streaming_segments,
                    )?;
                }
                StaleObjectPayload::Segments { .. } => {
                    Self::delete_stale_object_payload_metadata(
                        &meta_pg, &bucket, &key, version_id, payload,
                    )?;
                }
            }
        }

        drop(meta_pg);
        self.prune_completed_multipart_uploads_for_bucket_with_limit(
            bucket.as_str(),
            COMPLETED_MULTIPART_UPLOADS_PER_BUCKET_LIMIT,
        )?;
        let lifecycle_expiration = self.current_object_write_lifecycle_expiration(
            &bucket_info,
            key.as_str(),
            lifecycle_tags.as_deref(),
            lifecycle_size,
            lifecycle_last_modified,
        )?;
        if let Some(ref payload) = stale_payload {
            self.delete_stale_object_payload(&bucket, &key, payload);
        }

        Ok(CompleteMultipartUploadResult {
            etag: etag_str,
            version_id,
            managed_encryption,
            checksum_algorithm: checksum_algo,
            checksum_type,
            checksum_value,
            lifecycle_expiration,
        })
    }

    /// Abort an in-progress multipart upload.
    ///
    /// Transitions to Aborting, best-effort deletes all part shard sets,
    /// then deletes the upload and part metadata rows.
    pub fn abort_multipart_upload(&self, req: &MultipartObjectRequest) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::abort_multipart_upload",
            "bucket={:?} key={:?} upload_id={:?}",
            req.object.bucket_name(),
            req.object.key,
            req.upload_id()
        );
        match self.authorize_abort_multipart_upload(req)? {
            AuthorizedAbortMultipartUpload::Completed => Ok(()),
            AuthorizedAbortMultipartUpload::InProgress {
                bucket,
                key,
                upload_id,
            } => {
                if self
                    .read_runtime()
                    .abort_multipart_upload_internal_for(&bucket, &key, &upload_id)?
                {
                    Ok(())
                } else {
                    Err(ServerError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
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
        let part_number_marker = req.part_number_marker;
        let max_parts = req.max_parts;
        let AuthorizedListParts {
            bucket_info,
            key,
            upload,
            meta_pg,
        } = self.authorize_list_parts(req)?;

        let resp = meta_pg
            .list_multipart_parts(&ListPartsReq {
                upload_id: upload.upload_id.clone(),
                part_number_marker,
                max_parts,
            })
            .map_err(ServerError::Metadata)?;
        let upload_initiated_at = upload.initiated_at;
        let checksum_algorithm = upload.checksum.map(MultipartChecksumConfig::algorithm);
        let checksum_type = upload.checksum.map(MultipartChecksumConfig::checksum_type);
        drop(meta_pg);

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
                key.as_str(),
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

        let mut all_uploads: Vec<MultipartUploadRecord> = Vec::new();
        let mut hit_record_cap = false;
        self.pg_topology.for_each_pg(|pg_id| {
            if hit_record_cap {
                return Ok::<(), ServerError>(());
            }
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: optional_list_object_key(prefix)?,
                key_marker: optional_list_object_key(key_marker)?,
                upload_id_marker: upload_id_marker.cloned(),
                max_uploads: max_uploads.saturating_add(1),
            })?;
            all_uploads.extend(resp.uploads);
            if all_uploads.len() >= MAX_LIST_RECORDS {
                all_uploads.truncate(MAX_LIST_RECORDS);
                hit_record_cap = true;
            }
            Ok::<(), ServerError>(())
        })?;

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
}
