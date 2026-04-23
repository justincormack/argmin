use super::*;

impl SharedStorageNode {
    pub(super) fn load_multipart_upload_from_object_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, crate::error::MetadataError> {
        let upload = pg.get_multipart_upload(upload_id)?;
        if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
            return Err(crate::error::MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        Ok(upload)
    }

    pub fn load_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, BucketSnapshotLoadError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Ok(Self::load_multipart_upload_from_object_pg(
            &pg, bucket, key, upload_id,
        )?)
    }

    pub fn begin_upload_part_stream_session<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
        session_id: &SessionId,
        action: impl FnOnce(&MultipartUploadRecord) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let upload = Self::load_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        let result = action(&upload);
        if result.is_ok() {
            pg.create_stream_upload(&CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                },
                encryption: upload.encryption.clone(),
            })?;
        }
        Ok(result)
    }

    pub fn load_in_progress_multipart_upload_for_completion(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let upload = Self::load_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        if upload.state != UploadState::InProgress {
            return Err(crate::error::MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        Ok(upload)
    }

    pub fn finalize_upload_part_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        action: impl FnOnce(StreamUploadPartSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let session = pg.get_stream_upload(session_id)?;
        Self::validate_upload_part_stream_session(&session, bucket, key, upload_id, part_number)?;
        let upload = Self::load_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        let existing_part_generation = match pg.get_multipart_part(upload_id, part_number) {
            Ok(existing) => Some(existing.generation),
            Err(crate::error::MetadataError::PartNotFound { .. }) => None,
            Err(other) => return Err(other.into()),
        };
        let staging_segments = pg.list_stream_segments(session_id)?;
        match action(StreamUploadPartSnapshot {
            session,
            upload: upload.clone(),
            existing_part_generation,
            staging_segments,
        }) {
            Ok(prepared) => {
                let displaced_segments =
                    pg.commit_stream_part(session_id, &prepared.part, &prepared.segments)?;
                Ok(Ok(FinalizeStreamPartOutcome {
                    value: prepared.value,
                    upload,
                    generation: prepared.part.generation,
                    displaced_segments,
                }))
            }
            Err(error) => Ok(Err(error)),
        }
    }

    pub fn list_multipart_parts_for_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let upload = Self::load_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        let response = pg.list_multipart_parts(&ListPartsReq {
            upload_id: upload_id.clone(),
            part_number_marker,
            max_parts,
        })?;
        Ok(ListedMultipartParts { upload, response })
    }

    pub fn lookup_abort_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<AbortMultipartUploadLookup, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        match Self::load_multipart_upload_from_object_pg(&pg, bucket, key, upload_id) {
            Ok(upload) => Ok(AbortMultipartUploadLookup::InProgress(Box::new(upload))),
            Err(crate::error::MetadataError::NoSuchUpload { .. }) => {
                let Some(completed) = pg.get_completed_multipart_upload(upload_id)? else {
                    return Err(crate::error::MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    }
                    .into());
                };
                if completed.bucket != *bucket || completed.key != *key {
                    return Err(crate::error::MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    }
                    .into());
                }
                Ok(AbortMultipartUploadLookup::Completed(completed))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn delete_segment_shard_set(
        &self,
        shard_pg_id: u32,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        ec_k: u8,
        ec_m: u8,
    ) -> Result<(), ObjectPgActionError> {
        let pg = self.get_pg(shard_pg_id)?;
        let total = ec_k as usize + ec_m as usize;
        for i in 0..total {
            let shard_key = ShardKey::new(segment_okh, segment_vid.get(), i as u8);
            pg.delete_shard(&shard_key)?;
        }
        Ok(())
    }

    fn delete_streaming_segment_shards(
        &self,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), ObjectPgActionError> {
        for segment in segments {
            self.delete_segment_shard_set(
                segment.shard_pg_id,
                &segment.segment_okh,
                segment.segment_vid,
                segment.ec_k,
                segment.ec_m,
            )?;
        }
        Ok(())
    }

    fn delete_multipart_part_shards_best_effort(&self, parts: &[MultipartPartRecord]) {
        for part in parts {
            if part.part_okh == [0u8; 16] {
                continue;
            }
            let multipart_bucket = format!("mpu/{}", part.upload_id);
            let multipart_key = format!("{}/{}", part.part_number, part.generation);
            let shard_pg_id = self.pg_topology.shard_pg(
                multipart_bucket.as_str(),
                multipart_key.as_str(),
                part.part_vid.get(),
            );
            let Ok(shard_pg) = self.get_pg(shard_pg_id) else {
                continue;
            };
            let total = part.ec_k as usize + part.ec_m as usize;
            for i in 0..total {
                let shard_key = ShardKey::new(&part.part_okh, part.part_vid.get(), i as u8);
                let _ = shard_pg.delete_shard(&shard_key);
            }
        }
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ObjectPgActionError> {
        let (parts, streaming_segments) = {
            let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
            match Self::load_multipart_upload_from_object_pg(&pg, bucket, key, upload_id) {
                Ok(_) => {}
                Err(crate::error::MetadataError::NoSuchUpload { .. }) => return Ok(false),
                Err(error) => return Err(error.into()),
            }

            match pg.set_upload_state(upload_id, UploadState::Aborting) {
                Ok(()) => {}
                Err(crate::error::MetadataError::UploadNotInProgress { state })
                    if state == UploadState::Aborting as u8 => {}
                Err(crate::error::MetadataError::UploadNotInProgress { .. }) => return Ok(false),
                Err(error) => return Err(error.into()),
            }

            let parts = pg
                .list_multipart_parts(&ListPartsReq {
                    upload_id: upload_id.clone(),
                    part_number_marker: None,
                    max_parts: u32::MAX,
                })?
                .parts;
            let streaming_segments = pg.get_all_multipart_part_segments_for_upload(upload_id)?;
            (parts, streaming_segments)
        };

        self.delete_multipart_part_shards_best_effort(&parts);
        if !streaming_segments.is_empty() {
            self.delete_streaming_segment_shards(&streaming_segments)?;
        }

        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        if !streaming_segments.is_empty() {
            pg.delete_multipart_part_segments_by_upload_id(upload_id)?;
        }
        match pg.delete_multipart_upload(upload_id) {
            Ok(()) => Ok(true),
            Err(crate::error::MetadataError::NoSuchUpload { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn validate_upload_part_stream_session(
        session: &crate::StreamUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<(), ObjectPgActionError> {
        if session.state != StreamUploadState::InProgress {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "stream session is not in progress".to_string(),
            });
        }
        if session.bucket != bucket.as_str() || session.key != key.as_str() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }
        match &session.target {
            StreamUploadTarget::UploadPart {
                upload_id: sess_upload_id,
                part_number: sess_part_number,
            } if sess_upload_id == upload_id && *sess_part_number == part_number => Ok(()),
            StreamUploadTarget::UploadPart { .. } => Err(ObjectPgActionError::InvalidRequest {
                reason: "session upload_id/part_number mismatch".to_string(),
            }),
            StreamUploadTarget::PutObject => Err(ObjectPgActionError::InvalidRequest {
                reason: "session is not an UploadPart session".to_string(),
            }),
        }
    }
}
