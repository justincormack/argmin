use super::*;
use crate::{
    BucketVersioningState, CommitDirectPutObjectReq, CommitStreamPutReq, DirectPutCommitSnapshot,
    DirectPutWrittenSegment, EcShape, FinalizeDirectPutObjectOutcome, FinalizeStreamPutOutcome,
    MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord, MultipartReclaimRecord,
    ObjectEtag, ObjectLayout, ObjectPartRecord, ObjectSegmentRecord, ObjectSegmentsReclaimRecord,
    ObjectSegmentsReclaimSegmentRecord, PrepareStreamUploadSegmentAppendReq,
    PreparedStreamPutCommit, PutLiveObjectReq, StreamPutFinalizeSnapshot, StreamUploadRecord,
    StreamUploadSegmentRecord, VersionId, WrittenShardAck,
};

#[derive(Debug, Clone)]
enum StaleObjectPayloadMetadata {
    Segments {
        generation_id: GenerationId,
        segments: Vec<ObjectSegmentRecord>,
    },
    Multipart {
        generation_id: GenerationId,
        parts: Vec<ObjectPartRecord>,
        streaming_segments: Vec<MultipartPartSegmentRecord>,
    },
}

impl SharedStorageNode {
    pub fn write_direct_put_segment_shards(
        &self,
        transient_segment_id: &SessionId,
        segment_index: u32,
        segment_vid: GenerationId,
        segment_okh: &[u8; 16],
        shard_payloads: &[&[u8]],
    ) -> Result<DirectPutWrittenSegment, StoreError> {
        let shard_pg_id = self.pg_topology.shard_pg(
            &format!("segment/{}", transient_segment_id.as_str()),
            &segment_index.to_string(),
            segment_vid.get(),
        );
        let mut shard_batch: Vec<(ShardKey, &[u8])> = Vec::with_capacity(shard_payloads.len());
        for (shard_index, shard_payload) in shard_payloads.iter().enumerate() {
            shard_batch.push((
                ShardKey::new(segment_okh, segment_vid.get(), shard_index as u8),
                *shard_payload,
            ));
        }
        let written_shards = self
            .write_shard_files(shard_pg_id, &shard_batch)?
            .into_iter()
            .map(|(key, ack)| WrittenShardAck { key, ack })
            .collect();
        Ok(DirectPutWrittenSegment {
            shard_pg_id,
            written_shards,
        })
    }

    pub fn commit_direct_put_object<E>(
        &self,
        req: &CommitDirectPutObjectReq,
        written_shards: &[WrittenShardAck],
        action: impl FnOnce(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, ObjectPgActionError> {
        let meta_pg_id = self.pg_topology.object_pg_for(&req.bucket, &req.key);
        let (meta_pg, shard_pg) = match self.lock_two_pgs(meta_pg_id, req.shard_pg_id) {
            Ok(guards) => guards,
            Err(error) => {
                let shard_keys: Vec<ShardKey> = written_shards
                    .iter()
                    .map(|written| written.key.clone())
                    .collect();
                self.delete_shards_best_effort(req.shard_pg_id, &shard_keys);
                return Err(ObjectPgActionError::Store(error));
            }
        };
        let shard_pg = shard_pg.as_deref().unwrap_or(&meta_pg);
        let shard_keys: Vec<ShardKey> = written_shards
            .iter()
            .map(|written| written.key.clone())
            .collect();

        let existing_etag = match PgMetadataStore::get_object_meta(&*meta_pg, &req.bucket, &req.key)
        {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => {
                Self::cleanup_shards_locked(shard_pg, &shard_keys);
                return Err(other.into());
            }
        };

        if let Err(error) = action(DirectPutCommitSnapshot { existing_etag }) {
            Self::cleanup_shards_locked(shard_pg, shard_keys.as_slice());
            return Ok(Err(error));
        }

        let version_id = if req.versioning == BucketVersioningState::Enabled {
            match PgMetadataStore::next_version_id(&*meta_pg, &req.bucket, &req.key) {
                Ok(version_id) => version_id,
                Err(error) => {
                    Self::cleanup_shards_locked(shard_pg, shard_keys.as_slice());
                    return Err(error.into());
                }
            }
        } else {
            VersionId::Null
        };
        let generation_id =
            match PgMetadataStore::next_generation_id(&*meta_pg, &req.bucket, &req.key) {
                Ok(generation_id) => generation_id,
                Err(error) => {
                    Self::cleanup_shards_locked(shard_pg, shard_keys.as_slice());
                    return Err(error.into());
                }
            };
        let stale_payload = if version_id.is_null() {
            match Self::snapshot_overwritten_null_version_payload_from_object_pg(
                &meta_pg,
                &req.bucket,
                &req.key,
            ) {
                Ok(stale_payload) => stale_payload,
                Err(error) => {
                    Self::cleanup_shards_locked(shard_pg, shard_keys.as_slice());
                    return Err(error.into());
                }
            }
        } else {
            None
        };

        let shard_batch: Vec<(&ShardKey, WriteAck)> = written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        if let Err(err) = shard_pg.register_written_shards_batch(&shard_batch) {
            Self::cleanup_shards_locked(shard_pg, shard_keys.as_slice());
            return Err(err.into());
        }

        let segment_record = ObjectSegmentRecord {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            version_id,
            segment_index: req.segment_index,
            size: req.size,
            segment_crc64: req.segment_crc64,
            segment_okh: req.segment_okh,
            segment_vid: req.segment_vid,
            shard_pg_id: req.shard_pg_id,
            ec_k: req.ec.k,
            ec_m: req.ec.m,
        };
        let live_req = PutLiveObjectReq {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            version_id,
            owner: req.owner.clone(),
            acl_grants: req.acl_grants.clone(),
            public_read: req.public_read,
            generation_id,
            size: req.size,
            etag: ObjectEtag::single_part(req.etag_crc64),
            ec: req.ec,
            layout: ObjectLayout::Standard,
            tags: req.tags.clone(),
            metadata_blob: Some(req.metadata_blob.clone()),
            system_metadata_blob: Some(req.system_metadata_blob.clone()),
            object_lock: req.object_lock,
            encryption: req.encryption.clone(),
        };

        if let Err(err) = meta_pg.put_object_with_segments(&live_req, &[segment_record]) {
            Self::cleanup_shards_locked(shard_pg, shard_keys.as_slice());
            return Err(err.into());
        }
        Self::finalize_stream_put_stale_payload_metadata(
            &meta_pg,
            &req.bucket,
            &req.key,
            version_id,
            stale_payload.as_ref(),
        )?;

        let stored = PgMetadataStore::get_object_meta(&*meta_pg, &req.bucket, &req.key)?;
        let live_record = stored
            .as_live()
            .ok_or_else(|| crate::error::MetadataError::Db {
                context: "stored object missing live record after direct put",
                source: rusqlite::Error::QueryReturnedNoRows,
            })?;

        Ok(Ok(FinalizeDirectPutObjectOutcome {
            version_id,
            encryption: req.encryption.clone(),
            live_tags: live_record.tags.clone(),
            live_size: live_record.size,
            live_last_modified: live_record.last_modified,
            stale_generation_id: stale_payload
                .as_ref()
                .map(|payload| match payload {
                    StaleObjectPayloadMetadata::Segments { generation_id, .. }
                    | StaleObjectPayloadMetadata::Multipart { generation_id, .. } => generation_id,
                })
                .copied(),
        }))
    }

    fn validate_stream_upload_session_binding(
        session: &StreamUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
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
        Ok(())
    }

    fn reject_duplicate_stream_segment_index(
        pg: &PgStore,
        session_id: &SessionId,
        segment_index: u32,
    ) -> Result<(), ObjectPgActionError> {
        let existing_segments = pg.list_stream_segments(session_id)?;
        if existing_segments
            .iter()
            .any(|segment| segment.segment_index == segment_index)
        {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: format!("duplicate segment_index {segment_index}"),
            });
        }
        Ok(())
    }

    fn cleanup_shards_locked(shard_pg: &PgStore, shard_keys: &[ShardKey]) {
        for shard_key in shard_keys {
            let _ = shard_pg.delete_shard(shard_key);
        }
    }

    fn stream_segment_shard_pg_id(
        &self,
        session_id: &SessionId,
        segment_index: u32,
        segment_vid: GenerationId,
    ) -> u32 {
        self.pg_topology.shard_pg(
            format!("segment/{}", session_id.as_str()).as_str(),
            segment_index.to_string().as_str(),
            segment_vid.get(),
        )
    }

    pub fn load_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let session = pg.get_stream_upload(session_id)?;
        Self::validate_stream_upload_session_binding(&session, bucket, key)?;
        Ok(session)
    }

    pub fn prepare_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: &PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(StreamUploadTarget, StreamUploadSegmentRecord), ObjectPgActionError> {
        let meta_pg_id = self.pg_topology.object_pg_for(bucket, key);
        let pg = self.get_pg(meta_pg_id)?;
        let session = pg.get_stream_upload(&request.session_id)?;
        Self::validate_stream_upload_session_binding(&session, bucket, key)?;
        Self::reject_duplicate_stream_segment_index(
            &pg,
            &request.session_id,
            request.segment_index,
        )?;
        let segment_vid = pg.allocate_stream_segment_vid(&request.session_id)?;
        let segment_record = StreamUploadSegmentRecord {
            session_id: request.session_id.clone(),
            segment_index: request.segment_index,
            size: request.size,
            segment_crc64: request.segment_crc64,
            segment_okh: request.segment_okh,
            segment_vid,
            shard_pg_id: self.stream_segment_shard_pg_id(
                &request.session_id,
                request.segment_index,
                segment_vid,
            ),
            ec_k: request.ec.k,
            ec_m: request.ec.m,
        };
        Ok((session.target, segment_record))
    }

    pub fn commit_stream_segment_append(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        segment_index: u32,
        segment_record: &StreamUploadSegmentRecord,
        shard_batch: &[(&ShardKey, WriteAck)],
    ) -> Result<(), ObjectPgActionError> {
        let meta_pg_id = self.pg_topology.object_pg_for(bucket, key);
        let (meta_pg, shard_pg) = self.lock_two_pgs(meta_pg_id, segment_record.shard_pg_id)?;
        let shard_keys: Vec<ShardKey> = shard_batch
            .iter()
            .map(|(shard_key, _)| (*shard_key).clone())
            .collect();

        let cleanup = |shard_pg: &PgStore| Self::cleanup_shards_locked(shard_pg, &shard_keys);
        let cleanup_target = shard_pg.as_deref().unwrap_or(&*meta_pg);

        let session = match meta_pg.get_stream_upload(session_id) {
            Ok(session) => session,
            Err(err) => {
                cleanup(cleanup_target);
                return Err(err.into());
            }
        };
        if let Err(err) = Self::validate_stream_upload_session_binding(&session, bucket, key) {
            cleanup(cleanup_target);
            return Err(err);
        }
        if let Err(err) =
            Self::reject_duplicate_stream_segment_index(&meta_pg, session_id, segment_index)
        {
            cleanup(cleanup_target);
            return Err(err);
        }

        match shard_pg {
            None => {
                if let Err(err) = meta_pg
                    .register_written_shards_and_append_stream_segment(shard_batch, segment_record)
                {
                    cleanup(&meta_pg);
                    return Err(err.into());
                }
            }
            Some(shard_pg) => {
                if let Err(err) = shard_pg.register_written_shards_batch(shard_batch) {
                    cleanup(&shard_pg);
                    return Err(err.into());
                }
                if let Err(err) = meta_pg.append_stream_segment(segment_record) {
                    cleanup(&shard_pg);
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }

    pub fn create_put_object_stream_session_record(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        encryption: crate::types::ObjectEncryption,
    ) -> Result<(), ObjectPgActionError> {
        let object_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        object_pg.create_stream_upload(&CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::PutObject,
            encryption,
        })?;
        Ok(())
    }

    pub fn create_put_object_stream_session<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_reservation_snapshot(bucket, request, |snapshot| {
            let object_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
            let existing_object =
                Self::load_existing_live_object_from_object_pg(&object_pg, bucket, key)?;
            let result = match action(snapshot, existing_object) {
                Ok((value, create)) => {
                    object_pg.create_stream_upload(&create)?;
                    Ok(value)
                }
                Err(error) => Err(error),
            };
            Ok(result)
        })
    }

    pub fn abort_stream_upload_session(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<(), ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let session = pg.get_stream_upload(session_id)?;
        if session.bucket != bucket.as_str() || session.key != key.as_str() {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }

        let staging_segments = pg.list_stream_segments(session_id)?;
        pg.set_stream_upload_state(session_id, StreamUploadState::Aborted)?;
        pg.delete_stream_upload(session_id)?;
        drop(pg);

        for segment in &staging_segments {
            if let Ok(shard_pg) = self.get_pg(segment.shard_pg_id) {
                let total = segment.ec_k as usize + segment.ec_m as usize;
                for i in 0..total {
                    let shard_key =
                        ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
                    let _ = shard_pg.delete_shard(&shard_key);
                }
            }
        }

        Ok(())
    }

    pub fn delete_shards_best_effort(&self, shard_pg_id: u32, shard_keys: &[ShardKey]) {
        let Ok(shard_pg) = self.get_pg(shard_pg_id) else {
            return;
        };
        Self::cleanup_shards_locked(&shard_pg, shard_keys);
    }

    pub fn list_all_stream_uploads_best_effort(&self) -> Vec<StreamUploadRecord> {
        let mut sessions = Vec::new();
        for &pg_id in &self.pg_id_list {
            let Ok(pg) = self.get_pg(pg_id) else {
                continue;
            };
            let Ok(mut local) = pg.list_all_stream_uploads() else {
                continue;
            };
            sessions.append(&mut local);
        }
        sessions
    }

    pub fn finalize_put_object_stream<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        total_size: u64,
        action: impl FnOnce(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let session = pg.get_stream_upload(session_id)?;
        Self::validate_put_object_stream_session(&session, bucket, key)?;
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        let staging_segments = pg.list_stream_segments(session_id)?;

        match action(StreamPutFinalizeSnapshot {
            session,
            existing_etag,
        }) {
            Ok(prepared) => {
                let segments_total: u64 = staging_segments.iter().map(|segment| segment.size).sum();
                if segments_total != total_size {
                    return Err(ObjectPgActionError::InvalidRequest {
                        reason: format!(
                            "total_size mismatch: caller passed {total_size} but staged segments sum to {segments_total}"
                        ),
                    });
                }

                let version_id = if prepared.versioning == BucketVersioningState::Enabled {
                    PgMetadataStore::next_version_id(&*pg, bucket, key)?
                } else {
                    VersionId::Null
                };
                let generation_id = PgMetadataStore::next_generation_id(&*pg, bucket, key)?;
                let stale_payload = if version_id.is_null() {
                    Self::snapshot_overwritten_null_version_payload_from_object_pg(
                        &pg, bucket, key,
                    )?
                } else {
                    None
                };

                let committed_segments: Vec<ObjectSegmentRecord> = staging_segments
                    .iter()
                    .map(|segment| ObjectSegmentRecord {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version_id,
                        segment_index: segment.segment_index,
                        size: segment.size,
                        segment_crc64: segment.segment_crc64,
                        segment_okh: segment.segment_okh,
                        segment_vid: segment.segment_vid,
                        shard_pg_id: segment.shard_pg_id,
                        ec_k: segment.ec_k,
                        ec_m: segment.ec_m,
                    })
                    .collect();

                pg.commit_stream_put(
                    session_id,
                    &CommitStreamPutReq {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version_id,
                        owner: prepared.owner,
                        acl_grants: prepared.acl_grants,
                        public_read: prepared.public_read,
                        generation_id,
                        size: prepared.size,
                        etag_crc64: prepared.etag_crc64,
                        ec: prepared.ec,
                        tags: prepared.tags.clone(),
                        metadata_blob: Some(prepared.metadata_blob.clone()),
                        system_metadata_blob: Some(prepared.system_metadata_blob.clone()),
                        object_lock: prepared.object_lock,
                        encryption: prepared.encryption.clone(),
                    },
                    &committed_segments,
                )?;

                Self::finalize_stream_put_stale_payload_metadata(
                    &pg,
                    bucket,
                    key,
                    version_id,
                    stale_payload.as_ref(),
                )?;

                let stored = PgMetadataStore::get_object_meta(&*pg, bucket, key)?;
                let live_record =
                    stored
                        .as_live()
                        .ok_or_else(|| crate::error::MetadataError::Db {
                            context: "stored object missing live record after stream put",
                            source: rusqlite::Error::QueryReturnedNoRows,
                        })?;

                Ok(Ok(FinalizeStreamPutOutcome {
                    value: prepared.value,
                    version_id,
                    encryption: prepared.encryption,
                    live_tags: live_record.tags.clone(),
                    live_size: live_record.size,
                    live_last_modified: live_record.last_modified,
                    stale_generation_id: stale_payload
                        .as_ref()
                        .map(|payload| match payload {
                            StaleObjectPayloadMetadata::Segments { generation_id, .. }
                            | StaleObjectPayloadMetadata::Multipart { generation_id, .. } => {
                                generation_id
                            }
                        })
                        .copied(),
                }))
            }
            Err(error) => Ok(Err(error)),
        }
    }

    fn validate_put_object_stream_session(
        session: &crate::StreamUploadRecord,
        bucket: &BucketName,
        key: &ObjectKey,
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
        match session.target {
            StreamUploadTarget::PutObject => Ok(()),
            StreamUploadTarget::UploadPart { .. } => Err(ObjectPgActionError::InvalidRequest {
                reason: "session is not a PutObject session".to_string(),
            }),
        }
    }

    fn snapshot_overwritten_null_version_payload_from_object_pg(
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StaleObjectPayloadMetadata>, crate::error::MetadataError> {
        let stored = match PgMetadataStore::get_object_version(pg, bucket, key, VersionId::Null) {
            Ok(stored) => stored,
            Err(crate::error::MetadataError::ObjectNotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        let record = match stored {
            StoredObject::Live(record) => record,
            StoredObject::DeleteMarker(_) => return Ok(None),
        };

        match record.layout {
            ObjectLayout::MultipartManifest { .. } => {
                let parts = PgMetadataStore::get_object_parts(pg, bucket, key, VersionId::Null)?;
                let mut streaming_segments = Vec::new();
                for part in &parts {
                    if part.part_okh == [0u8; 16] {
                        streaming_segments.extend(PgMetadataStore::get_multipart_part_segments(
                            pg,
                            bucket,
                            key,
                            VersionId::Null,
                            part.part_number,
                        )?);
                    }
                }
                Ok(Some(StaleObjectPayloadMetadata::Multipart {
                    generation_id: record.generation_id,
                    parts,
                    streaming_segments,
                }))
            }
            ObjectLayout::Standard => {
                let segments =
                    PgMetadataStore::get_object_segments(pg, bucket, key, VersionId::Null)?;
                Ok(Some(StaleObjectPayloadMetadata::Segments {
                    generation_id: record.generation_id,
                    segments,
                }))
            }
        }
    }

    fn finalize_stream_put_stale_payload_metadata(
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        payload: Option<&StaleObjectPayloadMetadata>,
    ) -> Result<(), crate::error::MetadataError> {
        let Some(payload) = payload else {
            return Ok(());
        };
        match payload {
            StaleObjectPayloadMetadata::Segments {
                generation_id,
                segments,
            } => Self::enqueue_object_segments_reclaim_from_object_pg(
                pg,
                bucket,
                key,
                *generation_id,
                segments,
            ),
            StaleObjectPayloadMetadata::Multipart {
                generation_id,
                parts,
                streaming_segments,
            } => {
                Self::enqueue_multipart_reclaim_from_object_pg(
                    pg,
                    bucket,
                    key,
                    *generation_id,
                    parts,
                    streaming_segments,
                )?;
                if !streaming_segments.is_empty() {
                    PgMetadataStore::delete_multipart_part_segments(pg, bucket, key, version_id)?;
                }
                PgMetadataStore::delete_object_parts(pg, bucket, key, version_id)
            }
        }
    }

    fn enqueue_object_segments_reclaim_from_object_pg(
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), crate::error::MetadataError> {
        pg.put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: Self::now_millis(),
            segments: segments
                .iter()
                .map(|segment| ObjectSegmentsReclaimSegmentRecord {
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    shard_pg_id: segment.shard_pg_id,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                })
                .collect(),
        })
    }

    fn enqueue_multipart_reclaim_from_object_pg(
        pg: &crate::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        parts: &[ObjectPartRecord],
        streaming_segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), crate::error::MetadataError> {
        use std::collections::BTreeMap;

        let mut segments_by_part: BTreeMap<u32, Vec<MultipartReclaimPartSegmentRecord>> =
            BTreeMap::new();
        for segment in streaming_segments {
            segments_by_part
                .entry(segment.part_number)
                .or_default()
                .push(MultipartReclaimPartSegmentRecord {
                    part_number: segment.part_number,
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    shard_pg_id: segment.shard_pg_id,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                });
        }

        let parts = parts
            .iter()
            .map(|part| {
                if part.part_okh == [0u8; 16] {
                    MultipartReclaimPartRecord::Segments {
                        part_number: part.part_number,
                        segments: segments_by_part
                            .remove(&part.part_number)
                            .unwrap_or_default(),
                    }
                } else {
                    MultipartReclaimPartRecord::ShardSet {
                        part_number: part.part_number,
                        part_okh: part.part_okh,
                        part_vid: part.part_vid,
                        shard_pg_id: part.shard_pg_id,
                        ec: EcShape {
                            k: part.ec_k,
                            m: part.ec_m,
                        },
                    }
                }
            })
            .collect();

        pg.put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: Self::now_millis(),
            parts,
        })
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before UNIX_EPOCH")
            .as_millis() as u64
    }
}
