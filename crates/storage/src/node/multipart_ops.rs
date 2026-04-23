use super::*;
use crate::clock::current_time_millis;
use crate::types::{
    CompleteMultipartCommitOutcome, CompleteMultipartCommitRequest, CompletedMultipartStalePayload,
    CreateMultipartUploadOutcome, CreateMultipartUploadReq, EcShape, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, ObjectLayout, ObjectPartRecord,
    ObjectSegmentRecord, StoredObject, VersionId,
};

impl SharedStorageNode {
    fn load_existing_live_object_from_object_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, crate::error::MetadataError> {
        match PgMetadataStore::get_object_meta(pg, bucket, key) {
            Ok(StoredObject::Live(object)) => Ok(Some(StoredObject::Live(object))),
            Ok(StoredObject::DeleteMarker(_))
            | Err(crate::error::MetadataError::ObjectNotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) fn load_in_progress_multipart_upload_from_object_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, crate::error::MetadataError> {
        let upload = Self::load_multipart_upload_from_object_pg(pg, bucket, key, upload_id)?;
        if upload.state != UploadState::InProgress {
            return Err(crate::error::MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        Ok(upload)
    }

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

    pub fn create_multipart_upload<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateMultipartUploadReq), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        loop {
            let pg_id = self.pg_topology.bucket_pg_for(bucket);
            let bucket_pg = self.get_pg(pg_id)?;
            match PgMetadataStore::acquire_bucket_write_reservation(&*bucket_pg, bucket) {
                Ok(_info) => {
                    let snapshot = Self::load_bucket_snapshot_from_pg(&bucket_pg, bucket, request)?;
                    drop(bucket_pg);

                    let object_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
                    let existing_object =
                        Self::load_existing_live_object_from_object_pg(&object_pg, bucket, key)?;
                    let result = match action(snapshot, existing_object) {
                        Ok((value, create)) => {
                            object_pg.create_multipart_upload(&create)?;
                            let upload = object_pg.get_multipart_upload(&create.upload_id)?;
                            Ok(CreateMultipartUploadOutcome {
                                value,
                                initiated_at: upload.initiated_at,
                            })
                        }
                        Err(error) => Err(error),
                    };
                    drop(object_pg);

                    let release_result = self.release_bucket_write_reservation(bucket);
                    return Self::finish_bucket_write_snapshot(result, release_result);
                }
                Err(crate::error::MetadataError::BucketWriteDraining) => {
                    drop(bucket_pg);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(other) => return Err(other.into()),
            }
        }
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
        Ok(Self::load_in_progress_multipart_upload_from_object_pg(
            &pg, bucket, key, upload_id,
        )?)
    }

    pub fn load_multipart_completion_snapshot(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let _upload =
            Self::load_in_progress_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        let mut part_records = Vec::with_capacity(requested_part_numbers.len());
        for &part_number in requested_part_numbers {
            part_records.push(pg.get_multipart_part(upload_id, part_number)?);
        }
        Ok(MultipartCompletionSnapshot {
            existing_etag,
            part_records,
        })
    }

    pub fn load_multipart_completion_preflight(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let _upload =
            Self::load_in_progress_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        Ok(MultipartCompletionPreflight { existing_etag })
    }

    fn snapshot_overwritten_null_version_payload_from_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<CompletedMultipartStalePayload>, ObjectPgActionError> {
        let stored = match PgMetadataStore::get_object_version(pg, bucket, key, VersionId::Null) {
            Ok(stored) => stored,
            Err(crate::error::MetadataError::ObjectNotFound) => return Ok(None),
            Err(other) => return Err(other.into()),
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
                        let segments = PgMetadataStore::get_multipart_part_segments(
                            pg,
                            bucket,
                            key,
                            VersionId::Null,
                            part.part_number,
                        )?;
                        streaming_segments.extend(segments);
                    }
                }
                Ok(Some(CompletedMultipartStalePayload::Multipart {
                    generation_id: record.generation_id,
                    parts,
                    streaming_segments,
                }))
            }
            ObjectLayout::Standard => {
                let segments =
                    PgMetadataStore::get_object_segments(pg, bucket, key, VersionId::Null)?;
                Ok(Some(CompletedMultipartStalePayload::Segments {
                    generation_id: record.generation_id,
                    segments,
                }))
            }
        }
    }

    fn enqueue_object_segments_reclaim_from_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), ObjectPgActionError> {
        pg.put_object_segments_reclaim(&crate::types::ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: current_time_millis(),
            segments: segments
                .iter()
                .map(|segment| crate::types::ObjectSegmentsReclaimSegmentRecord {
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
        })?;
        Ok(())
    }

    fn enqueue_multipart_reclaim_from_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        parts: &[ObjectPartRecord],
        streaming_segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), ObjectPgActionError> {
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

        pg.put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: current_time_millis(),
            parts: parts
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
                .collect(),
        })?;
        Ok(())
    }

    fn finalize_completed_multipart_stale_payload_metadata_from_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        payload: &CompletedMultipartStalePayload,
    ) -> Result<(), ObjectPgActionError> {
        match payload {
            CompletedMultipartStalePayload::Segments {
                generation_id,
                segments,
            } => {
                Self::enqueue_object_segments_reclaim_from_pg(
                    pg,
                    bucket,
                    key,
                    *generation_id,
                    segments,
                )?;
                PgMetadataStore::delete_object_segments(pg, bucket, key, version_id)?;
            }
            CompletedMultipartStalePayload::Multipart {
                generation_id,
                parts,
                streaming_segments,
            } => {
                Self::enqueue_multipart_reclaim_from_pg(
                    pg,
                    bucket,
                    key,
                    *generation_id,
                    parts,
                    streaming_segments,
                )?;
            }
        }
        Ok(())
    }

    pub fn complete_multipart_upload_commit(
        &self,
        req: CompleteMultipartCommitRequest,
    ) -> Result<CompleteMultipartCommitOutcome, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(&req.bucket, &req.key))?;
        let version_id = if req.versioning == crate::types::BucketVersioningState::Enabled {
            PgMetadataStore::next_version_id(&*pg, &req.bucket, &req.key)?
        } else {
            VersionId::Null
        };
        let generation_id = PgMetadataStore::next_generation_id(&*pg, &req.bucket, &req.key)?;
        let stale_payload = if version_id.is_null() {
            Self::snapshot_overwritten_null_version_payload_from_pg(&pg, &req.bucket, &req.key)?
        } else {
            None
        };

        let object_parts: Vec<ObjectPartRecord> = req
            .part_records
            .iter()
            .map(|part| {
                let shard_pg_id = self.pg_topology.shard_pg(
                    format!("mpu/{}", part.upload_id).as_str(),
                    format!("{}/{}", part.part_number, part.generation).as_str(),
                    part.part_vid.get(),
                );
                ObjectPartRecord {
                    bucket: req.bucket.clone(),
                    key: req.key.clone(),
                    version_id,
                    part_number: part.part_number,
                    size: part.size,
                    etag: part.etag.clone(),
                    etag_kind: part.etag_kind,
                    part_okh: part.part_okh,
                    part_vid: part.part_vid,
                    ec_k: part.ec_k,
                    ec_m: part.ec_m,
                    shard_pg_id,
                    checksum: part.checksum.clone(),
                }
            })
            .collect();

        pg.complete_multipart_commit(
            &req.upload_id,
            req.completion_order,
            &crate::types::CommitMultipartReq {
                bucket: req.bucket.clone(),
                key: req.key.clone(),
                version_id,
                owner: req.owner,
                acl_grants: req.acl_grants,
                public_read: req.public_read,
                generation_id,
                size: req.size,
                etag_crc64: req.etag_crc64,
                ec: EcShape { k: 0, m: 0 },
                tags: req.tags,
                metadata_blob: req.metadata_blob,
                system_metadata_blob: req.system_metadata_blob,
                object_lock: req.object_lock,
                encryption: req.encryption,
            },
            &object_parts,
        )?;

        if let Some(ref payload) = stale_payload {
            Self::finalize_completed_multipart_stale_payload_metadata_from_pg(
                &pg,
                &req.bucket,
                &req.key,
                version_id,
                payload,
            )?;
        }

        let stored = PgMetadataStore::get_object_meta(&*pg, &req.bucket, &req.key)?;
        let live_record = stored
            .as_live()
            .ok_or_else(|| crate::error::MetadataError::Db {
                context: "completed multipart object missing live record",
                source: rusqlite::Error::QueryReturnedNoRows,
            })?;

        Ok(CompleteMultipartCommitOutcome {
            version_id,
            stale_payload,
            live_tags: live_record.tags.clone(),
            live_size: live_record.size,
            live_last_modified: live_record.last_modified,
        })
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

    pub fn load_in_progress_multipart_upload_for_listing(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Ok(Self::load_in_progress_multipart_upload_from_object_pg(
            &pg, bucket, key, upload_id,
        )?)
    }

    pub fn list_multipart_parts_for_upload<E, F>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number_marker: Option<u32>,
        max_parts: u32,
        authorize: F,
    ) -> Result<Result<ListedMultipartParts, E>, ObjectPgActionError>
    where
        F: FnOnce(&MultipartUploadRecord) -> Result<(), E>,
    {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let upload =
            Self::load_in_progress_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        match authorize(&upload) {
            Ok(()) => {}
            Err(error) => return Ok(Err(error)),
        }
        let response = pg.list_multipart_parts(&ListPartsReq {
            upload_id: upload_id.clone(),
            part_number_marker,
            max_parts,
        })?;
        Ok(Ok(ListedMultipartParts { upload, response }))
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
