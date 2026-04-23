use super::*;
use crate::{
    BucketVersioningState, CommitStreamPutReq, EcShape, FinalizeStreamPutOutcome,
    MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord, MultipartReclaimRecord,
    ObjectLayout, ObjectPartRecord, ObjectSegmentRecord, ObjectSegmentsReclaimRecord,
    ObjectSegmentsReclaimSegmentRecord, PreparedStreamPutCommit, StreamPutFinalizeSnapshot,
    StreamUploadRecord, VersionId,
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
