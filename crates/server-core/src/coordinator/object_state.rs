use checksum::{ChecksumAlgorithm, ChecksumType};
use s3_types::{BucketVersioningState, VersionId};
use storage::traits::{PgMetadataStore, ShardStore};
use storage::{
    BucketName, EcShape, GenerationId, LiveObjectRecord, MultipartPartSegmentRecord,
    MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord, MultipartReclaimRecord,
    ObjectEncryption, ObjectKey, ObjectLayout, ObjectPartRecord, ObjectSegmentRecord,
    ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord, OwnerIdentity,
    PutDeleteMarkerReq, PutObjectReq, SerializedMetadataBlob, SerializedSystemMetadataBlob,
    SerializedTagSet, ShardKey, StoredObject,
};

use super::{
    ActiveWriteEncryption, BucketSummary, Coordinator, LockedReadObject, ObjectPgGuards,
    PreparedPutCommit, PutCommitRequest, SegmentPayloadRecord,
};
use crate::conditional::{check_write_conditions, WriteCondition};
use crate::error::ServerError;
use crate::metadata_blob::MetadataBlob;
use crate::sse::{
    decrypt_managed_encryption_checksum, decrypt_sse_customer_checksum, SseCustomerRequest,
};
use crate::system_metadata::SystemMetadata;

#[derive(Debug, Clone)]
pub(super) struct SnapshottedMultipartPart {
    pub(super) record: ObjectPartRecord,
    pub(super) object_offset_start: usize,
    pub(super) segments: Vec<SegmentPayloadRecord>,
}

#[derive(Debug, Clone)]
pub(super) enum StaleObjectPayload {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DeletedLiveObjectKind {
    Segments,
    Multipart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DeletedLiveObjectReclaim {
    pub(super) generation_id: GenerationId,
    pub(super) kind: DeletedLiveObjectKind,
}

impl Coordinator {
    pub(super) fn prepare_put_commit_locked(
        &self,
        meta_pg: &storage::PgStore,
        bucket_info: &BucketSummary,
        req: &PutCommitRequest<'_>,
    ) -> Result<PreparedPutCommit, ServerError> {
        self.ensure_write_encryption_supported(&req.write_encryption.object_encryption())?;
        let metadata_blob = SerializedMetadataBlob::from(req.metadata_blob.serialize()?);
        let (system_metadata_blob, encryption) =
            Self::prepare_stored_system_metadata(req.system_metadata, req.write_encryption)?;

        if !req.cond.is_empty() {
            let existing_etag =
                match storage::PgMetadataStore::get_object_meta(meta_pg, req.bucket, req.key) {
                    Ok(stored) => stored.as_live().map(|record| record.etag.format()),
                    Err(storage::MetadataError::ObjectNotFound) => None,
                    Err(e) => return Err(ServerError::Metadata(e)),
                };
            if matches!(req.cond, WriteCondition::IfMatch(_)) && existing_etag.is_none() {
                return Err(ServerError::ObjectNotFound {
                    bucket: req.bucket.to_string(),
                    key: req.key.to_string(),
                });
            }
            check_write_conditions(req.cond, existing_etag.as_deref())?;
        }

        let version_id = if bucket_info.versioning == BucketVersioningState::Enabled {
            storage::PgMetadataStore::next_version_id(meta_pg, req.bucket, req.key)?
        } else {
            VersionId::Null
        };
        let generation_id =
            storage::PgMetadataStore::next_generation_id(meta_pg, req.bucket, req.key)?;
        let stale_payload = if version_id.is_null() {
            Self::snapshot_overwritten_null_version_payload(meta_pg, req.bucket, req.key)?
        } else {
            None
        };

        Ok(PreparedPutCommit {
            version_id,
            generation_id,
            tags: req.tags.map(SerializedTagSet::from),
            metadata_blob,
            system_metadata_blob,
            encryption,
            stale_payload,
        })
    }

    pub(super) fn object_system_metadata_with_default_checksum(
        system_metadata: &SystemMetadata,
        write_encryption: &ActiveWriteEncryption,
        crc64: u64,
    ) -> SystemMetadata {
        let mut normalized_system_metadata = system_metadata.clone();
        if let Some(checksum) = normalized_system_metadata.take_checksum() {
            normalized_system_metadata.set_checksum(
                checksum.algorithm(),
                checksum.checksum_type().or(Some(ChecksumType::FullObject)),
                checksum.value(),
            );
        }
        let can_store_checksum = matches!(
            write_encryption,
            ActiveWriteEncryption::None
                | ActiveWriteEncryption::SseCustomer { write: Some(_), .. }
                | ActiveWriteEncryption::Managed { write: Some(_), .. }
        );
        if normalized_system_metadata.checksum().is_some() || !can_store_checksum {
            return normalized_system_metadata;
        }
        use base64::Engine;
        let mut system_metadata = normalized_system_metadata;
        let checksum = base64::engine::general_purpose::STANDARD.encode(crc64.to_be_bytes());
        system_metadata.set_checksum(
            ChecksumAlgorithm::Crc64nvme,
            Some(ChecksumType::FullObject),
            checksum,
        );
        system_metadata
    }

    pub(super) fn finalize_put_commit_metadata_locked(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        stale_payload: Option<&StaleObjectPayload>,
    ) -> Result<(), ServerError> {
        if let Some(payload) = stale_payload {
            match payload {
                StaleObjectPayload::Segments {
                    generation_id,
                    segments,
                } => {
                    Self::enqueue_object_segments_reclaim(
                        meta_pg,
                        bucket,
                        key,
                        *generation_id,
                        segments,
                    )?;
                }
                StaleObjectPayload::Multipart { .. } => {
                    Self::delete_stale_object_payload_metadata(
                        meta_pg, bucket, key, version_id, payload,
                    )?;
                }
            }
        }

        Ok(())
    }

    pub(super) fn put_target_existing_live_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ServerError> {
        let meta_pg_id = self.object_pg_id_for(bucket, key);
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
        match storage::PgMetadataStore::get_object_meta(&*meta_pg, bucket, key) {
            Ok(object @ StoredObject::Live(_)) => Ok(Some(object)),
            Ok(StoredObject::DeleteMarker(_)) | Err(storage::MetadataError::ObjectNotFound) => {
                Ok(None)
            }
            Err(err) => Err(ServerError::Metadata(err)),
        }
    }

    pub(super) fn lookup_object_record(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ServerError> {
        match version_id {
            Some(vid) => storage::PgMetadataStore::get_object_version(meta_pg, bucket, key, vid),
            None => storage::PgMetadataStore::get_object_meta(meta_pg, bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound if version_id.is_some() => {
                ServerError::VersionNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: version_id.unwrap().to_string(),
                }
            }
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub(super) fn deserialize_user_metadata(
        metadata_blob: Option<&SerializedMetadataBlob>,
    ) -> Result<MetadataBlob, ServerError> {
        metadata_blob
            .map(|blob| MetadataBlob::deserialize(blob.as_slice()).map(|(m, _)| m))
            .transpose()?
            .map_or(Ok(MetadataBlob::new()), Ok)
    }

    fn deserialize_system_metadata(
        system_metadata_blob: Option<&SerializedSystemMetadataBlob>,
    ) -> Result<SystemMetadata, ServerError> {
        system_metadata_blob.map_or(Ok(SystemMetadata::new()), |blob| {
            SystemMetadata::deserialize(blob.as_slice())
        })
    }

    pub(super) fn prepare_stored_system_metadata(
        system_metadata: &SystemMetadata,
        write_encryption: &ActiveWriteEncryption,
    ) -> Result<(SerializedSystemMetadataBlob, ObjectEncryption), ServerError> {
        let mut stored_system_metadata = system_metadata.clone();
        let stored_encryption = match write_encryption {
            ActiveWriteEncryption::None => ObjectEncryption::None,
            ActiveWriteEncryption::SseCustomer {
                write: Some(sse_customer_write),
                ..
            } => {
                let checksum = stored_system_metadata.take_checksum();
                sse_customer_write.seal_checksum_metadata(checksum.as_ref())?
            }
            ActiveWriteEncryption::Managed { write, .. } => {
                let checksum = stored_system_metadata.take_checksum();
                if let Some(write) = write.as_ref() {
                    write.seal_checksum_metadata(checksum.as_ref())?
                } else if checksum.is_none() {
                    write_encryption.object_encryption()
                } else {
                    return Err(ServerError::InternalError {
                        reason: "SSE-S3 write context is required when storing checksum metadata for this object"
                            .to_string(),
                    });
                }
            }
            ActiveWriteEncryption::SseCustomer { write: None, .. } => {
                let checksum = stored_system_metadata.take_checksum();
                if checksum.is_none() {
                    write_encryption.object_encryption()
                } else {
                    return Err(ServerError::InvalidRequest {
                        reason:
                            "SSE-C headers are required when storing checksum metadata for this object"
                                .to_string(),
                    });
                }
            }
        };
        Ok((
            SerializedSystemMetadataBlob::from(stored_system_metadata.serialize()?),
            stored_encryption,
        ))
    }

    pub(super) fn deserialize_visible_system_metadata(
        &self,
        system_metadata_blob: Option<&SerializedSystemMetadataBlob>,
        encryption: &ObjectEncryption,
        sse_customer: Option<&SseCustomerRequest>,
    ) -> Result<SystemMetadata, ServerError> {
        let mut system_metadata = Self::deserialize_system_metadata(system_metadata_blob)?;
        match encryption {
            ObjectEncryption::SseCustomer(state) => {
                let Some(request) = sse_customer else {
                    return Ok(system_metadata);
                };
                if !state.encrypted_checksum_metadata.is_empty() {
                    let validator =
                        self.sse_c_validator
                            .as_ref()
                            .ok_or(ServerError::InternalError {
                                reason: "SSE-C validator key is not configured".to_string(),
                            })?;
                    if let Some(checksum) =
                        decrypt_sse_customer_checksum(validator, state, request)?
                    {
                        system_metadata.set_checksum(
                            checksum.algorithm(),
                            checksum.checksum_type(),
                            checksum.value(),
                        );
                    }
                }
            }
            ObjectEncryption::SseS3(state) => {
                if !state.encrypted_checksum_metadata.is_empty() {
                    let provider =
                        self.managed_key_provider
                            .as_ref()
                            .ok_or(ServerError::InternalError {
                                reason: "SSE-S3 key provider is not configured".to_string(),
                            })?;
                    if let Some(checksum) = decrypt_managed_encryption_checksum(provider, state)? {
                        system_metadata.set_checksum(
                            checksum.algorithm(),
                            checksum.checksum_type(),
                            checksum.value(),
                        );
                    }
                }
            }
            ObjectEncryption::None => {}
        }
        Ok(system_metadata)
    }

    /// Lock metadata PG for a consistent object read/delete view.
    ///
    /// Latest-version readers snapshot object metadata while holding the metadata
    /// PG lock, then construct `ReadHandle`s that acquire a generation-scoped
    /// payload lease before this guard is released. Committed payload
    /// generations are immutable, and reclaim is lease-gated, so read-side
    /// paths no longer need to relock a synthetic shard PG.
    pub(super) fn lock_object_pgs_for_read_typed<'a>(
        &'a self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let meta_pg_id = self.object_pg_id_for(bucket, key);
        let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
        let record = Self::lookup_object_record(&meta_guard, bucket, key, version_id)?;
        Ok(LockedReadObject {
            record,
            pgs: ObjectPgGuards::new(meta_guard),
        })
    }

    fn multipart_part_payloads(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part: &ObjectPartRecord,
        encryption: &ObjectEncryption,
    ) -> Result<Vec<SegmentPayloadRecord>, ServerError> {
        if part.part_okh != [0u8; 16] {
            return Ok(vec![SegmentPayloadRecord {
                segment_index: 0,
                size: part.size,
                segment_crc64: None,
                segment_okh: part.part_okh,
                segment_vid: part.part_vid,
                shard_pg_id: part.shard_pg_id,
                ec_k: part.ec_k,
                ec_m: part.ec_m,
                encryption: encryption.clone(),
            }]);
        }

        storage::PgMetadataStore::get_multipart_part_segments(
            meta_pg,
            bucket,
            key,
            version_id,
            part.part_number,
        )
        .map(|segments| {
            segments
                .into_iter()
                .map(|segment| SegmentPayloadRecord {
                    segment_index: segment.segment_index,
                    size: segment.size,
                    segment_crc64: segment.segment_crc64,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    shard_pg_id: segment.shard_pg_id,
                    ec_k: segment.ec_k,
                    ec_m: segment.ec_m,
                    encryption: encryption.clone(),
                })
                .collect()
        })
        .map_err(ServerError::Metadata)
    }

    pub(super) fn snapshot_multipart_parts(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        encryption: &ObjectEncryption,
    ) -> Result<Vec<SnapshottedMultipartPart>, ServerError> {
        let parts = storage::PgMetadataStore::get_object_parts(meta_pg, bucket, key, version_id)
            .map_err(ServerError::Metadata)?;
        let mut snapshotted = Vec::with_capacity(parts.len());
        let mut object_offset_start = 0usize;
        for part in parts {
            let part_size = part.size as usize;
            let segments =
                Self::multipart_part_payloads(meta_pg, bucket, key, version_id, &part, encryption)?;
            snapshotted.push(SnapshottedMultipartPart {
                record: part,
                object_offset_start,
                segments,
            });
            object_offset_start += part_size;
        }
        Ok(snapshotted)
    }

    pub(super) fn snapshot_multipart_parts_overlapping_range(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        encryption: &ObjectEncryption,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<SnapshottedMultipartPart>, ServerError> {
        let parts = storage::PgMetadataStore::get_object_parts_overlapping_range(
            meta_pg,
            bucket,
            key,
            version_id,
            start,
            end_exclusive,
        )
        .map_err(ServerError::Metadata)?;
        let mut snapshotted = Vec::with_capacity(parts.len());
        for part in parts {
            let segments = Self::multipart_part_payloads(
                meta_pg, bucket, key, version_id, &part.part, encryption,
            )?;
            snapshotted.push(SnapshottedMultipartPart {
                record: part.part,
                object_offset_start: part.object_offset_start as usize,
                segments,
            });
        }
        Ok(snapshotted)
    }

    pub(super) fn snapshot_overwritten_null_version_payload(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StaleObjectPayload>, ServerError> {
        let stored = match storage::PgMetadataStore::get_object_version(
            meta_pg,
            bucket,
            key,
            VersionId::Null,
        ) {
            Ok(stored) => stored,
            Err(storage::MetadataError::ObjectNotFound) => return Ok(None),
            Err(e) => return Err(ServerError::Metadata(e)),
        };
        let record = match stored {
            StoredObject::Live(record) => record,
            StoredObject::DeleteMarker(_) => return Ok(None),
        };

        match record.layout {
            ObjectLayout::MultipartManifest { .. } => {
                let parts = storage::PgMetadataStore::get_object_parts(
                    meta_pg,
                    bucket,
                    key,
                    VersionId::Null,
                )
                .map_err(ServerError::Metadata)?;
                let mut streaming_segments = Vec::new();
                for part in &parts {
                    if part.part_okh == [0u8; 16] {
                        let segments = storage::PgMetadataStore::get_multipart_part_segments(
                            meta_pg,
                            bucket,
                            key,
                            VersionId::Null,
                            part.part_number,
                        )
                        .map_err(ServerError::Metadata)?;
                        streaming_segments.extend(segments);
                    }
                }
                Ok(Some(StaleObjectPayload::Multipart {
                    generation_id: record.generation_id,
                    parts,
                    streaming_segments,
                }))
            }
            ObjectLayout::Standard => {
                let segments = storage::PgMetadataStore::get_object_segments(
                    meta_pg,
                    bucket,
                    key,
                    VersionId::Null,
                )
                .map_err(ServerError::Metadata)?;
                Ok(Some(StaleObjectPayload::Segments {
                    generation_id: record.generation_id,
                    segments,
                }))
            }
        }
    }

    pub(super) fn delete_stale_object_payload_metadata(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        payload: &StaleObjectPayload,
    ) -> Result<(), ServerError> {
        match payload {
            StaleObjectPayload::Segments {
                generation_id,
                segments,
            } => {
                Self::enqueue_object_segments_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    *generation_id,
                    segments,
                )?;
                storage::PgMetadataStore::delete_object_segments(meta_pg, bucket, key, version_id)
                    .map_err(ServerError::Metadata)
            }
            StaleObjectPayload::Multipart {
                generation_id,
                parts,
                streaming_segments,
            } => {
                Self::enqueue_multipart_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    *generation_id,
                    parts,
                    streaming_segments,
                )?;
                if !streaming_segments.is_empty() {
                    storage::PgMetadataStore::delete_multipart_part_segments(
                        meta_pg, bucket, key, version_id,
                    )
                    .map_err(ServerError::Metadata)?;
                }
                storage::PgMetadataStore::delete_object_parts(meta_pg, bucket, key, version_id)
                    .map_err(ServerError::Metadata)
            }
        }
    }

    fn reclaim_info_for_stale_payload(payload: &StaleObjectPayload) -> DeletedLiveObjectReclaim {
        match payload {
            StaleObjectPayload::Segments { generation_id, .. } => DeletedLiveObjectReclaim {
                generation_id: *generation_id,
                kind: DeletedLiveObjectKind::Segments,
            },
            StaleObjectPayload::Multipart { generation_id, .. } => DeletedLiveObjectReclaim {
                generation_id: *generation_id,
                kind: DeletedLiveObjectKind::Multipart,
            },
        }
    }

    pub(super) fn permanently_delete_live_object_locked(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        record: &LiveObjectRecord,
    ) -> Result<Option<DeletedLiveObjectReclaim>, ServerError> {
        let reclaim = if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
            let obj_parts =
                storage::PgMetadataStore::get_object_parts(meta_pg, bucket, key, record.version_id)
                    .map_err(ServerError::Metadata)?;
            let mut streaming_segments: Vec<MultipartPartSegmentRecord> = Vec::new();
            for part in &obj_parts {
                if part.part_okh == [0u8; 16] {
                    let segments = storage::PgMetadataStore::get_multipart_part_segments(
                        meta_pg,
                        bucket,
                        key,
                        record.version_id,
                        part.part_number,
                    )
                    .map_err(ServerError::Metadata)?;
                    streaming_segments.extend(segments);
                }
            }
            Self::enqueue_multipart_reclaim(
                meta_pg,
                bucket,
                key,
                record.generation_id,
                &obj_parts,
                &streaming_segments,
            )?;
            if !streaming_segments.is_empty() {
                storage::PgMetadataStore::delete_multipart_part_segments(
                    meta_pg,
                    bucket,
                    key,
                    record.version_id,
                )
                .map_err(ServerError::Metadata)?;
            }
            storage::PgMetadataStore::delete_object_parts(meta_pg, bucket, key, record.version_id)?;
            Some(DeletedLiveObjectReclaim {
                generation_id: record.generation_id,
                kind: DeletedLiveObjectKind::Multipart,
            })
        } else {
            let segments = storage::PgMetadataStore::get_object_segments(
                meta_pg,
                bucket,
                key,
                record.version_id,
            )
            .map_err(ServerError::Metadata)?;
            Self::enqueue_object_segments_reclaim(
                meta_pg,
                bucket,
                key,
                record.generation_id,
                &segments,
            )?;
            storage::PgMetadataStore::delete_object_segments(
                meta_pg,
                bucket,
                key,
                record.version_id,
            )
            .map_err(ServerError::Metadata)?;
            Some(DeletedLiveObjectReclaim {
                generation_id: record.generation_id,
                kind: DeletedLiveObjectKind::Segments,
            })
        };

        if record.version_id.is_null() {
            storage::PgMetadataStore::delete_object_meta(meta_pg, bucket, key)?;
        } else {
            storage::PgMetadataStore::delete_object_version(
                meta_pg,
                bucket,
                key,
                record.version_id,
            )
            .map_err(ServerError::Metadata)?;
        }

        Ok(reclaim)
    }

    pub(super) fn put_delete_marker_locked(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        owner: OwnerIdentity,
    ) -> Result<(), ServerError> {
        meta_pg
            .put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                owner,
            }))
            .map_err(ServerError::Metadata)
    }

    pub(super) fn expire_current_live_suspended_locked(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        record: &LiveObjectRecord,
        owner: OwnerIdentity,
    ) -> Result<Option<DeletedLiveObjectReclaim>, ServerError> {
        let stale_payload = if record.version_id.is_null() {
            Self::snapshot_overwritten_null_version_payload(meta_pg, bucket, key)?
        } else {
            None
        };

        Self::put_delete_marker_locked(meta_pg, bucket, key, VersionId::Null, owner)?;

        let reclaim = stale_payload
            .as_ref()
            .map(Self::reclaim_info_for_stale_payload);
        if let Some(payload) = stale_payload.as_ref() {
            Self::delete_stale_object_payload_metadata(
                meta_pg,
                bucket,
                key,
                VersionId::Null,
                payload,
            )?;
        }

        Ok(reclaim)
    }

    pub(super) fn delete_stale_object_payload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        payload: &StaleObjectPayload,
    ) {
        match payload {
            StaleObjectPayload::Segments { generation_id, .. } => self
                .read_runtime()
                .enqueue_object_payload_reclaim_for(bucket, key, *generation_id),
            StaleObjectPayload::Multipart { generation_id, .. } => self
                .read_runtime()
                .enqueue_object_payload_reclaim_for(bucket, key, *generation_id),
        }
    }

    pub(super) fn enqueue_object_segments_reclaim(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), ServerError> {
        meta_pg
            .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
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
            .map_err(ServerError::Metadata)
    }

    pub(super) fn enqueue_multipart_reclaim(
        meta_pg: &storage::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        parts: &[ObjectPartRecord],
        streaming_segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), ServerError> {
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

        meta_pg
            .put_multipart_reclaim(&MultipartReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id,
                created_at: Self::now_millis(),
                parts,
            })
            .map_err(ServerError::Metadata)
    }

    pub(super) fn delete_segment_shards_generic(
        &self,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), ServerError> {
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

    pub(super) fn delete_segment_shard_set(
        &self,
        shard_pg_id: u32,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        ec_k: u8,
        ec_m: u8,
    ) -> Result<(), ServerError> {
        let pg = self.storage_node.get_pg(shard_pg_id)?;
        let total = ec_k as usize + ec_m as usize;
        for i in 0..total {
            let shard_key = ShardKey::new(segment_okh, segment_vid.get(), i as u8);
            pg.delete_shard(&shard_key)?;
        }
        Ok(())
    }
}
