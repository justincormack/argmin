use super::*;
use crate::clock::current_time_millis;
use crate::types::{
    DeleteCurrentObjectOutcome, DeleteSpecificObjectVersionOutcome, DeletedCurrentObject,
    DeletedSpecificObjectVersion, EcShape, ExpireCurrentObjectOutcome,
    InsertCurrentDeleteMarkerOutcome, LiveObjectRecord, MultipartPartSegmentRecord,
    MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord, MultipartReclaimRecord,
    ObjectLayout, ObjectPartRecord, ObjectSegmentRecord, ObjectSegmentsReclaimRecord,
    ObjectSegmentsReclaimSegmentRecord, OwnerIdentity, PutDeleteMarkerReq, PutObjectReq,
};
use s3_types::{BucketVersioningState, VersionId};
use std::collections::HashSet;

enum StaleObjectPayload {
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
    fn enqueue_object_segments_reclaim(
        meta_pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), crate::error::MetadataError> {
        meta_pg.put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: current_time_millis(),
            segments: segments
                .iter()
                .map(|segment| ObjectSegmentsReclaimSegmentRecord {
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                })
                .collect(),
        })
    }

    fn enqueue_multipart_reclaim(
        meta_pg: &PgStore,
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
                    data_pg_id: segment.data_pg_id,
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
                        data_pg_id: part.data_pg_id,
                        ec: EcShape {
                            k: part.ec_k,
                            m: part.ec_m,
                        },
                    }
                }
            })
            .collect();

        meta_pg.put_multipart_reclaim(&MultipartReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: current_time_millis(),
            parts,
        })
    }

    fn delete_live_object_from_pg(
        meta_pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        record: &LiveObjectRecord,
    ) -> Result<DeletedSpecificObjectVersion, crate::error::MetadataError> {
        if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
            let obj_parts =
                PgMetadataStore::get_object_parts(meta_pg, bucket, key, record.version_id)?;
            let mut streaming_segments: Vec<MultipartPartSegmentRecord> = Vec::new();
            for part in &obj_parts {
                if part.part_okh == [0u8; 16] {
                    let segments = PgMetadataStore::get_multipart_part_segments(
                        meta_pg,
                        bucket,
                        key,
                        record.version_id,
                        part.part_number,
                    )?;
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
                PgMetadataStore::delete_multipart_part_segments(
                    meta_pg,
                    bucket,
                    key,
                    record.version_id,
                )?;
            }
            PgMetadataStore::delete_object_parts(meta_pg, bucket, key, record.version_id)?;
        } else {
            let segments =
                PgMetadataStore::get_object_segments(meta_pg, bucket, key, record.version_id)?;
            Self::enqueue_object_segments_reclaim(
                meta_pg,
                bucket,
                key,
                record.generation_id,
                &segments,
            )?;
            PgMetadataStore::delete_object_segments(meta_pg, bucket, key, record.version_id)?;
        }

        if record.version_id.is_null() {
            PgMetadataStore::delete_object_meta(meta_pg, bucket, key)?;
        } else {
            PgMetadataStore::delete_object_version(meta_pg, bucket, key, record.version_id)?;
        }
        Ok(DeletedSpecificObjectVersion::Live {
            generation_id: record.generation_id,
            layout: record.layout,
        })
    }

    fn snapshot_overwritten_null_version_payload(
        meta_pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StaleObjectPayload>, crate::error::MetadataError> {
        let stored =
            match PgMetadataStore::get_object_version(meta_pg, bucket, key, VersionId::Null) {
                Ok(stored) => stored,
                Err(crate::error::MetadataError::ObjectNotFound) => return Ok(None),
                Err(other) => return Err(other),
            };
        let record = match stored {
            StoredObject::Live(record) => record,
            StoredObject::DeleteMarker(_) => return Ok(None),
        };

        match record.layout {
            ObjectLayout::MultipartManifest { .. } => {
                let parts =
                    PgMetadataStore::get_object_parts(meta_pg, bucket, key, VersionId::Null)?;
                let mut streaming_segments = Vec::new();
                for part in &parts {
                    if part.part_okh == [0u8; 16] {
                        let segments = PgMetadataStore::get_multipart_part_segments(
                            meta_pg,
                            bucket,
                            key,
                            VersionId::Null,
                            part.part_number,
                        )?;
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
                let segments =
                    PgMetadataStore::get_object_segments(meta_pg, bucket, key, VersionId::Null)?;
                Ok(Some(StaleObjectPayload::Segments {
                    generation_id: record.generation_id,
                    segments,
                }))
            }
        }
    }

    fn delete_stale_object_payload_metadata(
        meta_pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        payload: &StaleObjectPayload,
    ) -> Result<(), crate::error::MetadataError> {
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
                PgMetadataStore::delete_object_segments(meta_pg, bucket, key, version_id)?;
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
                    PgMetadataStore::delete_multipart_part_segments(
                        meta_pg, bucket, key, version_id,
                    )?;
                }
                PgMetadataStore::delete_object_parts(meta_pg, bucket, key, version_id)?;
            }
        }
        Ok(())
    }

    fn expire_current_live_suspended_from_pg(
        meta_pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        owner: OwnerIdentity,
    ) -> Result<ExpireCurrentObjectOutcome, crate::error::MetadataError> {
        let stale_payload = Self::snapshot_overwritten_null_version_payload(meta_pg, bucket, key)?;
        meta_pg.put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner,
        }))?;
        let reclaim_generation_id = stale_payload.as_ref().map(|payload| match payload {
            StaleObjectPayload::Segments { generation_id, .. }
            | StaleObjectPayload::Multipart { generation_id, .. } => *generation_id,
        });
        if let Some(payload) = stale_payload.as_ref() {
            Self::delete_stale_object_payload_metadata(
                meta_pg,
                bucket,
                key,
                VersionId::Null,
                payload,
            )?;
        }
        Ok(ExpireCurrentObjectOutcome {
            reclaim_generation_id,
        })
    }

    pub fn delete_specific_object_version_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectPgActionError> {
        let meta_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = match PgMetadataStore::get_object_version(&*meta_pg, bucket, key, version_id) {
            Ok(stored) => Some(stored),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };

        let value = match action(stored.as_ref()) {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };

        let deleted = match stored.as_ref() {
            None => DeletedSpecificObjectVersion::Missing,
            Some(StoredObject::DeleteMarker(_)) => {
                PgMetadataStore::delete_object_version(&*meta_pg, bucket, key, version_id)?;
                DeletedSpecificObjectVersion::DeleteMarker
            }
            Some(StoredObject::Live(record)) => {
                Self::delete_live_object_from_pg(&meta_pg, bucket, key, record)?
            }
        };

        Ok(Ok(DeleteSpecificObjectVersionOutcome { value, deleted }))
    }

    pub fn delete_current_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectPgActionError> {
        let meta_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = match PgMetadataStore::get_object_meta(&*meta_pg, bucket, key) {
            Ok(stored) => Some(stored),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };

        let value = match action(stored.as_ref()) {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };

        let deleted = match stored.as_ref() {
            None => DeletedCurrentObject::Missing,
            Some(StoredObject::DeleteMarker(_)) => DeletedCurrentObject::DeleteMarker,
            Some(StoredObject::Live(record)) => {
                let deleted = Self::delete_live_object_from_pg(&meta_pg, bucket, key, record)?;
                match deleted {
                    DeletedSpecificObjectVersion::Missing => DeletedCurrentObject::Missing,
                    DeletedSpecificObjectVersion::DeleteMarker => {
                        DeletedCurrentObject::DeleteMarker
                    }
                    DeletedSpecificObjectVersion::Live {
                        generation_id,
                        layout,
                    } => DeletedCurrentObject::Live {
                        generation_id,
                        layout,
                    },
                }
            }
        };

        Ok(Ok(DeleteCurrentObjectOutcome { value, deleted }))
    }

    pub fn insert_current_delete_marker_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        owner: OwnerIdentity,
        action: impl FnOnce(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectPgActionError> {
        let meta_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = match PgMetadataStore::get_object_meta(&*meta_pg, bucket, key) {
            Ok(stored) => Some(stored),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        let value = match action(stored.as_ref()) {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let marker_vid = PgMetadataStore::next_version_id(&*meta_pg, bucket, key)?;
        meta_pg.put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: marker_vid,
            owner,
        }))?;
        Ok(Ok(InsertCurrentDeleteMarkerOutcome {
            value,
            version_id: marker_vid,
        }))
    }

    fn expire_current_object_from_object_pg_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        versioning: BucketVersioningState,
        owner: OwnerIdentity,
        should_expire: impl FnOnce(&LiveObjectRecord) -> Result<bool, E>,
    ) -> Result<Result<Option<ExpireCurrentObjectOutcome>, E>, ObjectPgActionError> {
        let meta_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = match PgMetadataStore::get_object_meta(&*meta_pg, bucket, key) {
            Ok(stored) => stored,
            Err(crate::error::MetadataError::ObjectNotFound) => return Ok(Ok(None)),
            Err(other) => return Err(other.into()),
        };
        let record = match stored {
            StoredObject::Live(record) if record.version_id == expected_version_id => record,
            _ => return Ok(Ok(None)),
        };

        let should_expire = match should_expire(&record) {
            Ok(should_expire) => should_expire,
            Err(error) => return Ok(Err(error)),
        };
        if !should_expire {
            return Ok(Ok(None));
        }

        let outcome = match versioning {
            BucketVersioningState::Disabled => {
                let deleted = Self::delete_live_object_from_pg(&meta_pg, bucket, key, &record)?;
                let reclaim_generation_id = match deleted {
                    DeletedSpecificObjectVersion::Live { generation_id, .. } => Some(generation_id),
                    DeletedSpecificObjectVersion::Missing
                    | DeletedSpecificObjectVersion::DeleteMarker => None,
                };
                ExpireCurrentObjectOutcome {
                    reclaim_generation_id,
                }
            }
            BucketVersioningState::Enabled => {
                let marker_vid = PgMetadataStore::next_version_id(&*meta_pg, bucket, key)?;
                meta_pg.put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: marker_vid,
                    owner,
                }))?;
                ExpireCurrentObjectOutcome {
                    reclaim_generation_id: None,
                }
            }
            BucketVersioningState::Suspended => {
                Self::expire_current_live_suspended_from_pg(&meta_pg, bucket, key, owner)?
            }
        };

        Ok(Ok(Some(outcome)))
    }

    pub fn expire_current_object_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        should_expire: impl FnOnce(Option<&str>, &LiveObjectRecord) -> Result<bool, E>,
    ) -> Result<Result<Option<ExpireCurrentObjectOutcome>, E>, ObjectPgActionError> {
        let Some((_bucket_guard, bucket_info, raw_lifecycle)) =
            self.lock_bucket_and_load_lifecycle_context(bucket)?
        else {
            return Ok(Ok(None));
        };
        if raw_lifecycle.is_none() {
            return Ok(Ok(None));
        }

        let owner = OwnerIdentity::new(
            bucket_info.owner_principal.clone(),
            bucket_info.owner_canonical_id.clone(),
        );
        self.expire_current_object_from_object_pg_if(
            bucket,
            key,
            expected_version_id,
            bucket_info.versioning,
            owner,
            |record| should_expire(raw_lifecycle.as_deref(), record),
        )
    }

    fn delete_noncurrent_live_versions_from_object_pg_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        select_versions: impl FnOnce(&[StoredObject]) -> Result<HashSet<VersionId>, E>,
    ) -> Result<Result<Vec<GenerationId>, E>, ObjectPgActionError> {
        let meta_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let versions = match PgMetadataStore::list_object_versions_for_key(&*meta_pg, bucket, key) {
            Ok(versions) => versions,
            Err(crate::error::MetadataError::ObjectNotFound) => return Ok(Ok(Vec::new())),
            Err(other) => return Err(other.into()),
        };
        let due_version_ids = match select_versions(&versions) {
            Ok(version_ids) => version_ids,
            Err(error) => return Ok(Err(error)),
        };
        if due_version_ids.is_empty() {
            return Ok(Ok(Vec::new()));
        }

        let mut reclaimed_generation_ids = Vec::new();
        for stored in versions {
            let Some(record) = stored.into_live() else {
                continue;
            };
            if !due_version_ids.contains(&record.version_id) {
                continue;
            }
            let deleted = Self::delete_live_object_from_pg(&meta_pg, bucket, key, &record)?;
            if let DeletedSpecificObjectVersion::Live { generation_id, .. } = deleted {
                reclaimed_generation_ids.push(generation_id);
            }
        }

        Ok(Ok(reclaimed_generation_ids))
    }

    pub fn delete_noncurrent_live_versions_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        select_versions: impl FnOnce(Option<&str>, &[StoredObject]) -> Result<HashSet<VersionId>, E>,
    ) -> Result<Result<Vec<GenerationId>, E>, ObjectPgActionError> {
        let Some((_bucket_guard, _bucket_info, raw_lifecycle)) =
            self.lock_bucket_and_load_lifecycle_context(bucket)?
        else {
            return Ok(Ok(Vec::new()));
        };
        if raw_lifecycle.is_none() {
            return Ok(Ok(Vec::new()));
        }

        self.delete_noncurrent_live_versions_from_object_pg_if(bucket, key, |versions| {
            select_versions(raw_lifecycle.as_deref(), versions)
        })
    }

    fn delete_expired_delete_marker_from_object_pg_if<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        should_delete: impl FnOnce(&[StoredObject]) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        let meta_pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let versions = match PgMetadataStore::list_object_versions_for_key(&*meta_pg, bucket, key) {
            Ok(versions) => versions,
            Err(crate::error::MetadataError::ObjectNotFound) => return Ok(Ok(false)),
            Err(other) => return Err(other.into()),
        };
        let should_delete = match should_delete(&versions) {
            Ok(should_delete) => should_delete,
            Err(error) => return Ok(Err(error)),
        };
        if !should_delete {
            return Ok(Ok(false));
        }
        PgMetadataStore::delete_object_version(&*meta_pg, bucket, key, expected_version_id)?;
        Ok(Ok(true))
    }

    pub fn delete_expired_delete_marker_if_due<E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        should_delete: impl FnOnce(Option<&str>, &[StoredObject]) -> Result<bool, E>,
    ) -> Result<Result<bool, E>, ObjectPgActionError> {
        let Some((_bucket_guard, _bucket_info, raw_lifecycle)) =
            self.lock_bucket_and_load_lifecycle_context(bucket)?
        else {
            return Ok(Ok(false));
        };
        if raw_lifecycle.is_none() {
            return Ok(Ok(false));
        }

        self.delete_expired_delete_marker_from_object_pg_if(
            bucket,
            key,
            expected_version_id,
            |versions| should_delete(raw_lifecycle.as_deref(), versions),
        )
    }
}
