use super::*;
use crate::{ObjectLayout, ObjectReadSnapshotMode, VersionId};

impl SharedStorageNode {
    #[cfg(test)]
    pub fn load_object_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<T, E>, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = match version_id {
            Some(version_id) => PgMetadataStore::get_object_version(&*pg, bucket, key, version_id)?,
            None => PgMetadataStore::get_object_meta(&*pg, bucket, key)?,
        };
        Ok(action(&stored))
    }

    #[cfg(test)]
    pub fn load_existing_live_object(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Option<StoredObject>, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Ok(Self::load_existing_live_object_from_object_pg(
            &pg, bucket, key,
        )?)
    }

    #[cfg(test)]
    pub fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Ok(PgMetadataStore::payload_reclaim_exists(
            &*pg,
            bucket,
            key,
            generation_id,
        )?)
    }

    #[cfg(test)]
    pub(crate) fn load_existing_live_object_from_object_pg(
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

    #[cfg(test)]
    pub fn load_object_read_auth_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Self::load_object_read_auth_subject_from_object_pg(&pg, bucket, key, version_id)
    }

    pub(crate) fn load_object_read_auth_subject_from_object_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<ObjectReadAuthSubject, ObjectPgActionError> {
        let stored = Self::load_stored_object_from_object_pg(pg, bucket, key, version_id)?;
        Ok(ObjectReadAuthSubject {
            identity: ObjectReadAuthSubjectIdentity::for_stored(&stored),
            stored,
        })
    }

    #[cfg(test)]
    pub fn load_object_read_snapshot_for_subject(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        Self::load_object_read_snapshot_for_subject_from_object_pg(
            &pg,
            bucket,
            key,
            version_id,
            expected_identity,
            snapshot_mode,
        )
    }

    pub(crate) fn load_object_read_snapshot_for_subject_from_object_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        expected_identity: &ObjectReadAuthSubjectIdentity,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let stored = match Self::load_stored_object_from_object_pg(pg, bucket, key, version_id) {
            Ok(stored) => stored,
            Err(ObjectPgActionError::Metadata(crate::error::MetadataError::ObjectNotFound)) => {
                return Err(ObjectPgActionError::StaleObjectReadSubject);
            }
            Err(error) => return Err(error),
        };
        if !expected_identity.matches_stored(&stored) {
            return Err(ObjectPgActionError::StaleObjectReadSubject);
        }
        Self::snapshot_object_read_from_pg(pg, bucket, key, &stored, snapshot_mode)
    }

    #[cfg(test)]
    pub fn load_object_read_snapshot_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        snapshot_mode: ObjectReadSnapshotMode,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = Self::load_stored_object_from_object_pg(&pg, bucket, key, version_id)?;
        let result = match action(&stored) {
            Ok(value) => Ok(ObjectReadSnapshotOutcome {
                value,
                snapshot: Self::snapshot_object_read_from_pg(
                    &pg,
                    bucket,
                    key,
                    &stored,
                    snapshot_mode,
                )?,
            }),
            Err(error) => Err(error),
        };
        Ok(result)
    }

    pub(crate) fn load_stored_object_from_object_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ObjectPgActionError> {
        Ok(match version_id {
            Some(version_id) => PgMetadataStore::get_object_version(pg, bucket, key, version_id)?,
            None => PgMetadataStore::get_object_meta(pg, bucket, key)?,
        })
    }

    fn snapshot_object_read_from_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        stored: &StoredObject,
        snapshot_mode: ObjectReadSnapshotMode,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let (object_segments, multipart_parts, multipart_part_segments) = match stored {
            StoredObject::DeleteMarker(_) => (Vec::new(), Vec::new(), Vec::new()),
            StoredObject::Live(record) => match (record.layout, snapshot_mode) {
                (_, ObjectReadSnapshotMode::MetadataOnly) => (Vec::new(), Vec::new(), Vec::new()),
                (ObjectLayout::Standard, ObjectReadSnapshotMode::StandardSegments)
                | (ObjectLayout::Standard, ObjectReadSnapshotMode::FullPayloadLayout) => (
                    PgMetadataStore::get_object_segments(pg, bucket, key, record.version_id)?,
                    Vec::new(),
                    Vec::new(),
                ),
                (ObjectLayout::Standard, ObjectReadSnapshotMode::MultipartParts) => {
                    (Vec::new(), Vec::new(), Vec::new())
                }
                (
                    ObjectLayout::MultipartManifest { .. },
                    ObjectReadSnapshotMode::MultipartParts,
                ) => (
                    Vec::new(),
                    PgMetadataStore::get_object_parts(pg, bucket, key, record.version_id)?,
                    Vec::new(),
                ),
                (
                    ObjectLayout::MultipartManifest { .. },
                    ObjectReadSnapshotMode::FullPayloadLayout,
                ) => {
                    let multipart_parts =
                        PgMetadataStore::get_object_parts(pg, bucket, key, record.version_id)?;
                    let mut multipart_part_segments = Vec::new();
                    for part in &multipart_parts {
                        multipart_part_segments.extend(
                            PgMetadataStore::get_multipart_part_segments(
                                pg,
                                bucket,
                                key,
                                record.version_id,
                                part.part_number,
                            )?,
                        );
                    }
                    (Vec::new(), multipart_parts, multipart_part_segments)
                }
                (
                    ObjectLayout::MultipartManifest { .. },
                    ObjectReadSnapshotMode::StandardSegments,
                ) => (Vec::new(), Vec::new(), Vec::new()),
            },
        };

        Ok(ObjectReadSnapshot {
            stored: stored.clone(),
            object_segments,
            multipart_parts,
            multipart_part_segments,
        })
    }
}
