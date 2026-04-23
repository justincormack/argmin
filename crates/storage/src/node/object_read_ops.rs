use super::*;
use crate::{ObjectLayout, VersionId};

impl SharedStorageNode {
    pub fn load_object_read_snapshot_if<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let stored = match version_id {
            Some(version_id) => PgMetadataStore::get_object_version(&*pg, bucket, key, version_id)?,
            None => PgMetadataStore::get_object_meta(&*pg, bucket, key)?,
        };
        let result = match action(&stored) {
            Ok(value) => Ok(ObjectReadSnapshotOutcome {
                value,
                snapshot: Self::snapshot_object_read_from_pg(&pg, bucket, key, &stored)?,
            }),
            Err(error) => Err(error),
        };
        Ok(result)
    }

    fn snapshot_object_read_from_pg(
        pg: &PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        stored: &StoredObject,
    ) -> Result<ObjectReadSnapshot, ObjectPgActionError> {
        let (object_segments, multipart_parts, multipart_part_segments) = match stored {
            StoredObject::DeleteMarker(_) => (Vec::new(), Vec::new(), Vec::new()),
            StoredObject::Live(record) => match record.layout {
                ObjectLayout::Standard => (
                    PgMetadataStore::get_object_segments(pg, bucket, key, record.version_id)?,
                    Vec::new(),
                    Vec::new(),
                ),
                ObjectLayout::MultipartManifest { .. } => {
                    let multipart_parts =
                        PgMetadataStore::get_object_parts(pg, bucket, key, record.version_id)?;
                    let mut multipart_part_segments = Vec::new();
                    for part in &multipart_parts {
                        if part.part_okh == [0u8; 16] {
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
                    }
                    (Vec::new(), multipart_parts, multipart_part_segments)
                }
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
