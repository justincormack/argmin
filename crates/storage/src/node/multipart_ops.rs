use super::*;
#[cfg(test)]
use crate::types::{CreateMultipartUploadOutcome, CreateMultipartUploadReq, StoredObject};

impl SharedStorageNode {
    pub fn next_completed_multipart_upload_order_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, BucketSnapshotLoadError> {
        let bucket_pg = self.get_pg(self.pg_topology.bucket_pg_for(bucket))?;
        Ok(bucket_pg.next_completed_multipart_upload_order_for_bucket(bucket)?)
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

    #[cfg(test)]
    pub(crate) fn create_multipart_upload<T, E>(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateMultipartUploadReq), E>,
    ) -> Result<Result<CreateMultipartUploadOutcome<T>, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_reservation_snapshot(bucket, request, |snapshot| {
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
            Ok(result)
        })
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

    pub fn load_in_progress_multipart_upload(
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

    #[cfg(feature = "test-hooks")]
    pub fn try_load_in_progress_multipart_upload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<MultipartUploadRecord>, ObjectPgActionError> {
        let pg_id = self.pg_topology.object_pg_for(bucket, key);
        let pg = match self.stores.get(&pg_id) {
            Some(pg) => pg,
            None => return Err(crate::error::StoreError::PgNotFound { pg_id }.into()),
        };
        let pg = match pg.try_lock() {
            Ok(pg) => pg,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
        };
        Ok(Some(
            Self::load_in_progress_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?,
        ))
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
}
