use super::*;

impl SharedStorageNode {
    #[cfg(any(test, feature = "test-hooks"))]
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

    #[cfg(any(test, feature = "test-hooks"))]
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

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn load_multipart_completion_snapshot(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        requested_part_numbers: &[u32],
    ) -> Result<MultipartCompletionSnapshot, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let upload =
            Self::load_in_progress_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(crate::error::MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
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

    #[cfg(test)]
    pub fn load_multipart_completion_preflight(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<MultipartCompletionPreflight, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let upload =
            Self::load_in_progress_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(crate::error::MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let existing_etag = match PgMetadataStore::get_object_meta(&*pg, bucket, key) {
            Ok(stored) => stored.as_live().map(|record| record.etag.format()),
            Err(crate::error::MetadataError::ObjectNotFound) => None,
            Err(other) => return Err(other.into()),
        };
        Ok(MultipartCompletionPreflight { existing_etag })
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub fn list_multipart_parts_for_authorized_upload(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, ObjectPgActionError> {
        let bucket = &authorized_upload.record().bucket;
        let key = &authorized_upload.record().key;
        let upload_id = &authorized_upload.record().upload_id;
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        let upload =
            Self::load_in_progress_multipart_upload_from_object_pg(&pg, bucket, key, upload_id)?;
        if upload != *authorized_upload.record() {
            return Err(crate::error::MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            }
            .into());
        }
        let response = pg.list_multipart_parts(&ListPartsReq {
            upload_id: upload_id.clone(),
            part_number_marker,
            max_parts,
        })?;
        Ok(ListedMultipartParts { upload, response })
    }

    #[cfg(test)]
    pub fn lookup_multipart_upload_management(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadManagementLookup, ObjectPgActionError> {
        let pg = self.get_pg(self.pg_topology.object_pg_for(bucket, key))?;
        match Self::load_multipart_upload_from_object_pg(&pg, bucket, key, upload_id) {
            Ok(upload) if upload.state == UploadState::InProgress => {
                return Ok(MultipartUploadManagementLookup::InProgress(Box::new(
                    upload,
                )));
            }
            Ok(upload) => {
                return Ok(MultipartUploadManagementLookup::NonInProgress(Box::new(
                    upload,
                )));
            }
            Err(crate::error::MetadataError::NoSuchUpload { .. }) => {}
            Err(error) => return Err(error.into()),
        }

        if let Some(completed) = pg.get_completed_multipart_upload(upload_id)? {
            if completed.bucket == *bucket && completed.key == *key {
                return Ok(MultipartUploadManagementLookup::Completed(completed));
            }
        }
        Ok(MultipartUploadManagementLookup::Missing)
    }
}
