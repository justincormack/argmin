use s3_types::VersionId;
#[cfg(test)]
use storage::ObjectLayout;
use storage::StoredObject;

#[cfg(test)]
use super::{
    maybe_run_multipart_delete_metadata_hook, maybe_run_object_segments_delete_metadata_hook,
};
use super::{
    AuthorizedDeleteObject, Coordinator, DeleteError, DeleteObjectRequest, DeleteObjectResult,
    DeleteObjectsRequest, DeleteObjectsResult, DeletedObject, LockedReadObject, TRACE_TARGET,
};
use crate::conditional::{check_delete_conditions, DeleteCondition};
use crate::error::ServerError;

impl Coordinator {
    fn apply_authorized_delete_object(
        &self,
        authorized: AuthorizedDeleteObject<'_>,
        cond: &DeleteCondition,
    ) -> Result<DeleteObjectResult, ServerError> {
        match authorized {
            AuthorizedDeleteObject::UnversionedMissing => {
                if !cond.is_empty() {
                    return Err(ServerError::PreconditionFailed);
                }
                Ok(DeleteObjectResult {
                    version_id: VersionId::Null,
                    delete_marker: false,
                })
            }
            AuthorizedDeleteObject::UnversionedStored {
                bucket,
                key,
                stored,
                pgs,
            } => {
                let StoredObject::Live(record) = stored else {
                    return Ok(DeleteObjectResult {
                        version_id: VersionId::Null,
                        delete_marker: false,
                    });
                };

                if !cond.is_empty() {
                    let etag_str = record.etag.format();
                    check_delete_conditions(cond, &etag_str)?;
                }

                let meta_pg = pgs.meta();
                let reclaim =
                    Self::permanently_delete_live_object_locked(meta_pg, &bucket, &key, &record)?;
                drop(pgs);
                #[cfg(test)]
                match record.layout {
                    ObjectLayout::MultipartManifest { .. } => {
                        maybe_run_multipart_delete_metadata_hook(bucket.as_str(), key.as_str());
                    }
                    ObjectLayout::Standard => {
                        maybe_run_object_segments_delete_metadata_hook(
                            bucket.as_str(),
                            key.as_str(),
                        );
                    }
                }
                if let Some(reclaim) = reclaim {
                    self.read_runtime().enqueue_object_payload_reclaim_for(
                        &bucket,
                        &key,
                        reclaim.generation_id,
                    );
                }

                Ok(DeleteObjectResult {
                    version_id: VersionId::Null,
                    delete_marker: false,
                })
            }
            AuthorizedDeleteObject::SpecificVersionMissing { version_id } => {
                Ok(DeleteObjectResult {
                    version_id,
                    delete_marker: false,
                })
            }
            AuthorizedDeleteObject::SpecificVersionStored {
                bucket,
                key,
                version_id,
                stored,
                pgs,
            } => {
                if !cond.is_empty() {
                    return Err(ServerError::NotImplemented {
                        feature: "conditional delete with versionId".to_string(),
                    });
                }

                let meta_pg = pgs.meta();
                match stored {
                    StoredObject::Live(record) => {
                        let reclaim = Self::permanently_delete_live_object_locked(
                            meta_pg, &bucket, &key, &record,
                        )?;
                        drop(pgs);
                        #[cfg(test)]
                        match record.layout {
                            ObjectLayout::MultipartManifest { .. } => {
                                maybe_run_multipart_delete_metadata_hook(
                                    bucket.as_str(),
                                    key.as_str(),
                                );
                            }
                            ObjectLayout::Standard => {
                                maybe_run_object_segments_delete_metadata_hook(
                                    bucket.as_str(),
                                    key.as_str(),
                                );
                            }
                        }
                        if let Some(reclaim) = reclaim {
                            self.read_runtime().enqueue_object_payload_reclaim_for(
                                &bucket,
                                &key,
                                reclaim.generation_id,
                            );
                        }

                        Ok(DeleteObjectResult {
                            version_id,
                            delete_marker: false,
                        })
                    }
                    StoredObject::DeleteMarker(_) => {
                        storage::PgMetadataStore::delete_object_version(
                            meta_pg, &bucket, &key, version_id,
                        )?;
                        Ok(DeleteObjectResult {
                            version_id,
                            delete_marker: true,
                        })
                    }
                }
            }
            AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                bucket,
                key,
                owner,
                current,
            } => {
                let marker_vid = match current {
                    Some(LockedReadObject {
                        record: stored,
                        pgs,
                    }) => {
                        if !cond.is_empty() {
                            let record = match stored {
                                StoredObject::Live(record) => record,
                                StoredObject::DeleteMarker(_) => {
                                    return Err(ServerError::PreconditionFailed);
                                }
                            };
                            let etag_str = record.etag.format();
                            check_delete_conditions(cond, &etag_str)?;
                        }

                        let meta_pg = pgs.meta();
                        let marker_vid =
                            storage::PgMetadataStore::next_version_id(meta_pg, &bucket, &key)?;
                        Self::put_delete_marker_locked(meta_pg, &bucket, &key, marker_vid, owner)?;
                        marker_vid
                    }
                    None => {
                        if !cond.is_empty() {
                            return Err(ServerError::PreconditionFailed);
                        }
                        let meta_pg_id = self.object_pg_id_for(&bucket, &key);
                        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
                        let marker_vid =
                            storage::PgMetadataStore::next_version_id(&*meta_pg, &bucket, &key)?;
                        Self::put_delete_marker_locked(&meta_pg, &bucket, &key, marker_vid, owner)?;
                        marker_vid
                    }
                };

                Ok(DeleteObjectResult {
                    version_id: marker_vid,
                    delete_marker: true,
                })
            }
        }
    }

    /// Delete an object.
    pub fn delete_object(
        &self,
        req: &DeleteObjectRequest,
    ) -> Result<DeleteObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_object",
            "bucket={:?} key={:?} version_id={:?} bypass={}",
            req.object.bucket_name(),
            req.object.key(),
            req.object.version_id,
            req.bypass_governance
        );
        let authorized = self.authorize_delete_object(req)?;
        self.apply_authorized_delete_object(authorized, req.cond)
    }

    /// Batch-delete objects.
    pub fn delete_objects(
        &self,
        req: &DeleteObjectsRequest,
    ) -> Result<DeleteObjectsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_objects",
            "bucket={:?} objects={} bypass={}",
            req.bucket.name(),
            req.entries.len(),
            req.bypass_governance
        );
        let entries = req.entries;
        self.checked_active_bucket_summary_for(
            req.bucket.name_typed(),
            req.expected_bucket_owner(),
        )?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match self
                .authorize_delete_objects_entry(req, entry)
                .and_then(|authorized| self.apply_authorized_delete_object(authorized, &entry.cond))
            {
                Ok(result) => {
                    deleted.push(DeletedObject {
                        key: entry.key.to_string(),
                        version_id: result.version_id,
                        delete_marker: result.delete_marker,
                    });
                }
                Err(e) => {
                    errors.push(DeleteError {
                        key: entry.key.to_string(),
                        version_id: entry.version_id,
                        code: e.s3_error_code().to_string(),
                        message: e.to_string(),
                    });
                }
            }
        }

        Ok(DeleteObjectsResult { deleted, errors })
    }
}
