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
    DeleteObjectsRequest, DeleteObjectsResult, DeletedObject, TRACE_TARGET,
};
use crate::conditional::{check_delete_conditions, DeleteCondition};
use crate::error::ServerError;

impl Coordinator {
    pub(super) fn apply_authorized_delete_object(
        &self,
        authorized: AuthorizedDeleteObject,
        cond: &DeleteCondition,
    ) -> Result<DeleteObjectResult, ServerError> {
        match authorized {
            AuthorizedDeleteObject::UnversionedDelete {
                bucket,
                key,
                requester,
                bucket_info,
                bucket_policy,
                bucket_tags,
            } => {
                let deleted = self
                    .storage_node
                    .delete_current_object_if(&bucket, &key, |stored| -> Result<(), ServerError> {
                        if !self.requester_can_delete_object_with_bucket_policy(
                            crate::coordinator::authz::BucketPolicyAccess {
                                requester: &requester,
                                bucket: &bucket_info,
                                bucket_tags: bucket_tags.as_deref(),
                                policy: bucket_policy.as_deref(),
                            },
                            key.as_str(),
                            stored,
                            Self::delete_object_policy_action(None),
                        )? {
                            return Err(ServerError::AccessDenied);
                        }

                        if !cond.is_empty() {
                            let record = match stored {
                                Some(StoredObject::Live(record)) => record,
                                _ => return Err(ServerError::PreconditionFailed),
                            };
                            let etag_str = record.etag.format();
                            check_delete_conditions(cond, &etag_str)?;
                        }
                        Ok(())
                    })
                    .map_err(|error| match error {
                        storage::ObjectPgActionError::Store(error) => ServerError::Store(error),
                        storage::ObjectPgActionError::InvalidRequest { reason } => {
                            ServerError::InvalidRequest { reason }
                        }
                        storage::ObjectPgActionError::Metadata(error) => {
                            ServerError::Metadata(error)
                        }
                        storage::ObjectPgActionError::StaleObjectReadSubject => {
                            ServerError::InternalError {
                                reason: "stale object read subject escaped storage retry loop"
                                    .to_string(),
                            }
                        }
                        storage::ObjectPgActionError::StaleStreamFinalizeSnapshot => {
                            ServerError::InternalError {
                                reason: "stale stream finalize snapshot escaped storage retry loop"
                                    .to_string(),
                            }
                        }
                    })??;

                if let storage::DeletedCurrentObject::Live {
                    generation_id,
                    layout,
                } = deleted.deleted
                {
                    #[cfg(test)]
                    match layout {
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
                    #[cfg(not(test))]
                    let _ = layout;
                    self.read_runtime().enqueue_object_payload_reclaim_for(
                        &bucket,
                        &key,
                        generation_id,
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
            AuthorizedDeleteObject::SpecificVersion {
                bucket,
                key,
                version_id,
                requester,
                bucket_info,
                bucket_policy,
                bucket_tags,
                bypass_governance,
            } => {
                if !cond.is_empty() {
                    return Err(ServerError::NotImplemented {
                        feature: "conditional delete with versionId".to_string(),
                    });
                }

                let deleted = self
                    .storage_node
                    .delete_specific_object_version_if(
                        &bucket,
                        &key,
                        version_id,
                        |stored| -> Result<(), ServerError> {
                            if !self.requester_can_delete_object_version_with_bucket_policy(
                                crate::coordinator::authz::BucketPolicyAccess {
                                    requester: &requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key.as_str(),
                                stored,
                                Self::delete_object_policy_action(Some(version_id)),
                                version_id,
                            )? {
                                return Err(ServerError::AccessDenied);
                            }

                            match stored {
                                None => {
                                    if bucket_info.object_lock.enabled
                                        && bypass_governance
                                        && !self
                                            .requester_can_bypass_governance_retention_for_missing_version_with_bucket_policy(
                                                &requester,
                                                &bucket_info,
                                                bucket_tags.as_deref(),
                                                key.as_str(),
                                                bucket_policy.as_deref(),
                                            )?
                                    {
                                        return Err(ServerError::AccessDenied);
                                    }
                                }
                                Some(StoredObject::Live(record)) => {
                                    let can_bypass_governance = self
                                        .requester_can_bypass_governance_retention_with_bucket_policy(
                                            &requester,
                                            &bucket_info,
                                            bucket_tags.as_deref(),
                                            stored.expect("stored live object"),
                                            bucket_policy.as_deref(),
                                        )?;
                                    Self::validate_delete_against_object_lock(
                                        record.object_lock,
                                        bypass_governance,
                                        can_bypass_governance,
                                        Self::current_unix_seconds()?,
                                    )?;
                                }
                                Some(StoredObject::DeleteMarker(_)) => {}
                            }
                            Ok(())
                        },
                    )
                    .map_err(|error| match error {
                        storage::ObjectPgActionError::Store(error) => ServerError::Store(error),
                        storage::ObjectPgActionError::InvalidRequest { reason } => {
                            ServerError::InvalidRequest { reason }
                        }
                        storage::ObjectPgActionError::Metadata(error) => {
                            ServerError::Metadata(error)
                        }
                        storage::ObjectPgActionError::StaleObjectReadSubject => {
                            ServerError::InternalError {
                                reason: "stale object read subject escaped storage retry loop"
                                    .to_string(),
                            }
                        }
                        storage::ObjectPgActionError::StaleStreamFinalizeSnapshot => {
                            ServerError::InternalError {
                                reason: "stale stream finalize snapshot escaped storage retry loop"
                                    .to_string(),
                            }
                        }
                    })??;

                match deleted.deleted {
                    storage::DeletedSpecificObjectVersion::Missing => Ok(DeleteObjectResult {
                        version_id,
                        delete_marker: false,
                    }),
                    storage::DeletedSpecificObjectVersion::DeleteMarker => Ok(DeleteObjectResult {
                        version_id,
                        delete_marker: true,
                    }),
                    storage::DeletedSpecificObjectVersion::Live {
                        generation_id,
                        layout,
                    } => {
                        #[cfg(test)]
                        match layout {
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
                        #[cfg(not(test))]
                        let _ = layout;
                        self.read_runtime().enqueue_object_payload_reclaim_for(
                            &bucket,
                            &key,
                            generation_id,
                        );

                        Ok(DeleteObjectResult {
                            version_id,
                            delete_marker: false,
                        })
                    }
                }
            }
            AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                bucket,
                key,
                owner,
                requester,
                bucket_info,
                bucket_policy,
                bucket_tags,
            } => {
                let marker = self
                    .storage_node
                    .insert_current_delete_marker_if(
                        &bucket,
                        &key,
                        owner,
                        |stored| -> Result<(), ServerError> {
                            if !self.requester_can_delete_object_with_bucket_policy(
                                crate::coordinator::authz::BucketPolicyAccess {
                                    requester: &requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key.as_str(),
                                stored,
                                Self::delete_object_policy_action(None),
                            )? {
                                return Err(ServerError::AccessDenied);
                            }
                            if !cond.is_empty() {
                                let record = match stored {
                                    Some(StoredObject::Live(record)) => record,
                                    _ => return Err(ServerError::PreconditionFailed),
                                };
                                let etag_str = record.etag.format();
                                check_delete_conditions(cond, &etag_str)?;
                            }
                            Ok(())
                        },
                    )
                    .map_err(|error| match error {
                        storage::ObjectPgActionError::Store(error) => ServerError::Store(error),
                        storage::ObjectPgActionError::InvalidRequest { reason } => {
                            ServerError::InvalidRequest { reason }
                        }
                        storage::ObjectPgActionError::Metadata(error) => {
                            ServerError::Metadata(error)
                        }
                        storage::ObjectPgActionError::StaleObjectReadSubject => {
                            ServerError::InternalError {
                                reason: "stale object read subject escaped storage retry loop"
                                    .to_string(),
                            }
                        }
                        storage::ObjectPgActionError::StaleStreamFinalizeSnapshot => {
                            ServerError::InternalError {
                                reason: "stale stream finalize snapshot escaped storage retry loop"
                                    .to_string(),
                            }
                        }
                    })??;

                Ok(DeleteObjectResult {
                    version_id: marker.version_id,
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
