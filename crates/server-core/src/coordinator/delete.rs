// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use s3_types::VersionId;
#[cfg(test)]
use storage::ObjectLayout;
#[cfg(test)]
use storage::StorageCluster;
use storage::{StorageClusterRouteAdmission, StoredObject};

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
    pub(super) fn map_delete_object_pg_action_error(
        error: storage::ObjectMetadataMutationFailure,
        cond: &DeleteCondition,
        key: &str,
    ) -> ServerError {
        if !cond.is_empty()
            && error.kind() == storage::ObjectMetadataMutationFailureKind::MetadataCommandContention
        {
            ServerError::ConditionalRequestConflict {
                key: key.to_string(),
                condition: "If-Match",
            }
        } else {
            match error.kind() {
                storage::ObjectMetadataMutationFailureKind::ResourceExhausted
                | storage::ObjectMetadataMutationFailureKind::MetadataCommandContention
                | storage::ObjectMetadataMutationFailureKind::RetryableConvergence => {
                    ServerError::SlowDown
                }
                storage::ObjectMetadataMutationFailureKind::ObjectNotFound
                | storage::ObjectMetadataMutationFailureKind::InternalError => {
                    ServerError::ObjectMetadataMutation(error)
                }
            }
        }
    }

    pub(super) fn apply_authorized_delete_object_on_admitted_route(
        &self,
        admission: &StorageClusterRouteAdmission,
        authorized: AuthorizedDeleteObject,
        cond: &DeleteCondition,
    ) -> Result<DeleteObjectResult, ServerError> {
        self.require_storage_route_admission(admission)?;
        match authorized {
            AuthorizedDeleteObject::UnversionedDelete {
                bucket,
                key,
                requester,
                bucket_info,
                bucket_policy,
                bucket_tags,
            } => {
                let route = admission
                    .active_object_metadata_mutation_route(&bucket, &key, None)
                    .map_err(super::map_store_failure)?;
                let deleted = route
                    .delete_current_object_if(|stored| -> Result<(), ServerError> {
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
                                _ => {
                                    return Err(ServerError::ObjectNotFound {
                                        bucket: bucket.as_str().to_string(),
                                        key: key.as_str().to_string(),
                                    });
                                }
                            };
                            let etag_str = record.etag.format();
                            check_delete_conditions(cond, &etag_str)?;
                        }
                        Ok(())
                    })
                    .map_err(|error| {
                        Self::map_delete_object_pg_action_error(error, cond, key.as_str())
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
                    route.enqueue_object_payload_reclaim(generation_id);
                }

                Ok(DeleteObjectResult {
                    version_id: None,
                    delete_marker: false,
                })
            }
            AuthorizedDeleteObject::SpecificVersionMissing { version_id } => {
                Ok(DeleteObjectResult {
                    version_id: Some(version_id),
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

                let route = admission
                    .active_object_metadata_mutation_route(&bucket, &key, Some(version_id))
                    .map_err(super::map_store_failure)?;
                let deleted = route
                    .delete_specific_object_version_if(|stored| -> Result<(), ServerError> {
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
                        })
                    .map_err(|error| {
                        Self::map_delete_object_pg_action_error(error, cond, key.as_str())
                    })??;

                match deleted.deleted {
                    storage::DeletedSpecificObjectVersion::Missing => Ok(DeleteObjectResult {
                        version_id: Some(version_id),
                        delete_marker: false,
                    }),
                    storage::DeletedSpecificObjectVersion::DeleteMarker => Ok(DeleteObjectResult {
                        version_id: Some(version_id),
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
                        route.enqueue_object_payload_reclaim(generation_id);

                        Ok(DeleteObjectResult {
                            version_id: Some(version_id),
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
                let route = admission
                    .active_object_metadata_mutation_route(&bucket, &key, None)
                    .map_err(super::map_store_failure)?;
                let marker = route
                    .insert_current_delete_marker_if(
                        bucket_info.versioning,
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
                                    _ => {
                                        return Err(ServerError::ObjectNotFound {
                                            bucket: bucket.as_str().to_string(),
                                            key: key.as_str().to_string(),
                                        });
                                    }
                                };
                                let etag_str = record.etag.format();
                                check_delete_conditions(cond, &etag_str)?;
                            }
                            Ok(())
                        },
                    )
                    .map_err(|error| {
                        Self::map_delete_object_pg_action_error(error, cond, key.as_str())
                    })??;

                Ok(DeleteObjectResult {
                    version_id: Some(marker.version_id),
                    delete_marker: true,
                })
            }
        }
    }

    #[cfg(test)]
    pub(super) fn apply_authorized_delete_object(
        &self,
        _storage_node: &std::sync::Arc<StorageCluster>,
        authorized: AuthorizedDeleteObject,
        cond: &DeleteCondition,
    ) -> Result<DeleteObjectResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.apply_authorized_delete_object_on_admitted_route(&admission, authorized, cond)
    }

    /// Delete an object.
    pub fn delete_object(
        &self,
        req: &DeleteObjectRequest,
    ) -> Result<DeleteObjectResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_object_on_admitted_route(&admission, req)
    }

    pub fn delete_object_on_admitted_route(
        &self,
        admission: &StorageClusterRouteAdmission,
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
        self.require_storage_route_admission(admission)?;
        let authorized = self.authorize_delete_object_on_admitted_route(admission, req)?;
        self.apply_authorized_delete_object_on_admitted_route(admission, authorized, req.cond)
    }

    /// Batch-delete objects.
    pub fn delete_objects(
        &self,
        req: &DeleteObjectsRequest,
    ) -> Result<DeleteObjectsResult, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.delete_objects_on_admitted_route(&admission, req)
    }

    pub fn delete_objects_on_admitted_route(
        &self,
        admission: &StorageClusterRouteAdmission,
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
        self.require_storage_route_admission(admission)?;
        let entries = req.entries;
        self.checked_active_bucket_summary_for_admitted_route(
            admission,
            req.bucket.name_typed(),
            req.expected_bucket_owner(),
        )?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match self
                .authorize_delete_objects_entry_on_admitted_route(admission, req, entry)
                .and_then(|authorized| {
                    self.apply_authorized_delete_object_on_admitted_route(
                        admission,
                        authorized,
                        &entry.cond,
                    )
                }) {
                Ok(result) => {
                    deleted.push(DeletedObject {
                        key: entry.key.to_string(),
                        version_id: result.version_id.unwrap_or(VersionId::Null),
                        delete_marker: result.delete_marker,
                    });
                }
                Err(e) => {
                    errors.push(DeleteError {
                        key: entry.key.to_string(),
                        version_id: entry.version_id.map(Into::into),
                        code: e.s3_error_code().to_string(),
                        message: e.to_string(),
                    });
                }
            }
        }

        Ok(DeleteObjectsResult { deleted, errors })
    }
}
