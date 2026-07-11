use checksum::RawChecksum;
use std::sync::Arc;
use storage::{ObjectLayout, StoredObject};

use super::read_core::snapshotted_multipart_parts_from_storage;
#[cfg(test)]
use super::{maybe_run_multipart_snapshot_hook, maybe_run_object_read_snapshot_hook};
use super::{
    segment_payloads_from_object_segments, AuthorizedObjectRead, Coordinator,
    GetObjectAttributesRequest, GetObjectAttributesResult, GetObjectPartRequest,
    GetObjectPartResult, GetObjectRangeRequest, GetObjectRangeResult, GetObjectRequest,
    GetObjectResult, HeadObjectPartResult, HeadObjectResult, ObjectPartEntry, ObjectPartsInfo,
    ObjectVersionRequest, ReadHandle, ReadObjectContext, TRACE_TARGET,
};
use crate::conditional::check_read_conditions;
use crate::error::ServerError;

impl Coordinator {
    fn authorized_tag_count(
        &self,
        attribute_permissions: super::authz_results::ObjectAttributePermissions,
        tags: Option<&storage::SerializedTagSet>,
    ) -> Result<Option<usize>, ServerError> {
        if !attribute_permissions.tag_count_visible() {
            return Ok(None);
        }

        let Some(tags) = tags else {
            return Ok(None);
        };

        let count = Self::parse_serialized_tag_set(tags.as_str())?.len();
        Ok((count > 0).then_some(count))
    }

    /// Get an object from storage.
    pub fn get_object(&self, req: &GetObjectRequest) -> Result<GetObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object",
            "bucket={:?} key={:?} version_id={:?}",
            req.object.bucket_name(),
            req.object.key(),
            req.object.version_id
        );
        let bucket = req.object.bucket_name();
        let key = req.object.key();
        let version_id = req.object.version_id;
        let cond = req.cond;
        let storage_node = self.storage_node();
        let read_runtime = self.read_runtime_for_storage_node(Arc::clone(&storage_node));
        let AuthorizedObjectRead {
            bucket: bucket_summary,
            snapshot,
            attribute_permissions,
        } = self.authorize_get_object_with_storage_node(&storage_node, req)?;
        let storage::ObjectReadSnapshot {
            stored,
            object_segments,
            multipart_parts,
            multipart_part_segments,
        } = snapshot;
        #[cfg(test)]
        maybe_run_object_read_snapshot_hook(bucket, key);

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;
        let sse_customer =
            self.prepare_sse_customer_read_access(&record.encryption, req.sse_customer)?;
        let emit_lifecycle_expiration = version_id.is_none();

        if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
            let obj_parts = snapshotted_multipart_parts_from_storage(
                multipart_parts,
                multipart_part_segments,
                &record.encryption,
            );

            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            let body = ReadHandle::from_multipart(
                read_runtime.clone(),
                req.object.bucket_name_typed(),
                req.object.key_typed(),
                record.generation_id,
                obj_parts,
                record.size as usize,
                req.sse_customer.cloned(),
            )?;
            let lifecycle_expiration = if emit_lifecycle_expiration {
                self.current_object_lifecycle_expiration_with_storage_node(
                    &storage_node,
                    &bucket_summary,
                    key,
                    record.tags.as_deref(),
                    record.size,
                    record.last_modified,
                )?
            } else {
                None
            };
            #[cfg(test)]
            maybe_run_multipart_snapshot_hook(bucket, key);

            Ok(GetObjectResult {
                body,
                metadata,
                system_metadata,
                object_lock: attribute_permissions.visible_object_lock(record.object_lock),
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                version_id: record.version_id,
                tag_count: self
                    .authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
                managed_encryption: record.encryption.managed_encryption_algorithm(),
                sse_customer,
                lifecycle_expiration,
            })
        } else {
            let etag_crc = record.etag.crc64();
            let user_size = record.size as usize;

            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            let body = if user_size == 0 {
                ReadHandle::from_buffered_bytes(vec![])
            } else {
                let body = ReadHandle::from_segments(
                    ReadObjectContext {
                        runtime: read_runtime,
                        bucket: req.object.bucket_name_typed(),
                        key: req.object.key_typed(),
                        generation_id: record.generation_id,
                        sse_customer_request: req.sse_customer.cloned(),
                    },
                    segment_payloads_from_object_segments(
                        object_segments,
                        record.encryption.clone(),
                    ),
                    user_size,
                    Some(etag_crc),
                )?;
                body
            };
            let lifecycle_expiration = if emit_lifecycle_expiration {
                self.current_object_lifecycle_expiration_with_storage_node(
                    &storage_node,
                    &bucket_summary,
                    key,
                    record.tags.as_deref(),
                    record.size,
                    record.last_modified,
                )?
            } else {
                None
            };

            Ok(GetObjectResult {
                body,
                metadata,
                system_metadata,
                object_lock: attribute_permissions.visible_object_lock(record.object_lock),
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                version_id: record.version_id,
                tag_count: self
                    .authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
                managed_encryption: record.encryption.managed_encryption_algorithm(),
                sse_customer,
                lifecycle_expiration,
            })
        }
    }

    /// Retrieve a single part of an object by part number.
    ///
    /// For multipart objects, returns the data for the specified part along with
    /// its checksum and byte range within the full object.
    /// For non-multipart objects, `part_number == 1` returns the full body.
    pub fn get_object_part(
        &self,
        req: &GetObjectPartRequest,
    ) -> Result<GetObjectPartResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_part",
            "bucket={:?} key={:?} part_number={} version_id={:?}",
            req.object.bucket_name(),
            req.object.key(),
            req.part_number,
            req.object.version_id
        );
        let bucket = req.object.bucket_name();
        let key = req.object.key();
        let version_id = req.object.version_id;
        let part_number = req.part_number;
        let cond = req.cond;
        let storage_node = self.storage_node();
        let read_runtime = self.read_runtime_for_storage_node(Arc::clone(&storage_node));
        let AuthorizedObjectRead {
            bucket: bucket_summary,
            snapshot,
            attribute_permissions,
        } = self.authorize_get_object_with_storage_node(
            &storage_node,
            &GetObjectRequest {
                object: ObjectVersionRequest::new(
                    req.object.bucket_name_typed().clone(),
                    req.object.key_typed().clone(),
                    req.object.version_id,
                    req.object.requester().clone(),
                    req.expected_bucket_owner(),
                ),
                cond: req.cond,
                sse_customer: req.sse_customer,
            },
        )?;
        let storage::ObjectReadSnapshot {
            stored,
            object_segments,
            multipart_parts,
            multipart_part_segments,
        } = snapshot;
        #[cfg(test)]
        maybe_run_object_read_snapshot_hook(bucket, key);

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;
        let sse_customer =
            self.prepare_sse_customer_read_access(&record.encryption, req.sse_customer)?;
        let emit_lifecycle_expiration = version_id.is_none();

        if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
            let obj_parts = snapshotted_multipart_parts_from_storage(
                multipart_parts,
                multipart_part_segments,
                &record.encryption,
            );

            let part = obj_parts
                .iter()
                .find(|p| p.record.part_number == part_number)
                .ok_or_else(|| ServerError::InvalidPartNumber {
                    part_number,
                    parts_count: obj_parts.len() as u32,
                })?;

            let part_start = part.object_offset_start as u64;
            let part_end = part_start + part.record.size.saturating_sub(1);

            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            let checksum = if let Some(raw) = &part.record.checksum {
                match system_metadata.checksum_algorithm() {
                    Some(algo) => Some(RawChecksum::new(algo, raw.as_slice()).map_err(|err| {
                        ServerError::InternalError {
                            reason: format!(
                                "stored checksum length {} does not match {} (expected {})",
                                err.actual_len,
                                err.algorithm.as_str(),
                                err.expected_len,
                            ),
                        }
                    })?),
                    None => None,
                }
            } else {
                None
            };

            let mut part_body = part.clone();
            part_body.object_offset_start = 0;
            let body = ReadHandle::from_multipart(
                read_runtime.clone(),
                req.object.bucket_name_typed(),
                req.object.key_typed(),
                record.generation_id,
                vec![part_body],
                part.record.size as usize,
                req.sse_customer.cloned(),
            )?;
            let lifecycle_expiration = if emit_lifecycle_expiration {
                self.current_object_lifecycle_expiration_with_storage_node(
                    &storage_node,
                    &bucket_summary,
                    key,
                    record.tags.as_deref(),
                    record.size,
                    record.last_modified,
                )?
            } else {
                None
            };
            #[cfg(test)]
            maybe_run_multipart_snapshot_hook(bucket, key);

            Ok(GetObjectPartResult {
                body,
                metadata,
                system_metadata,
                object_lock: attribute_permissions.visible_object_lock(record.object_lock),
                etag: etag_str,
                size: record.size,
                part_size: part.record.size,
                last_modified: record.last_modified,
                part_start,
                part_end,
                parts_count: obj_parts.len() as u32,
                version_id: record.version_id,
                tag_count: self
                    .authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
                checksum,
                managed_encryption: record.encryption.managed_encryption_algorithm(),
                sse_customer,
                lifecycle_expiration,
            })
        } else {
            if part_number != 1 {
                return Err(ServerError::InvalidPartNumber {
                    part_number,
                    parts_count: 1,
                });
            }

            let etag_crc = record.etag.crc64();
            let user_size = record.size as usize;

            let body = if user_size == 0 {
                ReadHandle::from_buffered_bytes(vec![])
            } else {
                let body = ReadHandle::from_segments(
                    ReadObjectContext {
                        runtime: read_runtime,
                        bucket: req.object.bucket_name_typed(),
                        key: req.object.key_typed(),
                        generation_id: record.generation_id,
                        sse_customer_request: req.sse_customer.cloned(),
                    },
                    segment_payloads_from_object_segments(
                        object_segments,
                        record.encryption.clone(),
                    ),
                    user_size,
                    Some(etag_crc),
                )?;
                body
            };
            let lifecycle_expiration = if emit_lifecycle_expiration {
                self.current_object_lifecycle_expiration_with_storage_node(
                    &storage_node,
                    &bucket_summary,
                    key,
                    record.tags.as_deref(),
                    record.size,
                    record.last_modified,
                )?
            } else {
                None
            };

            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            Ok(GetObjectPartResult {
                body,
                metadata,
                system_metadata,
                object_lock: attribute_permissions.visible_object_lock(record.object_lock),
                etag: etag_str,
                size: record.size,
                part_size: record.size,
                last_modified: record.last_modified,
                part_start: 0,
                part_end: record.size.saturating_sub(1),
                parts_count: 1,
                version_id: record.version_id,
                tag_count: self
                    .authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
                checksum: None,
                managed_encryption: record.encryption.managed_encryption_algorithm(),
                sse_customer,
                lifecycle_expiration,
            })
        }
    }

    /// Head a single part of an object by part number (no body).
    pub fn head_object_part(
        &self,
        req: &GetObjectPartRequest,
    ) -> Result<HeadObjectPartResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::head_object_part",
            "bucket={:?} key={:?} part_number={} version_id={:?}",
            req.object.bucket_name(),
            req.object.key(),
            req.part_number,
            req.object.version_id
        );
        let bucket = req.object.bucket_name();
        let key = req.object.key();
        let version_id = req.object.version_id;
        let part_number = req.part_number;
        let cond = req.cond;
        let storage_node = self.storage_node();
        let AuthorizedObjectRead {
            bucket: bucket_summary,
            snapshot,
            attribute_permissions,
        } = self.authorize_head_object_for_part_with_storage_node(
            &storage_node,
            &GetObjectRequest {
                object: ObjectVersionRequest::new(
                    req.object.bucket_name_typed().clone(),
                    req.object.key_typed().clone(),
                    req.object.version_id,
                    req.object.requester().clone(),
                    req.expected_bucket_owner(),
                ),
                cond: req.cond,
                sse_customer: req.sse_customer,
            },
        )?;
        let storage::ObjectReadSnapshot {
            stored,
            object_segments: _,
            multipart_parts,
            multipart_part_segments: _,
        } = snapshot;

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;
        let sse_customer =
            self.prepare_sse_customer_read_access(&record.encryption, req.sse_customer)?;
        let emit_lifecycle_expiration = version_id.is_none();

        if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
            let obj_parts = multipart_parts;
            let lifecycle_expiration = if emit_lifecycle_expiration {
                self.current_object_lifecycle_expiration_with_storage_node(
                    &storage_node,
                    &bucket_summary,
                    key,
                    record.tags.as_deref(),
                    record.size,
                    record.last_modified,
                )?
            } else {
                None
            };

            let part_index = obj_parts
                .iter()
                .position(|p| p.part_number == part_number)
                .ok_or_else(|| ServerError::InvalidPartNumber {
                    part_number,
                    parts_count: obj_parts.len() as u32,
                })?;
            let part = &obj_parts[part_index];
            let part_start = obj_parts[..part_index].iter().map(|p| p.size).sum::<u64>();
            let part_end = part_start + part.size.saturating_sub(1);

            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            let checksum = if let Some(raw) = &part.checksum {
                match system_metadata.checksum_algorithm() {
                    Some(algo) => Some(RawChecksum::new(algo, raw.as_slice()).map_err(|err| {
                        ServerError::InternalError {
                            reason: format!(
                                "stored checksum length {} does not match {} (expected {})",
                                err.actual_len,
                                err.algorithm.as_str(),
                                err.expected_len,
                            ),
                        }
                    })?),
                    None => None,
                }
            } else {
                None
            };

            Ok(HeadObjectPartResult {
                metadata,
                system_metadata,
                object_lock: attribute_permissions.visible_object_lock(record.object_lock),
                etag: etag_str,
                part_size: part.size,
                part_start,
                part_end,
                total_size: record.size,
                last_modified: record.last_modified,
                parts_count: obj_parts.len() as u32,
                version_id: record.version_id,
                tag_count: self
                    .authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
                checksum,
                managed_encryption: record.encryption.managed_encryption_algorithm(),
                sse_customer,
                lifecycle_expiration,
            })
        } else {
            if part_number != 1 {
                return Err(ServerError::InvalidPartNumber {
                    part_number,
                    parts_count: 1,
                });
            }
            let lifecycle_expiration = if emit_lifecycle_expiration {
                self.current_object_lifecycle_expiration_with_storage_node(
                    &storage_node,
                    &bucket_summary,
                    key,
                    record.tags.as_deref(),
                    record.size,
                    record.last_modified,
                )?
            } else {
                None
            };

            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            Ok(HeadObjectPartResult {
                metadata,
                system_metadata,
                object_lock: attribute_permissions.visible_object_lock(record.object_lock),
                etag: etag_str,
                part_size: record.size,
                part_start: 0,
                part_end: record.size.saturating_sub(1),
                total_size: record.size,
                last_modified: record.last_modified,
                parts_count: 1,
                version_id: record.version_id,
                tag_count: self
                    .authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
                checksum: None,
                managed_encryption: record.encryption.managed_encryption_algorithm(),
                sse_customer,
                lifecycle_expiration,
            })
        }
    }

    /// Head object: returns metadata without body.
    ///
    /// Metadata is always read from the DB row (no shard read needed).
    pub fn head_object(&self, req: &GetObjectRequest) -> Result<HeadObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::head_object",
            "bucket={:?} key={:?} version_id={:?}",
            req.object.bucket_name(),
            req.object.key(),
            req.object.version_id
        );
        let bucket = req.object.bucket_name();
        let key = req.object.key();
        let version_id = req.object.version_id;
        let cond = req.cond;
        let storage_node = self.storage_node();
        let AuthorizedObjectRead {
            bucket: bucket_summary,
            snapshot,
            attribute_permissions,
        } = self.authorize_head_object_with_storage_node(&storage_node, req)?;
        let storage::ObjectReadSnapshot {
            stored,
            object_segments: _,
            multipart_parts: _,
            multipart_part_segments: _,
        } = snapshot;
        #[cfg(test)]
        maybe_run_object_read_snapshot_hook(bucket, key);

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(marker) => {
                return Err(if version_id.is_some() {
                    ServerError::HeadDeleteMarkerMethodNotAllowed {
                        version_id: marker.version_id,
                        last_modified: marker.last_modified,
                    }
                } else {
                    ServerError::DeleteMarkerHit {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    }
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;
        let sse_customer =
            self.prepare_sse_customer_read_access(&record.encryption, req.sse_customer)?;
        let emit_lifecycle_expiration = version_id.is_none();
        let lifecycle_expiration = if emit_lifecycle_expiration {
            self.current_object_lifecycle_expiration_with_storage_node(
                &storage_node,
                &bucket_summary,
                key,
                record.tags.as_deref(),
                record.size,
                record.last_modified,
            )?
        } else {
            None
        };

        let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
        let system_metadata = self.deserialize_visible_system_metadata(
            record.system_metadata_blob.as_ref(),
            &record.encryption,
            req.sse_customer,
        )?;

        Ok(HeadObjectResult {
            metadata,
            system_metadata,
            object_lock: attribute_permissions.visible_object_lock(record.object_lock),
            etag: etag_str,
            size: record.size,
            last_modified: record.last_modified,
            version_id: record.version_id,
            tag_count: self.authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
            managed_encryption: record.encryption.managed_encryption_algorithm(),
            sse_customer,
            lifecycle_expiration,
        })
    }

    /// Retrieve object attributes, optionally including multipart ObjectParts
    /// with pagination support.
    pub fn get_object_attributes(
        &self,
        req: &GetObjectAttributesRequest,
    ) -> Result<GetObjectAttributesResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_attributes",
            "bucket={:?} key={:?} want_parts={} version_id={:?}",
            req.object.bucket_name(),
            req.object.key(),
            req.want_parts,
            req.object.version_id
        );
        let bucket = req.object.bucket_name();
        let key = req.object.key();
        let cond = req.cond;
        let want_parts = req.want_parts;
        let part_number_marker = req.part_number_marker;
        let max_parts = req.max_parts;
        let AuthorizedObjectRead {
            bucket: _,
            snapshot,
            attribute_permissions: _,
        } = self.authorize_get_object_attributes(req)?;
        let storage::ObjectReadSnapshot {
            stored,
            object_segments: _,
            multipart_parts,
            multipart_part_segments: _,
        } = snapshot;

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;
        let sse_customer =
            self.prepare_sse_customer_read_access(&record.encryption, req.sse_customer)?;

        let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
        let system_metadata = self.deserialize_visible_system_metadata(
            record.system_metadata_blob.as_ref(),
            &record.encryption,
            req.sse_customer,
        )?;

        let object_parts =
            if want_parts && matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
                let has_checksum = system_metadata.checksum_algorithm().is_some();

                if has_checksum {
                    let all_parts = multipart_parts.clone();
                    let total_parts_count = all_parts.len() as u32;
                    let marker = part_number_marker.unwrap_or(0);

                    let filtered: Vec<_> = all_parts
                        .into_iter()
                        .filter(|p| p.part_number > marker)
                        .collect();

                    let is_truncated = max_parts > 0 && filtered.len() > max_parts as usize;
                    let take_count = (max_parts as usize).min(filtered.len());
                    let page: Vec<ObjectPartEntry> = filtered
                        .into_iter()
                        .take(take_count)
                        .map(|p| {
                            use base64::Engine;
                            let checksum = p.checksum.as_ref().map(|bytes| {
                                base64::engine::general_purpose::STANDARD.encode(bytes)
                            });
                            ObjectPartEntry {
                                part_number: p.part_number,
                                size: p.size,
                                checksum,
                            }
                        })
                        .collect();

                    let next_part_number_marker = if page.is_empty() {
                        Some(marker)
                    } else {
                        page.last().map(|p| p.part_number)
                    };

                    Some(ObjectPartsInfo {
                        total_parts_count,
                        has_detail: true,
                        parts: page,
                        is_truncated,
                        next_part_number_marker,
                        max_parts,
                        part_number_marker: marker,
                    })
                } else {
                    let all_parts = multipart_parts;
                    let total_parts_count = all_parts.len() as u32;

                    Some(ObjectPartsInfo {
                        total_parts_count,
                        has_detail: false,
                        parts: Vec::new(),
                        is_truncated: false,
                        next_part_number_marker: None,
                        max_parts,
                        part_number_marker: part_number_marker.unwrap_or(0),
                    })
                }
            } else {
                None
            };

        Ok(GetObjectAttributesResult {
            metadata,
            system_metadata,
            etag: etag_str,
            size: record.size,
            last_modified: record.last_modified,
            version_id: record.version_id,
            object_parts,
            managed_encryption: record.encryption.managed_encryption_algorithm(),
            sse_customer,
        })
    }

    /// Get a byte range of an object from storage (for HTTP Range requests).
    ///
    /// Returns 206 Partial Content data.
    pub fn get_object_range(
        &self,
        req: &GetObjectRangeRequest,
    ) -> Result<GetObjectRangeResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_range",
            "bucket={:?} key={:?} version_id={:?} requested_range={}",
            req.object.bucket_name(),
            req.object.key(),
            req.object.version_id,
            req.range
        );
        let bucket = req.object.bucket_name();
        let key = req.object.key();
        let version_id = req.object.version_id;
        let range = req.range;
        let cond = req.cond;
        let storage_node = self.storage_node();
        let read_runtime = self.read_runtime_for_storage_node(Arc::clone(&storage_node));
        let AuthorizedObjectRead {
            bucket: bucket_summary,
            snapshot,
            attribute_permissions,
        } = self.authorize_get_object_with_storage_node(
            &storage_node,
            &GetObjectRequest {
                object: ObjectVersionRequest::new(
                    req.object.bucket_name_typed().clone(),
                    req.object.key_typed().clone(),
                    req.object.version_id,
                    req.object.requester().clone(),
                    req.expected_bucket_owner(),
                ),
                cond: req.cond,
                sse_customer: req.sse_customer,
            },
        )?;
        let storage::ObjectReadSnapshot {
            stored,
            object_segments,
            multipart_parts,
            multipart_part_segments,
        } = snapshot;
        #[cfg(test)]
        maybe_run_object_read_snapshot_hook(bucket, key);

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;
        let sse_customer =
            self.prepare_sse_customer_read_access(&record.encryption, req.sse_customer)?;
        let emit_lifecycle_expiration = version_id.is_none();

        let (user_start, user_end) = match range.resolve(record.size) {
            Some(resolved) => resolved,
            None => {
                #[cfg(feature = "deep-tracing")]
                if let Some(trace) = observability::current_context() {
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "get_object_range_invalid",
                        Some(format_args!(
                            "bucket={:?} key={:?} version_id={:?} requested_range={} object_size={}",
                            bucket, key, version_id, range, record.size
                        )),
                    );
                }
                return Err(ServerError::InvalidRange {
                    range_requested: range.to_string(),
                    total_size: record.size,
                });
            }
        };
        #[cfg(feature = "deep-tracing")]
        if let Some(trace) = observability::current_context() {
            let _ = observability::event_in_context(
                &trace,
                TRACE_TARGET,
                "get_object_range_resolved",
                Some(format_args!(
                    "bucket={:?} key={:?} version_id={:?} requested_range={} object_size={} resolved_start={} resolved_end={} resolved_len={}",
                    bucket,
                    key,
                    version_id,
                    range,
                    record.size,
                    user_start,
                    user_end,
                    user_end - user_start + 1
                )),
            );
        }

        let (metadata, system_metadata, body) = if matches!(
            record.layout,
            ObjectLayout::MultipartManifest { .. }
        ) {
            let obj_parts = snapshotted_multipart_parts_from_storage(
                multipart_parts,
                multipart_part_segments,
                &record.encryption,
            );
            if !obj_parts.iter().any(|part| {
                let part_start = part.object_offset_start as u64;
                let part_end_exclusive = part_start + part.record.size;
                part_end_exclusive > user_start && part_start <= user_end
            }) {
                return Err(ServerError::InternalError {
                    reason: format!(
                        "multipart range resolved to no parts for {bucket}/{key} version {version_id:?} at {user_start}-{user_end}"
                    ),
                });
            }

            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            let body = ReadHandle::from_multipart_range(
                read_runtime.clone(),
                req.object.bucket_name_typed(),
                req.object.key_typed(),
                record.generation_id,
                obj_parts,
                (user_start as usize, user_end as usize),
                req.sse_customer.cloned(),
            )?;
            #[cfg(test)]
            maybe_run_multipart_snapshot_hook(bucket, key);
            (metadata, system_metadata, body)
        } else {
            let metadata = Self::deserialize_user_metadata(record.metadata_blob.as_ref())?;
            let system_metadata = self.deserialize_visible_system_metadata(
                record.system_metadata_blob.as_ref(),
                &record.encryption,
                req.sse_customer,
            )?;

            let body = ReadHandle::from_segments_range(
                ReadObjectContext {
                    runtime: read_runtime,
                    bucket: req.object.bucket_name_typed(),
                    key: req.object.key_typed(),
                    generation_id: record.generation_id,
                    sse_customer_request: req.sse_customer.cloned(),
                },
                segment_payloads_from_object_segments(object_segments, record.encryption.clone()),
                user_start as usize,
                user_end as usize,
            )?;

            (metadata, system_metadata, body)
        };
        let lifecycle_expiration = if emit_lifecycle_expiration {
            self.current_object_lifecycle_expiration_with_storage_node(
                &storage_node,
                &bucket_summary,
                key,
                record.tags.as_deref(),
                record.size,
                record.last_modified,
            )?
        } else {
            None
        };

        Ok(GetObjectRangeResult {
            body,
            metadata,
            system_metadata,
            object_lock: attribute_permissions.visible_object_lock(record.object_lock),
            etag: etag_str,
            size: record.size,
            last_modified: record.last_modified,
            range_start: user_start,
            range_end: user_end,
            version_id: record.version_id,
            tag_count: self.authorized_tag_count(attribute_permissions, record.tags.as_ref())?,
            managed_encryption: record.encryption.managed_encryption_algorithm(),
            sse_customer,
            lifecycle_expiration,
        })
    }
}
