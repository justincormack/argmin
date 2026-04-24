use checksum::{ChecksumAlgorithm, ChecksumType};
#[cfg(test)]
use s3_types::VersionId;
#[cfg(test)]
use storage::StoredObject;
use storage::{
    BucketName, GenerationId, ObjectEncryption, ObjectKey, ObjectPartRecord,
    SerializedMetadataBlob, SerializedSystemMetadataBlob,
};

use super::{ActiveWriteEncryption, Coordinator, SegmentPayloadRecord};
#[cfg(test)]
use super::{LockedReadObject, ObjectPgGuards};
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

#[derive(Debug, Clone, Copy)]
pub(super) struct StaleObjectPayload {
    pub(super) generation_id: GenerationId,
}

impl From<storage::CompletedMultipartStalePayload> for StaleObjectPayload {
    fn from(value: storage::CompletedMultipartStalePayload) -> Self {
        match value {
            storage::CompletedMultipartStalePayload::Segments { generation_id, .. }
            | storage::CompletedMultipartStalePayload::Multipart { generation_id, .. } => {
                Self { generation_id }
            }
        }
    }
}

impl Coordinator {
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

    #[cfg(test)]
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
    #[cfg(test)]
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

    pub(super) fn delete_stale_object_payload(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        payload: &StaleObjectPayload,
    ) {
        self.read_runtime()
            .enqueue_object_payload_reclaim_for(bucket, key, payload.generation_id);
    }
}
