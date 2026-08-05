use checksum::{ChecksumAlgorithm, ChecksumType};
use storage::{
    ObjectEncryption, ObjectReadMultipartPart, SerializedMetadataBlob, SerializedSystemMetadataBlob,
};

use super::{ActiveWriteEncryption, Coordinator, SegmentPayloadRecord};
use crate::error::ServerError;
use crate::metadata_blob::MetadataBlob;
use crate::sse::{
    decrypt_managed_encryption_checksum, decrypt_sse_customer_checksum, SseCustomerRequest,
};
use crate::system_metadata::SystemMetadata;

#[derive(Debug, Clone)]
pub(super) struct SnapshottedMultipartPart {
    pub(super) part: ObjectReadMultipartPart,
    pub(super) object_offset_start: usize,
    pub(super) segments: Vec<SegmentPayloadRecord>,
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
        if normalized_system_metadata.checksum().is_some()
            || !write_encryption.can_store_checksum_metadata()
        {
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

    pub(super) fn deserialize_user_metadata(
        metadata_blob: Option<&SerializedMetadataBlob>,
    ) -> Result<MetadataBlob, ServerError> {
        metadata_blob
            .map(|blob| MetadataBlob::deserialize(blob.as_slice()))
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
                if !state.encrypted_checksum_metadata().is_empty() {
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
                if !state.encrypted_checksum_metadata().is_empty() {
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
}
