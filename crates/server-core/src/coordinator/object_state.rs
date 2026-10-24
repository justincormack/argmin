// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use checksum::{ChecksumAlgorithm, ChecksumType};
use storage::{
    ObjectEncryption, ObjectEncryptionSubject, ObjectReadMultipartPart, SerializedMetadataBlob,
    SerializedSystemMetadataBlob,
};

use super::{ActiveWriteEncryption, Coordinator, SegmentPayloadRecord};
use crate::error::ServerError;
use crate::metadata_blob::MetadataBlob;
use crate::sse::{
    decrypt_authorized_managed_encryption_checksum, decrypt_authorized_sse_customer_checksum,
    decrypt_managed_encryption_checksum, decrypt_sse_customer_checksum, SseCustomerRequest,
};
use crate::system_metadata::{ObjectChecksumMetadata, SystemMetadata};

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

    pub(super) fn prepare_unsealed_stored_system_metadata(
        system_metadata: &SystemMetadata,
    ) -> Result<(SerializedSystemMetadataBlob, Option<ObjectChecksumMetadata>), ServerError> {
        let mut stored_system_metadata = system_metadata.clone();
        let checksum = stored_system_metadata.take_checksum();
        Ok((
            SerializedSystemMetadataBlob::from(stored_system_metadata.serialize()?),
            checksum,
        ))
    }

    pub(super) fn deserialize_visible_system_metadata(
        &self,
        system_metadata_blob: Option<&SerializedSystemMetadataBlob>,
        encryption: &ObjectEncryption,
        encryption_subject: Option<&ObjectEncryptionSubject>,
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
                    let checksum = match encryption_subject {
                        Some(subject) => decrypt_authorized_sse_customer_checksum(
                            validator, state, request, subject,
                        )?,
                        None => decrypt_sse_customer_checksum(validator, state, request)?,
                    };
                    if let Some(checksum) = checksum {
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
                    let checksum = match encryption_subject {
                        Some(subject) => decrypt_authorized_managed_encryption_checksum(
                            provider, state, subject,
                        )?,
                        None => decrypt_managed_encryption_checksum(provider, state)?,
                    };
                    if let Some(checksum) = checksum {
                        system_metadata.set_checksum(
                            checksum.algorithm(),
                            checksum.checksum_type(),
                            checksum.value(),
                        );
                    }
                }
            }
        }
        Ok(system_metadata)
    }
}
