// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::request_types::{
    BucketScopedRequest, ExpectedBucketOwnerRequest, PutObjectAcl, PutObjectWriteAcl, Requester,
};
use super::response_types::BucketSummary;
use crate::error::ServerError;
use crate::sse::{ManagedEncryptionWriteContext, SseCustomerWriteContext};
use crate::system_metadata::ObjectChecksumMetadata;
use s3_types::AclGrants;
use storage::{
    BucketName, ManagedEncryptionAlgorithm, ObjectEncryption, ObjectKey, ObjectLockState,
    SseCustomerObjectState, SseS3ObjectState,
};

#[derive(Debug, Clone)]
pub(super) struct ValidatedBucket(pub(super) BucketSummary);

impl ValidatedBucket {
    pub(super) fn into_inner(self) -> BucketSummary {
        self.0
    }
}

impl std::ops::Deref for ValidatedBucket {
    type Target = BucketSummary;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AuthorizedPutObjectAcl {
    None,
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
    AwsExecRead,
    BucketOwnerRead,
    BucketOwnerFullControl,
}

impl AuthorizedPutObjectAcl {
    pub(super) const fn as_borrowed(self) -> PutObjectAcl<'static> {
        match self {
            Self::None => PutObjectAcl::None,
            Self::Private => PutObjectAcl::Private,
            Self::PublicRead => PutObjectAcl::PublicRead,
            Self::PublicReadWrite => PutObjectAcl::PublicReadWrite,
            Self::AuthenticatedRead => PutObjectAcl::AuthenticatedRead,
            Self::AwsExecRead => PutObjectAcl::AwsExecRead,
            Self::BucketOwnerRead => PutObjectAcl::BucketOwnerRead,
            Self::BucketOwnerFullControl => PutObjectAcl::BucketOwnerFullControl,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AuthorizedPutObjectWriteAcl {
    None,
    Canned(AuthorizedPutObjectAcl),
    Grants(AclGrants),
}

impl AuthorizedPutObjectWriteAcl {
    pub(super) fn from_parsed(acl: &PutObjectWriteAcl<'_>) -> Self {
        match acl {
            PutObjectWriteAcl::None => Self::None,
            PutObjectWriteAcl::Canned(acl) => Self::Canned(match acl {
                PutObjectAcl::None => AuthorizedPutObjectAcl::None,
                PutObjectAcl::Private => AuthorizedPutObjectAcl::Private,
                PutObjectAcl::PublicRead => AuthorizedPutObjectAcl::PublicRead,
                PutObjectAcl::PublicReadWrite => AuthorizedPutObjectAcl::PublicReadWrite,
                PutObjectAcl::AuthenticatedRead => AuthorizedPutObjectAcl::AuthenticatedRead,
                PutObjectAcl::AwsExecRead => AuthorizedPutObjectAcl::AwsExecRead,
                PutObjectAcl::BucketOwnerRead => AuthorizedPutObjectAcl::BucketOwnerRead,
                PutObjectAcl::BucketOwnerFullControl => {
                    AuthorizedPutObjectAcl::BucketOwnerFullControl
                }
                PutObjectAcl::Invalid(value) => {
                    unreachable!("validated PutObject ACL must not remain invalid: {value}")
                }
            }),
            PutObjectWriteAcl::Grants(acl_grants) => Self::Grants(acl_grants.clone()),
        }
    }

    pub(super) fn as_borrowed(&self) -> PutObjectWriteAcl<'_> {
        match self {
            Self::None => PutObjectWriteAcl::None,
            Self::Canned(acl) => PutObjectWriteAcl::Canned(acl.as_borrowed()),
            Self::Grants(acl_grants) => PutObjectWriteAcl::Grants(acl_grants.clone()),
        }
    }
}

#[derive(Debug)]
pub struct AuthorizedPutObjectWrite {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) requester: Requester,
    pub(super) expected_bucket_owner: Option<String>,
    pub(super) acl: AuthorizedPutObjectWriteAcl,
    pub(super) requested_object_lock: ObjectLockState,
    pub(super) tags: Option<s3_types::TagSet>,
    pub(super) write_encryption: ActiveWriteEncryption,
}

impl AuthorizedPutObjectWrite {
    pub(super) fn bucket(&self) -> &str {
        self.bucket.as_str()
    }

    pub(super) fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn bucket_typed(&self) -> &BucketName {
        &self.bucket
    }

    pub fn key_typed(&self) -> &ObjectKey {
        &self.key
    }

    pub(super) fn requester(&self) -> &Requester {
        &self.requester
    }

    pub(super) fn expected_bucket_owner(&self) -> Option<&str> {
        self.expected_bucket_owner.as_deref()
    }

    pub(super) fn acl(&self) -> PutObjectWriteAcl<'_> {
        self.acl.as_borrowed()
    }

    pub(super) fn requested_object_lock(&self) -> ObjectLockState {
        self.requested_object_lock
    }

    pub(super) fn tags(&self) -> Option<&s3_types::TagSet> {
        self.tags.as_ref()
    }
}

impl BucketScopedRequest for AuthorizedPutObjectWrite {
    fn bucket_name_typed(&self) -> &BucketName {
        AuthorizedPutObjectWrite::bucket_typed(self)
    }
}

impl ExpectedBucketOwnerRequest for AuthorizedPutObjectWrite {
    fn expected_bucket_owner(&self) -> Option<&str> {
        AuthorizedPutObjectWrite::expected_bucket_owner(self)
    }
}

#[derive(Debug, Clone)]
pub(super) struct ActiveWriteEncryption {
    kind: ActiveWriteEncryptionKind,
}

#[derive(Debug, Clone)]
enum ActiveWriteEncryptionKind {
    SseCustomer {
        state: SseCustomerObjectState,
        write: Option<SseCustomerWriteContext>,
    },
    Managed {
        state: SseS3ObjectState,
        write: ManagedEncryptionWriteContext,
    },
}

impl ActiveWriteEncryption {
    pub(super) fn sse_customer(write: SseCustomerWriteContext) -> Self {
        let ObjectEncryption::SseCustomer(state) = write.encryption() else {
            unreachable!("SSE-C write context must contain SSE-C encryption state");
        };
        Self {
            kind: ActiveWriteEncryptionKind::SseCustomer {
                state: state.clone(),
                write: Some(write),
            },
        }
    }

    pub(super) fn sse_customer_without_write_context(state: SseCustomerObjectState) -> Self {
        Self {
            kind: ActiveWriteEncryptionKind::SseCustomer { state, write: None },
        }
    }

    pub(super) fn managed(
        algorithm: ManagedEncryptionAlgorithm,
        write: ManagedEncryptionWriteContext,
    ) -> Self {
        debug_assert_eq!(algorithm, ManagedEncryptionAlgorithm::Aes256);
        let ObjectEncryption::SseS3(state) = write.encryption() else {
            unreachable!("managed write context must contain SSE-S3 encryption state");
        };
        Self {
            kind: ActiveWriteEncryptionKind::Managed {
                state: state.clone(),
                write,
            },
        }
    }

    pub(super) fn is_sse_customer(&self) -> bool {
        matches!(self.kind, ActiveWriteEncryptionKind::SseCustomer { .. })
    }

    pub(super) fn can_store_checksum_metadata(&self) -> bool {
        matches!(
            self.kind,
            ActiveWriteEncryptionKind::SseCustomer { write: Some(_), .. }
                | ActiveWriteEncryptionKind::Managed { .. }
        )
    }

    pub(super) fn object_encryption(&self) -> ObjectEncryption {
        match &self.kind {
            ActiveWriteEncryptionKind::SseCustomer { state, .. } => {
                ObjectEncryption::SseCustomer(state.clone())
            }
            ActiveWriteEncryptionKind::Managed { state, .. } => {
                ObjectEncryption::SseS3(state.clone())
            }
        }
    }

    pub(super) fn seal_checksum_metadata(
        &self,
        checksum: Option<&ObjectChecksumMetadata>,
    ) -> Result<ObjectEncryption, ServerError> {
        match &self.kind {
            ActiveWriteEncryptionKind::SseCustomer {
                write: Some(write), ..
            } => write.seal_checksum_metadata(checksum),
            ActiveWriteEncryptionKind::SseCustomer { state, write: None } => {
                if checksum.is_none() {
                    Ok(ObjectEncryption::SseCustomer(state.clone()))
                } else {
                    Err(ServerError::InvalidRequest {
                        reason: "SSE-C headers are required when storing checksum metadata for this object"
                            .to_string(),
                    })
                }
            }
            ActiveWriteEncryptionKind::Managed { write, .. } => {
                write.seal_checksum_metadata(checksum)
            }
        }
    }

    pub(super) fn encrypt_segment(
        &self,
        segment_index: u32,
        data: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        match &self.kind {
            ActiveWriteEncryptionKind::SseCustomer {
                write: Some(sse_customer),
                ..
            } => sse_customer.encrypt_segment(segment_index, data),
            ActiveWriteEncryptionKind::Managed { write, .. } => {
                write.encrypt_segment(segment_index, data)
            }
            ActiveWriteEncryptionKind::SseCustomer { write: None, .. } => {
                Err(ServerError::InvalidRequest {
                    reason: "SSE-C write context is required for streaming segment encryption"
                        .to_string(),
                })
            }
        }
    }

    pub(super) fn encrypt_direct_put_segment(
        &self,
        subject: &storage::SegmentEncryptionSubject,
        segment_index: u32,
        data: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        let logical_plaintext_len =
            u64::try_from(data.len()).map_err(|_| ServerError::InternalError {
                reason: "direct PUT segment length does not fit the encryption subject".to_string(),
            })?;
        if !subject.matches_standard_object_segment(
            &self.object_encryption(),
            segment_index,
            logical_plaintext_len,
        ) {
            return Err(ServerError::InternalError {
                reason:
                    "direct PUT segment subject does not match the write encryption instance or geometry"
                        .to_string(),
            });
        }
        self.encrypt_segment(segment_index, data)
    }
}
