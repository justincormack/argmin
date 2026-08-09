// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::request_types::{
    BucketScopedRequest, ExpectedBucketOwnerRequest, PutObjectAcl, PutObjectWriteAcl, Requester,
};
use super::response_types::BucketSummary;
use crate::error::ServerError;
use crate::sse::{ManagedEncryptionWriteContext, SseCustomerWriteContext};
use s3_types::AclGrants;
use storage::{
    BucketName, ManagedEncryptionAlgorithm, ObjectEncryption, ObjectKey, ObjectLockState,
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
pub enum ActiveWriteEncryption {
    None,
    SseCustomer {
        encryption: ObjectEncryption,
        write: Option<SseCustomerWriteContext>,
    },
    Managed {
        encryption: ObjectEncryption,
        algorithm: ManagedEncryptionAlgorithm,
        write: Option<ManagedEncryptionWriteContext>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum ActiveWriteEncryptionRef<'a> {
    None,
    SseCustomer(&'a SseCustomerWriteContext),
    Managed {
        algorithm: ManagedEncryptionAlgorithm,
        write: &'a ManagedEncryptionWriteContext,
    },
}

impl ActiveWriteEncryption {
    pub(super) fn none() -> Self {
        Self::None
    }

    pub(super) fn sse_customer(write: SseCustomerWriteContext) -> Self {
        Self::SseCustomer {
            encryption: write.encryption().clone(),
            write: Some(write),
        }
    }

    pub(super) fn managed(
        algorithm: ManagedEncryptionAlgorithm,
        write: ManagedEncryptionWriteContext,
    ) -> Self {
        Self::Managed {
            encryption: write.encryption().clone(),
            algorithm,
            write: Some(write),
        }
    }

    pub fn as_ref(&self) -> ActiveWriteEncryptionRef<'_> {
        match self {
            Self::None => ActiveWriteEncryptionRef::None,
            Self::SseCustomer {
                write: Some(sse_customer),
                ..
            } => ActiveWriteEncryptionRef::SseCustomer(sse_customer),
            Self::SseCustomer { write: None, .. } => ActiveWriteEncryptionRef::None,
            Self::Managed {
                algorithm,
                write: Some(write),
                ..
            } => ActiveWriteEncryptionRef::Managed {
                algorithm: *algorithm,
                write,
            },
            Self::Managed { write: None, .. } => ActiveWriteEncryptionRef::None,
        }
    }

    pub(super) fn is_sse_customer(&self) -> bool {
        matches!(self, Self::SseCustomer { .. })
    }

    pub(super) fn can_store_checksum_metadata(&self) -> bool {
        matches!(
            self,
            Self::None
                | Self::SseCustomer { write: Some(_), .. }
                | Self::Managed { write: Some(_), .. }
        )
    }

    pub(super) fn object_encryption(&self) -> ObjectEncryption {
        match self {
            Self::None => ObjectEncryption::None,
            Self::SseCustomer { encryption, .. } | Self::Managed { encryption, .. } => {
                encryption.clone()
            }
        }
    }

    pub fn encrypt_segment(&self, segment_index: u32, data: &[u8]) -> Result<Vec<u8>, ServerError> {
        match self {
            Self::None => Ok(data.to_vec()),
            Self::SseCustomer {
                write: Some(sse_customer),
                ..
            } => sse_customer.encrypt_segment(segment_index, data),
            Self::Managed {
                write: Some(write), ..
            } => write.encrypt_segment(segment_index, data),
            Self::SseCustomer { write: None, .. } => Err(ServerError::InvalidRequest {
                reason: "SSE-C write context is required for streaming segment encryption"
                    .to_string(),
            }),
            Self::Managed { write: None, .. } => Err(ServerError::InternalError {
                reason: "SSE-S3 write context is required for streaming segment encryption"
                    .to_string(),
            }),
        }
    }

    pub(super) fn from_stored_and_active(
        encryption: &ObjectEncryption,
        active: ActiveWriteEncryptionRef<'_>,
    ) -> Result<Self, ServerError> {
        match (encryption, active) {
            (ObjectEncryption::None, ActiveWriteEncryptionRef::None) => Ok(Self::None),
            (ObjectEncryption::None, ActiveWriteEncryptionRef::SseCustomer(_))
            | (ObjectEncryption::None, ActiveWriteEncryptionRef::Managed { .. }) => {
                Err(ServerError::InvalidRequest {
                    reason: "write encryption context does not match unencrypted session"
                        .to_string(),
                })
            }
            (
                ObjectEncryption::SseCustomer(state),
                ActiveWriteEncryptionRef::SseCustomer(sse_customer),
            ) => {
                if sse_customer.encryption() != encryption {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-C write context does not match session encryption state"
                            .to_string(),
                    });
                }
                let _ = state;
                Ok(Self::sse_customer(sse_customer.clone()))
            }
            (ObjectEncryption::SseCustomer(state), ActiveWriteEncryptionRef::None) => {
                Ok(Self::SseCustomer {
                    encryption: ObjectEncryption::SseCustomer(state.clone()),
                    write: None,
                })
            }
            (ObjectEncryption::SseCustomer(_), ActiveWriteEncryptionRef::Managed { .. }) => {
                Err(ServerError::InvalidRequest {
                    reason: "managed write context may not be used for an SSE-C session"
                        .to_string(),
                })
            }
            (
                ObjectEncryption::SseS3(_),
                ActiveWriteEncryptionRef::Managed { algorithm, write },
            ) => {
                if Some(algorithm) != encryption.managed_encryption_algorithm() {
                    return Err(ServerError::InternalError {
                        reason: "SSE-S3 session is missing managed write encryption context"
                            .to_string(),
                    });
                }
                if write.encryption() != encryption {
                    return Err(ServerError::InvalidRequest {
                        reason: "SSE-S3 write context does not match session encryption state"
                            .to_string(),
                    });
                }
                Ok(Self::managed(algorithm, write.clone()))
            }
            (ObjectEncryption::SseS3(state), ActiveWriteEncryptionRef::None) => Ok(Self::Managed {
                encryption: ObjectEncryption::SseS3(state.clone()),
                algorithm: ManagedEncryptionAlgorithm::Aes256,
                write: None,
            }),
            (ObjectEncryption::SseS3(_), ActiveWriteEncryptionRef::SseCustomer(_)) => {
                Err(ServerError::InvalidRequest {
                    reason: "SSE-C headers may not be used for an SSE-S3 session".to_string(),
                })
            }
        }
    }
}
