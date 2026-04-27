use std::sync::Arc;

mod modern;
mod policy;

#[cfg(test)]
use s3_types::StoredLegalHoldStatus;
use s3_types::{
    aws_account_id_from_principal, AclGrant, AclGrantee, AclGrants, AclPermission,
    BucketVersioningState, CanonicalUserId, LifecycleConfigError, VersionId,
};
use storage::{
    BucketName, BucketObjectLockConfig, BucketObjectOwnership, BucketOwnershipControls,
    BucketState, ManagedEncryptionAlgorithm, MultipartUploadRecord, ObjectKey,
    ObjectReadSnapshotMode, OwnerIdentity, PublicAccessBlockConfig, StoredObject, UploadState,
};

pub(super) use self::modern::{
    BoeBucketSummary, ModernObjectReadAuthorization, ModernObjectWriteAuthorization,
    ModernReadAction, ModernWriteAction,
};
use super::authz_results::{
    AuthorizedAbortMultipartUpload, AuthorizedBeginStreamPart, AuthorizedBucketConfigAccess,
    AuthorizedBucketSubresourceBodyGet, AuthorizedBucketSubresourceDelete,
    AuthorizedBucketSubresourceGet, AuthorizedBucketSubresourcePut,
    AuthorizedCompleteMultipartUpload, AuthorizedCopyObject, AuthorizedCreateBucket,
    AuthorizedCreateMultipartUpload, AuthorizedDeleteBucket, AuthorizedDeleteBucketEncryption,
    AuthorizedDeleteObject, AuthorizedGetBucketAbac, AuthorizedGetBucketAcl,
    AuthorizedGetBucketEncryption, AuthorizedGetBucketLocation,
    AuthorizedGetBucketObjectLockConfiguration, AuthorizedGetBucketOwnershipControls,
    AuthorizedGetBucketPolicyStatus, AuthorizedGetBucketPublicAccessBlock,
    AuthorizedGetBucketVersioning, AuthorizedHeadBucket, AuthorizedListBuckets,
    AuthorizedListMultipartUploads, AuthorizedListObjectVersions, AuthorizedListObjectsV2,
    AuthorizedMultipartPartWrite, AuthorizedObjectRead, AuthorizedPutBucketAbac,
    AuthorizedPutBucketAcl, AuthorizedPutBucketEncryption, AuthorizedPutBucketLifecycle,
    AuthorizedPutBucketObjectLockConfiguration, AuthorizedPutBucketOwnershipControls,
    AuthorizedPutBucketPolicy, AuthorizedPutBucketPublicAccessBlock, AuthorizedPutBucketVersioning,
    AuthorizedUploadPartCopy,
};
#[cfg(test)]
use super::authz_results::{
    AuthorizedGetObjectAcl, AuthorizedGetObjectLegalHold, AuthorizedGetObjectRetention,
    AuthorizedObjectTagsAccess, AuthorizedPutObjectAclUpdate, AuthorizedPutObjectLegalHold,
    AuthorizedPutObjectRetention, LoadedObjectState,
};
use super::authz_types::{AuthorizedPutObjectWrite, AuthorizedPutObjectWriteAcl, ValidatedBucket};
#[cfg(test)]
use super::bucket_handles::LoadedObjectHandle;
use super::bucket_handles::{
    BucketHandleLoader, BucketHandleRequest, LoadedBucketHandle, LoadedBucketValue,
};
#[cfg(test)]
use super::request_types::ListPartsRequest;
use super::request_types::{
    authorization_policy_context_for_put_object_write_acl, AuthorizePutObjectRequest,
    BeginStreamPartRequest, BucketAcl, BucketRequest, BucketScopedAuthorizationRequest,
    BucketScopedRequest, BucketTagControlRequest, CompleteMultipartUploadRequest,
    CopyObjectRequest, CreateBucketAcl, CreateBucketRequest, CreateMultipartUploadRequest,
    DeleteEntry, DeleteObjectRequest, DeleteObjectsRequest, ExpectedBucketOwnerRequest,
    GetObjectAttributesRequest, GetObjectRequest, ListBucketsRequest, ListMultipartUploadsRequest,
    ListObjectVersionsRequest, ListObjectsV2Request, MultipartObjectRequest, ObjectRequest,
    ObjectVersionRequest, PutBucketAbacRequest, PutBucketAclInput, PutBucketAclRequest,
    PutBucketConfigRequest, PutBucketEncryptionRequest, PutBucketObjectLockConfigurationRequest,
    PutBucketOwnershipControlsRequest, PutBucketPolicyRequest, PutBucketPublicAccessBlockRequest,
    PutBucketVersioningRequest, PutObjectAcl, PutObjectPolicyContext, PutObjectWriteAcl, Requester,
    TaggingDirective, UploadPartCopyRequest,
};
#[cfg(test)]
use super::request_types::{
    PutObjectAclInput, PutObjectAclRequest, PutObjectLegalHoldRequest, PutObjectRetentionRequest,
    PutObjectTagsRequest,
};
#[cfg(test)]
use super::response_types::GetObjectAclResult;
use super::response_types::{BucketSummary, GetBucketAclResult, ModernBucketSummary};
use super::Coordinator;
#[cfg(test)]
use super::{
    maybe_run_bucket_policy_fast_path_hook, maybe_run_bucket_policy_storage_load_hook,
    maybe_run_bucket_write_handle_loaded_hook, should_probe_delete_object_lookup,
    should_probe_multipart_complete_auth_lookup, should_probe_object_read_snapshot,
};
use crate::error::ServerError;
use crate::sse::{SseCustomerRequest, SseCustomerSegmentScope};

#[cfg(test)]
#[derive(Clone, Copy)]
enum ObjectBucketPolicyRequirement {
    Required,
}

#[derive(Clone, Copy)]
enum MissingObjectDiscovery {
    ReadBucket,
    ReadObjectAttributes,
    #[cfg(test)]
    BucketAdmin,
    #[cfg(test)]
    ObjectAcl,
}

impl MissingObjectDiscovery {
    fn requester_can_discover_missing(
        self,
        coord: &Coordinator,
        access: BucketPolicyAccess<'_>,
        key: &str,
        version_id: Option<VersionId>,
    ) -> Result<bool, ServerError> {
        match self {
            Self::ReadBucket => Ok(Coordinator::requester_can_discover_missing_object(
                access.requester,
                access.bucket,
            ) || coord
                .requester_can_list_bucket_with_bucket_policy(access)?),
            Self::ReadObjectAttributes => {
                Coordinator::requester_can_discover_missing_object_attrs_with_bucket_policy(
                    coord, access, key, version_id,
                )
            }
            #[cfg(test)]
            Self::BucketAdmin => Ok(Coordinator::requester_can_bucket_owner_account_admin(
                access.requester,
                access.bucket,
            )),
            #[cfg(test)]
            Self::ObjectAcl => Ok(Coordinator::requester_can_discover_missing_object_acl(
                access.requester,
                access.bucket,
            )),
        }
    }
}

enum ObjectPolicyTarget<'a> {
    Existing(&'a StoredObject),
    MissingKey(&'a str),
}

#[derive(Clone, Copy)]
pub(super) struct BucketPolicyRequestContext<'a> {
    pub(super) requester: &'a Requester,
    pub(super) bucket: &'a BucketSummary,
    pub(super) bucket_tags: Option<&'a [(String, String)]>,
    pub(super) action: auth::PolicyAction,
    pub(super) policy_context: PutObjectPolicyContext<'a>,
    pub(super) policy: Option<&'a auth::BucketPolicy>,
}

#[derive(Clone, Copy)]
pub(super) struct BucketPolicyAccess<'a> {
    pub(super) requester: &'a Requester,
    pub(super) bucket: &'a BucketSummary,
    pub(super) bucket_tags: Option<&'a [(String, String)]>,
    pub(super) policy: Option<&'a auth::BucketPolicy>,
}

impl<'a> BucketPolicyAccess<'a> {
    fn request(
        self,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'a>,
    ) -> BucketPolicyRequestContext<'a> {
        BucketPolicyRequestContext {
            requester: self.requester,
            bucket: self.bucket,
            bucket_tags: self.bucket_tags,
            action,
            policy_context,
            policy: self.policy,
        }
    }
}

#[derive(Clone, Copy)]
struct BucketPolicyActionAuthorization<'a> {
    request: BucketPolicyRequestContext<'a>,
    default_allowed: bool,
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) enum ObjectAclAuthorization<'a> {
    ReadWithPolicy(auth::PolicyAction),
    WriteWithPolicy {
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'a>,
    },
}

#[derive(Clone, Copy)]
enum ExistingObjectTagsMode {
    Available,
    Unavailable,
}

struct CopySourceReadSnapshotRequest<'a> {
    requester: &'a Requester,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    version_id: Option<VersionId>,
    expected_bucket_owner: Option<&'a str>,
    policy_action: auth::PolicyAction,
    existing_object_tags_mode: ExistingObjectTagsMode,
}

struct AuthorizedObjectReadSnapshotRequest<'a> {
    requester: &'a Requester,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    version_id: Option<VersionId>,
    expected_bucket_owner: Option<&'a str>,
    missing_discovery: MissingObjectDiscovery,
    modern_action: ModernReadAction,
    snapshot_mode: ObjectReadSnapshotMode,
}

impl Coordinator {
    pub(super) fn requester_can_bucket_admin(requester: &Requester, owner_principal: &str) -> bool {
        requester.principal_opt() == Some(owner_principal)
    }

    pub(super) fn requester_can_bucket_owner_account_admin(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || (requester.authorization_profile() == auth::AuthorizationProfile::OwnerAccountAdmin
                && Self::requester_is_bucket_owner_account(requester, bucket))
    }

    pub(super) fn requester_has_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        owner_canonical_id: &CanonicalUserId,
        permission: AclPermission,
    ) -> bool {
        requester.canonical_user_id().is_some_and(|id| {
            (acl_grants.allows_canonical_user(id, permission)
                && (id != owner_canonical_id
                    || requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin))
                || acl_grants.allows_authenticated_users(permission)
        })
    }

    pub(super) fn requester_has_nonpublic_object_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        owner_canonical_id: &CanonicalUserId,
        permission: AclPermission,
    ) -> bool {
        requester.canonical_user_id().is_some_and(|id| {
            acl_grants.allows_canonical_user(id, permission)
                && (id != owner_canonical_id
                    || requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin)
        })
    }

    pub(super) fn requester_has_nonpublic_bucket_acl_permission(
        requester: &Requester,
        acl_grants: &AclGrants,
        owner_canonical_id: &CanonicalUserId,
        permission: AclPermission,
    ) -> bool {
        requester.canonical_user_id().is_some_and(|id| {
            acl_grants.allows_canonical_user(id, permission)
                && (id != owner_canonical_id
                    || requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin)
        })
    }

    pub(super) fn acl_grants_public_read(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_all_users(AclPermission::Read)
    }

    pub(super) fn acl_grants_public_write(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_all_users(AclPermission::Write)
    }

    pub(super) fn acl_grants_grant_public_read(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_public_groups(AclPermission::Read)
    }

    pub(super) fn acl_grants_grant_public_write(acl_grants: &AclGrants) -> bool {
        acl_grants.allows_public_groups(AclPermission::Write)
    }

    pub(super) fn requester_can_object_write(
        requester: &Requester,
        bucket: &BucketSummary,
        acl_grants: &AclGrants,
        public_write: bool,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
            || Self::requester_has_acl_permission(
                requester,
                acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::Write,
            )
            || public_write
    }

    pub(super) fn requester_can_read_bucket(
        requester: &Requester,
        bucket: &BucketSummary,
        owner_principal: &str,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> bool {
        let acl_allows_read = if Self::ignores_public_acls(bucket.public_access_block.as_ref()) {
            Self::requester_has_nonpublic_bucket_acl_permission(
                requester,
                acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::Read,
            )
        } else {
            Self::requester_has_acl_permission(
                requester,
                acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::Read,
            )
        };

        requester.principal_opt() == Some(owner_principal) || acl_allows_read || public_read
    }

    pub(super) fn requester_can_read_object(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_owner_account_admin(requester, bucket);
        }

        let acl_allows_read = object.acl_grants().is_some_and(|grants| {
            if Self::ignores_public_acls(bucket.public_access_block.as_ref()) {
                Self::requester_has_nonpublic_object_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::Read,
                )
            } else {
                Self::requester_has_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::Read,
                )
            }
        });

        Self::requester_matches_owner_identity(requester, object.owner())
            || acl_allows_read
            || (object.public_read()
                && !Self::ignores_public_acls(bucket.public_access_block.as_ref()))
    }

    pub(super) fn requester_can_read_bucket_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_owner_account_admin(requester, bucket);
        }

        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                &bucket.acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::ReadAcp,
            )
    }

    pub(super) fn requester_can_write_bucket_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_owner_account_admin(requester, bucket);
        }

        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                &bucket.acl_grants,
                &bucket.owner_canonical_id,
                AclPermission::WriteAcp,
            )
    }

    pub(super) fn requester_can_read_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && Self::requester_can_bucket_owner_account_admin(requester, bucket))
            || Self::requester_matches_owner_identity(requester, object.owner())
            || object.acl_grants().is_some_and(|grants| {
                Self::requester_has_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::ReadAcp,
                )
            })
    }

    pub(super) fn requester_can_write_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
        object: &StoredObject,
    ) -> bool {
        (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && Self::requester_can_bucket_owner_account_admin(requester, bucket))
            || requester
                .account()
                .is_some_and(|_| Self::requester_matches_owner_identity(requester, object.owner()))
            || object.acl_grants().is_some_and(|grants| {
                Self::requester_has_acl_permission(
                    requester,
                    grants,
                    &object.owner().canonical_id,
                    AclPermission::WriteAcp,
                )
            })
    }

    pub(super) fn requester_matches_owner_identity(
        requester: &Requester,
        owner: &OwnerIdentity,
    ) -> bool {
        if requester.is_anonymous() {
            return owner.principal == OwnerIdentity::ANONYMOUS_UPLOAD_PRINCIPAL
                && owner.canonical_id == CanonicalUserId::anonymous_upload();
        }
        requester.account().is_some_and(|account| {
            account.principal() == owner.principal
                || (account.canonical_user_id() == &owner.canonical_id
                    && requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin)
        })
    }

    pub(super) fn requester_is_bucket_owner_account(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        let Some(account) = requester.account() else {
            return false;
        };
        if account.principal() == bucket.owner_principal {
            return true;
        }

        let Some(requester_account_id) = aws_account_id_from_principal(account.principal()) else {
            return false;
        };
        Self::bucket_owner_account_id(&bucket.owner_principal) == Some(requester_account_id)
    }

    fn bucket_owner_account_id(owner_principal: &str) -> Option<&str> {
        aws_account_id_from_principal(owner_principal)
    }

    pub(super) fn requester_is_bucket_owner_account_root_principal(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if requester.authorization_profile() != auth::AuthorizationProfile::OwnerAccountAdmin {
            return false;
        }

        let Some(account) = requester.account() else {
            return false;
        };
        let Some(requester_account_id) = aws_account_id_from_principal(account.principal()) else {
            return false;
        };
        let Some(bucket_owner_account_id) = Self::bucket_owner_account_id(&bucket.owner_principal)
        else {
            return false;
        };
        if requester_account_id != bucket_owner_account_id {
            return false;
        }

        account.principal() == format!("arn:aws:iam::{requester_account_id}:root")
    }

    pub(super) fn requester_can_discover_missing_object(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_read_bucket(
            requester,
            bucket,
            &bucket.owner_principal,
            &bucket.acl_grants,
            Self::effective_public_read(bucket),
        ) || (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && Self::requester_can_bucket_owner_account_admin(requester, bucket))
    }

    pub(super) fn requester_can_discover_missing_object_attrs(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Self::requester_can_bucket_admin(requester, &bucket.owner_principal);
        }

        Self::requester_can_read_bucket(
            requester,
            bucket,
            &bucket.owner_principal,
            &bucket.acl_grants,
            Self::effective_public_read(bucket),
        )
    }

    pub(super) fn requester_can_discover_missing_object_acl(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || (Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
                && Self::requester_can_bucket_owner_account_admin(requester, bucket))
    }

    pub(super) fn requester_can_manage_multipart_upload(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &MultipartUploadRecord,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
            || Self::requester_matches_owner_identity(requester, &upload.owner)
            || upload.initiator.as_ref().is_some_and(|initiator| {
                Self::requester_matches_owner_identity(requester, initiator)
            })
    }

    pub(super) fn requester_can_manage_completed_multipart_upload(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &storage::CompletedMultipartUploadRecord,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
            || Self::requester_matches_owner_identity(requester, &upload.owner)
            || upload.initiator.as_ref().is_some_and(|initiator| {
                Self::requester_matches_owner_identity(requester, initiator)
            })
    }

    pub(super) fn requester_can_write_multipart_upload(
        requester: &Requester,
        bucket: &BucketSummary,
        upload: &MultipartUploadRecord,
    ) -> bool {
        Self::requester_can_object_write(
            requester,
            bucket,
            &bucket.acl_grants,
            Self::effective_public_write(bucket),
        ) && Self::requester_can_manage_multipart_upload(requester, bucket, upload)
    }

    pub(super) fn requester_can_write_multipart_upload_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        upload: &MultipartUploadRecord,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_put_object_action_with_bucket_policy(
            BucketPolicyActionAuthorization {
                request: BucketPolicyRequestContext {
                    requester,
                    bucket,
                    bucket_tags,
                    action: auth::PolicyAction::PutObject,
                    policy_context,
                    policy,
                },
                default_allowed: Self::requester_can_write_multipart_upload(
                    requester, bucket, upload,
                ),
            },
            upload.key.as_str(),
        )
    }

    pub(super) fn modern_write_multipart_upload_with_bucket_policy(
        requester: &Requester,
        bucket: BoeBucketSummary<'_>,
        bucket_tags: Option<&[(String, String)]>,
        upload: &MultipartUploadRecord,
        policy_context: &PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<ModernObjectWriteAuthorization, ServerError> {
        modern::write_multipart_upload_with_bucket_policy(
            requester,
            bucket,
            bucket_tags,
            upload,
            policy_context,
            policy,
        )
    }

    pub(super) fn with_multipart_upload_managed_encryption_policy_context<'a>(
        policy_context: PutObjectPolicyContext<'a>,
        upload: &'a MultipartUploadRecord,
    ) -> PutObjectPolicyContext<'a> {
        if policy_context.managed_encryption.is_some() {
            return policy_context;
        }

        match upload.encryption.managed_encryption_algorithm() {
            Some(algorithm) => policy_context.with_managed_encryption(Some(algorithm)),
            None => policy_context,
        }
    }

    pub(super) fn requester_can_manage_object_tags(
        requester: &Requester,
        bucket: &BucketSummary,
        _object: &StoredObject,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
    }

    pub(super) fn is_bucket_owner_enforced(config: Option<&BucketOwnershipControls>) -> bool {
        config.is_some_and(|config| {
            config.object_ownership == BucketObjectOwnership::BucketOwnerEnforced
        })
    }

    pub(super) fn is_bucket_owner_preferred(config: Option<&BucketOwnershipControls>) -> bool {
        config.is_some_and(|config| {
            config.object_ownership == BucketObjectOwnership::BucketOwnerPreferred
        })
    }

    pub(super) fn ignores_public_acls(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.ignore_public_acls)
    }

    pub(super) fn blocks_public_acls(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.block_public_acls)
    }

    pub(super) fn blocks_public_policy(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.block_public_policy)
    }

    pub(super) fn restricts_public_buckets(config: Option<&PublicAccessBlockConfig>) -> bool {
        config.is_some_and(|config| config.restrict_public_buckets)
    }

    pub(super) fn effective_public_read(bucket: &BucketSummary) -> bool {
        bucket.public_read && !Self::ignores_public_acls(bucket.public_access_block.as_ref())
    }

    pub(super) fn effective_public_write(bucket: &BucketSummary) -> bool {
        bucket.public_write && !Self::ignores_public_acls(bucket.public_access_block.as_ref())
    }

    pub(super) fn get_object_policy_action(version_id: Option<VersionId>) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersion
        } else {
            auth::PolicyAction::GetObject
        }
    }

    pub(super) fn get_object_attributes_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersionAttributes
        } else {
            auth::PolicyAction::GetObjectAttributes
        }
    }

    pub(super) fn get_object_acl_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersionAcl
        } else {
            auth::PolicyAction::GetObjectAcl
        }
    }

    pub(super) fn put_object_acl_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::PutObjectVersionAcl
        } else {
            auth::PolicyAction::PutObjectAcl
        }
    }

    pub(super) fn get_object_tagging_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::GetObjectVersionTagging
        } else {
            auth::PolicyAction::GetObjectTagging
        }
    }

    pub(super) fn put_object_tagging_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::PutObjectVersionTagging
        } else {
            auth::PolicyAction::PutObjectTagging
        }
    }

    pub(super) fn delete_object_tagging_policy_action(
        version_id: Option<VersionId>,
    ) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::DeleteObjectVersionTagging
        } else {
            auth::PolicyAction::DeleteObjectTagging
        }
    }

    pub(super) fn delete_object_policy_action(version_id: Option<VersionId>) -> auth::PolicyAction {
        if version_id.is_some() {
            auth::PolicyAction::DeleteObjectVersion
        } else {
            auth::PolicyAction::DeleteObject
        }
    }

    #[cfg(test)]
    pub(super) fn cached_bucket_policy(
        &self,
        bucket: &BucketSummary,
    ) -> Result<Option<Arc<auth::BucketPolicy>>, ServerError> {
        if !bucket.bucket_policy_present {
            return Ok(None);
        }

        if let Some(cached) = self.cached_bucket_policy_if_fresh(bucket) {
            return Ok(Some(cached));
        }

        let raw_policy = self
            .storage_node
            .get_bucket_subresource(&bucket.name, storage::BucketSubresourceKind::Policy)
            .map_err(|error| match error {
                storage::BucketSnapshotLoadError::Store(other) => ServerError::Store(other),
                storage::BucketSnapshotLoadError::Metadata(
                    storage::MetadataError::BucketNotFound { name },
                ) => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                storage::BucketSnapshotLoadError::Metadata(other) => ServerError::Metadata(other),
            })?;
        let parsed_policy = match raw_policy {
            Some(policy) => Arc::new(auth::parse_bucket_policy(&policy).map_err(|e| {
                ServerError::InternalError {
                    reason: format!(
                        "stored bucket policy for {} failed to parse at request time: {}",
                        bucket.name,
                        e.reason()
                    ),
                }
            })?),
            None => {
                return Ok(None);
            }
        };
        Ok(Some(parsed_policy))
    }

    pub(super) fn cached_bucket_policy_for_loaded_handle(
        &self,
        bucket: &LoadedBucketHandle,
    ) -> Result<Option<Arc<auth::BucketPolicy>>, ServerError> {
        let bucket_summary = bucket.bucket();
        if !bucket_summary.bucket_policy_present {
            return Ok(None);
        }

        if let Some(cached) = self.cached_bucket_policy_if_fresh(bucket_summary) {
            return Ok(Some(cached));
        }

        let parsed_policy = match bucket.policy() {
            LoadedBucketValue::Loaded(raw_policy) => {
                Arc::new(auth::parse_bucket_policy(raw_policy).map_err(|e| {
                    ServerError::InternalError {
                        reason: format!(
                            "stored bucket policy for {} failed to parse at request time: {}",
                            bucket_summary.name,
                            e.reason()
                        ),
                    }
                })?)
            }
            LoadedBucketValue::Missing | LoadedBucketValue::NotRequested => return Ok(None),
        };
        Ok(Some(parsed_policy))
    }

    pub(super) fn cached_bucket_policy_if_fresh(
        &self,
        bucket: &BucketSummary,
    ) -> Option<Arc<auth::BucketPolicy>> {
        if !bucket.bucket_policy_present {
            return None;
        }
        self.parsed_bucket_fast_path_policy_if_fresh(&bucket.name, bucket.bucket_policy_generation)
    }

    pub(super) fn loaded_bucket_tags_for_policy(
        bucket: &LoadedBucketHandle,
    ) -> Result<Option<Vec<(String, String)>>, ServerError> {
        match bucket.tags() {
            LoadedBucketValue::NotRequested => Ok(None),
            LoadedBucketValue::Missing => Ok(Some(Vec::new())),
            LoadedBucketValue::Loaded(tags_xml) => {
                Self::parse_serialized_tag_set(tags_xml).map(Some)
            }
        }
    }

    fn evaluate_bucket_policy_for_object_request(
        context: policy::ObjectPolicyEvaluationContext<'_>,
        object: &StoredObject,
        existing_object_tags_mode: ExistingObjectTagsMode,
        bucket_tags: &[(String, String)],
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        policy::evaluate_bucket_policy_for_object_request(
            context,
            object,
            existing_object_tags_mode,
            bucket_tags,
        )
    }

    fn bucket_policy_decision_for_bucket_loaded_with_tags(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        policy::bucket_policy_decision_for_bucket_loaded_with_tags(
            self,
            requester,
            bucket,
            bucket_tags,
            action,
            policy,
        )
    }

    fn bucket_policy_decision_for_loaded_handle(
        &self,
        requester: &Requester,
        bucket: &LoadedBucketHandle,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        policy::bucket_policy_decision_for_loaded_handle(self, requester, bucket, action, policy)
    }

    fn bucket_policy_decision_for_loaded_handle_with_context(
        &self,
        requester: &Requester,
        bucket: &LoadedBucketHandle,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        policy::bucket_policy_decision_for_loaded_handle_with_context(
            self,
            requester,
            bucket,
            action,
            policy_context,
            policy,
        )
    }

    #[cfg(test)]
    pub(super) fn bucket_policy_decision_for_put_object_action(
        &self,
        request: BucketPolicyRequestContext<'_>,
        key: &str,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        policy::bucket_policy_decision_for_put_object_action(self, request, key)
    }

    fn requester_can_put_object_action_with_bucket_policy(
        &self,
        authorization: BucketPolicyActionAuthorization<'_>,
        key: &str,
    ) -> Result<bool, ServerError> {
        policy::requester_can_put_object_action_with_bucket_policy(self, authorization, key)
    }

    pub(super) fn modern_put_object_authorization_with_bucket_policy(
        requester: &Requester,
        bucket: BoeBucketSummary<'_>,
        bucket_tags: Option<&[(String, String)]>,
        key: &str,
        action: ModernWriteAction,
        policy_context: &PutObjectPolicyContext<'_>,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<ModernObjectWriteAuthorization, ServerError> {
        modern::put_object_authorization_with_bucket_policy(
            requester,
            bucket,
            bucket_tags,
            key,
            action,
            policy_context,
            policy,
        )
    }

    fn modern_delete_object_authorization_with_bucket_policy(
        requester: &Requester,
        bucket: BoeBucketSummary<'_>,
        bucket_tags: Option<&[(String, String)]>,
        key: &str,
        object: Option<&StoredObject>,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        modern::delete_object_authorization_with_bucket_policy(
            requester,
            bucket,
            bucket_tags,
            key,
            object,
            action,
            policy,
        )
    }

    fn bucket_policy_allows_with_fallback<F>(
        requester: &Requester,
        bucket: &BucketSummary,
        decision: auth::PolicyEvaluation,
        fallback: F,
    ) -> bool
    where
        F: FnOnce() -> bool,
    {
        policy::bucket_policy_allows_with_fallback(requester, bucket, decision, fallback)
    }

    fn bucket_policy_allows_with_root_principal_bypass<F>(
        requester: &Requester,
        bucket: &BucketSummary,
        decision: auth::PolicyEvaluation,
        fallback: F,
    ) -> bool
    where
        F: FnOnce() -> bool,
    {
        policy::bucket_policy_allows_with_root_principal_bypass(
            requester, bucket, decision, fallback,
        )
    }

    fn requester_can_object_action_with_bucket_policy<F>(
        &self,
        request: BucketPolicyRequestContext<'_>,
        object: &StoredObject,
        fallback: F,
    ) -> Result<bool, ServerError>
    where
        F: FnOnce() -> bool,
    {
        policy::requester_can_object_action_with_bucket_policy(self, request, object, fallback)
    }

    fn requester_can_object_action_with_unavailable_existing_tags_with_bucket_policy<F>(
        &self,
        request: BucketPolicyRequestContext<'_>,
        object: &StoredObject,
        fallback: F,
    ) -> Result<bool, ServerError>
    where
        F: FnOnce() -> bool,
    {
        policy::requester_can_object_action_with_unavailable_existing_tags_with_bucket_policy(
            self, request, object, fallback,
        )
    }

    fn requester_can_read_family_object_action_with_bucket_policy<F>(
        &self,
        request: BucketPolicyRequestContext<'_>,
        object: &StoredObject,
        existing_object_tags_mode: ExistingObjectTagsMode,
        fallback: F,
    ) -> Result<bool, ServerError>
    where
        F: FnOnce() -> bool,
    {
        policy::requester_can_read_family_object_action_with_bucket_policy(
            self,
            request,
            object,
            existing_object_tags_mode,
            fallback,
        )
    }

    fn requester_can_missing_object_action_with_bucket_policy<F>(
        &self,
        request: BucketPolicyRequestContext<'_>,
        key: &str,
        fallback: F,
    ) -> Result<bool, ServerError>
    where
        F: FnOnce() -> bool,
    {
        policy::requester_can_missing_object_action_with_bucket_policy(self, request, key, fallback)
    }

    fn object_policy_decision(
        &self,
        request: BucketPolicyRequestContext<'_>,
        target: ObjectPolicyTarget<'_>,
    ) -> Result<auth::PolicyEvaluation, ServerError> {
        policy::object_policy_decision(self, request, target)
    }

    pub(super) fn modern_read_object_authorization_with_bucket_policy(
        requester: &Requester,
        bucket: BoeBucketSummary<'_>,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        action: ModernReadAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<ModernObjectReadAuthorization, ServerError> {
        modern::read_object_authorization_with_bucket_policy(
            requester,
            bucket,
            bucket_tags,
            object,
            action,
            policy,
        )
    }

    pub(super) fn requester_can_read_object_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_read_family_object_action_with_bucket_policy(
            BucketPolicyRequestContext {
                requester,
                bucket,
                bucket_tags,
                action,
                policy_context: PutObjectPolicyContext::default(),
                policy,
            },
            object,
            ExistingObjectTagsMode::Available,
            || Self::requester_can_read_object(requester, bucket, object),
        )
    }

    pub(super) fn requester_can_read_object_without_existing_tags_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_read_family_object_action_with_bucket_policy(
            BucketPolicyRequestContext {
                requester,
                bucket,
                bucket_tags,
                action,
                policy_context: PutObjectPolicyContext::default(),
                policy,
            },
            object,
            ExistingObjectTagsMode::Unavailable,
            || Self::requester_can_read_object(requester, bucket, object),
        )
    }

    pub(super) fn requester_can_discover_missing_object_attrs_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
        key: &str,
        version_id: Option<VersionId>,
    ) -> Result<bool, ServerError> {
        let read_allowed = self.requester_can_missing_object_action_with_bucket_policy(
            access.request(
                Self::get_object_policy_action(version_id),
                PutObjectPolicyContext::default(),
            ),
            key,
            || Self::requester_can_discover_missing_object(access.requester, access.bucket),
        )?;
        let attrs_allowed = self.requester_can_missing_object_action_with_bucket_policy(
            access.request(
                Self::get_object_attributes_policy_action(version_id),
                PutObjectPolicyContext::default(),
            ),
            key,
            || Self::requester_can_discover_missing_object_attrs(access.requester, access.bucket),
        )?;

        Ok(read_allowed
            && attrs_allowed
            && self.requester_can_list_bucket_with_bucket_policy(access)?)
    }

    pub(super) fn requester_can_manage_object_tags_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
        object: &StoredObject,
        action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
    ) -> Result<bool, ServerError> {
        self.requester_can_object_action_with_bucket_policy(
            access.request(
                action,
                PutObjectPolicyContext::default()
                    .with_request_object_tags_xml(request_object_tags_xml),
            ),
            object,
            || Self::requester_can_manage_object_tags(access.requester, access.bucket, object),
        )
    }

    pub(super) fn requester_can_manage_object_lock_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_object_action_with_unavailable_existing_tags_with_bucket_policy(
            BucketPolicyRequestContext {
                requester,
                bucket,
                bucket_tags,
                action,
                policy_context: PutObjectPolicyContext::default(),
                policy,
            },
            object,
            || Self::requester_can_bucket_owner_account_admin(requester, bucket),
        )
    }

    pub(super) fn requester_can_delete_object_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
        key: &str,
        object: Option<&StoredObject>,
        action: auth::PolicyAction,
    ) -> Result<bool, ServerError> {
        let decision = self.object_policy_decision(
            access.request(action, PutObjectPolicyContext::default()),
            match object {
                Some(object) => ObjectPolicyTarget::Existing(object),
                None => ObjectPolicyTarget::MissingKey(key),
            },
        )?;

        Ok(Self::bucket_policy_allows_with_fallback(
            access.requester,
            access.bucket,
            decision,
            || {
                Self::requester_can_object_write(
                    access.requester,
                    access.bucket,
                    &access.bucket.acl_grants,
                    Self::effective_public_write(access.bucket),
                )
            },
        ))
    }

    pub(super) fn requester_can_read_object_acl_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_read_family_object_action_with_bucket_policy(
            BucketPolicyRequestContext {
                requester,
                bucket,
                bucket_tags,
                action,
                policy_context: PutObjectPolicyContext::default(),
                policy,
            },
            object,
            ExistingObjectTagsMode::Available,
            || Self::requester_can_read_object_acl(requester, bucket, object),
        )
    }

    pub(super) fn requester_can_write_object_acl_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
        object: &StoredObject,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
    ) -> Result<bool, ServerError> {
        self.requester_can_object_action_with_bucket_policy(
            access.request(action, policy_context),
            object,
            || Self::requester_can_write_object_acl(access.requester, access.bucket, object),
        )
    }

    pub(super) fn requester_can_put_object_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
        key: &str,
        policy_context: PutObjectPolicyContext<'_>,
        existing_object: Option<&StoredObject>,
    ) -> Result<bool, ServerError> {
        let default_allowed = if let Some(object) = existing_object {
            Self::requester_can_bucket_owner_account_admin(access.requester, access.bucket)
                || Self::requester_has_acl_permission(
                    access.requester,
                    &access.bucket.acl_grants,
                    &access.bucket.owner_canonical_id,
                    AclPermission::Write,
                )
                || (Self::effective_public_write(access.bucket)
                    && Self::requester_matches_owner_identity(access.requester, object.owner()))
        } else {
            Self::requester_can_object_write(
                access.requester,
                access.bucket,
                &access.bucket.acl_grants,
                Self::effective_public_write(access.bucket),
            )
        };
        let can_put_object = self.requester_can_put_object_action_with_bucket_policy(
            BucketPolicyActionAuthorization {
                request: access.request(auth::PolicyAction::PutObject, policy_context),
                default_allowed,
            },
            key,
        )?;
        if !can_put_object {
            return Ok(false);
        }

        if policy_context.request_object_tags_xml.is_none() {
            return Ok(true);
        }

        self.requester_can_put_object_action_with_bucket_policy(
            BucketPolicyActionAuthorization {
                request: access.request(auth::PolicyAction::PutObjectTagging, policy_context),
                default_allowed: Self::requester_can_bucket_owner_account_admin(
                    access.requester,
                    access.bucket,
                ),
            },
            key,
        )
    }

    fn requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        action: auth::PolicyAction,
        policy: Option<&auth::BucketPolicy>,
        default_allowed: bool,
    ) -> Result<bool, ServerError> {
        let decision = self.bucket_policy_decision_for_bucket_loaded_with_tags(
            requester,
            bucket,
            bucket_tags,
            action,
            policy,
        )?;
        Ok(Self::bucket_policy_allows_with_fallback(
            requester,
            bucket,
            decision,
            || default_allowed,
        ))
    }

    fn load_bucket_handle_for_bucket_policy_read(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        self.load_bucket_handle_for_bucket_read(req, BucketHandleRequest::new())
    }

    pub(super) fn load_bucket_handle_for_object_policy_read(
        &self,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();

        #[cfg(test)]
        maybe_run_bucket_policy_storage_load_hook(bucket.as_str());
        self.bucket_handle_loader()
            .load_bucket(bucket, expected_bucket_owner, request)
    }

    fn load_bucket_handle_for_modern_object_read(
        &self,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();

        if let Some(info) = self.get_bucket_fast_path_if_fresh(bucket) {
            if info.state != BucketState::Active {
                return Err(ServerError::BucketNotFound {
                    name: bucket.to_string(),
                });
            }

            let bucket_info = Self::modern_bucket_summary_fast(info.clone());
            Self::ensure_expected_bucket_owner_modern(&bucket_info, expected_bucket_owner)?;

            if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
                let cached_policy = match &info.policy {
                    storage::BucketFastPathPolicy::Absent => Some(LoadedBucketValue::Missing),
                    storage::BucketFastPathPolicy::Loaded(policy) => {
                        Some(LoadedBucketValue::Loaded(policy.clone()))
                    }
                };
                let cached_tags = if info.bucket_abac_enabled {
                    match &info.tags {
                        storage::BucketFastPathTags::Loaded(tags) => {
                            Some(LoadedBucketValue::Loaded(tags.clone()))
                        }
                        storage::BucketFastPathTags::Missing => Some(LoadedBucketValue::Missing),
                        storage::BucketFastPathTags::NotApplicable => None,
                    }
                } else {
                    Some(LoadedBucketValue::NotRequested)
                };

                if let (Some(policy), Some(tags)) = (cached_policy, cached_tags) {
                    #[cfg(test)]
                    maybe_run_bucket_policy_fast_path_hook(bucket.as_str());
                    return Ok(LoadedBucketHandle::new(
                        Self::bucket_summary_for_boe_modern_fast_path(bucket_info),
                        request,
                        policy,
                        tags,
                        LoadedBucketValue::NotRequested,
                        LoadedBucketValue::NotRequested,
                    ));
                }
            }
        }

        #[cfg(test)]
        maybe_run_bucket_policy_storage_load_hook(bucket.as_str());
        let snapshot = match self
            .storage_node
            .load_bucket_snapshot(bucket, request.resolve_to_storage_request())
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.remove_bucket_fast_path(bucket);
                return Err(BucketHandleLoader::map_bucket_snapshot_error(err));
            }
        };
        let bucket_is_boe =
            Self::is_bucket_owner_enforced(snapshot.bucket.ownership_controls.as_ref());
        if bucket_is_boe {
            self.upsert_bucket_fast_path((&snapshot).into())?;
        } else {
            self.remove_bucket_fast_path(bucket);
        }
        self.bucket_handle_loader()
            .load_bucket_handle_from_snapshot(snapshot, expected_bucket_owner, request)
    }

    pub(super) fn with_bucket_write_handle_for<R, T>(
        &self,
        req: &R,
        request: BucketHandleRequest,
        action: impl FnOnce(LoadedBucketHandle) -> Result<T, ServerError>,
    ) -> Result<T, ServerError>
    where
        R: BucketScopedRequest + ExpectedBucketOwnerRequest + ?Sized,
    {
        let expected_bucket_owner = req.expected_bucket_owner();
        self.storage_node
            .with_bucket_write_snapshot(
                req.bucket_name_typed(),
                request.resolve_to_storage_request(),
                |snapshot| {
                    let bucket = self
                        .bucket_handle_loader()
                        .load_bucket_handle_from_snapshot(
                            snapshot,
                            expected_bucket_owner,
                            request,
                        )?;
                    #[cfg(test)]
                    maybe_run_bucket_write_handle_loaded_hook(req.bucket_name_typed().as_str());
                    action(bucket)
                },
            )
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
    }

    fn load_bucket_handle_for_bucket_read(
        &self,
        req: &BucketRequest<'_>,
        request: BucketHandleRequest,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let base_request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.bucket_handle_loader().load_bucket(
            req.name_typed(),
            req.expected_bucket_owner(),
            base_request.merge(request),
        )
    }

    fn loaded_bucket_subresource_body(
        value: &LoadedBucketValue<String>,
    ) -> Result<Option<String>, ServerError> {
        match value {
            LoadedBucketValue::Loaded(value) => Ok(Some(value.clone())),
            LoadedBucketValue::Missing => Ok(None),
            LoadedBucketValue::NotRequested => Err(ServerError::InternalError {
                reason: "bucket subresource body was not requested during handle load".to_string(),
            }),
        }
    }

    fn authorize_loaded_bucket_action_for(
        &self,
        req: &BucketRequest<'_>,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        let default_allowed = default_allowed(&req.requester, bucket.bucket());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let allowed = self.requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
            &req.requester,
            bucket.bucket(),
            Self::loaded_bucket_tags_for_policy(&bucket)?.as_deref(),
            action,
            bucket_policy.as_deref(),
            default_allowed,
        )?;
        if !allowed {
            return Err(ServerError::AccessDenied);
        }
        Ok(bucket)
    }

    fn authorize_loaded_bucket_write_action_for<R>(
        &self,
        req: &R,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.with_bucket_write_handle_for(
            req,
            BucketHandleRequest::new()
                .requiring_policy_view()
                .requiring_bucket_tags_if_abac_enabled(),
            |bucket| {
                let default_allowed = default_allowed(req.requester(), bucket.bucket());
                let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
                let allowed = self
                    .requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
                        req.requester(),
                        bucket.bucket(),
                        Self::loaded_bucket_tags_for_policy(&bucket)?.as_deref(),
                        action,
                        bucket_policy.as_deref(),
                        default_allowed,
                    )?;
                if !allowed {
                    return Err(ServerError::AccessDenied);
                }
                Ok(bucket)
            },
        )
    }

    fn authorize_loaded_bucket_write_policy_action_for<R>(
        &self,
        req: &R,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.with_bucket_write_handle_for(
            req,
            BucketHandleRequest::new()
                .requiring_policy_view()
                .requiring_bucket_tags_if_abac_enabled(),
            |bucket| {
                let default_allowed = default_allowed(req.requester(), bucket.bucket());
                let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
                let decision = self.bucket_policy_decision_for_loaded_handle(
                    req.requester(),
                    &bucket,
                    action,
                    bucket_policy.as_deref(),
                )?;
                if !Self::bucket_policy_allows_with_root_principal_bypass(
                    req.requester(),
                    bucket.bucket(),
                    decision,
                    || default_allowed,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Ok(bucket)
            },
        )
    }

    fn authorize_loaded_bucket_owner_account_admin_write_for<R>(
        &self,
        req: &R,
    ) -> Result<LoadedBucketHandle, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.with_bucket_write_handle_for(req, BucketHandleRequest::new(), |bucket| {
            if !Self::requester_can_bucket_owner_account_admin(req.requester(), bucket.bucket()) {
                return Err(ServerError::AccessDenied);
            }
            Ok(bucket)
        })
    }

    fn authorize_loaded_bucket_policy_action_for(
        &self,
        req: &BucketRequest<'_>,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        let default_allowed = default_allowed(&req.requester, bucket.bucket());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            action,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_root_principal_bypass(
            &req.requester,
            bucket.bucket(),
            decision,
            || default_allowed,
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(bucket)
    }

    fn requester_can_list_bucket_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
    ) -> Result<bool, ServerError> {
        self.requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
            access.requester,
            access.bucket,
            access.bucket_tags,
            auth::PolicyAction::ListBucket,
            access.policy,
            Self::requester_can_read_bucket(
                access.requester,
                access.bucket,
                &access.bucket.owner_principal,
                &access.bucket.acl_grants,
                Self::effective_public_read(access.bucket),
            ),
        )
    }

    pub(super) fn parse_policy_existing_object_tags(
        object: &StoredObject,
    ) -> Result<Vec<(String, String)>, ServerError> {
        let Some(tags_xml) = object.as_live().and_then(|record| record.tags.as_deref()) else {
            return Ok(Vec::new());
        };
        Self::parse_serialized_tag_set(tags_xml)
    }

    pub(super) fn parse_serialized_tag_set(
        tags_xml: &str,
    ) -> Result<Vec<(String, String)>, ServerError> {
        let mut tags = Vec::new();
        let mut remaining = tags_xml;

        while let Some(tag_start) = remaining.find("<Tag>") {
            remaining = &remaining[tag_start + "<Tag>".len()..];
            let Some(tag_end) = remaining.find("</Tag>") else {
                return Err(ServerError::InternalError {
                    reason: "stored object tags missing </Tag> terminator".to_string(),
                });
            };
            let tag_xml = &remaining[..tag_end];
            let key = Self::xml_unescape(Self::extract_xml_text(
                tag_xml,
                "Key",
                "stored object tags missing <Key>",
            )?)?;
            let value = Self::xml_unescape(Self::extract_xml_text(
                tag_xml,
                "Value",
                "stored object tags missing <Value>",
            )?)?;
            tags.push((key, value));
            remaining = &remaining[tag_end + "</Tag>".len()..];
        }

        Ok(tags)
    }

    pub(super) fn extract_xml_text<'a>(
        xml: &'a str,
        tag: &str,
        missing_reason: &'static str,
    ) -> Result<&'a str, ServerError> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let Some(start) = xml.find(&open) else {
            return Err(ServerError::InternalError {
                reason: missing_reason.to_string(),
            });
        };
        let content = &xml[start + open.len()..];
        let Some(end) = content.find(&close) else {
            return Err(ServerError::InternalError {
                reason: format!("stored object tags missing closing </{tag}>"),
            });
        };
        Ok(&content[..end])
    }

    pub(super) fn xml_unescape(value: &str) -> Result<String, ServerError> {
        let mut out = String::with_capacity(value.len());
        let mut chars = value.chars();

        while let Some(ch) = chars.next() {
            if ch != '&' {
                out.push(ch);
                continue;
            }

            let mut entity = String::new();
            loop {
                let Some(next) = chars.next() else {
                    return Err(ServerError::InternalError {
                        reason: "stored object tags ended mid-entity".to_string(),
                    });
                };
                entity.push(next);
                if next == ';' {
                    break;
                }
            }

            match entity.as_str() {
                "amp;" => out.push('&'),
                "lt;" => out.push('<'),
                "gt;" => out.push('>'),
                "quot;" => out.push('"'),
                "apos;" => out.push('\''),
                _ => {
                    return Err(ServerError::InternalError {
                        reason: format!("stored object tags contain unsupported entity &{entity}"),
                    });
                }
            }
        }

        Ok(out)
    }

    pub(super) fn ensure_sse_c_allowed(
        bucket: &BucketSummary,
        uses_sse_c: bool,
    ) -> Result<(), ServerError> {
        if uses_sse_c && bucket.encryption.sse_c_blocked {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn ensure_expected_bucket_owner(
        bucket: &BucketSummary,
        expected_bucket_owner: Option<&str>,
    ) -> Result<(), ServerError> {
        if expected_bucket_owner.is_some_and(|expected| expected != bucket.owner_principal) {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn validate_expected_bucket_owner(
        bucket: BucketSummary,
        expected_bucket_owner: Option<&str>,
    ) -> Result<ValidatedBucket, ServerError> {
        Self::ensure_expected_bucket_owner(&bucket, expected_bucket_owner)?;
        Ok(ValidatedBucket(bucket))
    }

    fn ensure_expected_bucket_owner_modern(
        bucket: &ModernBucketSummary,
        expected_bucket_owner: Option<&str>,
    ) -> Result<(), ServerError> {
        if expected_bucket_owner.is_some_and(|expected| expected != bucket.owner_principal) {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn requester_principal_required(requester: &Requester) -> Result<&str, ServerError> {
        requester.principal_opt().ok_or(ServerError::AccessDenied)
    }

    pub(super) fn bucket_owner_identity(bucket: &BucketSummary) -> OwnerIdentity {
        OwnerIdentity::new(
            bucket.owner_principal.clone(),
            bucket.owner_canonical_id.clone(),
        )
    }

    pub(super) fn owner_full_control_grants(owner: &OwnerIdentity) -> AclGrants {
        AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(owner.canonical_id.clone()),
            AclPermission::FullControl,
        )])
    }

    pub(super) fn bucket_acl_grants_from_canned(
        bucket_owner: &OwnerIdentity,
        acl: BucketAcl,
    ) -> Result<AclGrants, ServerError> {
        let mut grants: Vec<AclGrant> = Self::owner_full_control_grants(bucket_owner)
            .iter()
            .cloned()
            .collect();
        match acl {
            BucketAcl::Private => {}
            BucketAcl::PublicRead => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
            }
            BucketAcl::PublicReadWrite => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Write));
            }
            BucketAcl::AuthenticatedRead => {
                grants.push(AclGrant::new(
                    AclGrantee::AuthenticatedUsers,
                    AclPermission::Read,
                ));
            }
        }
        Ok(AclGrants::new(grants))
    }

    #[cfg(test)]
    pub(super) fn bucket_acl_grants_from_flags(
        bucket_owner: &OwnerIdentity,
        public_read: bool,
        public_write: bool,
    ) -> AclGrants {
        let mut grants: Vec<AclGrant> = Self::owner_full_control_grants(bucket_owner)
            .iter()
            .cloned()
            .collect();
        if public_read {
            grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
        }
        if public_write {
            grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Write));
        }
        AclGrants::new(grants)
    }

    pub(super) fn ensure_put_bucket_acl_supported(
        bucket: &BucketSummary,
        acl: BucketAcl,
    ) -> Result<(), ServerError> {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref()) {
            return Err(ServerError::AccessControlListNotSupported);
        }
        if Self::blocks_public_acls(bucket.public_access_block.as_ref()) && acl.is_public() {
            return Err(ServerError::AccessDenied);
        }
        Ok(())
    }

    pub(super) fn requester_owner_identity(requester: &Requester) -> Option<OwnerIdentity> {
        if requester.is_anonymous() {
            return Some(OwnerIdentity::anonymous_upload());
        }
        requester.account().map(|account| {
            OwnerIdentity::new(
                account.principal().to_string(),
                account.canonical_user_id().clone(),
            )
        })
    }

    pub(super) fn effective_object_owner(
        bucket: &BucketSummary,
        requester: &Requester,
        acl: PutObjectAcl<'_>,
    ) -> OwnerIdentity {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            || (Self::is_bucket_owner_preferred(bucket.ownership_controls.as_ref())
                && matches!(acl, PutObjectAcl::BucketOwnerFullControl))
        {
            return Self::bucket_owner_identity(bucket);
        }

        Self::requester_owner_identity(requester)
            .unwrap_or_else(|| Self::bucket_owner_identity(bucket))
    }

    pub(super) fn effective_put_object_owner(
        bucket: &BucketSummary,
        requester: &Requester,
        acl: &PutObjectWriteAcl<'_>,
    ) -> OwnerIdentity {
        let canned = match acl {
            PutObjectWriteAcl::None | PutObjectWriteAcl::Grants(_) => PutObjectAcl::None,
            PutObjectWriteAcl::Canned(acl) => *acl,
        };
        Self::effective_object_owner(bucket, requester, canned)
    }

    pub(super) fn ensure_put_object_acl_supported(
        bucket: &BucketSummary,
        acl: PutObjectAcl<'_>,
    ) -> Result<(), ServerError> {
        if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
            && !acl.is_supported_with_bucket_owner_enforced()
        {
            return Err(ServerError::AccessControlListNotSupported);
        }
        if Self::blocks_public_acls(bucket.public_access_block.as_ref()) && acl.is_public() {
            return Err(ServerError::AccessDenied);
        }
        match acl {
            PutObjectAcl::Invalid(value) => {
                return Err(ServerError::InvalidArgument {
                    reason: format!("invalid x-amz-acl value: {value}"),
                });
            }
            PutObjectAcl::None
            | PutObjectAcl::Private
            | PutObjectAcl::PublicRead
            | PutObjectAcl::PublicReadWrite
            | PutObjectAcl::AuthenticatedRead
            | PutObjectAcl::AwsExecRead
            | PutObjectAcl::BucketOwnerRead
            | PutObjectAcl::BucketOwnerFullControl => {}
        }

        Ok(())
    }

    pub(super) fn ensure_put_object_write_acl_supported(
        bucket: &BucketSummary,
        acl: &PutObjectWriteAcl<'_>,
    ) -> Result<(), ServerError> {
        match acl {
            PutObjectWriteAcl::None => {
                Self::ensure_put_object_acl_supported(bucket, PutObjectAcl::None)
            }
            PutObjectWriteAcl::Canned(acl) => Self::ensure_put_object_acl_supported(bucket, *acl),
            PutObjectWriteAcl::Grants(acl_grants) => {
                if Self::is_bucket_owner_enforced(bucket.ownership_controls.as_ref())
                    && !Self::acl_grants_owner_full_control_only(
                        &bucket.owner_canonical_id,
                        acl_grants,
                    )
                {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                Self::ensure_supported_object_acl_grants(acl_grants)?;
                if Self::blocks_public_acls(bucket.public_access_block.as_ref())
                    && (Self::acl_grants_grant_public_read(acl_grants)
                        || Self::acl_grants_grant_public_write(acl_grants))
                {
                    return Err(ServerError::AccessDenied);
                }
                Ok(())
            }
        }
    }

    pub(super) fn object_acl_grants_for_write(
        bucket: &BucketSummary,
        owner: &OwnerIdentity,
        acl: PutObjectAcl<'_>,
    ) -> AclGrants {
        let mut grants = vec![AclGrant::new(
            AclGrantee::CanonicalUser(owner.canonical_id.clone()),
            AclPermission::FullControl,
        )];
        match acl {
            PutObjectAcl::PublicRead => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
            }
            PutObjectAcl::PublicReadWrite => {
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Read));
                grants.push(AclGrant::new(AclGrantee::AllUsers, AclPermission::Write));
            }
            PutObjectAcl::AuthenticatedRead => {
                grants.push(AclGrant::new(
                    AclGrantee::AuthenticatedUsers,
                    AclPermission::Read,
                ));
            }
            PutObjectAcl::AwsExecRead => {
                grants.push(AclGrant::new(
                    AclGrantee::CanonicalUser(CanonicalUserId::aws_exec_read()),
                    AclPermission::Read,
                ));
            }
            PutObjectAcl::BucketOwnerRead if bucket.owner_canonical_id != owner.canonical_id => {
                grants.push(AclGrant::new(
                    AclGrantee::CanonicalUser(bucket.owner_canonical_id.clone()),
                    AclPermission::Read,
                ));
            }
            PutObjectAcl::BucketOwnerFullControl
                if bucket.owner_canonical_id != owner.canonical_id =>
            {
                grants.push(AclGrant::new(
                    AclGrantee::CanonicalUser(bucket.owner_canonical_id.clone()),
                    AclPermission::FullControl,
                ));
            }
            PutObjectAcl::None
            | PutObjectAcl::Private
            | PutObjectAcl::BucketOwnerFullControl
            | PutObjectAcl::BucketOwnerRead
            | PutObjectAcl::Invalid(_) => {}
        }
        AclGrants::new(grants)
    }

    pub(super) fn object_acl_grants_for_put_object(
        bucket: &BucketSummary,
        owner: &OwnerIdentity,
        acl: &PutObjectWriteAcl<'_>,
    ) -> AclGrants {
        match acl {
            PutObjectWriteAcl::None => {
                Self::object_acl_grants_for_write(bucket, owner, PutObjectAcl::None)
            }
            PutObjectWriteAcl::Canned(acl) => {
                Self::object_acl_grants_for_write(bucket, owner, *acl)
            }
            PutObjectWriteAcl::Grants(acl_grants) => acl_grants.clone(),
        }
    }

    pub(super) fn ensure_supported_bucket_acl_grants(
        _acl_grants: &AclGrants,
    ) -> Result<(), ServerError> {
        Ok(())
    }

    pub(super) fn ensure_supported_object_acl_grants(
        _acl_grants: &AclGrants,
    ) -> Result<(), ServerError> {
        Ok(())
    }

    pub(super) fn acl_grants_owner_full_control_only(
        owner_canonical_id: &CanonicalUserId,
        acl_grants: &AclGrants,
    ) -> bool {
        let expected = AclGrants::new(vec![AclGrant::new(
            AclGrantee::CanonicalUser(owner_canonical_id.clone()),
            AclPermission::FullControl,
        )]);
        acl_grants == &expected
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn authorize_put_object_tags(
        &self,
        req: &PutObjectTagsRequest<'_>,
    ) -> Result<AuthorizedObjectTagsAccess, ServerError> {
        let stored = self.authorize_object_tagging_access(
            &req.object,
            Self::put_object_tagging_policy_action(req.object.version_id),
            Some(req.tags),
        )?;
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        Ok(AuthorizedObjectTagsAccess {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_get_object_tags(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedObjectTagsAccess, ServerError> {
        let stored = self.authorize_object_tagging_read(
            req,
            Self::get_object_tagging_policy_action(req.version_id),
        )?;
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        Ok(AuthorizedObjectTagsAccess {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
        })
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn authorize_delete_object_tags(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedObjectTagsAccess, ServerError> {
        let stored = self.authorize_object_tagging_access(
            req,
            Self::delete_object_tagging_policy_action(req.version_id),
            None,
        )?;
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        Ok(AuthorizedObjectTagsAccess {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_get_object_acl(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedGetObjectAcl, ServerError> {
        let LoadedObjectState {
            bucket_info,
            record: stored,
            ..
        } = self
            .authorize_object_acl_read(req, Self::get_object_acl_policy_action(req.version_id))?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        let result = if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
            let owner = Self::bucket_owner_identity(&bucket_info);
            GetObjectAclResult {
                owner_principal: owner.principal,
                owner_canonical_id: owner.canonical_id.clone(),
                acl_grants: AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(owner.canonical_id),
                    AclPermission::FullControl,
                )]),
                version_id: live.version_id,
            }
        } else {
            GetObjectAclResult {
                owner_principal: live.owner.principal.clone(),
                owner_canonical_id: live.owner.canonical_id.clone(),
                acl_grants: live.acl_grants.clone(),
                version_id: live.version_id,
            }
        };
        Ok(AuthorizedGetObjectAcl { result })
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn authorize_put_object_acl(
        &self,
        req: &PutObjectAclRequest<'_>,
    ) -> Result<AuthorizedPutObjectAclUpdate, ServerError> {
        if req.object.requester().is_anonymous() {
            return Err(ServerError::AnonymousApiAccessDenied);
        }
        let LoadedObjectState {
            bucket_info,
            record: stored,
            ..
        } = self.authorize_object_acl_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            ObjectAclAuthorization::WriteWithPolicy {
                action: Self::put_object_acl_policy_action(req.object.version_id),
                policy_context: req.authorization_policy_context()?,
            },
            req.object.expected_bucket_owner(),
        )?;
        if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
            return Err(ServerError::AccessControlListNotSupported);
        }
        let acl_grants = match &req.acl {
            PutObjectAclInput::Canned(acl) => {
                Self::ensure_put_object_acl_supported(&bucket_info, *acl)?;
                let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
                Self::object_acl_grants_for_write(&bucket_info, &live.owner, *acl)
            }
            PutObjectAclInput::Grants(acl_grants) => {
                Self::ensure_supported_object_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        let public_read = Self::acl_grants_public_read(&acl_grants);
        if Self::blocks_public_acls(bucket_info.public_access_block.as_ref())
            && (Self::acl_grants_grant_public_read(&acl_grants)
                || Self::acl_grants_grant_public_write(&acl_grants))
        {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedPutObjectAclUpdate {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: stored.version_id(),
            acl_grants,
            public_read,
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_put_object_retention(
        &self,
        req: &PutObjectRetentionRequest<'_>,
    ) -> Result<AuthorizedPutObjectRetention, ServerError> {
        let LoadedObjectState {
            bucket_info,
            bucket_policy,
            bucket_tags,
            record: stored,
            ..
        } = self.authorize_object_lock_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            auth::PolicyAction::PutObjectRetention,
            req.object.expected_bucket_owner(),
        )?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        let can_bypass_governance = self
            .requester_can_bypass_governance_retention_with_bucket_policy(
                req.object.requester(),
                &bucket_info,
                bucket_tags.as_deref(),
                &stored,
                bucket_policy.as_deref(),
            )?;
        Self::validate_retention_update(
            live.object_lock.retention,
            req.retention,
            req.bypass_governance,
            can_bypass_governance,
        )?;
        Ok(AuthorizedPutObjectRetention {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: live.version_id,
            retention: req.retention,
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_get_object_retention(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedGetObjectRetention, ServerError> {
        let LoadedObjectState { record: stored, .. } =
            self.authorize_object_lock_read(req, auth::PolicyAction::GetObjectRetention)?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        Ok(AuthorizedGetObjectRetention {
            retention: live.object_lock.retention,
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_put_object_legal_hold(
        &self,
        req: &PutObjectLegalHoldRequest<'_>,
    ) -> Result<AuthorizedPutObjectLegalHold, ServerError> {
        let LoadedObjectState { record: stored, .. } = self.authorize_object_lock_access(
            req.object.requester(),
            req.object.bucket_name_typed(),
            req.object.key_typed(),
            req.object.version_id,
            auth::PolicyAction::PutObjectLegalHold,
            req.object.expected_bucket_owner(),
        )?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        Ok(AuthorizedPutObjectLegalHold {
            bucket: req.object.bucket_name_typed().clone(),
            key: req.object.key_typed().clone(),
            version_id: live.version_id,
            legal_hold: StoredLegalHoldStatus::from_legal_hold_status(Some(req.legal_hold)),
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_get_object_legal_hold(
        &self,
        req: &ObjectVersionRequest<'_>,
    ) -> Result<AuthorizedGetObjectLegalHold, ServerError> {
        let LoadedObjectState { record: stored, .. } =
            self.authorize_object_lock_read(req, auth::PolicyAction::GetObjectLegalHold)?;
        let live = stored.as_live().ok_or(ServerError::MethodNotAllowed)?;
        Ok(AuthorizedGetObjectLegalHold {
            legal_hold: live.object_lock.legal_hold.as_legal_hold_status(),
        })
    }

    pub(super) fn authorize_create_bucket(
        &self,
        req: &CreateBucketRequest,
    ) -> Result<AuthorizedCreateBucket, ServerError> {
        let owner_account = req.requester.account().ok_or(ServerError::AccessDenied)?;
        let locked_to_account_region =
            self.validate_create_bucket_namespace(&req.name, req.namespace, owner_account)?;
        if req.ownership == BucketObjectOwnership::BucketOwnerEnforced && req.acl.is_explicit() {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }
        let owner = OwnerIdentity::new(
            owner_account.principal(),
            owner_account.canonical_user_id().clone(),
        );
        let acl_grants = match &req.acl {
            CreateBucketAcl::DefaultPrivate => Self::owner_full_control_grants(&owner),
            CreateBucketAcl::Canned(acl) => Self::bucket_acl_grants_from_canned(&owner, *acl)?,
            CreateBucketAcl::Grants(acl_grants) => {
                Self::ensure_supported_bucket_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        if Self::acl_grants_grant_public_read(&acl_grants)
            || Self::acl_grants_grant_public_write(&acl_grants)
        {
            return Err(ServerError::InvalidBucketAclWithBlockPublicAccessError);
        }
        Ok(AuthorizedCreateBucket {
            name: req.name.clone(),
            requester: req.requester.clone(),
            owner,
            locked_to_account_region,
            acl: req.acl.clone(),
            ownership: req.ownership,
            object_lock_enabled: req.object_lock_enabled,
            acl_grants,
        })
    }

    pub(super) fn authorize_head_bucket(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedHeadBucket, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_read_fallback = || {
            Self::requester_can_read_bucket(
                req.requester(),
                bucket.bucket(),
                &bucket.bucket().owner_principal,
                &bucket.bucket().acl_grants,
                Self::effective_public_read(bucket.bucket()),
            )
        };
        let list_decision = self.bucket_policy_decision_for_loaded_handle(
            req.requester(),
            &bucket,
            auth::PolicyAction::ListBucket,
            bucket_policy.as_deref(),
        )?;
        let location_decision = self.bucket_policy_decision_for_loaded_handle(
            req.requester(),
            &bucket,
            auth::PolicyAction::GetBucketLocation,
            bucket_policy.as_deref(),
        )?;
        let list_allowed = Self::bucket_policy_allows_with_fallback(
            req.requester(),
            bucket.bucket(),
            list_decision,
            bucket_read_fallback,
        );
        let location_allowed = Self::bucket_policy_allows_with_fallback(
            req.requester(),
            bucket.bucket(),
            location_decision,
            bucket_read_fallback,
        );
        if !(list_allowed && location_allowed) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedHeadBucket {
            bucket_info: bucket.bucket().clone(),
        })
    }

    pub(super) fn authorize_delete_bucket(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucket, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            req,
            auth::PolicyAction::DeleteBucket,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedDeleteBucket {
            name: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_cors(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourcePut, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketCors,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketSubresourcePut {
            bucket: req.bucket.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Cors,
            body: req.config.to_string(),
        })
    }

    pub(super) fn authorize_get_bucket_cors(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceBodyGet, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read(
            req,
            BucketHandleRequest::new().requiring_cors_view(),
        )?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let allowed = self.requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
            &req.requester,
            bucket.bucket(),
            Self::loaded_bucket_tags_for_policy(&bucket)?.as_deref(),
            auth::PolicyAction::GetBucketCors,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        )?;
        if !allowed {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedBucketSubresourceBodyGet {
            body: Self::loaded_bucket_subresource_body(bucket.cors())?,
        })
    }

    pub(super) fn authorize_get_bucket_tagging(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceBodyGet, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read(
            req,
            BucketHandleRequest::new().requiring_bucket_tags(),
        )?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketTagging,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedBucketSubresourceBodyGet {
            body: Self::loaded_bucket_subresource_body(bucket.tags())?,
        })
    }

    /// Creates an internal authorization token for HTTP CORS evaluation.
    ///
    /// This intentionally bypasses normal bucket-config authorization because
    /// CORS preflight handling and actual-response header decoration need the
    /// stored CORS rules without turning those paths into authenticated bucket
    /// config reads.
    pub(super) fn authorize_load_bucket_cors_config_for(
        &self,
        name: &BucketName,
    ) -> AuthorizedBucketSubresourceGet {
        AuthorizedBucketSubresourceGet {
            bucket: name.clone(),
            kind: storage::BucketSubresourceKind::Cors,
        }
    }

    pub(super) fn authorize_delete_bucket_cors(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            req,
            auth::PolicyAction::PutBucketCors,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Cors,
        })
    }

    pub(super) fn authorize_put_bucket_tagging(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourcePut, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketTagging,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().bucket_abac_enabled {
            return Err(ServerError::BadRequest {
                reason: "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To add tags to this bucket, initiate a TagResource request. To delete tags from this bucket, initiate an UntagResource request.".to_string(),
            });
        }
        Ok(AuthorizedBucketSubresourcePut {
            bucket: req.bucket.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Tagging,
            body: req.config.to_string(),
        })
    }

    pub(super) fn authorize_delete_bucket_tagging(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for(
            req,
            auth::PolicyAction::PutBucketTagging,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().bucket_abac_enabled {
            return Err(ServerError::BadRequest {
                reason: "This S3 general purpose bucket has attribute-based access control (ABAC) enabled. To delete tags from this bucket, initiate an UntagResource request.".to_string(),
            });
        }
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Tagging,
        })
    }

    fn validate_tag_resource_account_id(
        bucket_info: &BucketSummary,
        account_id: &str,
    ) -> Result<(), ServerError> {
        if aws_account_id_from_principal(&bucket_info.owner_principal) == Some(account_id) {
            return Ok(());
        }
        Err(ServerError::AccessDenied)
    }

    pub(super) fn authorize_bucket_tag_control(
        &self,
        req: &BucketTagControlRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let bucket = self.authorize_loaded_bucket_owner_account_admin_write_for(&req.bucket)?;
        Self::validate_tag_resource_account_id(bucket.bucket(), req.account_id)?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.bucket.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_abac(
        &self,
        req: &PutBucketAbacRequest<'_>,
    ) -> Result<AuthorizedPutBucketAbac, ServerError> {
        let _bucket = self.authorize_loaded_bucket_owner_account_admin_write_for(&req.bucket)?;
        Ok(AuthorizedPutBucketAbac {
            bucket: req.bucket.name_typed().clone(),
            enabled: req.enabled,
        })
    }

    pub(super) fn authorize_get_bucket_abac(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketAbac, ServerError> {
        let bucket = self.bucket_handle_loader().load_bucket(
            req.name_typed(),
            req.expected_bucket_owner(),
            BucketHandleRequest::new(),
        )?;
        if !Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketAbac {
            enabled: bucket.bucket().bucket_abac_enabled,
        })
    }

    pub(super) fn authorize_put_bucket_policy(
        &self,
        req: &PutBucketPolicyRequest<'_>,
    ) -> Result<AuthorizedPutBucketPolicy, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_policy_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketPolicy,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        let parsed_policy =
            auth::parse_bucket_policy(req.config).map_err(|e| ServerError::MalformedPolicy {
                reason: e.reason().to_string(),
            })?;
        parsed_policy
            .validate_evaluable_object_conditions()
            .map_err(|e| ServerError::MalformedPolicy {
                reason: e.reason().to_string(),
            })?;
        let normalized_policy = parsed_policy.normalized_json();
        if normalized_policy.len() > auth::bucket_policy::MAX_BUCKET_POLICY_BYTES {
            return Err(ServerError::MalformedPolicy {
                reason: format!(
                    "Normalized policy document exceeds the maximum allowed size of {} bytes",
                    auth::bucket_policy::MAX_BUCKET_POLICY_BYTES
                ),
            });
        }
        let policy_is_public = parsed_policy.is_public();
        if Self::blocks_public_policy(bucket.bucket().public_access_block.as_ref())
            && policy_is_public
        {
            return Err(ServerError::BlockPublicPolicyAccessDenied {
                requester_principal: Self::requester_principal_required(&req.bucket.requester)?
                    .to_string(),
                bucket: req.bucket.name.to_string(),
            });
        }
        Ok(AuthorizedPutBucketPolicy {
            bucket: req.bucket.name_typed().clone(),
            body: normalized_policy,
            policy_is_public,
        })
    }

    pub(super) fn authorize_get_bucket_policy(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceBodyGet, ServerError> {
        let bucket = self.authorize_loaded_bucket_policy_action_for(
            req,
            auth::PolicyAction::GetBucketPolicy,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketSubresourceBodyGet {
            body: Self::loaded_bucket_subresource_body(bucket.policy())?,
        })
    }

    pub(super) fn authorize_delete_bucket_policy(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_policy_action_for(
            req,
            auth::PolicyAction::DeleteBucketPolicy,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Policy,
        })
    }

    pub(super) fn authorize_put_bucket_public_access_block(
        &self,
        req: &PutBucketPublicAccessBlockRequest<'_>,
    ) -> Result<AuthorizedPutBucketPublicAccessBlock, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketPublicAccessBlock,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedPutBucketPublicAccessBlock {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    pub(super) fn authorize_get_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketPublicAccessBlock, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetBucketPublicAccessBlock,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedGetBucketPublicAccessBlock {
            config: bucket.bucket().public_access_block,
        })
    }

    pub(super) fn authorize_delete_bucket_public_access_block(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            req,
            auth::PolicyAction::PutBucketPublicAccessBlock,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_ownership_controls(
        &self,
        req: &PutBucketOwnershipControlsRequest<'_>,
    ) -> Result<AuthorizedPutBucketOwnershipControls, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketOwnershipControls,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if Self::is_bucket_owner_enforced(Some(&req.config))
            && !Self::acl_grants_owner_full_control_only(
                &bucket.bucket().owner_canonical_id,
                &bucket.bucket().acl_grants,
            )
        {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }
        Ok(AuthorizedPutBucketOwnershipControls {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    pub(super) fn authorize_get_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketOwnershipControls, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetBucketOwnershipControls,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedGetBucketOwnershipControls {
            config: bucket.bucket().ownership_controls,
        })
    }

    pub(super) fn authorize_delete_bucket_ownership_controls(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketConfigAccess, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            req,
            auth::PolicyAction::PutBucketOwnershipControls,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketConfigAccess {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_put_bucket_lifecycle(
        &self,
        req: &PutBucketConfigRequest<'_>,
    ) -> Result<AuthorizedPutBucketLifecycle, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutLifecycleConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        s3_types::parse_lifecycle_configuration_xml(req.config.as_bytes()).map_err(|error| {
            match error {
                LifecycleConfigError::MalformedXml { reason } => {
                    ServerError::MalformedXML { reason }
                }
                LifecycleConfigError::InvalidRequest { reason } => {
                    ServerError::InvalidRequest { reason }
                }
                LifecycleConfigError::InvalidArgument { reason } => {
                    ServerError::InvalidArgument { reason }
                }
                LifecycleConfigError::NotImplemented { feature } => {
                    ServerError::NotImplemented { feature }
                }
            }
        })?;
        Ok(AuthorizedPutBucketLifecycle {
            bucket: req.bucket.name_typed().clone(),
            body: req.config.to_string(),
        })
    }

    pub(super) fn authorize_get_bucket_lifecycle(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceBodyGet, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_read(
            req,
            BucketHandleRequest::new().requiring_lifecycle_view(),
        )?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let allowed = self.requester_can_bucket_action_with_preloaded_tags_with_bucket_policy(
            &req.requester,
            bucket.bucket(),
            Self::loaded_bucket_tags_for_policy(&bucket)?.as_deref(),
            auth::PolicyAction::GetLifecycleConfiguration,
            bucket_policy.as_deref(),
            Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        )?;
        if !allowed {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedBucketSubresourceBodyGet {
            body: Self::loaded_bucket_subresource_body(bucket.lifecycle())?,
        })
    }

    /// Creates an internal authorization token for lifecycle state loads.
    ///
    /// This intentionally bypasses request auth because the coordinator is
    /// loading already-authoritative stored lifecycle state for internal
    /// lifecycle evaluation such as response-header computation.
    pub(super) fn authorize_load_bucket_lifecycle_for(
        &self,
        name: &BucketName,
    ) -> AuthorizedBucketSubresourceGet {
        AuthorizedBucketSubresourceGet {
            bucket: name.clone(),
            kind: storage::BucketSubresourceKind::Lifecycle,
        }
    }

    pub(super) fn authorize_delete_bucket_lifecycle(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedBucketSubresourceDelete, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            req,
            auth::PolicyAction::PutLifecycleConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedBucketSubresourceDelete {
            bucket: req.name_typed().clone(),
            kind: storage::BucketSubresourceKind::Lifecycle,
        })
    }

    pub(super) fn authorize_put_bucket_encryption(
        &self,
        req: &PutBucketEncryptionRequest<'_>,
    ) -> Result<AuthorizedPutBucketEncryption, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutEncryptionConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedPutBucketEncryption {
            bucket: req.bucket.name_typed().clone(),
            config: req.config,
        })
    }

    pub(super) fn authorize_put_bucket_versioning(
        &self,
        req: &PutBucketVersioningRequest<'_>,
    ) -> Result<AuthorizedPutBucketVersioning, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketVersioning,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().object_lock.enabled && req.state != BucketVersioningState::Enabled {
            return Err(ServerError::InvalidBucketState);
        }
        Ok(AuthorizedPutBucketVersioning {
            bucket: req.bucket.name_typed().clone(),
            state: req.state,
        })
    }

    pub(super) fn authorize_get_bucket_versioning(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketVersioning, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketVersioning,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketVersioning {
            state: bucket.bucket().versioning,
        })
    }

    pub(super) fn authorize_get_bucket_location(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketLocation, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketLocation,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_owner_account_admin(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketLocation)
    }

    pub(super) fn authorize_list_objects_v2(
        &self,
        req: &ListObjectsV2Request<'_>,
    ) -> Result<AuthorizedListObjectsV2, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(&req.bucket)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.bucket.requester,
            &bucket,
            auth::PolicyAction::ListBucket,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.bucket.requester,
            bucket.bucket(),
            policy_decision,
            || {
                Self::requester_can_read_bucket(
                    &req.bucket.requester,
                    bucket.bucket(),
                    &bucket.bucket().owner_principal,
                    &bucket.bucket().acl_grants,
                    Self::effective_public_read(bucket.bucket()),
                )
            },
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListObjectsV2 {
            bucket_info: bucket.bucket().clone(),
        })
    }

    pub(super) fn authorize_list_buckets(
        &self,
        req: &ListBucketsRequest,
    ) -> Result<AuthorizedListBuckets, ServerError> {
        let requester = req.requester.account().ok_or(ServerError::AccessDenied)?;
        Ok(AuthorizedListBuckets {
            owner_canonical_id: requester.canonical_user_id().clone(),
        })
    }

    pub(super) fn authorize_list_object_versions(
        &self,
        req: &ListObjectVersionsRequest<'_>,
    ) -> Result<AuthorizedListObjectVersions, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(&req.bucket)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.bucket.requester,
            &bucket,
            auth::PolicyAction::ListBucketVersions,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.bucket.requester,
            bucket.bucket(),
            policy_decision,
            || {
                Self::requester_can_read_bucket(
                    &req.bucket.requester,
                    bucket.bucket(),
                    &bucket.bucket().owner_principal,
                    &bucket.bucket().acl_grants,
                    Self::effective_public_read(bucket.bucket()),
                )
            },
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListObjectVersions {
            bucket_info: bucket.bucket().clone(),
        })
    }

    pub(super) fn authorize_list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsRequest<'_>,
    ) -> Result<AuthorizedListMultipartUploads, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(&req.bucket)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.bucket.requester,
            &bucket,
            auth::PolicyAction::ListBucketMultipartUploads,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.bucket.requester,
            bucket.bucket(),
            policy_decision,
            || {
                Self::requester_can_read_bucket(
                    &req.bucket.requester,
                    bucket.bucket(),
                    &bucket.bucket().owner_principal,
                    &bucket.bucket().acl_grants,
                    Self::effective_public_read(bucket.bucket()),
                )
            },
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedListMultipartUploads {
            bucket: bucket.bucket().name.clone(),
        })
    }

    pub(super) fn authorize_put_bucket_object_lock_configuration(
        &self,
        req: &PutBucketObjectLockConfigurationRequest<'_>,
    ) -> Result<AuthorizedPutBucketObjectLockConfiguration, ServerError> {
        let bucket = self.authorize_loaded_bucket_write_action_for(
            &req.bucket,
            auth::PolicyAction::PutBucketObjectLockConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if bucket.bucket().versioning != BucketVersioningState::Enabled {
            return Err(ServerError::InvalidBucketState);
        }

        let final_enabled =
            bucket.bucket().object_lock.enabled || req.config.object_lock_enabled.is_some();
        if !final_enabled {
            return Err(ServerError::InvalidRequest {
                reason: "Object Lock must be enabled before configuring this bucket".to_string(),
            });
        }

        Ok(AuthorizedPutBucketObjectLockConfiguration {
            bucket: req.bucket.name_typed().clone(),
            config: BucketObjectLockConfig {
                enabled: true,
                default_retention: req.config.default_retention,
            },
        })
    }

    pub(super) fn authorize_get_bucket_encryption(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketEncryption, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetEncryptionConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedGetBucketEncryption {
            config: bucket.bucket().encryption,
        })
    }

    pub(super) fn authorize_delete_bucket_encryption(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedDeleteBucketEncryption, ServerError> {
        let _bucket = self.authorize_loaded_bucket_write_action_for(
            req,
            auth::PolicyAction::PutEncryptionConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        Ok(AuthorizedDeleteBucketEncryption {
            bucket: req.name_typed().clone(),
        })
    }

    pub(super) fn authorize_get_bucket_object_lock_configuration(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketObjectLockConfiguration, ServerError> {
        let bucket = self.authorize_loaded_bucket_action_for(
            req,
            auth::PolicyAction::GetBucketObjectLockConfiguration,
            Self::requester_can_bucket_owner_account_admin,
        )?;
        if !bucket.bucket().object_lock.enabled {
            return Err(ServerError::ObjectLockConfigurationNotFound {
                bucket: req.name.to_string(),
            });
        }
        Ok(AuthorizedGetBucketObjectLockConfiguration {
            config: bucket.bucket().object_lock,
        })
    }

    pub(super) fn authorize_get_bucket_policy_status(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketPolicyStatus, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        if !bucket.bucket().bucket_policy_present {
            if !Self::requester_can_bucket_admin(&req.requester, &bucket.bucket().owner_principal) {
                return Err(ServerError::AccessDenied);
            }
            return Err(ServerError::NoSuchBucketPolicy {
                bucket: req.name.to_string(),
            });
        }
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketPolicyStatus,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_bucket_admin(&req.requester, &bucket.bucket().owner_principal),
        ) {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedGetBucketPolicyStatus {
            is_public: bucket.bucket().bucket_policy_public,
        })
    }

    pub(super) fn authorize_get_bucket_acl(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<AuthorizedGetBucketAcl, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let policy_decision = self.bucket_policy_decision_for_loaded_handle(
            &req.requester,
            &bucket,
            auth::PolicyAction::GetBucketAcl,
            bucket_policy.as_deref(),
        )?;
        if !Self::bucket_policy_allows_with_fallback(
            &req.requester,
            bucket.bucket(),
            policy_decision,
            || Self::requester_can_read_bucket_acl(&req.requester, bucket.bucket()),
        ) {
            return Err(ServerError::AccessDenied);
        }
        let result = if Self::is_bucket_owner_enforced(bucket.bucket().ownership_controls.as_ref())
        {
            let owner = Self::bucket_owner_identity(bucket.bucket());
            GetBucketAclResult {
                owner_principal: owner.principal,
                owner_canonical_id: owner.canonical_id.clone(),
                acl_grants: AclGrants::new(vec![AclGrant::new(
                    AclGrantee::CanonicalUser(owner.canonical_id),
                    AclPermission::FullControl,
                )]),
            }
        } else {
            let bucket = bucket.bucket().clone();
            GetBucketAclResult {
                owner_principal: bucket.owner_principal,
                owner_canonical_id: bucket.owner_canonical_id,
                acl_grants: bucket.acl_grants,
            }
        };
        Ok(AuthorizedGetBucketAcl { result })
    }

    pub(super) fn authorize_put_bucket_acl(
        &self,
        req: &PutBucketAclRequest<'_>,
    ) -> Result<AuthorizedPutBucketAcl, ServerError> {
        let bucket = self.with_bucket_write_handle_for(
            &req.bucket,
            BucketHandleRequest::new()
                .requiring_policy_view()
                .requiring_bucket_tags_if_abac_enabled(),
            |bucket| {
                let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
                let policy_decision = self.bucket_policy_decision_for_loaded_handle_with_context(
                    &req.bucket.requester,
                    &bucket,
                    auth::PolicyAction::PutBucketAcl,
                    req.authorization_policy_context()?,
                    bucket_policy.as_deref(),
                )?;
                if !Self::bucket_policy_allows_with_fallback(
                    &req.bucket.requester,
                    bucket.bucket(),
                    policy_decision,
                    || Self::requester_can_write_bucket_acl(&req.bucket.requester, bucket.bucket()),
                ) {
                    return Err(ServerError::AccessDenied);
                }
                Ok(bucket)
            },
        )?;
        let owner = Self::bucket_owner_identity(bucket.bucket());
        let acl_grants = match &req.acl {
            PutBucketAclInput::Canned(acl) => {
                Self::ensure_put_bucket_acl_supported(bucket.bucket(), *acl)?;
                Self::bucket_acl_grants_from_canned(&owner, *acl)?
            }
            PutBucketAclInput::Grants(acl_grants) => {
                if Self::is_bucket_owner_enforced(bucket.bucket().ownership_controls.as_ref()) {
                    return Err(ServerError::AccessControlListNotSupported);
                }
                Self::ensure_supported_bucket_acl_grants(acl_grants)?;
                acl_grants.clone()
            }
        };
        let public_read = Self::acl_grants_public_read(&acl_grants);
        let public_write = Self::acl_grants_public_write(&acl_grants);
        if Self::blocks_public_acls(bucket.bucket().public_access_block.as_ref())
            && (Self::acl_grants_grant_public_read(&acl_grants)
                || Self::acl_grants_grant_public_write(&acl_grants))
        {
            return Err(ServerError::AccessDenied);
        }
        Ok(AuthorizedPutBucketAcl {
            bucket: req.bucket.name_typed().clone(),
            acl_grants,
            public_read,
            public_write,
        })
    }

    pub(super) fn requester_can_bypass_governance_retention(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_is_bucket_owner_account(requester, bucket)
    }

    pub(super) fn requester_can_bypass_governance_retention_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_object_action_with_unavailable_existing_tags_with_bucket_policy(
            BucketPolicyRequestContext {
                requester,
                bucket,
                bucket_tags,
                action: auth::PolicyAction::BypassGovernanceRetention,
                policy_context: PutObjectPolicyContext::default(),
                policy,
            },
            object,
            || Self::requester_can_bypass_governance_retention(requester, bucket),
        )
    }

    pub(super) fn requester_can_bypass_governance_retention_for_missing_version_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        key: &str,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<bool, ServerError> {
        self.requester_can_put_object_action_with_bucket_policy(
            BucketPolicyActionAuthorization {
                request: BucketPolicyRequestContext {
                    requester,
                    bucket,
                    bucket_tags,
                    action: auth::PolicyAction::BypassGovernanceRetention,
                    policy_context: PutObjectPolicyContext::default(),
                    policy,
                },
                default_allowed: Self::requester_can_bypass_governance_retention(requester, bucket),
            },
            key,
        )
    }

    pub(super) fn authorize_put_object_write(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.with_bucket_write_handle_for(&req.object, request, |bucket| {
            let existing_object = self
                .storage_node
                .load_existing_live_object(req.object.bucket.name_typed(), req.object.key_typed())
                .map_err(Self::map_object_pg_action_error)?;
            self.authorize_put_object_write_with_existing_object(
                req,
                &bucket,
                existing_object.as_ref(),
            )
        })
    }

    pub(super) fn authorize_put_object_write_with_existing_object(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
        bucket: &LoadedBucketHandle,
        existing_object: Option<&StoredObject>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let key = req.object.key();
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(bucket)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(bucket)?;
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
            let modern_bucket =
                BoeBucketSummary::new(&modern_bucket_info).expect("BOE branch requires BOE bucket");
            if Self::modern_put_object_authorization_with_bucket_policy(
                req.object.requester(),
                modern_bucket,
                bucket_tags.as_deref(),
                key,
                ModernWriteAction::PutObject,
                &req.policy_context,
                bucket_policy.as_deref(),
            )? != ModernObjectWriteAuthorization::Allowed
            {
                return Err(ServerError::AccessDenied);
            }
        } else if !self.requester_can_put_object_with_bucket_policy(
            BucketPolicyAccess {
                requester: req.object.requester(),
                bucket: &bucket_info,
                bucket_tags: bucket_tags.as_deref(),
                policy: bucket_policy.as_deref(),
            },
            key,
            req.policy_context,
            existing_object,
        )? {
            return Err(ServerError::AccessDenied);
        }
        let write_encryption = self.resolve_write_encryption(&bucket_info, req.encryption)?;
        if write_encryption.is_sse_customer() && bucket_info.encryption.sse_c_blocked {
            return Err(ServerError::SseCBlockedAccessDenied {
                requester_principal: Self::requester_principal_required(req.object.requester())?
                    .to_string(),
                action: "s3:PutObject".to_string(),
                resource: format!("arn:aws:s3:::{}/{}", bucket_info.name, key),
            });
        }
        Self::ensure_put_object_write_acl_supported(&bucket_info, &req.acl)?;
        Self::validate_requested_object_lock_state(&bucket_info, req.object_lock)?;
        Ok(AuthorizedPutObjectWrite {
            bucket: req.object.bucket.name_typed().clone(),
            key: req.object.key_typed().clone(),
            requester: req.object.requester().clone(),
            expected_bucket_owner: req.object.expected_bucket_owner().map(str::to_string),
            acl: AuthorizedPutObjectWriteAcl::from_parsed(&req.acl),
            requested_object_lock: req.object_lock,
            tags: req.tags.map(str::to_string),
            write_encryption,
        })
    }

    fn authorize_delete_object_impl(
        &self,
        object: &ObjectVersionRequest<'_>,
        bypass_governance: bool,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        let bucket = object.bucket_name_typed();
        let key = object.key_typed();
        let key_str = key.as_str();
        let request_version_id = object.version_id();
        let requester = object.requester();
        let bucket_handle =
            self.load_bucket_handle_for_object_policy_read(bucket, object.expected_bucket_owner())?;
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::new(&modern_bucket_info);
        #[cfg(test)]
        if should_probe_delete_object_lookup(bucket.as_str()) {
            let object_pg_ready = self
                .storage_node
                .try_probe_object_pg_available(bucket, key)
                .map_err(Self::map_object_pg_action_error)?;
            if !object_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: object pg still locked before delete_object lookup"
                        .to_string(),
                });
            }
        }

        match (bucket_info.versioning, request_version_id) {
            (BucketVersioningState::Disabled, _) => {
                match self
                    .storage_node
                    .load_object_if(bucket, key, None, |stored| {
                        let allowed = if Self::is_bucket_owner_enforced(
                            bucket_info.ownership_controls.as_ref(),
                        ) {
                            Self::modern_delete_object_authorization_with_bucket_policy(
                                requester,
                                modern_bucket.expect("BOE branch requires BOE bucket"),
                                bucket_tags.as_deref(),
                                key_str,
                                Some(stored),
                                Self::delete_object_policy_action(None),
                                bucket_policy.as_deref(),
                            )?
                        } else {
                            self.requester_can_delete_object_with_bucket_policy(
                                BucketPolicyAccess {
                                    requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key_str,
                                Some(stored),
                                Self::delete_object_policy_action(None),
                            )?
                        };
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(())
                    }) {
                    Ok(Ok(())) => Ok(AuthorizedDeleteObject::UnversionedDelete {
                        bucket: object.bucket_name_typed().clone(),
                        key: object.key_typed().clone(),
                        requester: requester.clone(),
                        bucket_info,
                        bucket_policy,
                        bucket_tags,
                    }),
                    Ok(Err(error)) => Err(error),
                    Err(storage::ObjectPgActionError::Metadata(
                        storage::MetadataError::ObjectNotFound,
                    )) => {
                        let allowed = if Self::is_bucket_owner_enforced(
                            bucket_info.ownership_controls.as_ref(),
                        ) {
                            Self::modern_delete_object_authorization_with_bucket_policy(
                                requester,
                                modern_bucket.expect("BOE branch requires BOE bucket"),
                                bucket_tags.as_deref(),
                                key_str,
                                None,
                                Self::delete_object_policy_action(None),
                                bucket_policy.as_deref(),
                            )?
                        } else {
                            self.requester_can_delete_object_with_bucket_policy(
                                BucketPolicyAccess {
                                    requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key_str,
                                None,
                                Self::delete_object_policy_action(None),
                            )?
                        };
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::UnversionedDelete {
                            bucket: object.bucket_name_typed().clone(),
                            key: object.key_typed().clone(),
                            requester: requester.clone(),
                            bucket_info,
                            bucket_policy,
                            bucket_tags,
                        })
                    }
                    Err(other) => Err(Self::map_object_pg_action_error(other)),
                }
            }
            (_, Some(version_id)) => {
                match self
                    .storage_node
                    .load_object_if(bucket, key, Some(version_id), |stored| {
                        let allowed = if Self::is_bucket_owner_enforced(
                            bucket_info.ownership_controls.as_ref(),
                        ) {
                            Self::modern_delete_object_authorization_with_bucket_policy(
                                requester,
                                modern_bucket.expect("BOE branch requires BOE bucket"),
                                bucket_tags.as_deref(),
                                key_str,
                                Some(stored),
                                Self::delete_object_policy_action(Some(version_id)),
                                bucket_policy.as_deref(),
                            )?
                        } else {
                            self.requester_can_delete_object_with_bucket_policy(
                                BucketPolicyAccess {
                                    requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key_str,
                                Some(stored),
                                Self::delete_object_policy_action(Some(version_id)),
                            )?
                        };
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }

                        if let StoredObject::Live(record) = stored {
                            let can_bypass_governance = self
                                .requester_can_bypass_governance_retention_with_bucket_policy(
                                    requester,
                                    &bucket_info,
                                    bucket_tags.as_deref(),
                                    stored,
                                    bucket_policy.as_deref(),
                                )?;
                            Self::validate_delete_against_object_lock(
                                record.object_lock,
                                bypass_governance,
                                can_bypass_governance,
                                Self::current_unix_seconds()?,
                            )?;
                        }
                        Ok(())
                    }) {
                    Ok(Ok(())) => Ok(AuthorizedDeleteObject::SpecificVersion {
                        bucket: object.bucket_name_typed().clone(),
                        key: object.key_typed().clone(),
                        version_id,
                        requester: requester.clone(),
                        bucket_info,
                        bucket_policy,
                        bucket_tags,
                        bypass_governance,
                    }),
                    Ok(Err(error)) => Err(error),
                    Err(storage::ObjectPgActionError::Metadata(
                        storage::MetadataError::ObjectNotFound,
                    )) => {
                        let allowed = if Self::is_bucket_owner_enforced(
                            bucket_info.ownership_controls.as_ref(),
                        ) {
                            Self::modern_delete_object_authorization_with_bucket_policy(
                                requester,
                                modern_bucket.expect("BOE branch requires BOE bucket"),
                                bucket_tags.as_deref(),
                                key_str,
                                None,
                                Self::delete_object_policy_action(Some(version_id)),
                                bucket_policy.as_deref(),
                            )?
                        } else {
                            self.requester_can_delete_object_with_bucket_policy(
                                BucketPolicyAccess {
                                    requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key_str,
                                None,
                                Self::delete_object_policy_action(Some(version_id)),
                            )?
                        };
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        if bucket_info.object_lock.enabled
                            && bypass_governance
                            && !self.requester_can_bypass_governance_retention_for_missing_version_with_bucket_policy(
                                requester,
                                &bucket_info,
                                bucket_tags.as_deref(),
                                key_str,
                                bucket_policy.as_deref(),
                            )?
                        {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::SpecificVersionMissing { version_id })
                    }
                    Err(other) => Err(Self::map_object_pg_action_error(other)),
                }
            }
            (_, None) => {
                let owner =
                    Self::effective_object_owner(&bucket_info, requester, PutObjectAcl::None);
                match self
                    .storage_node
                    .load_object_if(bucket, key, None, |stored| {
                        let allowed = if Self::is_bucket_owner_enforced(
                            bucket_info.ownership_controls.as_ref(),
                        ) {
                            Self::modern_delete_object_authorization_with_bucket_policy(
                                requester,
                                modern_bucket.expect("BOE branch requires BOE bucket"),
                                bucket_tags.as_deref(),
                                key_str,
                                Some(stored),
                                Self::delete_object_policy_action(None),
                                bucket_policy.as_deref(),
                            )?
                        } else {
                            self.requester_can_delete_object_with_bucket_policy(
                                BucketPolicyAccess {
                                    requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key_str,
                                Some(stored),
                                Self::delete_object_policy_action(None),
                            )?
                        };
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(())
                    }) {
                    Ok(Ok(())) => Ok(AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                        bucket: object.bucket_name_typed().clone(),
                        key: object.key_typed().clone(),
                        owner,
                        requester: requester.clone(),
                        bucket_info,
                        bucket_policy,
                        bucket_tags,
                    }),
                    Ok(Err(error)) => Err(error),
                    Err(storage::ObjectPgActionError::Metadata(
                        storage::MetadataError::ObjectNotFound,
                    )) => {
                        let allowed = if Self::is_bucket_owner_enforced(
                            bucket_info.ownership_controls.as_ref(),
                        ) {
                            Self::modern_delete_object_authorization_with_bucket_policy(
                                requester,
                                modern_bucket.expect("BOE branch requires BOE bucket"),
                                bucket_tags.as_deref(),
                                key_str,
                                None,
                                Self::delete_object_policy_action(None),
                                bucket_policy.as_deref(),
                            )?
                        } else {
                            self.requester_can_delete_object_with_bucket_policy(
                                BucketPolicyAccess {
                                    requester,
                                    bucket: &bucket_info,
                                    bucket_tags: bucket_tags.as_deref(),
                                    policy: bucket_policy.as_deref(),
                                },
                                key_str,
                                None,
                                Self::delete_object_policy_action(None),
                            )?
                        };
                        if !allowed {
                            return Err(ServerError::AccessDenied);
                        }
                        Ok(AuthorizedDeleteObject::CurrentDeleteMarkerInsert {
                            bucket: object.bucket_name_typed().clone(),
                            key: object.key_typed().clone(),
                            owner,
                            requester: requester.clone(),
                            bucket_info,
                            bucket_policy,
                            bucket_tags,
                        })
                    }
                    Err(other) => Err(Self::map_object_pg_action_error(other)),
                }
            }
        }
    }

    pub(super) fn authorize_delete_object(
        &self,
        req: &DeleteObjectRequest<'_>,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        self.authorize_delete_object_impl(&req.object, req.bypass_governance)
    }

    pub(super) fn authorize_delete_objects_entry(
        &self,
        req: &DeleteObjectsRequest<'_>,
        entry: &DeleteEntry,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        let object = ObjectVersionRequest::from_object(
            ObjectRequest::new(
                req.bucket.name_typed().clone(),
                entry.key.clone(),
                req.bucket.requester.clone(),
                req.expected_bucket_owner(),
            ),
            entry.version_id,
        );
        self.authorize_delete_object_impl(&object, req.bypass_governance)
    }

    pub(super) fn authorize_copy_object(
        &self,
        req: &CopyObjectRequest<'_>,
    ) -> Result<AuthorizedCopyObject, ServerError> {
        let src_version_id = req.source.version_id;
        let requester = &req.destination.bucket.requester;
        let acl = req.acl.clone();
        let acl_policy_context = authorization_policy_context_for_put_object_write_acl(
            "CopyObject",
            &acl,
            req.policy_context,
        )?;
        let copy_source_policy_value = req.source.version_id.map_or_else(
            || format!("{}/{}", req.source.bucket, req.source.key),
            |version_id| {
                format!(
                    "{}/{}?versionId={}",
                    req.source.bucket, req.source.key, version_id
                )
            },
        );
        let metadata_directive = req.directive.policy_condition_value();
        let request_object_tags_xml = match &req.tagging {
            TaggingDirective::Copy => None,
            TaggingDirective::Replace(tags) => *tags,
        };
        let copy_policy_context = PutObjectPolicyContext::new(
            Some(copy_source_policy_value.as_str()),
            metadata_directive,
            acl_policy_context.canned_acl,
        )
        .with_acl_grant_headers(
            acl_policy_context.grant_read,
            acl_policy_context.grant_write,
            acl_policy_context.grant_read_acp,
            acl_policy_context.grant_write_acp,
            acl_policy_context.grant_full_control,
        )
        .with_request_object_tags_xml(request_object_tags_xml);
        let dst_policy_context = req
            .destination_encryption
            .with_policy_context(copy_policy_context);
        let destination = self.authorize_put_object_write(&AuthorizePutObjectRequest {
            object: ObjectRequest::new(
                req.destination.bucket.name_typed().clone(),
                req.destination.key_typed().clone(),
                requester.clone(),
                req.expected_bucket_owner(),
            ),
            acl,
            policy_context: dst_policy_context,
            object_lock: req.object_lock,
            tags: request_object_tags_xml,
            encryption: req.destination_encryption,
        })?;
        let source = self.authorize_copy_source_read_snapshot(CopySourceReadSnapshotRequest {
            requester,
            bucket: &req.source.bucket,
            key: &req.source.key,
            version_id: src_version_id,
            expected_bucket_owner: req.source.expected_bucket_owner(),
            policy_action: Self::get_object_policy_action(src_version_id),
            existing_object_tags_mode: ExistingObjectTagsMode::Unavailable,
        })?;

        Ok(AuthorizedCopyObject {
            source,
            destination,
        })
    }

    pub(super) fn authorize_create_multipart_upload_with_existing_object(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
        bucket: &LoadedBucketHandle,
        existing_object: Option<&StoredObject>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        let key = req.object.key();
        let policy_context = req.effective_policy_context()?;
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        if req.object.requester().is_anonymous() {
            return Err(ServerError::AccessDenied);
        }
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(bucket)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(bucket)?;
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
            let modern_bucket =
                BoeBucketSummary::new(&modern_bucket_info).expect("BOE branch requires BOE bucket");
            if Self::modern_put_object_authorization_with_bucket_policy(
                req.object.requester(),
                modern_bucket,
                bucket_tags.as_deref(),
                key,
                ModernWriteAction::CreateMultipartUpload,
                &policy_context,
                bucket_policy.as_deref(),
            )? != ModernObjectWriteAuthorization::Allowed
            {
                return Err(ServerError::AccessDenied);
            }
        } else if !self.requester_can_put_object_with_bucket_policy(
            BucketPolicyAccess {
                requester: req.object.requester(),
                bucket: &bucket_info,
                bucket_tags: bucket_tags.as_deref(),
                policy: bucket_policy.as_deref(),
            },
            key,
            policy_context,
            existing_object,
        )? {
            return Err(ServerError::AccessDenied);
        }
        Self::ensure_sse_c_allowed(
            &bucket_info,
            req.encryption.sse_customer_request().is_some(),
        )?;
        let write_encryption = self.resolve_write_encryption(&bucket_info, req.encryption)?;
        Self::ensure_put_object_write_acl_supported(&bucket_info, &req.acl)?;
        let initiator = Self::requester_owner_identity(req.object.requester());
        let owner =
            Self::effective_put_object_owner(&bucket_info, req.object.requester(), &req.acl);
        let acl_grants = Self::object_acl_grants_for_put_object(&bucket_info, &owner, &req.acl);
        let public_read = Self::acl_grants_public_read(&acl_grants);
        Self::validate_requested_object_lock_state(&bucket_info, req.object_lock)?;
        Self::ensure_sse_c_allowed(&bucket_info, write_encryption.is_sse_customer())?;

        Ok(AuthorizedCreateMultipartUpload {
            bucket_info: bucket_info.into_inner(),
            bucket: req.object.bucket.name_typed().clone(),
            key: req.object.key_typed().clone(),
            tags: req.tags.map(str::to_string),
            checksum: req.checksum,
            initiator,
            owner,
            acl_grants,
            public_read,
            object_lock: req.object_lock,
            write_encryption,
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_create_multipart_upload(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.with_bucket_write_handle_for(&req.object, request, |bucket| {
            let existing_object = self
                .storage_node
                .load_existing_live_object(req.object.bucket.name_typed(), req.object.key_typed())
                .map_err(Self::map_object_pg_action_error)?;
            self.authorize_create_multipart_upload_with_existing_object(
                req,
                &bucket,
                existing_object.as_ref(),
            )
        })
    }

    pub(super) fn authorize_upload_part_copy(
        &self,
        req: &UploadPartCopyRequest<'_>,
    ) -> Result<AuthorizedUploadPartCopy, ServerError> {
        let src_version_id = req.source.version_id;
        let dst_bucket = req.upload.bucket_name_typed();
        let dst_key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let part_number = req.part_number;
        let requester = req.upload.requester();
        let copy_source_policy_value = req.source.version_id.map_or_else(
            || format!("{}/{}", req.source.bucket, req.source.key),
            |version_id| {
                format!(
                    "{}/{}?versionId={}",
                    req.source.bucket, req.source.key, version_id
                )
            },
        );
        let policy_context =
            PutObjectPolicyContext::new(Some(copy_source_policy_value.as_str()), None, None)
                .with_sse_customer_algorithm(req.sse_customer.map(SseCustomerRequest::algorithm));

        let dst_bucket_handle = self
            .load_bucket_handle_for_object_policy_read(dst_bucket, req.expected_bucket_owner())?;
        let dst_bucket_info = ValidatedBucket(dst_bucket_handle.bucket().clone());
        let dst_bucket_policy = self.cached_bucket_policy_for_loaded_handle(&dst_bucket_handle)?;
        let dst_bucket_tags = Self::loaded_bucket_tags_for_policy(&dst_bucket_handle)?;
        let dst_upload = self
            .storage_node
            .load_in_progress_multipart_upload(dst_bucket, dst_key, upload_id)
            .map_err(Self::map_object_pg_action_error)?;
        let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
            policy_context,
            &dst_upload,
        );
        let modern_bucket_info = ModernBucketSummary::from(&*dst_bucket_info);
        if Self::is_bucket_owner_enforced(dst_bucket_info.ownership_controls.as_ref()) {
            let modern_bucket =
                BoeBucketSummary::new(&modern_bucket_info).expect("BOE branch requires BOE bucket");
            if Self::modern_write_multipart_upload_with_bucket_policy(
                requester,
                modern_bucket,
                dst_bucket_tags.as_deref(),
                &dst_upload,
                &policy_context,
                dst_bucket_policy.as_deref(),
            )? != ModernObjectWriteAuthorization::Allowed
            {
                return Err(ServerError::AccessDenied);
            }
        } else if !self.requester_can_write_multipart_upload_with_bucket_policy(
            requester,
            &dst_bucket_info,
            dst_bucket_tags.as_deref(),
            &dst_upload,
            policy_context,
            dst_bucket_policy.as_deref(),
        )? {
            return Err(ServerError::AccessDenied);
        }
        Self::ensure_sse_c_allowed(
            &dst_bucket_info,
            dst_upload.encryption.uses_sse_customer_headers(),
        )?;
        self.ensure_write_encryption_supported(&dst_upload.encryption)?;
        let sse_customer = self.prepare_existing_sse_customer_write_context(
            &dst_upload.encryption,
            req.sse_customer,
            SseCustomerSegmentScope::multipart_part(part_number)?,
            true,
        )?;

        let source = self.authorize_copy_source_read_snapshot(CopySourceReadSnapshotRequest {
            requester,
            bucket: &req.source.bucket,
            key: &req.source.key,
            version_id: src_version_id,
            expected_bucket_owner: req.source.expected_bucket_owner(),
            policy_action: Self::get_object_policy_action(src_version_id),
            existing_object_tags_mode: ExistingObjectTagsMode::Available,
        })?;
        Ok(AuthorizedUploadPartCopy {
            source,
            destination: AuthorizedMultipartPartWrite {
                bucket: req.upload.bucket_name_typed().clone(),
                key: req.upload.key_typed().clone(),
                upload_id: dst_upload.upload_id.clone(),
                part_number,
                upload: dst_upload,
                sse_customer,
            },
        })
    }

    pub(super) fn authorize_begin_stream_part_with_upload(
        &self,
        req: &BeginStreamPartRequest<'_>,
        bucket_handle: &LoadedBucketHandle,
        upload: &MultipartUploadRecord,
    ) -> Result<AuthorizedBeginStreamPart, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let part_number = req.part_number;
        let policy_context = req.effective_policy_context();
        let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(bucket_handle)?;
        let bucket_tags = Self::loaded_bucket_tags_for_policy(bucket_handle)?;
        if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        if upload.state != UploadState::InProgress {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        let policy_context =
            Self::with_multipart_upload_managed_encryption_policy_context(policy_context, upload);
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
            let modern_bucket =
                BoeBucketSummary::new(&modern_bucket_info).expect("BOE branch requires BOE bucket");
            if Self::modern_write_multipart_upload_with_bucket_policy(
                req.upload.requester(),
                modern_bucket,
                bucket_tags.as_deref(),
                upload,
                &policy_context,
                bucket_policy.as_deref(),
            )? != ModernObjectWriteAuthorization::Allowed
            {
                return Err(ServerError::AccessDenied);
            }
        } else if !self.requester_can_write_multipart_upload_with_bucket_policy(
            req.upload.requester(),
            &bucket_info,
            bucket_tags.as_deref(),
            upload,
            policy_context,
            bucket_policy.as_deref(),
        )? {
            return Err(ServerError::AccessDenied);
        }
        Self::ensure_sse_c_allowed(&bucket_info, upload.encryption.uses_sse_customer_headers())?;
        self.ensure_write_encryption_supported(&upload.encryption)?;
        let sse_customer = self.prepare_existing_sse_customer_write_context(
            &upload.encryption,
            req.sse_customer,
            SseCustomerSegmentScope::multipart_part(part_number)?,
            true,
        )?;

        Ok(AuthorizedBeginStreamPart {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload.upload_id.clone(),
            part_number,
            upload: upload.clone(),
            sse_customer,
        })
    }

    #[cfg(test)]
    pub(super) fn authorize_begin_stream_part(
        &self,
        req: &BeginStreamPartRequest<'_>,
    ) -> Result<AuthorizedBeginStreamPart, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.with_bucket_write_handle_for(&req.upload, request, |bucket_handle| {
            let upload = self
                .storage_node
                .load_multipart_upload(bucket, key, upload_id)
                .map_err(BucketHandleLoader::map_bucket_snapshot_error)?;
            self.authorize_begin_stream_part_with_upload(req, &bucket_handle, &upload)
        })
    }

    pub(super) fn authorize_complete_multipart_upload(
        &self,
        req: &CompleteMultipartUploadRequest<'_>,
    ) -> Result<AuthorizedCompleteMultipartUpload, ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.with_bucket_write_handle_for(&req.upload, request, |bucket_handle| {
            let bucket_info = ValidatedBucket(bucket_handle.bucket().clone());
            let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket_handle)?;
            let bucket_tags = Self::loaded_bucket_tags_for_policy(&bucket_handle)?;
            let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
            let modern_bucket = BoeBucketSummary::new(&modern_bucket_info);
            #[cfg(test)]
            let upload =
                if should_probe_multipart_complete_auth_lookup(bucket.as_str(), key.as_str()) {
                    self.storage_node
                        .try_load_in_progress_multipart_upload(bucket, key, upload_id)
                        .map_err(Self::map_object_pg_action_error)
                        .and_then(|upload| {
                            upload.ok_or_else(|| ServerError::InternalError {
                                reason: "multipart complete auth lookup would block".to_string(),
                            })
                        })?
                } else {
                    self.storage_node
                        .load_in_progress_multipart_upload(bucket, key, upload_id)
                        .map_err(Self::map_object_pg_action_error)?
                };
            #[cfg(not(test))]
            let upload = self
                .storage_node
                .load_in_progress_multipart_upload(bucket, key, upload_id)
                .map_err(Self::map_object_pg_action_error)?;
            let policy_context = Self::with_multipart_upload_managed_encryption_policy_context(
                PutObjectPolicyContext::default().with_sse_customer_algorithm(
                    req.sse_customer.map(SseCustomerRequest::algorithm),
                ),
                &upload,
            );
            if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_ref()) {
                if Self::modern_write_multipart_upload_with_bucket_policy(
                    req.upload.requester(),
                    modern_bucket.expect("BOE branch requires BOE bucket"),
                    bucket_tags.as_deref(),
                    &upload,
                    &policy_context,
                    bucket_policy.as_deref(),
                )? != ModernObjectWriteAuthorization::Allowed
                {
                    return Err(ServerError::AccessDenied);
                }
            } else if !self.requester_can_write_multipart_upload_with_bucket_policy(
                req.upload.requester(),
                &bucket_info,
                bucket_tags.as_deref(),
                &upload,
                policy_context,
                bucket_policy.as_deref(),
            )? {
                return Err(ServerError::AccessDenied);
            }
            Self::ensure_sse_c_allowed(
                &bucket_info,
                upload.encryption.uses_sse_customer_headers(),
            )?;
            let multipart_write_encryption = self.resume_write_encryption(
                &upload.encryption,
                req.sse_customer,
                SseCustomerSegmentScope::object(),
                false,
            )?;

            Ok(AuthorizedCompleteMultipartUpload {
                bucket_info: bucket_info.into_inner(),
                bucket: req.upload.bucket_name_typed().clone(),
                key: req.upload.key_typed().clone(),
                upload_id: upload.upload_id.clone(),
                upload,
                multipart_write_encryption,
            })
        })
    }

    pub(super) fn authorize_abort_multipart_upload(
        &self,
        req: &MultipartObjectRequest<'_>,
    ) -> Result<AuthorizedAbortMultipartUpload, ServerError> {
        let bucket = req.object.bucket_name_typed();
        let key = req.object.key_typed();
        let upload_id = req.upload_id_typed();
        let bucket_info =
            self.checked_active_bucket_summary_for(bucket, req.expected_bucket_owner())?;
        let authorized = match self
            .storage_node
            .lookup_abort_multipart_upload(bucket, key, upload_id)
            .map_err(Self::map_object_pg_action_error)?
        {
            storage::AbortMultipartUploadLookup::InProgress(upload) => {
                let upload = *upload;
                if upload.bucket != bucket.as_str() || upload.key != key.as_str() {
                    return Err(ServerError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                if !Self::requester_can_manage_multipart_upload(
                    req.object.requester(),
                    &bucket_info,
                    &upload,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                AuthorizedAbortMultipartUpload::InProgress {
                    bucket: req.bucket_name_typed().clone(),
                    key: req.key_typed().clone(),
                    upload_id: upload_id.clone(),
                }
            }
            storage::AbortMultipartUploadLookup::Completed(completed) => {
                if completed.bucket != bucket.as_str() || completed.key != key.as_str() {
                    return Err(ServerError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                if !Self::requester_can_manage_completed_multipart_upload(
                    req.object.requester(),
                    &bucket_info,
                    &completed,
                ) {
                    return Err(ServerError::AccessDenied);
                }
                AuthorizedAbortMultipartUpload::Completed
            }
        };
        Ok(authorized)
    }

    #[cfg(test)]
    pub(super) fn authorize_list_parts(
        &self,
        req: &ListPartsRequest<'_>,
    ) -> Result<(), ServerError> {
        let bucket = req.upload.bucket_name_typed();
        let key = req.upload.key_typed();
        let upload_id = req.upload.upload_id_typed();
        let bucket_info =
            self.checked_active_bucket_summary_for(bucket, req.expected_bucket_owner())?;
        let upload = self
            .storage_node
            .load_in_progress_multipart_upload_for_listing(bucket, key, upload_id)
            .map_err(Self::map_object_pg_action_error)?;
        if !Self::requester_can_manage_multipart_upload(
            req.upload.requester(),
            &bucket_info,
            &upload,
        ) {
            return Err(ServerError::AccessDenied);
        }

        Ok(())
    }

    #[cfg(test)]
    fn load_locked_object_state_from_loaded_bucket(
        &self,
        requester: &Requester,
        object: &LoadedObjectHandle<'_>,
        policy_requirement: ObjectBucketPolicyRequirement,
        missing_discovery: MissingObjectDiscovery,
    ) -> Result<LoadedObjectState, ServerError> {
        let bucket = object.bucket();
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let bucket_policy = match policy_requirement {
            ObjectBucketPolicyRequirement::Required => {
                self.cached_bucket_policy_for_loaded_handle(bucket)?
            }
        };
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(bucket)?
        } else {
            None
        };
        let can_discover_missing = missing_discovery.requester_can_discover_missing(
            self,
            BucketPolicyAccess {
                requester,
                bucket: &bucket_info,
                bucket_tags: bucket_tags.as_deref(),
                policy: bucket_policy.as_deref(),
            },
            object.key().as_str(),
            object.version_id(),
        )?;
        let record = match self.lookup_object_record(
            &bucket.bucket().name,
            object.key(),
            object.version_id(),
        ) {
            Ok(record) => record,
            Err(ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. })
                if !can_discover_missing =>
            {
                return Err(ServerError::AccessDenied);
            }
            Err(other) => return Err(other),
        };

        Ok(LoadedObjectState {
            bucket_info,
            bucket_policy,
            bucket_tags,
            record,
        })
    }

    fn authorize_object_read_snapshot(
        &self,
        req: AuthorizedObjectReadSnapshotRequest<'_>,
    ) -> Result<(BucketSummary, storage::ObjectReadSnapshot), ServerError> {
        let bucket =
            self.load_bucket_handle_for_modern_object_read(req.bucket, req.expected_bucket_owner)?;
        let bucket_summary = bucket.bucket().clone();
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let modern_bucket_info = ModernBucketSummary::from(&*bucket_info);
        let modern_bucket = BoeBucketSummary::new(&modern_bucket_info);
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(&bucket)?
        } else {
            None
        };
        let can_discover_missing = req.missing_discovery.requester_can_discover_missing(
            self,
            BucketPolicyAccess {
                requester: req.requester,
                bucket: &bucket_info,
                bucket_tags: bucket_tags.as_deref(),
                policy: bucket_policy.as_deref(),
            },
            req.key.as_str(),
            req.version_id,
        )?;
        #[cfg(test)]
        if should_probe_object_read_snapshot(bucket.bucket().name.as_str()) {
            let object_pg_ready = self
                .storage_node
                .try_probe_object_pg_available(&bucket.bucket().name, req.key)
                .map_err(|error| {
                    Self::map_object_read_snapshot_error(
                        &bucket.bucket().name,
                        req.key,
                        req.version_id,
                        can_discover_missing,
                        error,
                    )
                })?;
            if !object_pg_ready {
                return Err(ServerError::InternalError {
                    reason: "test probe: object pg still locked before object read snapshot"
                        .to_string(),
                });
            }
        }
        let outcome = self
            .storage_node
            .load_object_read_snapshot_if(
                &bucket.bucket().name,
                req.key,
                req.version_id,
                req.snapshot_mode,
                |stored| {
                    let allowed = match req.modern_action {
                        ModernReadAction::ReadCurrent | ModernReadAction::ReadVersion => {
                            if let Some(modern_bucket) = modern_bucket {
                                match Self::modern_read_object_authorization_with_bucket_policy(
                                    req.requester,
                                    modern_bucket,
                                    bucket_tags.as_deref(),
                                    stored,
                                    req.modern_action,
                                    bucket_policy.as_deref(),
                                )? {
                                    ModernObjectReadAuthorization::Allowed => true,
                                    ModernObjectReadAuthorization::Denied => false,
                                }
                            } else {
                                self.requester_can_read_object_with_bucket_policy(
                                    req.requester,
                                    &bucket_info,
                                    bucket_tags.as_deref(),
                                    stored,
                                    req.modern_action.policy_action(),
                                    bucket_policy.as_deref(),
                                )?
                            }
                        }
                        ModernReadAction::AttributesCurrent
                        | ModernReadAction::AttributesVersion => {
                            if let Some(modern_bucket) = modern_bucket {
                                match Self::modern_read_object_authorization_with_bucket_policy(
                                    req.requester,
                                    modern_bucket,
                                    bucket_tags.as_deref(),
                                    stored,
                                    req.modern_action,
                                    bucket_policy.as_deref(),
                                )? {
                                    ModernObjectReadAuthorization::Allowed => true,
                                    ModernObjectReadAuthorization::Denied => false,
                                }
                            } else {
                                let read_action = match req.modern_action {
                                    ModernReadAction::AttributesCurrent => {
                                        auth::PolicyAction::GetObject
                                    }
                                    ModernReadAction::AttributesVersion => {
                                        auth::PolicyAction::GetObjectVersion
                                    }
                                    _ => unreachable!(),
                                };
                                self.requester_can_read_object_with_bucket_policy(
                                    req.requester,
                                    &bucket_info,
                                    bucket_tags.as_deref(),
                                    stored,
                                    read_action,
                                    bucket_policy.as_deref(),
                                )? && self.requester_can_read_object_without_existing_tags_with_bucket_policy(
                                    req.requester,
                                    &bucket_info,
                                    bucket_tags.as_deref(),
                                    stored,
                                    req.modern_action.policy_action(),
                                    bucket_policy.as_deref(),
                                )?
                            }
                        }
                    };
                    if allowed {
                        Ok(())
                    } else {
                        Err(ServerError::AccessDenied)
                    }
                },
            )
            .map_err(|error| {
                Self::map_object_read_snapshot_error(
                    &bucket.bucket().name,
                    req.key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok((bucket_summary, outcome.snapshot))
    }

    fn authorize_copy_source_read_snapshot(
        &self,
        req: CopySourceReadSnapshotRequest<'_>,
    ) -> Result<storage::ObjectReadSnapshot, ServerError> {
        let bucket =
            self.load_bucket_handle_for_object_policy_read(req.bucket, req.expected_bucket_owner)?;
        let bucket_info = ValidatedBucket(bucket.bucket().clone());
        let bucket_policy = self.cached_bucket_policy_for_loaded_handle(&bucket)?;
        let bucket_tags = if bucket_policy.is_some() {
            Self::loaded_bucket_tags_for_policy(&bucket)?
        } else {
            None
        };
        let can_discover_missing = MissingObjectDiscovery::ReadBucket
            .requester_can_discover_missing(
                self,
                BucketPolicyAccess {
                    requester: req.requester,
                    bucket: &bucket_info,
                    bucket_tags: bucket_tags.as_deref(),
                    policy: bucket_policy.as_deref(),
                },
                req.key.as_str(),
                req.version_id,
            )?;
        let outcome = self
            .storage_node
            .load_object_read_snapshot_if(
                &bucket.bucket().name,
                req.key,
                req.version_id,
                ObjectReadSnapshotMode::FullPayloadLayout,
                |stored| {
                    let allowed = if matches!(
                        req.existing_object_tags_mode,
                        ExistingObjectTagsMode::Available
                    ) {
                        self.requester_can_read_object_with_bucket_policy(
                            req.requester,
                            &bucket_info,
                            bucket_tags.as_deref(),
                            stored,
                            req.policy_action,
                            bucket_policy.as_deref(),
                        )?
                    } else {
                        self.requester_can_read_object_without_existing_tags_with_bucket_policy(
                            req.requester,
                            &bucket_info,
                            bucket_tags.as_deref(),
                            stored,
                            req.policy_action,
                            bucket_policy.as_deref(),
                        )?
                    };
                    if allowed {
                        Ok(())
                    } else {
                        Err(ServerError::AccessDenied)
                    }
                },
            )
            .map_err(|error| {
                Self::map_object_read_snapshot_error(
                    &bucket.bucket().name,
                    req.key,
                    req.version_id,
                    can_discover_missing,
                    error,
                )
            })??;
        Ok(outcome.snapshot)
    }

    pub(super) fn map_object_read_snapshot_error(
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        can_discover_missing: bool,
        error: storage::ObjectPgActionError,
    ) -> ServerError {
        match error {
            storage::ObjectPgActionError::Metadata(storage::MetadataError::ObjectNotFound) => {
                if !can_discover_missing {
                    ServerError::AccessDenied
                } else if let Some(version_id) = version_id {
                    ServerError::VersionNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        version_id: version_id.to_string(),
                    }
                } else {
                    ServerError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    }
                }
            }
            storage::ObjectPgActionError::Store(error) => ServerError::Store(error),
            storage::ObjectPgActionError::Metadata(error) => ServerError::Metadata(error),
            storage::ObjectPgActionError::InvalidRequest { reason } => {
                ServerError::InvalidRequest { reason }
            }
        }
    }

    #[cfg(test)]
    fn ensure_loaded_object_tagging_allowed(
        &self,
        requester: &Requester,
        loaded: &LoadedObjectState,
        policy_action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
    ) -> Result<(), ServerError> {
        if self.requester_can_manage_object_tags_with_bucket_policy(
            BucketPolicyAccess {
                requester,
                bucket: &loaded.bucket_info,
                bucket_tags: loaded.bucket_tags.as_deref(),
                policy: loaded.bucket_policy.as_deref(),
            },
            &loaded.record,
            policy_action,
            request_object_tags_xml,
        )? {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn authorize_object_tagging_access(
        &self,
        object: &ObjectVersionRequest<'_>,
        policy_action: auth::PolicyAction,
        request_object_tags_xml: Option<&str>,
    ) -> Result<StoredObject, ServerError> {
        let bucket = self.load_bucket_handle_for_object_policy_read(
            object.object.bucket_name_typed(),
            object.expected_bucket_owner(),
        )?;
        let loaded_object = match object.version_id {
            Some(version_id) => {
                bucket.load_object_version(object.object.key_typed().clone(), version_id)
            }
            None => bucket.load_object(object.object.key_typed().clone()),
        };
        let loaded = self.load_locked_object_state_from_loaded_bucket(
            object.object.requester(),
            &loaded_object,
            ObjectBucketPolicyRequirement::Required,
            MissingObjectDiscovery::BucketAdmin,
        )?;
        self.ensure_loaded_object_tagging_allowed(
            object.object.requester(),
            &loaded,
            policy_action,
            request_object_tags_xml,
        )?;
        Ok(loaded.record)
    }

    #[cfg(test)]
    fn authorize_object_tagging_read(
        &self,
        object: &ObjectVersionRequest<'_>,
        policy_action: auth::PolicyAction,
    ) -> Result<StoredObject, ServerError> {
        let bucket = self.load_bucket_handle_for_object_policy_read(
            object.object.bucket_name_typed(),
            object.expected_bucket_owner(),
        )?;
        let loaded_object = match object.version_id {
            Some(version_id) => {
                bucket.load_object_version(object.object.key_typed().clone(), version_id)
            }
            None => bucket.load_object(object.object.key_typed().clone()),
        };
        let loaded = self.load_locked_object_state_from_loaded_bucket(
            object.object.requester(),
            &loaded_object,
            ObjectBucketPolicyRequirement::Required,
            MissingObjectDiscovery::BucketAdmin,
        )?;
        self.ensure_loaded_object_tagging_allowed(
            object.object.requester(),
            &loaded,
            policy_action,
            None,
        )?;
        Ok(loaded.record)
    }

    #[cfg(test)]
    fn ensure_loaded_object_acl_allowed(
        &self,
        requester: &Requester,
        loaded: &LoadedObjectState,
        authorization: ObjectAclAuthorization<'_>,
    ) -> Result<(), ServerError> {
        let allowed = match authorization {
            ObjectAclAuthorization::WriteWithPolicy {
                action: policy_action,
                policy_context,
            } => self.requester_can_write_object_acl_with_bucket_policy(
                BucketPolicyAccess {
                    requester,
                    bucket: &loaded.bucket_info,
                    bucket_tags: loaded.bucket_tags.as_deref(),
                    policy: loaded.bucket_policy.as_deref(),
                },
                &loaded.record,
                policy_action,
                policy_context,
            )?,
            ObjectAclAuthorization::ReadWithPolicy(policy_action) => self
                .requester_can_read_object_acl_with_bucket_policy(
                    requester,
                    &loaded.bucket_info,
                    loaded.bucket_tags.as_deref(),
                    &loaded.record,
                    policy_action,
                    loaded.bucket_policy.as_deref(),
                )?,
        };
        if allowed {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn authorize_object_acl_access(
        &self,
        requester: &Requester,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        authorization: ObjectAclAuthorization<'_>,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedObjectState, ServerError> {
        let bucket =
            self.load_bucket_handle_for_object_policy_read(bucket, expected_bucket_owner)?;
        let loaded_object = match version_id {
            Some(version_id) => bucket.load_object_version(key.clone(), version_id),
            None => bucket.load_object(key.clone()),
        };
        let loaded = self.load_locked_object_state_from_loaded_bucket(
            requester,
            &loaded_object,
            ObjectBucketPolicyRequirement::Required,
            MissingObjectDiscovery::ObjectAcl,
        )?;
        self.ensure_loaded_object_acl_allowed(requester, &loaded, authorization)?;
        Ok(loaded)
    }

    #[cfg(test)]
    fn authorize_object_acl_read(
        &self,
        req: &ObjectVersionRequest<'_>,
        policy_action: auth::PolicyAction,
    ) -> Result<LoadedObjectState, ServerError> {
        let bucket = self.load_bucket_handle_for_object_policy_read(
            req.object.bucket_name_typed(),
            req.expected_bucket_owner(),
        )?;
        let loaded_object = match req.version_id {
            Some(version_id) => {
                bucket.load_object_version(req.object.key_typed().clone(), version_id)
            }
            None => bucket.load_object(req.object.key_typed().clone()),
        };
        let loaded = self.load_locked_object_state_from_loaded_bucket(
            req.object.requester(),
            &loaded_object,
            ObjectBucketPolicyRequirement::Required,
            MissingObjectDiscovery::ObjectAcl,
        )?;
        self.ensure_loaded_object_acl_allowed(
            req.object.requester(),
            &loaded,
            ObjectAclAuthorization::ReadWithPolicy(policy_action),
        )?;
        Ok(loaded)
    }

    #[cfg(test)]
    fn ensure_loaded_object_lock_allowed(
        &self,
        requester: &Requester,
        loaded: &LoadedObjectState,
        policy_action: auth::PolicyAction,
    ) -> Result<(), ServerError> {
        if self.requester_can_manage_object_lock_with_bucket_policy(
            requester,
            &loaded.bucket_info,
            loaded.bucket_tags.as_deref(),
            &loaded.record,
            policy_action,
            loaded.bucket_policy.as_deref(),
        )? {
            Ok(())
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    #[cfg(test)]
    fn authorize_object_lock_access(
        &self,
        requester: &Requester,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: Option<VersionId>,
        policy_action: auth::PolicyAction,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedObjectState, ServerError> {
        let bucket =
            self.load_bucket_handle_for_object_policy_read(bucket, expected_bucket_owner)?;
        let loaded_object = match version_id {
            Some(version_id) => bucket.load_object_version(key.clone(), version_id),
            None => bucket.load_object(key.clone()),
        };
        let loaded = self.load_locked_object_state_from_loaded_bucket(
            requester,
            &loaded_object,
            ObjectBucketPolicyRequirement::Required,
            MissingObjectDiscovery::BucketAdmin,
        )?;
        self.ensure_loaded_object_lock_allowed(requester, &loaded, policy_action)?;
        Self::ensure_object_lock_bucket(&loaded.bucket_info)?;
        Ok(loaded)
    }

    #[cfg(test)]
    fn authorize_object_lock_read(
        &self,
        req: &ObjectVersionRequest<'_>,
        policy_action: auth::PolicyAction,
    ) -> Result<LoadedObjectState, ServerError> {
        let bucket = self.load_bucket_handle_for_object_policy_read(
            req.object.bucket_name_typed(),
            req.expected_bucket_owner(),
        )?;
        let loaded_object = match req.version_id {
            Some(version_id) => {
                bucket.load_object_version(req.object.key_typed().clone(), version_id)
            }
            None => bucket.load_object(req.object.key_typed().clone()),
        };
        let loaded = self.load_locked_object_state_from_loaded_bucket(
            req.object.requester(),
            &loaded_object,
            ObjectBucketPolicyRequirement::Required,
            MissingObjectDiscovery::BucketAdmin,
        )?;
        self.ensure_loaded_object_lock_allowed(req.object.requester(), &loaded, policy_action)?;
        Self::ensure_object_lock_bucket(&loaded.bucket_info)?;
        Ok(loaded)
    }

    pub(super) fn authorize_get_object(
        &self,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) =
            self.authorize_object_read_snapshot(AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadBucket,
                modern_action: ModernReadAction::from_get_object_version(req.object.version_id),
                snapshot_mode: ObjectReadSnapshotMode::FullPayloadLayout,
            })?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }

    pub(super) fn authorize_head_object(
        &self,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) =
            self.authorize_object_read_snapshot(AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadBucket,
                modern_action: ModernReadAction::from_get_object_version(req.object.version_id),
                snapshot_mode: ObjectReadSnapshotMode::MetadataOnly,
            })?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }

    pub(super) fn authorize_get_object_attributes(
        &self,
        req: &GetObjectAttributesRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) =
            self.authorize_object_read_snapshot(AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadObjectAttributes,
                modern_action: ModernReadAction::from_get_object_attributes_version(
                    req.object.version_id,
                ),
                snapshot_mode: if req.want_parts {
                    ObjectReadSnapshotMode::MultipartParts
                } else {
                    ObjectReadSnapshotMode::MetadataOnly
                },
            })?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }

    pub(super) fn authorize_head_object_for_part(
        &self,
        req: &GetObjectRequest<'_>,
    ) -> Result<AuthorizedObjectRead, ServerError> {
        let (bucket, snapshot) =
            self.authorize_object_read_snapshot(AuthorizedObjectReadSnapshotRequest {
                requester: req.object.requester(),
                bucket: req.object.bucket_name_typed(),
                key: req.object.key_typed(),
                version_id: req.object.version_id,
                expected_bucket_owner: req.expected_bucket_owner(),
                missing_discovery: MissingObjectDiscovery::ReadBucket,
                modern_action: ModernReadAction::from_get_object_version(req.object.version_id),
                snapshot_mode: ObjectReadSnapshotMode::MultipartParts,
            })?;
        Ok(AuthorizedObjectRead { bucket, snapshot })
    }
}

#[cfg(test)]
mod modern_auth_tests {
    use super::{Coordinator, ModernObjectReadAuthorization, ModernReadAction};
    use crate::coordinator::authz::BoeBucketSummary;
    use crate::coordinator::request_types::Requester;
    use crate::coordinator::response_types::ModernBucketSummary;
    use s3_types::{AccountIdentity, BucketVersioningState, CanonicalUserId, VersionId};
    use storage::{
        AclGrants, BucketName, BucketObjectLockConfig, BucketObjectOwnership,
        BucketOwnershipControls, EcShape, EffectiveBucketEncryptionConfig, GenerationId,
        ObjectEncryption, ObjectEtag, ObjectKey, ObjectLayout, ObjectLockState, OwnerIdentity,
        PublicAccessBlockConfig, StorageClass, StoredObject,
    };

    fn modern_bucket(ownership: Option<BucketObjectOwnership>) -> ModernBucketSummary {
        ModernBucketSummary {
            name: BucketName::try_from("test-bucket").unwrap(),
            owner_principal: "arn:aws:iam::111122223333:user/bucket-owner".to_string(),
            owner_canonical_id: CanonicalUserId::from_principal(
                "arn:aws:iam::111122223333:user/bucket-owner",
            ),
            created_at: 0,
            versioning: BucketVersioningState::Suspended,
            object_lock: BucketObjectLockConfig::default(),
            public_access_block: None,
            ownership_controls: ownership
                .map(|object_ownership| BucketOwnershipControls { object_ownership }),
            bucket_policy_present: false,
            bucket_policy_public: false,
            bucket_policy_generation: 0,
            bucket_lifecycle_present: false,
            bucket_lifecycle_generation: 0,
            bucket_abac_enabled: false,
            encryption: EffectiveBucketEncryptionConfig::default(),
        }
    }

    fn stored_live_object(owner_principal: &str) -> StoredObject {
        StoredObject::Live(storage::LiveObjectRecord {
            bucket: BucketName::try_from("test-bucket").unwrap(),
            key: ObjectKey::try_from("key").unwrap(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal(owner_principal),
            acl_grants: AclGrants::default(),
            public_read: false,
            generation_id: GenerationId::MIN,
            size: 4,
            etag: ObjectEtag::single_part(1),
            last_modified: 0,
            became_noncurrent_at: None,
            storage_class: StorageClass::Standard,
            ec: EcShape { k: 1, m: 0 },
            layout: ObjectLayout::Standard,
            tags: None,
            metadata_blob: None,
            system_metadata_blob: None,
            object_lock: ObjectLockState::default(),
            encryption: ObjectEncryption::None,
        })
    }

    fn requester(principal: &str) -> Requester {
        Requester::authenticated(AccountIdentity::from_principal(principal))
    }

    fn owner_admin_requester() -> Requester {
        Requester::authenticated_owner_account_admin(AccountIdentity::from_principal(
            "arn:aws:iam::111122223333:user/owner-admin",
        ))
    }

    fn shared_canonical_owner_admin_requester() -> Requester {
        let owner_canonical =
            CanonicalUserId::from_principal("arn:aws:iam::111122223333:user/bucket-owner");
        Requester::authenticated_owner_account_admin(AccountIdentity::new(
            "arn:aws:iam::111122223333:user/shared-other",
            owner_canonical,
            "shared-other",
        ))
    }

    fn parse_policy(body: &str) -> auth::BucketPolicy {
        auth::parse_bucket_policy(body).unwrap()
    }

    #[test]
    fn modern_read_auth_allows_explicit_policy_allow_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Allowed);
    }

    #[test]
    fn modern_read_auth_denies_without_policy_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            None,
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_explicit_policy_deny_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::777788889999:user/other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_explicit_policy_deny_for_shared_canonical_owner_admin_on_boe_bucket()
    {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::111122223333:user/bucket-owner");
        let requester = shared_canonical_owner_admin_requester();
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::111122223333:user/shared-other"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_explicit_policy_deny_for_bucket_owner_root_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::111122223333:user/bucket-owner");
        let requester = Requester::authenticated_owner_account_admin(
            AccountIdentity::from_principal("arn:aws:iam::111122223333:root"),
        );
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::111122223333:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_denies_non_owner_without_policy_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = requester("arn:aws:iam::444455556666:user/other");

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            None,
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }

    #[test]
    fn modern_read_auth_allows_bucket_owner_admin_on_boe_bucket() {
        let bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        let object = stored_live_object("arn:aws:iam::444455556666:user/object-owner");
        let requester = owner_admin_requester();

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            None,
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Allowed);
    }

    #[test]
    fn modern_read_auth_ignores_public_policy_when_restrict_public_buckets_blocks_it() {
        let mut bucket = modern_bucket(Some(BucketObjectOwnership::BucketOwnerEnforced));
        bucket.bucket_policy_public = true;
        bucket.public_access_block = Some(PublicAccessBlockConfig {
            block_public_acls: false,
            ignore_public_acls: false,
            block_public_policy: false,
            restrict_public_buckets: true,
        });
        let object = stored_live_object("arn:aws:iam::111122223333:user/object-owner");
        let requester = requester("arn:aws:iam::777788889999:user/other");
        let policy = parse_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::test-bucket/*"}]}"#,
        );

        let outcome = Coordinator::modern_read_object_authorization_with_bucket_policy(
            &requester,
            BoeBucketSummary::new(&bucket).unwrap(),
            None,
            &object,
            ModernReadAction::ReadCurrent,
            Some(&policy),
        )
        .unwrap();

        assert_eq!(outcome, ModernObjectReadAuthorization::Denied);
    }
}
