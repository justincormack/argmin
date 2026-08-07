use std::{borrow::Cow, sync::Arc};

mod acl;
mod bucket;
#[cfg(test)]
pub(super) mod modern;
#[cfg(not(test))]
mod modern;
mod policy;

use s3_types::{
    aws_account_id_from_principal, AclGrant, AclGrantee, AclGrants, AclPermission,
    BucketVersioningState, CanonicalUserId, LifecycleConfigError, VersionId,
};
use storage::{
    BucketName, BucketObjectLockConfig, BucketObjectOwnership, BucketOwnershipControls,
    BucketState, ManagedEncryptionAlgorithm, ObjectKey, ObjectReadSnapshotMode, OwnerIdentity,
    PublicAccessBlockConfig, StorageCluster, StorageClusterRouteAdmission, StoredObject,
};

use self::acl::NonBoeLoadedBucketHandle;
use self::modern::BoeLoadedBucketHandle;
pub(super) use self::modern::ModernReadAction;
use super::authz_results::{
    AuthorizedAbortMultipartUpload, AuthorizedBeginStreamPart, AuthorizedBucketConfigAccess,
    AuthorizedCompleteMultipartUpload, AuthorizedCopyObject, AuthorizedCopySourceRead,
    AuthorizedCreateBucket, AuthorizedCreateMultipartUpload, AuthorizedDeleteBucket,
    AuthorizedDeleteBucketCors, AuthorizedDeleteBucketEncryption, AuthorizedDeleteBucketLifecycle,
    AuthorizedDeleteBucketPolicy, AuthorizedDeleteBucketTagging, AuthorizedDeleteObject,
    AuthorizedGetBucketAbac, AuthorizedGetBucketAcl, AuthorizedGetBucketCors,
    AuthorizedGetBucketEncryption, AuthorizedGetBucketLifecycle, AuthorizedGetBucketLocation,
    AuthorizedGetBucketObjectLockConfiguration, AuthorizedGetBucketOwnershipControls,
    AuthorizedGetBucketPolicy, AuthorizedGetBucketPolicyStatus,
    AuthorizedGetBucketPublicAccessBlock, AuthorizedGetBucketTagging,
    AuthorizedGetBucketVersioning, AuthorizedHeadBucket, AuthorizedListBuckets,
    AuthorizedListMultipartUploads, AuthorizedListObjectVersions, AuthorizedListObjectsV2,
    AuthorizedListParts, AuthorizedLoadBucketCorsConfig, AuthorizedMultipartPartWrite,
    AuthorizedObjectRead, AuthorizedPutBucketAbac, AuthorizedPutBucketAcl, AuthorizedPutBucketCors,
    AuthorizedPutBucketEncryption, AuthorizedPutBucketLifecycle,
    AuthorizedPutBucketObjectLockConfiguration, AuthorizedPutBucketOwnershipControls,
    AuthorizedPutBucketPolicy, AuthorizedPutBucketPublicAccessBlock, AuthorizedPutBucketTagging,
    AuthorizedPutBucketVersioning, AuthorizedUploadPartCopy, ObjectAttributePermissions,
};
use super::authz_types::{AuthorizedPutObjectWrite, AuthorizedPutObjectWriteAcl, ValidatedBucket};
use super::bucket_handles::{
    BucketHandleLoader, BucketHandleRequest, LoadedBucketHandle, LoadedBucketSubresources,
    LoadedBucketValue,
};
use super::request_types::{
    authorization_policy_context_for_put_object_write_acl, AuthorizePutObjectRequest,
    BeginStreamPartRequest, BucketAcl, BucketRequest, BucketScopedAuthorizationRequest,
    BucketScopedRequest, BucketTagControlAction, BucketTagControlRequest,
    CompleteMultipartUploadRequest, CopyObjectRequest, CreateBucketAcl, CreateBucketRequest,
    CreateMultipartUploadRequest, DeleteEntry, DeleteObjectRequest, DeleteObjectsRequest,
    ExpectedBucketOwnerRequest, GetObjectAttributesRequest, GetObjectRequest, ListBucketsRequest,
    ListMultipartUploadsRequest, ListObjectVersionsRequest, ListObjectsV2Request, ListPartsRequest,
    MultipartObjectRequest, ObjectRequest, ObjectVersionRequest, PutBucketAbacRequest,
    PutBucketAclInput, PutBucketAclRequest, PutBucketConfigRequest, PutBucketEncryptionRequest,
    PutBucketObjectLockConfigurationRequest, PutBucketOwnershipControlsRequest,
    PutBucketPolicyRequest, PutBucketPublicAccessBlockRequest, PutBucketTagControlRequest,
    PutBucketTagsForUntagResourceRequest, PutBucketTagsRequest, PutBucketVersioningRequest,
    PutObjectAcl, PutObjectPolicyContext, PutObjectWriteAcl, Requester, TaggingDirective,
    UntagBucketTagControlRequest, UploadPartCopyRequest,
};
use super::response_types::{BucketSummary, GetBucketAclResult, ModernBucketSummary};
use super::Coordinator;
#[cfg(test)]
use super::{
    maybe_run_abort_multipart_auth_lookup_hook, maybe_run_abort_multipart_bucket_summary_hook,
    maybe_run_bucket_policy_fast_path_hook, maybe_run_bucket_policy_storage_load_hook,
    should_probe_multipart_complete_auth_lookup,
};
use crate::error::ServerError;
use crate::sse::{SseCustomerRequest, SseCustomerSegmentScope};

#[derive(Clone, Copy)]
enum MissingObjectDiscovery {
    ReadBucket,
    ReadObjectAttributes,
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

#[derive(Clone, Copy)]
pub(super) struct AuthenticatedMultipartWriteAuthorization<'a> {
    requester: &'a Requester,
    bucket: &'a BucketSummary,
    bucket_tags: Option<&'a [(String, String)]>,
    key: &'a ObjectKey,
    upload_id: &'a storage::UploadId,
    policy_context: PutObjectPolicyContext<'a>,
    policy: Option<&'a auth::BucketPolicy>,
}

#[derive(Clone, Copy)]
enum ExistingObjectTagsMode {
    Available,
    Unavailable,
    NotEvaluable,
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

struct ObjectReadSnapshotRoute<'a> {
    route: storage::ActiveObjectReadRoute<'a>,
    retain_payload: bool,
}

struct RoutedObjectReadSnapshotOutcome<T> {
    value: T,
    snapshot: Arc<storage::ObjectReadSnapshot>,
    payload_handoff: Option<storage::LeasedObjectReadSnapshot>,
}

impl ObjectReadSnapshotRoute<'_> {
    fn load<T, E>(
        &self,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<RoutedObjectReadSnapshotOutcome<T>, E>, storage::ObjectPgActionError> {
        if self.retain_payload {
            self.route
                .load_leased_object_read_snapshot_if(action)
                .map(|outcome| {
                    outcome.map(|outcome| {
                        let (value, snapshot, payload_handoff) = outcome.into_parts();
                        RoutedObjectReadSnapshotOutcome {
                            value,
                            snapshot,
                            payload_handoff: Some(payload_handoff),
                        }
                    })
                })
        } else {
            self.route
                .load_object_read_snapshot_if(action)
                .map(|outcome| {
                    outcome.map(|outcome| RoutedObjectReadSnapshotOutcome {
                        value: outcome.value,
                        snapshot: Arc::new(outcome.snapshot),
                        payload_handoff: None,
                    })
                })
        }
    }

    #[cfg(test)]
    fn try_probe_object_pg_available(&self) -> Result<bool, storage::ObjectPgActionError> {
        self.route.try_probe_object_pg_available()
    }
}

enum ObjectAuthLoadedBucketHandle<'a> {
    Boe(BoeLoadedBucketHandle<'a>),
    NonBoe(NonBoeLoadedBucketHandle<'a>),
}

impl<'a> ObjectAuthLoadedBucketHandle<'a> {
    fn classify(bucket: &'a LoadedBucketHandle) -> Self {
        if Coordinator::is_bucket_owner_enforced(bucket.bucket().ownership_controls.as_ref()) {
            Self::Boe(BoeLoadedBucketHandle::assume_boe(bucket))
        } else {
            Self::NonBoe(NonBoeLoadedBucketHandle::assume_non_boe(bucket))
        }
    }
}

impl Coordinator {
    pub(super) fn stored_object_tag_set(
        tags: &s3_types::TagSet,
    ) -> Result<storage::SerializedTagSet, ServerError> {
        storage::SerializedTagSet::from_tag_set(tags.clone()).map_err(|error| {
            ServerError::InternalError {
                reason: format!("authorized object tags exceed the stored object limit: {error}"),
            }
        })
    }

    pub(super) fn stored_object_tags(
        tags: Option<&s3_types::TagSet>,
    ) -> Result<Option<storage::SerializedTagSet>, ServerError> {
        tags.map(Self::stored_object_tag_set).transpose()
    }

    pub(in crate::coordinator) fn authorize_put_object_write_with_existing_object(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
        bucket: &LoadedBucketHandle,
        existing_object: Option<&StoredObject>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        match ObjectAuthLoadedBucketHandle::classify(bucket) {
            ObjectAuthLoadedBucketHandle::Boe(bucket) => self
                .authorize_put_object_write_with_existing_object_boe(req, bucket, existing_object),
            ObjectAuthLoadedBucketHandle::NonBoe(bucket) => self
                .authorize_put_object_write_with_existing_object_non_boe(
                    req,
                    bucket,
                    existing_object,
                ),
        }
    }

    fn authorize_delete_object_impl(
        &self,
        admission: &StorageClusterRouteAdmission,
        object: &ObjectVersionRequest<'_>,
        bypass_governance: bool,
    ) -> Result<AuthorizedDeleteObject, ServerError> {
        self.require_storage_route_admission(admission)?;
        let bucket_handle = self.load_bucket_handle_for_object_policy_read_on_admitted_route(
            admission,
            object.bucket_name_typed(),
            object.expected_bucket_owner(),
        )?;
        match ObjectAuthLoadedBucketHandle::classify(&bucket_handle) {
            ObjectAuthLoadedBucketHandle::Boe(bucket_handle) => self
                .authorize_delete_object_impl_boe(
                    admission,
                    object,
                    bypass_governance,
                    bucket_handle,
                ),
            ObjectAuthLoadedBucketHandle::NonBoe(bucket_handle) => self
                .authorize_delete_object_impl_non_boe(
                    admission,
                    object,
                    bypass_governance,
                    bucket_handle,
                ),
        }
    }

    pub(in crate::coordinator) fn authorize_create_multipart_upload_with_existing_object(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
        bucket: &LoadedBucketHandle,
        existing_object: Option<&StoredObject>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        match ObjectAuthLoadedBucketHandle::classify(bucket) {
            ObjectAuthLoadedBucketHandle::Boe(bucket) => {
                self.authorize_create_multipart_upload_with_existing_object_boe(req, bucket)
            }
            ObjectAuthLoadedBucketHandle::NonBoe(bucket) => self
                .authorize_create_multipart_upload_with_existing_object_non_boe(
                    req,
                    bucket,
                    existing_object,
                ),
        }
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_upload_part_copy(
        &self,
        req: &UploadPartCopyRequest<'_>,
    ) -> Result<AuthorizedUploadPartCopy, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.authorize_upload_part_copy_on_admitted_route(&admission, req)
    }

    pub(in crate::coordinator) fn authorize_upload_part_copy_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &UploadPartCopyRequest<'_>,
    ) -> Result<AuthorizedUploadPartCopy, ServerError> {
        self.require_storage_route_admission(admission)?;
        let multipart_route = admission
            .active_multipart_object_route(req.upload.bucket_name_typed(), req.upload.key_typed())
            .map_err(super::map_store_error)?;
        let dst_bucket_handle = self.load_bucket_handle_for_object_policy_read_on_admitted_route(
            admission,
            req.upload.bucket_name_typed(),
            req.expected_bucket_owner(),
        )?;
        match ObjectAuthLoadedBucketHandle::classify(&dst_bucket_handle) {
            ObjectAuthLoadedBucketHandle::Boe(dst_bucket_handle) => self
                .authorize_upload_part_copy_boe(
                    &multipart_route,
                    admission,
                    req,
                    dst_bucket_handle,
                ),
            ObjectAuthLoadedBucketHandle::NonBoe(dst_bucket_handle) => self
                .authorize_upload_part_copy_non_boe(
                    &multipart_route,
                    admission,
                    req,
                    dst_bucket_handle,
                ),
        }
    }

    pub(in crate::coordinator) fn authorize_begin_stream_part_with_upload(
        &self,
        req: &BeginStreamPartRequest<'_>,
        bucket_handle: &LoadedBucketHandle,
        upload: storage::MultipartUploadPartCandidate,
    ) -> Result<AuthorizedBeginStreamPart, ServerError> {
        match ObjectAuthLoadedBucketHandle::classify(bucket_handle) {
            ObjectAuthLoadedBucketHandle::Boe(bucket_handle) => {
                self.authorize_begin_stream_part_with_upload_boe(req, bucket_handle, upload)
            }
            ObjectAuthLoadedBucketHandle::NonBoe(bucket_handle) => {
                self.authorize_begin_stream_part_with_upload_non_boe(req, bucket_handle, upload)
            }
        }
    }

    #[cfg(test)]
    pub(in crate::coordinator) fn authorize_complete_multipart_upload(
        &self,
        req: &CompleteMultipartUploadRequest<'_>,
    ) -> Result<AuthorizedCompleteMultipartUpload, ServerError> {
        let admission = self.admit_storage_route_for_request()?;
        self.authorize_complete_multipart_upload_on_admitted_route(&admission, req)
    }

    pub(in crate::coordinator) fn authorize_complete_multipart_upload_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &CompleteMultipartUploadRequest<'_>,
    ) -> Result<AuthorizedCompleteMultipartUpload, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled()
            .requiring_lifecycle_view();
        let multipart_route = admission
            .active_multipart_object_route(req.upload.bucket_name_typed(), req.upload.key_typed())
            .map_err(super::map_store_error)?;
        self.with_bucket_write_handle_on_admitted_route(
            admission,
            &req.upload,
            request,
            |bucket_handle| match ObjectAuthLoadedBucketHandle::classify(&bucket_handle) {
                ObjectAuthLoadedBucketHandle::Boe(bucket_handle) => self
                    .authorize_complete_multipart_upload_boe_on_admitted_route(
                        &multipart_route,
                        req,
                        bucket_handle,
                    ),
                ObjectAuthLoadedBucketHandle::NonBoe(bucket_handle) => self
                    .authorize_complete_multipart_upload_non_boe_on_admitted_route(
                        &multipart_route,
                        req,
                        bucket_handle,
                    ),
            },
        )
    }

    fn authorize_object_read_snapshot_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: AuthorizedObjectReadSnapshotRequest<'_>,
    ) -> Result<
        (
            BucketSummary,
            Arc<storage::ObjectReadSnapshot>,
            ObjectAttributePermissions,
            Option<storage::LeasedObjectReadSnapshot>,
        ),
        ServerError,
    > {
        self.require_storage_route_admission(admission)?;
        let bucket = self.load_bucket_handle_for_modern_object_read_on_admitted_route(
            admission,
            req.bucket,
            req.expected_bucket_owner,
        )?;
        let route = ObjectReadSnapshotRoute {
            route: admission
                .active_object_read_route(req.bucket, req.key, req.version_id, req.snapshot_mode)
                .map_err(super::map_store_error)?,
            retain_payload: req.snapshot_mode == ObjectReadSnapshotMode::FullPayloadLayout,
        };
        match ObjectAuthLoadedBucketHandle::classify(&bucket) {
            ObjectAuthLoadedBucketHandle::Boe(bucket) => {
                self.authorize_object_read_snapshot_boe(&route, req, bucket)
            }
            ObjectAuthLoadedBucketHandle::NonBoe(bucket) => {
                self.authorize_object_read_snapshot_non_boe(&route, req, bucket)
            }
        }
    }

    fn authorize_copy_source_read_snapshot(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: CopySourceReadSnapshotRequest<'_>,
    ) -> Result<AuthorizedCopySourceRead, ServerError> {
        self.require_storage_route_admission(admission)?;
        let bucket = self.load_bucket_handle_for_modern_object_read_on_admitted_route(
            admission,
            req.bucket,
            req.expected_bucket_owner,
        )?;
        let route = ObjectReadSnapshotRoute {
            route: admission
                .active_object_read_route(
                    req.bucket,
                    req.key,
                    req.version_id,
                    ObjectReadSnapshotMode::FullPayloadLayout,
                )
                .map_err(super::map_store_error)?,
            retain_payload: true,
        };
        match ObjectAuthLoadedBucketHandle::classify(&bucket) {
            ObjectAuthLoadedBucketHandle::Boe(bucket) => {
                self.authorize_copy_source_read_snapshot_boe(&route, req, bucket)
            }
            ObjectAuthLoadedBucketHandle::NonBoe(bucket) => {
                self.authorize_copy_source_read_snapshot_non_boe(&route, req, bucket)
            }
        }
    }
}

impl Coordinator {
    pub(super) fn finalize_authorized_put_object_write_after_auth(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
        bucket_info: &ValidatedBucket,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        let key = req.object.key();
        let write_encryption = self.resolve_write_encryption(bucket_info, req.encryption)?;
        if write_encryption.is_sse_customer() && bucket_info.encryption.sse_c_blocked {
            return Err(ServerError::SseCBlockedAccessDenied {
                requester_principal: Self::requester_principal_required(req.object.requester())?
                    .to_string(),
                action: "s3:PutObject".to_string(),
                resource: format!("arn:aws:s3:::{}/{}", bucket_info.name, key),
            });
        }
        Self::ensure_put_object_write_acl_supported(bucket_info, &req.acl)?;
        Self::validate_requested_object_lock_state(bucket_info, req.object_lock)?;
        Ok(AuthorizedPutObjectWrite {
            bucket: req.object.bucket.name_typed().clone(),
            key: req.object.key_typed().clone(),
            requester: req.object.requester().clone(),
            expected_bucket_owner: req.object.expected_bucket_owner().map(str::to_string),
            acl: AuthorizedPutObjectWriteAcl::from_parsed(&req.acl),
            requested_object_lock: req.object_lock,
            tags: req.tags.cloned(),
            write_encryption,
        })
    }

    pub(super) fn finalize_authorized_create_multipart_upload_after_auth(
        &self,
        req: &CreateMultipartUploadRequest<'_>,
        bucket_info: &ValidatedBucket,
        lifecycle: Option<s3_types::BucketLifecycleConfiguration>,
    ) -> Result<AuthorizedCreateMultipartUpload, ServerError> {
        Self::ensure_sse_c_allowed(bucket_info, req.encryption.sse_customer_request().is_some())?;
        let write_encryption = self.resolve_write_encryption(bucket_info, req.encryption)?;
        Self::ensure_put_object_write_acl_supported(bucket_info, &req.acl)?;
        let owner = Self::effective_put_object_owner(bucket_info, req.object.requester(), &req.acl);
        let initiator =
            Self::requester_owner_identity(req.object.requester()).unwrap_or_else(|| owner.clone());
        let acl_grants = Self::object_acl_grants_for_put_object(bucket_info, &owner, &req.acl);
        let public_read = Self::acl_grants_public_read(&acl_grants);
        Self::validate_requested_object_lock_state(bucket_info, req.object_lock)?;
        Self::ensure_sse_c_allowed(bucket_info, write_encryption.is_sse_customer())?;

        Ok(AuthorizedCreateMultipartUpload {
            lifecycle,
            tags: req.tags.cloned(),
            checksum: req.checksum,
            initiator,
            owner,
            acl_grants,
            public_read,
            object_lock: req.object_lock,
            write_encryption,
        })
    }

    pub(super) fn requester_can_bucket_admin(requester: &Requester, owner_principal: &str) -> bool {
        requester.configured_principal() == Some(owner_principal)
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
        let effective_acl_grants = Self::effective_acl_grants(bucket, acl_grants);
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
            || Self::requester_has_acl_permission(
                requester,
                effective_acl_grants.as_ref(),
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
        let effective_acl_grants = Self::effective_acl_grants(bucket, acl_grants);
        let acl_allows_read = Self::requester_has_acl_permission(
            requester,
            effective_acl_grants.as_ref(),
            &bucket.owner_canonical_id,
            AclPermission::Read,
        );

        requester.configured_principal() == Some(owner_principal) || acl_allows_read || public_read
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
            let effective_acl_grants = Self::effective_acl_grants(bucket, grants);
            Self::requester_has_acl_permission(
                requester,
                effective_acl_grants.as_ref(),
                &object.owner().canonical_id,
                AclPermission::Read,
            )
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

        let effective_acl_grants = Self::effective_acl_grants(bucket, &bucket.acl_grants);
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                effective_acl_grants.as_ref(),
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

        let effective_acl_grants = Self::effective_acl_grants(bucket, &bucket.acl_grants);
        Self::requester_can_bucket_admin(requester, &bucket.owner_principal)
            || Self::requester_has_acl_permission(
                requester,
                effective_acl_grants.as_ref(),
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
                let effective_acl_grants = Self::effective_acl_grants(bucket, grants);
                Self::requester_has_acl_permission(
                    requester,
                    effective_acl_grants.as_ref(),
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
                let effective_acl_grants = Self::effective_acl_grants(bucket, grants);
                Self::requester_has_acl_permission(
                    requester,
                    effective_acl_grants.as_ref(),
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
        let Some(principal) = requester.configured_principal() else {
            return false;
        };
        requester.account().is_some_and(|account| {
            principal == owner.principal
                || (account.canonical_user_id() == &owner.canonical_id
                    && requester.authorization_profile()
                        == auth::AuthorizationProfile::OwnerAccountAdmin)
        })
    }

    pub(super) fn requester_is_bucket_owner_account(
        requester: &Requester,
        bucket: &BucketSummary,
    ) -> bool {
        let Some(principal) = requester.configured_principal() else {
            return false;
        };
        let Some(account) = requester.account() else {
            return false;
        };
        if principal == bucket.owner_principal {
            return true;
        }

        let Some(requester_account_id) = account.account_id() else {
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
        let Some(requester_account_id) = account.account_id() else {
            return false;
        };
        let Some(bucket_owner_account_id) = Self::bucket_owner_account_id(&bucket.owner_principal)
        else {
            return false;
        };
        if requester_account_id != bucket_owner_account_id {
            return false;
        }

        let root_arn = format!("arn:aws:iam::{requester_account_id}:root");
        requester.configured_principal() == Some(root_arn.as_str())
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

    pub(super) fn requester_can_manage_multipart_upload_identity(
        requester: &Requester,
        bucket: &BucketSummary,
        owner: &OwnerIdentity,
        initiator: &OwnerIdentity,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
            || Self::requester_matches_owner_identity(requester, owner)
            || Self::requester_matches_owner_identity(requester, initiator)
    }

    pub(super) fn requester_can_manage_authenticated_multipart_upload_id(
        requester: &Requester,
        bucket: &BucketSummary,
        upload_id: &storage::UploadId,
    ) -> bool {
        Self::requester_can_bucket_owner_account_admin(requester, bucket)
            || requester.configured_principal().is_some_and(|principal| {
                bucket
                    .multipart_upload_id_authority
                    .was_issued_for_principal(upload_id, principal)
            })
    }

    fn requester_can_write_multipart_upload_identity(
        requester: &Requester,
        bucket: &BucketSummary,
        owner: &OwnerIdentity,
        initiator: &OwnerIdentity,
    ) -> bool {
        Self::requester_can_object_write(
            requester,
            bucket,
            &bucket.acl_grants,
            Self::effective_public_write(bucket),
        ) && Self::requester_can_manage_multipart_upload_identity(
            requester, bucket, owner, initiator,
        )
    }

    pub(super) fn requester_can_write_multipart_completion_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        upload: &storage::MultipartUploadCompletionCandidate,
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
                default_allowed: Self::requester_can_write_multipart_upload_identity(
                    requester,
                    bucket,
                    upload.owner(),
                    upload.initiator(),
                ),
            },
            upload.key().as_str(),
        )
    }

    pub(super) fn requester_can_write_multipart_part_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        upload: &storage::MultipartUploadPartCandidate,
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
                default_allowed: Self::requester_can_write_multipart_upload_identity(
                    requester,
                    bucket,
                    upload.owner(),
                    upload.initiator(),
                ),
            },
            upload.key().as_str(),
        )
    }

    pub(super) fn requester_can_write_authenticated_multipart_upload_id_with_bucket_policy(
        &self,
        authorization: AuthenticatedMultipartWriteAuthorization<'_>,
    ) -> Result<bool, ServerError> {
        self.requester_can_put_object_action_with_bucket_policy(
            BucketPolicyActionAuthorization {
                request: BucketPolicyRequestContext {
                    requester: authorization.requester,
                    bucket: authorization.bucket,
                    bucket_tags: authorization.bucket_tags,
                    action: auth::PolicyAction::PutObject,
                    policy_context: authorization.policy_context,
                    policy: authorization.policy,
                },
                default_allowed: Self::requester_can_object_write(
                    authorization.requester,
                    authorization.bucket,
                    &authorization.bucket.acl_grants,
                    Self::effective_public_write(authorization.bucket),
                ) && Self::requester_can_manage_authenticated_multipart_upload_id(
                    authorization.requester,
                    authorization.bucket,
                    authorization.upload_id,
                ),
            },
            authorization.key.as_str(),
        )
    }

    pub(super) fn with_multipart_completion_managed_encryption_policy_context<'a>(
        policy_context: PutObjectPolicyContext<'a>,
        upload: &'a storage::MultipartUploadCompletionCandidate,
    ) -> PutObjectPolicyContext<'a> {
        if policy_context.managed_encryption.is_some() {
            return policy_context;
        }

        match upload.encryption().managed_encryption_algorithm() {
            Some(algorithm) => policy_context.with_managed_encryption(Some(algorithm)),
            None => policy_context,
        }
    }

    pub(super) fn with_multipart_part_managed_encryption_policy_context<'a>(
        policy_context: PutObjectPolicyContext<'a>,
        upload: &'a storage::MultipartUploadPartCandidate,
    ) -> PutObjectPolicyContext<'a> {
        if policy_context.managed_encryption.is_some() {
            return policy_context;
        }

        match upload.encryption().managed_encryption_algorithm() {
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

    pub(super) fn effective_acl_grants<'a>(
        bucket: &BucketSummary,
        acl_grants: &'a AclGrants,
    ) -> Cow<'a, AclGrants> {
        if !Self::ignores_public_acls(bucket.public_access_block.as_ref()) {
            return Cow::Borrowed(acl_grants);
        }

        Cow::Owned(AclGrants::new(
            acl_grants
                .iter()
                .filter(|grant| matches!(grant.grantee(), AclGrantee::CanonicalUser(_)))
                .cloned()
                .collect(),
        ))
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
            .storage_node()
            .get_bucket_subresource(&bucket.name, storage::OpaqueBucketSubresourceKind::Policy)
            .map_err(Self::map_bucket_snapshot_load_error)?;
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

        if let Some(cached) = self.parsed_bucket_fast_path_policy_for_identity_if_fresh(
            &bucket_summary.name,
            bucket.fast_path_identity(),
            bucket_summary.bucket_policy_generation,
        ) {
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

    #[cfg(test)]
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
            LoadedBucketValue::Loaded(tags) => Ok(Some(tags.clone().into_pairs())),
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
            ExistingObjectTagsMode::NotEvaluable,
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
                PutObjectPolicyContext::default().with_version_id(version_id),
            ),
            key,
            || Self::requester_can_discover_missing_object(access.requester, access.bucket),
        )?;
        let attrs_allowed = self.requester_can_missing_object_action_with_bucket_policy(
            access.request(
                Self::get_object_attributes_policy_action(version_id),
                PutObjectPolicyContext::default().with_version_id(version_id),
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
        request_object_tags: Option<&s3_types::TagSet>,
    ) -> Result<bool, ServerError> {
        self.requester_can_object_action_with_bucket_policy(
            access.request(
                action,
                PutObjectPolicyContext::default().with_request_object_tags(request_object_tags),
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

    pub(super) fn object_attribute_permissions_with_bucket_policy(
        &self,
        requester: &Requester,
        bucket: &BucketSummary,
        bucket_tags: Option<&[(String, String)]>,
        object: &StoredObject,
        action: ModernReadAction,
        policy: Option<&auth::BucketPolicy>,
    ) -> Result<ObjectAttributePermissions, ServerError> {
        if !action.discloses_optional_attributes() {
            return Ok(ObjectAttributePermissions::default());
        }
        let object_lock_retention = self.requester_can_manage_object_lock_with_bucket_policy(
            requester,
            bucket,
            bucket_tags,
            object,
            auth::PolicyAction::GetObjectRetention,
            policy,
        )?;
        let object_lock_legal_hold = self.requester_can_manage_object_lock_with_bucket_policy(
            requester,
            bucket,
            bucket_tags,
            object,
            auth::PolicyAction::GetObjectLegalHold,
            policy,
        )?;
        let tag_count = self.requester_can_manage_object_tags_with_bucket_policy(
            BucketPolicyAccess {
                requester,
                bucket,
                bucket_tags,
                policy,
            },
            object,
            action.tagging_policy_action(),
            None,
        )?;

        Ok(ObjectAttributePermissions::new(
            object_lock_retention,
            object_lock_legal_hold,
            tag_count,
        ))
    }

    pub(super) fn requester_can_delete_object_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
        key: &str,
        object: Option<&StoredObject>,
        action: auth::PolicyAction,
    ) -> Result<bool, ServerError> {
        self.requester_can_delete_object_with_policy_context(
            access,
            key,
            object,
            action,
            PutObjectPolicyContext::default(),
        )
    }

    pub(super) fn requester_can_delete_object_version_with_bucket_policy(
        &self,
        access: BucketPolicyAccess<'_>,
        key: &str,
        object: Option<&StoredObject>,
        action: auth::PolicyAction,
        version_id: VersionId,
    ) -> Result<bool, ServerError> {
        self.requester_can_delete_object_with_policy_context(
            access,
            key,
            object,
            action,
            PutObjectPolicyContext::default().with_version_id(Some(version_id)),
        )
    }

    fn requester_can_delete_object_with_policy_context(
        &self,
        access: BucketPolicyAccess<'_>,
        key: &str,
        object: Option<&StoredObject>,
        action: auth::PolicyAction,
        policy_context: PutObjectPolicyContext<'_>,
    ) -> Result<bool, ServerError> {
        let decision = self.object_policy_decision(
            access.request(action, policy_context),
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
            let effective_acl_grants =
                Self::effective_acl_grants(access.bucket, &access.bucket.acl_grants);
            Self::requester_can_bucket_owner_account_admin(access.requester, access.bucket)
                || Self::requester_has_acl_permission(
                    access.requester,
                    effective_acl_grants.as_ref(),
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
        let put_object_request = access.request(auth::PolicyAction::PutObject, policy_context);
        let can_put_object = if let Some(object) = existing_object {
            self.requester_can_object_action_with_bucket_policy(put_object_request, object, || {
                default_allowed
            })?
        } else {
            self.requester_can_put_object_action_with_bucket_policy(
                BucketPolicyActionAuthorization {
                    request: put_object_request,
                    default_allowed,
                },
                key,
            )?
        };
        if !can_put_object {
            return Ok(false);
        }

        if let (Some(_), Some(object)) = (policy_context.if_match, existing_object) {
            let can_read = self.requester_can_read_object_with_bucket_policy(
                access.requester,
                access.bucket,
                access.bucket_tags,
                object,
                auth::PolicyAction::GetObject,
                access.policy,
            )?;
            if !can_read {
                return Ok(false);
            }
        }

        if policy_context.request_object_tags.is_none() {
            return Ok(true);
        }

        let put_tagging_default_allowed =
            Self::requester_can_bucket_owner_account_admin(access.requester, access.bucket);
        let put_tagging_request =
            access.request(auth::PolicyAction::PutObjectTagging, policy_context);
        if let Some(object) = existing_object {
            self.requester_can_object_action_with_bucket_policy(put_tagging_request, object, || {
                put_tagging_default_allowed
            })
        } else {
            self.requester_can_put_object_action_with_bucket_policy(
                BucketPolicyActionAuthorization {
                    request: put_tagging_request,
                    default_allowed: put_tagging_default_allowed,
                },
                key,
            )
        }
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

    #[cfg(test)]
    fn load_bucket_handle_for_bucket_policy_read(
        &self,
        req: &BucketRequest<'_>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        self.load_bucket_handle_for_bucket_policy_read_with_storage_node(&self.storage_node(), req)
    }

    pub(super) fn load_bucket_handle_for_bucket_policy_read_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.bucket_handle_loader().load_bucket_on_admitted_route(
            admission,
            req.name_typed(),
            req.expected_bucket_owner(),
            request,
        )
    }

    #[cfg(test)]
    pub(super) fn load_bucket_handle_for_bucket_policy_read_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &BucketRequest<'_>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.bucket_handle_loader().load_bucket_with_storage_node(
            storage_node,
            req.name_typed(),
            req.expected_bucket_owner(),
            request,
        )
    }

    pub(super) fn load_bucket_handle_for_object_policy_read_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        self.require_storage_route_admission(admission)?;
        let request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();

        #[cfg(test)]
        maybe_run_bucket_policy_storage_load_hook(bucket.as_str());
        self.bucket_handle_loader().load_bucket_on_admitted_route(
            admission,
            bucket,
            expected_bucket_owner,
            request,
        )
    }

    fn load_bucket_handle_for_modern_object_read_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<LoadedBucketHandle, ServerError> {
        self.require_storage_route_admission(admission)?;
        self.load_bucket_handle_for_modern_object_read_with_snapshot_loader(
            bucket,
            expected_bucket_owner,
            |request| {
                admission
                    .active_bucket_route(bucket)
                    .map_err(super::map_store_error)?
                    .load_bucket_snapshot(request)
                    .map_err(BucketHandleLoader::map_bucket_snapshot_error)
            },
        )
    }

    fn load_bucket_handle_for_modern_object_read_with_snapshot_loader(
        &self,
        bucket: &BucketName,
        expected_bucket_owner: Option<&str>,
        load_snapshot: impl FnOnce(
            storage::BucketSnapshotRequest,
        ) -> Result<storage::BucketSnapshot, ServerError>,
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
                            Some(LoadedBucketValue::Loaded(tags.tag_set().clone()))
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
                    let bucket_execution_generation = info.bucket_execution_generation;
                    let bucket_incarnation_generation = info.bucket_incarnation_generation;
                    return Ok(LoadedBucketHandle::new(
                        Self::bucket_summary_for_boe_modern_fast_path(bucket_info),
                        bucket_execution_generation,
                        bucket_incarnation_generation,
                        request,
                        LoadedBucketSubresources::new(
                            policy,
                            tags,
                            LoadedBucketValue::NotRequested,
                            LoadedBucketValue::NotRequested,
                        ),
                    ));
                }
            }
        }

        #[cfg(test)]
        maybe_run_bucket_policy_storage_load_hook(bucket.as_str());
        let snapshot = match load_snapshot(request.resolve_to_storage_request()) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.remove_bucket_fast_path(bucket);
                return Err(err);
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

    #[cfg(test)]
    pub(super) fn with_bucket_write_handle_for<R, T>(
        &self,
        req: &R,
        request: BucketHandleRequest,
        action: impl FnOnce(LoadedBucketHandle) -> Result<T, ServerError>,
    ) -> Result<T, ServerError>
    where
        R: BucketScopedRequest + ExpectedBucketOwnerRequest + ?Sized,
    {
        self.with_bucket_write_handle_for_storage_node(&self.storage_node(), req, request, action)
    }

    pub(super) fn with_bucket_write_handle_for_storage_node<R, T>(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &R,
        request: BucketHandleRequest,
        action: impl FnOnce(LoadedBucketHandle) -> Result<T, ServerError>,
    ) -> Result<T, ServerError>
    where
        R: BucketScopedRequest + ExpectedBucketOwnerRequest + ?Sized,
    {
        let expected_bucket_owner = req.expected_bucket_owner();
        storage_node
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
                    self.maybe_run_bucket_write_handle_loaded_hook(
                        req.bucket_name_typed().as_str(),
                    );
                    action(bucket)
                },
            )
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
    }

    pub(super) fn with_bucket_write_handle_on_admitted_route<R, T>(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &R,
        request: BucketHandleRequest,
        action: impl FnOnce(LoadedBucketHandle) -> Result<T, ServerError>,
    ) -> Result<T, ServerError>
    where
        R: BucketScopedRequest + ExpectedBucketOwnerRequest + ?Sized,
    {
        self.require_storage_route_admission(admission)?;
        let expected_bucket_owner = req.expected_bucket_owner();
        admission
            .active_bucket_route(req.bucket_name_typed())
            .map_err(super::map_store_error)?
            .with_bucket_write_snapshot(request.resolve_to_storage_request(), |snapshot| {
                let bucket = self
                    .bucket_handle_loader()
                    .load_bucket_handle_from_snapshot(snapshot, expected_bucket_owner, request)?;
                #[cfg(test)]
                self.maybe_run_bucket_write_handle_loaded_hook(req.bucket_name_typed().as_str());
                action(bucket)
            })
            .map_err(BucketHandleLoader::map_bucket_snapshot_error)?
    }

    #[cfg(test)]
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

    fn load_bucket_handle_for_bucket_read_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
        request: BucketHandleRequest,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let base_request = BucketHandleRequest::new()
            .requiring_policy_view()
            .requiring_bucket_tags_if_abac_enabled();
        self.bucket_handle_loader().load_bucket_on_admitted_route(
            admission,
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

    #[cfg(test)]
    fn authorize_loaded_bucket_action_for(
        &self,
        req: &BucketRequest<'_>,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let bucket = self.load_bucket_handle_for_bucket_policy_read(req)?;
        self.authorize_loaded_bucket_action_for_loaded_handle(req, bucket, action, default_allowed)
    }

    fn authorize_loaded_bucket_action_for_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let bucket =
            self.load_bucket_handle_for_bucket_policy_read_on_admitted_route(admission, req)?;
        self.authorize_loaded_bucket_action_for_loaded_handle(req, bucket, action, default_allowed)
    }

    fn authorize_loaded_bucket_action_for_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError> {
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

    fn authorize_loaded_bucket_write_action_on_admitted_route<R>(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &R,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.with_bucket_write_handle_on_admitted_route(
            admission,
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

    fn authorize_loaded_bucket_write_policy_action_on_admitted_route<R>(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &R,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.with_bucket_write_handle_on_admitted_route(
            admission,
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

    fn authorize_loaded_bucket_owner_account_admin_write_on_admitted_route<R>(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &R,
    ) -> Result<LoadedBucketHandle, ServerError>
    where
        R: BucketScopedAuthorizationRequest + ?Sized,
    {
        self.with_bucket_write_handle_on_admitted_route(
            admission,
            req,
            BucketHandleRequest::new(),
            |bucket| {
                if !Self::requester_can_bucket_owner_account_admin(req.requester(), bucket.bucket())
                {
                    return Err(ServerError::AccessDenied);
                }
                Ok(bucket)
            },
        )
    }

    fn authorize_loaded_bucket_policy_action_for_on_admitted_route(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &BucketRequest<'_>,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let bucket =
            self.load_bucket_handle_for_bucket_policy_read_on_admitted_route(admission, req)?;
        self.authorize_loaded_bucket_policy_action_for_loaded_handle(
            req,
            bucket,
            action,
            default_allowed,
        )
    }

    fn authorize_loaded_bucket_policy_action_for_loaded_handle(
        &self,
        req: &BucketRequest<'_>,
        bucket: LoadedBucketHandle,
        action: auth::PolicyAction,
        default_allowed: impl FnOnce(&Requester, &BucketSummary) -> bool,
    ) -> Result<LoadedBucketHandle, ServerError> {
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
        let Some(tags) = object.as_live().and_then(|record| record.tags.as_deref()) else {
            return Ok(Vec::new());
        };
        Ok(tags.clone().into_pairs())
    }

    #[cfg(test)]
    pub(super) fn parse_serialized_tag_set(
        tags_xml: &str,
    ) -> Result<Vec<(String, String)>, ServerError> {
        s3_types::TagSet::parse_canonical_xml(tags_xml, usize::MAX)
            .map(s3_types::TagSet::into_pairs)
            .map_err(|error| ServerError::InternalError {
                reason: error.to_string(),
            })
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
        requester
            .configured_principal()
            .ok_or(ServerError::AccessDenied)
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
        let account = requester.account()?;
        let principal = requester.configured_principal()?;
        Some(OwnerIdentity::new(
            principal.to_string(),
            account.canonical_user_id().clone(),
        ))
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
}
