#![allow(dead_code)]

use s3_types::VersionId;
use std::sync::Arc;
use storage::{
    BucketName, BucketSnapshot, BucketSnapshotRequest, BucketSnapshotTagsRequest, ObjectKey,
    StorageCluster, StorageClusterRouteAdmission,
};

use super::{BucketSummary, Coordinator};
use crate::error::ServerError;

/// Bucket-tag loading policy for a request family.
///
/// Request code does not decide ad hoc whether ABAC tags are needed. Instead,
/// the request family declares its logical tag dependency up front and initial
/// bucket acquisition resolves that against bucket state in one step.
/// Bucket state a request family declares up front.
///
/// This is a semantic wrapper over the storage snapshot request shape rather
/// than a second parallel struct. The coordinator keeps request-family naming
/// here, while storage owns the concrete snapshot fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct BucketHandleRequest(BucketSnapshotRequest);

impl BucketHandleRequest {
    pub(super) const fn new() -> Self {
        Self(BucketSnapshotRequest {
            policy: false,
            tags: BucketSnapshotTagsRequest::NotRequested,
            lifecycle: false,
            cors: false,
        })
    }

    pub(super) const fn requiring_policy_view(mut self) -> Self {
        self.0.policy = true;
        self
    }

    pub(super) const fn requiring_bucket_tags(mut self) -> Self {
        self.0.tags = BucketSnapshotTagsRequest::Always;
        self
    }

    pub(super) const fn requiring_bucket_tags_if_abac_enabled(mut self) -> Self {
        self.0.tags = BucketSnapshotTagsRequest::IfBucketAbacEnabled;
        self
    }

    pub(super) const fn requiring_lifecycle_view(mut self) -> Self {
        self.0.lifecycle = true;
        self
    }

    pub(super) const fn requiring_cors_view(mut self) -> Self {
        self.0.cors = true;
        self
    }

    pub(super) const fn policy_view(self) -> bool {
        self.0.policy
    }

    pub(super) const fn lifecycle_view(self) -> bool {
        self.0.lifecycle
    }

    pub(super) const fn cors_view(self) -> bool {
        self.0.cors
    }

    pub(super) const fn merge(self, other: Self) -> Self {
        Self(BucketSnapshotRequest {
            policy: self.0.policy || other.0.policy,
            tags: match (self.0.tags, other.0.tags) {
                (BucketSnapshotTagsRequest::Always, _) | (_, BucketSnapshotTagsRequest::Always) => {
                    BucketSnapshotTagsRequest::Always
                }
                (BucketSnapshotTagsRequest::IfBucketAbacEnabled, _)
                | (_, BucketSnapshotTagsRequest::IfBucketAbacEnabled) => {
                    BucketSnapshotTagsRequest::IfBucketAbacEnabled
                }
                (
                    BucketSnapshotTagsRequest::NotRequested,
                    BucketSnapshotTagsRequest::NotRequested,
                ) => BucketSnapshotTagsRequest::NotRequested,
            },
            lifecycle: self.0.lifecycle || other.0.lifecycle,
            cors: self.0.cors || other.0.cors,
        })
    }

    pub(super) const fn resolve_to_storage_request(self) -> BucketSnapshotRequest {
        self.0
    }
}

/// Result of loading one bucket subresource declared by [`BucketHandleRequest`].
///
/// `NotRequested` is distinct from `Missing`: later request-family migrations
/// should be able to prove that bucket state was either requested up front or
/// intentionally absent, rather than expanded ad hoc after object derivation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) enum LoadedBucketValue<T> {
    #[default]
    NotRequested,
    Missing,
    Loaded(T),
}

impl<T> LoadedBucketValue<T> {
    pub(super) const fn is_requested(&self) -> bool {
        !matches!(self, Self::NotRequested)
    }

    pub(super) fn as_ref(&self) -> LoadedBucketValue<&T> {
        match self {
            Self::NotRequested => LoadedBucketValue::NotRequested,
            Self::Missing => LoadedBucketValue::Missing,
            Self::Loaded(value) => LoadedBucketValue::Loaded(value),
        }
    }
}

/// Request-scoped loaded bucket handle.
///
/// This is intentionally snapshot-oriented: it owns bucket metadata and the
/// requested bucket subresources, but it does not keep a PG mutex for the
/// lifetime of the request.
#[derive(Debug)]
pub(super) struct LoadedBucketHandle {
    bucket: BucketSummary,
    bucket_execution_generation: u64,
    bucket_incarnation_generation: u64,
    request: BucketHandleRequest,
    policy: LoadedBucketValue<String>,
    tags: LoadedBucketValue<s3_types::TagSet>,
    lifecycle: LoadedBucketValue<String>,
    cors: LoadedBucketValue<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct LoadedBucketSubresources {
    policy: LoadedBucketValue<String>,
    tags: LoadedBucketValue<s3_types::TagSet>,
    lifecycle: LoadedBucketValue<String>,
    cors: LoadedBucketValue<String>,
}

impl LoadedBucketSubresources {
    pub(super) const fn new(
        policy: LoadedBucketValue<String>,
        tags: LoadedBucketValue<s3_types::TagSet>,
        lifecycle: LoadedBucketValue<String>,
        cors: LoadedBucketValue<String>,
    ) -> Self {
        Self {
            policy,
            tags,
            lifecycle,
            cors,
        }
    }
}

impl LoadedBucketHandle {
    pub(super) fn new(
        bucket: BucketSummary,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
        request: BucketHandleRequest,
        subresources: LoadedBucketSubresources,
    ) -> Self {
        Self {
            bucket,
            bucket_execution_generation,
            bucket_incarnation_generation,
            request,
            policy: subresources.policy,
            tags: subresources.tags,
            lifecycle: subresources.lifecycle,
            cors: subresources.cors,
        }
    }

    pub(super) const fn bucket(&self) -> &BucketSummary {
        &self.bucket
    }

    pub(super) const fn bucket_execution_generation(&self) -> u64 {
        self.bucket_execution_generation
    }

    pub(super) const fn bucket_incarnation_generation(&self) -> u64 {
        self.bucket_incarnation_generation
    }

    pub(super) const fn fast_path_identity(&self) -> storage::BucketFastPathIdentity {
        storage::BucketFastPathIdentity {
            bucket_execution_generation: self.bucket_execution_generation,
            bucket_incarnation_generation: self.bucket_incarnation_generation,
        }
    }

    pub(super) const fn request(&self) -> BucketHandleRequest {
        self.request
    }

    pub(super) const fn policy(&self) -> &LoadedBucketValue<String> {
        &self.policy
    }

    pub(super) const fn tags(&self) -> &LoadedBucketValue<s3_types::TagSet> {
        &self.tags
    }

    pub(super) const fn lifecycle(&self) -> &LoadedBucketValue<String> {
        &self.lifecycle
    }

    pub(super) const fn cors(&self) -> &LoadedBucketValue<String> {
        &self.cors
    }

    /// Derive an object handle from this loaded bucket snapshot.
    ///
    /// This is borrow-based so object access stays tied to the request-scoped
    /// bucket handle it was derived from.
    pub(super) fn load_object<'a>(&'a self, key: ObjectKey) -> LoadedObjectHandle<'a> {
        LoadedObjectHandle {
            bucket: self,
            key,
            version_id: None,
        }
    }

    pub(super) fn load_object_version<'a>(
        &'a self,
        key: ObjectKey,
        version_id: VersionId,
    ) -> LoadedObjectHandle<'a> {
        LoadedObjectHandle {
            bucket: self,
            key,
            version_id: Some(version_id),
        }
    }
}

/// Request-scoped object handle derived from a loaded bucket handle.
#[derive(Debug)]
pub(super) struct LoadedObjectHandle<'a> {
    bucket: &'a LoadedBucketHandle,
    key: ObjectKey,
    version_id: Option<VersionId>,
}

impl<'a> LoadedObjectHandle<'a> {
    pub(super) const fn bucket(&self) -> &LoadedBucketHandle {
        self.bucket
    }

    pub(super) const fn key(&self) -> &ObjectKey {
        &self.key
    }

    pub(super) const fn version_id(&self) -> Option<VersionId> {
        self.version_id
    }
}

/// Loader for request-scoped bucket handles.
///
/// Request paths are meant to talk to this loader, not to PGs.
///
/// This is still transitional scaffolding: it centralizes bucket loading, but
/// it is not yet the final single-use request-family entry point that will
/// make duplicate bucket acquisition structurally impossible.
pub(super) struct BucketHandleLoader<'a> {
    coordinator: &'a Coordinator,
}

impl<'a> BucketHandleLoader<'a> {
    pub(super) const fn new(coordinator: &'a Coordinator) -> Self {
        Self { coordinator }
    }

    pub(super) fn load_bucket(
        self,
        name: &BucketName,
        expected_bucket_owner: Option<&str>,
        request: BucketHandleRequest,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let storage_node = self.coordinator.storage_node();
        self.load_bucket_with_storage_node(&storage_node, name, expected_bucket_owner, request)
    }

    pub(super) fn load_bucket_with_storage_node(
        self,
        storage_node: &Arc<StorageCluster>,
        name: &BucketName,
        expected_bucket_owner: Option<&str>,
        request: BucketHandleRequest,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let snapshot = storage_node
            .load_bucket_snapshot(name, request.resolve_to_storage_request())
            .map_err(Self::map_bucket_snapshot_error)?;
        self.load_bucket_handle_from_snapshot(snapshot, expected_bucket_owner, request)
    }

    pub(super) fn load_bucket_on_admitted_route(
        self,
        admission: &StorageClusterRouteAdmission,
        name: &BucketName,
        expected_bucket_owner: Option<&str>,
        request: BucketHandleRequest,
    ) -> Result<LoadedBucketHandle, ServerError> {
        self.coordinator
            .require_storage_route_admission(admission)?;
        let snapshot = admission
            .active_bucket_route(name)
            .map_err(super::map_store_failure)?
            .load_bucket_snapshot(request.resolve_to_storage_request())
            .map_err(Self::map_bucket_snapshot_error)?;
        self.load_bucket_handle_from_snapshot(snapshot, expected_bucket_owner, request)
    }

    pub(super) fn load_bucket_handle_from_snapshot(
        &self,
        snapshot: BucketSnapshot,
        expected_bucket_owner: Option<&str>,
        request: BucketHandleRequest,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let bucket_execution_generation = snapshot.bucket.bucket_execution_generation;
        let bucket_incarnation_generation = snapshot.bucket.bucket_incarnation_generation;
        let bucket = Coordinator::validate_expected_bucket_owner(
            Coordinator::bucket_summary(snapshot.bucket),
            expected_bucket_owner,
        )?
        .into_inner();

        Ok(LoadedBucketHandle::new(
            bucket,
            bucket_execution_generation,
            bucket_incarnation_generation,
            request,
            LoadedBucketSubresources::new(
                Self::from_storage_subresource(snapshot.policy),
                Self::from_storage_tags(snapshot.tags),
                Self::from_storage_subresource(snapshot.lifecycle),
                Self::from_storage_subresource(snapshot.cors),
            ),
        ))
    }

    fn from_storage_subresource(
        value: storage::LoadedBucketSubresource<String>,
    ) -> LoadedBucketValue<String> {
        match value {
            storage::LoadedBucketSubresource::NotRequested => LoadedBucketValue::NotRequested,
            storage::LoadedBucketSubresource::Missing => LoadedBucketValue::Missing,
            storage::LoadedBucketSubresource::Loaded(value) => LoadedBucketValue::Loaded(value),
        }
    }

    fn from_storage_tags(
        value: storage::LoadedBucketSubresource<storage::SerializedBucketTagSet>,
    ) -> LoadedBucketValue<s3_types::TagSet> {
        match value {
            storage::LoadedBucketSubresource::NotRequested => LoadedBucketValue::NotRequested,
            storage::LoadedBucketSubresource::Missing => LoadedBucketValue::Missing,
            storage::LoadedBucketSubresource::Loaded(value) => {
                LoadedBucketValue::Loaded(value.tag_set().clone())
            }
        }
    }

    pub(super) fn map_bucket_snapshot_error(
        error: storage::BucketSnapshotLoadFailure,
    ) -> ServerError {
        Coordinator::map_bucket_snapshot_load_error(error)
    }
}

impl Coordinator {
    pub(super) const fn bucket_handle_loader(&self) -> BucketHandleLoader<'_> {
        BucketHandleLoader::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::test_support::{
        bucket_request_with_expected_owner, bucket_tag_set, put_bucket_lifecycle_test,
        put_bucket_policy_test, setup_coordinator, test_requester,
    };
    use crate::coordinator::{
        CreateBucketAcl, CreateBucketRequest, PutBucketAbacRequest, PutBucketTagsRequest,
    };
    use s3_types::BucketNamespace;
    use storage::BucketObjectOwnership;
    use test_util::tempdir;

    fn create_bucket(coord: &Coordinator, name: &str) {
        coord
            .create_bucket(&CreateBucketRequest {
                name: BucketName::try_from(name).unwrap(),
                requester: test_requester(),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
    }

    #[test]
    fn load_bucket_handle_fetches_only_requested_subresources() {
        let tmp = tempdir();
        let coord = setup_coordinator(tmp.path());
        create_bucket(&coord, "bucket");
        put_bucket_policy_test(
            &coord,
            "bucket",
            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::bucket/*\"}]}",
            test_requester(),
            None,
        )
        .unwrap();
        coord
            .put_bucket_tags(&PutBucketTagsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                tags: bucket_tag_set("<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>"),
            })
            .unwrap();
        put_bucket_lifecycle_test(
            &coord,
            "bucket",
            "<LifecycleConfiguration><Rule><ID>r1</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
            test_requester(),
            None,
        )
        .unwrap();

        let loaded = coord
            .bucket_handle_loader()
            .load_bucket(
                &BucketName::try_from("bucket").unwrap(),
                None,
                BucketHandleRequest::new()
                    .requiring_policy_view()
                    .requiring_bucket_tags(),
            )
            .unwrap();

        assert!(matches!(loaded.policy(), LoadedBucketValue::Loaded(_)));
        assert!(matches!(loaded.tags(), LoadedBucketValue::Loaded(_)));
        assert!(matches!(
            loaded.lifecycle(),
            LoadedBucketValue::NotRequested
        ));
        assert!(matches!(loaded.cors(), LoadedBucketValue::NotRequested));
    }

    #[test]
    fn load_bucket_handle_loads_tags_when_abac_enabled() {
        let tmp = tempdir();
        let coord = setup_coordinator(tmp.path());
        create_bucket(&coord, "bucket");
        coord
            .put_bucket_tags(&PutBucketTagsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                tags: bucket_tag_set("<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>"),
            })
            .unwrap();
        coord
            .put_bucket_abac(&PutBucketAbacRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                enabled: true,
            })
            .unwrap();

        let loaded = coord
            .bucket_handle_loader()
            .load_bucket(
                &BucketName::try_from("bucket").unwrap(),
                None,
                BucketHandleRequest::new().requiring_bucket_tags_if_abac_enabled(),
            )
            .unwrap();

        assert!(matches!(loaded.tags(), LoadedBucketValue::Loaded(_)));
    }

    #[test]
    fn load_bucket_handle_skips_tags_when_abac_disabled() {
        let tmp = tempdir();
        let coord = setup_coordinator(tmp.path());
        create_bucket(&coord, "bucket");
        coord
            .put_bucket_tags(&PutBucketTagsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                tags: bucket_tag_set("<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>"),
            })
            .unwrap();

        let loaded = coord
            .bucket_handle_loader()
            .load_bucket(
                &BucketName::try_from("bucket").unwrap(),
                None,
                BucketHandleRequest::new().requiring_bucket_tags_if_abac_enabled(),
            )
            .unwrap();

        assert!(matches!(loaded.tags(), LoadedBucketValue::NotRequested));
    }

    #[test]
    fn load_bucket_handle_marks_missing_requested_subresources() {
        let tmp = tempdir();
        let coord = setup_coordinator(tmp.path());
        create_bucket(&coord, "bucket");

        let loaded = coord
            .bucket_handle_loader()
            .load_bucket(
                &BucketName::try_from("bucket").unwrap(),
                None,
                BucketHandleRequest::new()
                    .requiring_policy_view()
                    .requiring_lifecycle_view()
                    .requiring_cors_view(),
            )
            .unwrap();

        assert!(matches!(loaded.policy(), LoadedBucketValue::Missing));
        assert!(matches!(loaded.lifecycle(), LoadedBucketValue::Missing));
        assert!(matches!(loaded.cors(), LoadedBucketValue::Missing));
        assert!(matches!(loaded.tags(), LoadedBucketValue::NotRequested));
    }

    #[test]
    fn loaded_object_handle_borrows_bucket_handle() {
        let tmp = tempdir();
        let coord = setup_coordinator(tmp.path());
        create_bucket(&coord, "bucket");

        let loaded = coord
            .bucket_handle_loader()
            .load_bucket(
                &BucketName::try_from("bucket").unwrap(),
                None,
                BucketHandleRequest::new(),
            )
            .unwrap();
        let object = loaded.load_object(ObjectKey::try_from("key").unwrap());

        assert_eq!(object.bucket().bucket().name.as_str(), "bucket");
        assert_eq!(object.key().as_str(), "key");
        assert_eq!(object.version_id(), None);
    }

    #[test]
    fn load_bucket_handle_checks_expected_owner() {
        let tmp = tempdir();
        let coord = setup_coordinator(tmp.path());
        create_bucket(&coord, "bucket");

        let err = coord
            .bucket_handle_loader()
            .load_bucket(
                &BucketName::try_from("bucket").unwrap(),
                Some("999988887777"),
                BucketHandleRequest::new(),
            )
            .unwrap_err();

        assert!(matches!(err, ServerError::AccessDenied));
    }
}
