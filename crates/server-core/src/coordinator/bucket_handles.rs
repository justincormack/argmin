#![allow(dead_code)]

use s3_types::VersionId;
use storage::{
    BucketName, BucketSnapshot, BucketSnapshotPair, BucketSnapshotRequest,
    BucketSnapshotTagsRequest, ObjectKey,
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

    const fn resolve_to_storage_request(self) -> BucketSnapshotRequest {
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
#[derive(Debug, PartialEq, Eq)]
pub(super) struct LoadedBucketHandle {
    bucket: BucketSummary,
    request: BucketHandleRequest,
    policy: LoadedBucketValue<String>,
    tags: LoadedBucketValue<String>,
    lifecycle: LoadedBucketValue<String>,
    cors: LoadedBucketValue<String>,
}

impl LoadedBucketHandle {
    pub(super) fn new(
        bucket: BucketSummary,
        request: BucketHandleRequest,
        policy: LoadedBucketValue<String>,
        tags: LoadedBucketValue<String>,
        lifecycle: LoadedBucketValue<String>,
        cors: LoadedBucketValue<String>,
    ) -> Self {
        Self {
            bucket,
            request,
            policy,
            tags,
            lifecycle,
            cors,
        }
    }

    pub(super) const fn bucket(&self) -> &BucketSummary {
        &self.bucket
    }

    pub(super) const fn request(&self) -> BucketHandleRequest {
        self.request
    }

    pub(super) const fn policy(&self) -> &LoadedBucketValue<String> {
        &self.policy
    }

    pub(super) const fn tags(&self) -> &LoadedBucketValue<String> {
        &self.tags
    }

    pub(super) const fn lifecycle(&self) -> &LoadedBucketValue<String> {
        &self.lifecycle
    }

    pub(super) const fn cors(&self) -> &LoadedBucketValue<String> {
        &self.cors
    }

    /// Phase-0 object-handle skeleton.
    ///
    /// This borrow-based API makes the intended derivation shape explicit
    /// before request-family migrations start.
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

/// Phase-0 skeleton for object derivation from a loaded bucket handle.
#[derive(Debug, PartialEq, Eq)]
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

/// Ordered source/destination bucket handles for two-bucket request paths.
///
/// Same-bucket source/destination flows deliberately collapse to one
/// underlying bucket handle with the union of both roles' declared needs.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum LoadedBucketPair {
    Same {
        bucket: Box<LoadedBucketHandle>,
    },
    Distinct {
        source: Box<LoadedBucketHandle>,
        destination: Box<LoadedBucketHandle>,
    },
}

impl LoadedBucketPair {
    fn same(bucket: LoadedBucketHandle) -> Self {
        Self::Same {
            bucket: Box::new(bucket),
        }
    }

    fn distinct(source: LoadedBucketHandle, destination: LoadedBucketHandle) -> Self {
        Self::Distinct {
            source: Box::new(source),
            destination: Box::new(destination),
        }
    }

    pub(super) const fn source(&self) -> &LoadedBucketHandle {
        match self {
            Self::Same { bucket } => bucket,
            Self::Distinct { source, .. } => source,
        }
    }

    pub(super) const fn destination(&self) -> &LoadedBucketHandle {
        match self {
            Self::Same { bucket } => bucket,
            Self::Distinct { destination, .. } => destination,
        }
    }
}

/// Phase-0 storage-boundary loader for request-scoped bucket handles.
///
/// Request paths are meant to talk to this loader, not to PGs. It is the
/// place where same-bucket coalescing and dual-bucket acquisition ordering
/// live before later phases migrate real request families over.
///
/// This is still transitional scaffolding: it centralizes bucket loading for
/// phase 0, but it is not yet the final single-use request-family entry point
/// that will make duplicate bucket acquisition structurally impossible.
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
        let snapshot = self
            .coordinator
            .storage_node
            .load_bucket_snapshot(name, request.resolve_to_storage_request())
            .map_err(Self::map_bucket_snapshot_error)?;
        self.load_bucket_handle_from_snapshot(snapshot, expected_bucket_owner, request)
    }

    pub(super) fn load_bucket_pair(
        self,
        source: (&BucketName, Option<&str>, BucketHandleRequest),
        destination: (&BucketName, Option<&str>, BucketHandleRequest),
    ) -> Result<LoadedBucketPair, ServerError> {
        if source.0 == destination.0 {
            let merged_request = source.2.merge(destination.2);
            let snapshot = self
                .coordinator
                .storage_node
                .load_bucket_snapshot(source.0, merged_request.resolve_to_storage_request())
                .map_err(Self::map_bucket_snapshot_error)?;
            let bucket =
                self.load_bucket_handle_from_snapshot(snapshot, source.1, merged_request)?;
            Coordinator::ensure_expected_bucket_owner(bucket.bucket(), destination.1)?;
            return Ok(LoadedBucketPair::same(bucket));
        }

        let snapshots = self
            .coordinator
            .storage_node
            .load_bucket_snapshot_pair(
                (source.0, source.2.resolve_to_storage_request()),
                (destination.0, destination.2.resolve_to_storage_request()),
            )
            .map_err(Self::map_bucket_snapshot_error)?;
        let (source_handle, destination_handle) = match snapshots {
            BucketSnapshotPair::Same { bucket } => (
                self.load_bucket_handle_from_snapshot((*bucket).clone(), source.1, source.2)?,
                self.load_bucket_handle_from_snapshot(*bucket, destination.1, destination.2)?,
            ),
            BucketSnapshotPair::Distinct {
                source: source_snapshot,
                destination: destination_snapshot,
            } => (
                self.load_bucket_handle_from_snapshot(*source_snapshot, source.1, source.2)?,
                self.load_bucket_handle_from_snapshot(
                    *destination_snapshot,
                    destination.1,
                    destination.2,
                )?,
            ),
        };

        Ok(LoadedBucketPair::distinct(
            source_handle,
            destination_handle,
        ))
    }

    fn load_bucket_handle_from_snapshot(
        &self,
        snapshot: BucketSnapshot,
        expected_bucket_owner: Option<&str>,
        request: BucketHandleRequest,
    ) -> Result<LoadedBucketHandle, ServerError> {
        self.coordinator
            .storage_node
            .upsert_bucket_fast_path((&snapshot.bucket).into());
        let bucket = Coordinator::validate_expected_bucket_owner(
            Coordinator::bucket_summary(snapshot.bucket),
            expected_bucket_owner,
        )?
        .into_inner();

        Ok(LoadedBucketHandle::new(
            bucket,
            request,
            Self::from_storage_subresource(snapshot.policy),
            Self::from_storage_subresource(snapshot.tags),
            Self::from_storage_subresource(snapshot.lifecycle),
            Self::from_storage_subresource(snapshot.cors),
        ))
    }

    pub(super) fn load_bucket_handle_from_reserved_pg(
        &self,
        bucket: BucketSummary,
        request: BucketHandleRequest,
        bucket_pg: &storage::PgStore,
    ) -> Result<LoadedBucketHandle, ServerError> {
        let policy = if request.policy_view() {
            Self::from_direct_subresource(Coordinator::load_bucket_subresource_from_pg(
                bucket_pg,
                &bucket.name,
                storage::BucketSubresourceKind::Policy,
            )?)
        } else {
            LoadedBucketValue::NotRequested
        };

        let tags = match request.resolve_to_storage_request().tags {
            BucketSnapshotTagsRequest::NotRequested => LoadedBucketValue::NotRequested,
            BucketSnapshotTagsRequest::IfBucketAbacEnabled if !bucket.bucket_abac_enabled => {
                LoadedBucketValue::NotRequested
            }
            BucketSnapshotTagsRequest::IfBucketAbacEnabled | BucketSnapshotTagsRequest::Always => {
                Self::from_direct_subresource(Coordinator::load_bucket_subresource_from_pg(
                    bucket_pg,
                    &bucket.name,
                    storage::BucketSubresourceKind::Tagging,
                )?)
            }
        };

        let lifecycle = if request.lifecycle_view() {
            Self::from_direct_subresource(Coordinator::load_bucket_subresource_from_pg(
                bucket_pg,
                &bucket.name,
                storage::BucketSubresourceKind::Lifecycle,
            )?)
        } else {
            LoadedBucketValue::NotRequested
        };

        let cors = if request.cors_view() {
            Self::from_direct_subresource(Coordinator::load_bucket_subresource_from_pg(
                bucket_pg,
                &bucket.name,
                storage::BucketSubresourceKind::Cors,
            )?)
        } else {
            LoadedBucketValue::NotRequested
        };

        Ok(LoadedBucketHandle::new(
            bucket, request, policy, tags, lifecycle, cors,
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

    fn from_direct_subresource(value: Option<String>) -> LoadedBucketValue<String> {
        match value {
            Some(value) => LoadedBucketValue::Loaded(value),
            None => LoadedBucketValue::Missing,
        }
    }

    fn map_bucket_snapshot_error(error: storage::BucketSnapshotLoadError) -> ServerError {
        match error {
            storage::BucketSnapshotLoadError::Store(error) => ServerError::Store(error),
            storage::BucketSnapshotLoadError::Metadata(error) => match error {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            },
        }
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
        bucket_request_with_expected_owner, put_bucket_lifecycle_test, put_bucket_policy_test,
        setup_coordinator_with_pg_count, test_requester,
    };
    use crate::coordinator::{
        CreateBucketAcl, CreateBucketRequest, PutBucketAbacRequest, PutBucketConfigRequest,
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
        let coord = setup_coordinator_with_pg_count(tmp.path(), 4);
        create_bucket(&coord, "bucket");
        put_bucket_policy_test(
            &coord,
            "bucket",
            "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
            test_requester(),
            None,
        )
        .unwrap();
        coord
            .put_bucket_tags(&PutBucketConfigRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                config: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
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
        let coord = setup_coordinator_with_pg_count(tmp.path(), 4);
        create_bucket(&coord, "bucket");
        coord
            .put_bucket_tags(&PutBucketConfigRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                config: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
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
        let coord = setup_coordinator_with_pg_count(tmp.path(), 4);
        create_bucket(&coord, "bucket");
        coord
            .put_bucket_tags(&PutBucketConfigRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                config: "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
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
        let coord = setup_coordinator_with_pg_count(tmp.path(), 4);
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
    fn load_bucket_pair_handles_preserves_source_destination_roles() {
        let tmp = tempdir();
        let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
        create_bucket(&coord, "source");
        create_bucket(&coord, "destination");
        put_bucket_policy_test(
            &coord,
            "source",
            "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
            test_requester(),
            None,
        )
        .unwrap();
        coord
            .put_bucket_tags(&PutBucketConfigRequest {
                bucket: bucket_request_with_expected_owner("destination", test_requester(), None),
                config: "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            })
            .unwrap();

        let loaded = coord
            .bucket_handle_loader()
            .load_bucket_pair(
                (
                    &BucketName::try_from("source").unwrap(),
                    None,
                    BucketHandleRequest::new().requiring_policy_view(),
                ),
                (
                    &BucketName::try_from("destination").unwrap(),
                    None,
                    BucketHandleRequest::new().requiring_bucket_tags(),
                ),
            )
            .unwrap();

        assert_eq!(loaded.source().bucket().name.as_str(), "source");
        assert_eq!(loaded.destination().bucket().name.as_str(), "destination");
        assert!(matches!(
            loaded.source().policy(),
            LoadedBucketValue::Loaded(_)
        ));
        assert!(matches!(
            loaded.source().tags(),
            LoadedBucketValue::NotRequested
        ));
        assert!(matches!(
            loaded.destination().tags(),
            LoadedBucketValue::Loaded(_)
        ));
        assert!(matches!(
            loaded.destination().policy(),
            LoadedBucketValue::NotRequested
        ));
    }

    #[test]
    fn load_bucket_pair_same_bucket_uses_one_underlying_handle() {
        let tmp = tempdir();
        let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
        create_bucket(&coord, "bucket");
        put_bucket_policy_test(
            &coord,
            "bucket",
            "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
            test_requester(),
            None,
        )
        .unwrap();
        coord
            .put_bucket_tags(&PutBucketConfigRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                config: "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            })
            .unwrap();

        let loaded = coord
            .bucket_handle_loader()
            .load_bucket_pair(
                (
                    &BucketName::try_from("bucket").unwrap(),
                    None,
                    BucketHandleRequest::new().requiring_policy_view(),
                ),
                (
                    &BucketName::try_from("bucket").unwrap(),
                    None,
                    BucketHandleRequest::new().requiring_bucket_tags(),
                ),
            )
            .unwrap();

        assert!(matches!(loaded, LoadedBucketPair::Same { .. }));
        assert!(std::ptr::eq(loaded.source(), loaded.destination()));
        assert!(matches!(
            loaded.source().policy(),
            LoadedBucketValue::Loaded(_)
        ));
        assert!(matches!(
            loaded.source().tags(),
            LoadedBucketValue::Loaded(_)
        ));
    }

    #[test]
    fn load_bucket_pair_same_bucket_validates_both_expected_owners() {
        let tmp = tempdir();
        let coord = setup_coordinator_with_pg_count(tmp.path(), 1);
        create_bucket(&coord, "bucket");

        let err = coord
            .bucket_handle_loader()
            .load_bucket_pair(
                (
                    &BucketName::try_from("bucket").unwrap(),
                    Some(test_requester().principal_opt().unwrap()),
                    BucketHandleRequest::new(),
                ),
                (
                    &BucketName::try_from("bucket").unwrap(),
                    Some("999988887777"),
                    BucketHandleRequest::new(),
                ),
            )
            .unwrap_err();

        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn loaded_object_handle_borrows_bucket_handle() {
        let tmp = tempdir();
        let coord = setup_coordinator_with_pg_count(tmp.path(), 4);
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
        let coord = setup_coordinator_with_pg_count(tmp.path(), 4);
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
