use std::fmt;

use crate::control_plane;
use crate::{
    BucketName, ClusterEpoch, GenerationId, ObjectKey, ObjectPgActionError, PgId, PgState,
    SessionId, StorageCluster, StorageClusterRuntimeMapHandle, StoredObject, StreamUploadTarget,
};

/// Failure while constructing an opaque storage-topology test scenario.
#[derive(Debug)]
pub struct TestStorageTopologyScenarioError {
    context: &'static str,
    detail: String,
}

impl TestStorageTopologyScenarioError {
    fn new(context: &'static str, detail: impl fmt::Display) -> Self {
        Self {
            context,
            detail: detail.to_string(),
        }
    }
}

impl fmt::Display for TestStorageTopologyScenarioError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.context, self.detail)
    }
}

impl std::error::Error for TestStorageTopologyScenarioError {}

/// Storage-owned placement selection and topology transitions for
/// cross-crate behavioral tests.
///
/// No PG identifier, generation reservation, or route snapshot crosses this
/// boundary. Callers select only the semantic placement relation needed by
/// the behavior under test.
pub trait StorageClusterTopologyTestSupport {
    fn test_find_object_key_on_metadata_pg_distinct_from_bucket(
        &self,
        bucket: &BucketName,
        prefix: &str,
    ) -> Option<ObjectKey>;

    fn test_find_object_key_on_same_metadata_pg_as_bucket(
        &self,
        bucket: &BucketName,
        prefix: &str,
    ) -> Option<ObjectKey>;

    fn test_find_object_keys_on_distinct_metadata_pgs(
        &self,
        bucket: &BucketName,
        prefixes: &[&str],
    ) -> Option<Vec<ObjectKey>>;

    fn test_find_fresh_object_key_with_metadata_pg_after_data_pg(
        &self,
        bucket: &BucketName,
        prefix: &str,
    ) -> Option<ObjectKey>;

    fn test_current_object_has_metadata_pg_after_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<bool, ObjectPgActionError>;

    fn test_stream_put_session_crosses_metadata_and_data_pgs(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, ObjectPgActionError>;
}

/// Storage-owned runtime-map transitions for cross-crate behavioral tests.
///
/// The runtime-map handle is the sole topology authority: the scenario derives
/// the current cluster, local stores, routes, and epoch from this exact
/// publication domain.
pub trait StorageClusterRuntimeMapTopologyTestSupport {
    fn test_install_next_epoch_with_object_metadata_pg_peering(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError>;
}

impl StorageClusterTopologyTestSupport for StorageCluster {
    fn test_find_object_key_on_metadata_pg_distinct_from_bucket(
        &self,
        bucket: &BucketName,
        prefix: &str,
    ) -> Option<ObjectKey> {
        let bucket_pg_id = self.test_bucket_pg_id_for(bucket);
        find_test_object_key(self, bucket, prefix, |object_pg_id, _| {
            object_pg_id != bucket_pg_id
        })
    }

    fn test_find_object_key_on_same_metadata_pg_as_bucket(
        &self,
        bucket: &BucketName,
        prefix: &str,
    ) -> Option<ObjectKey> {
        let bucket_pg_id = self.test_bucket_pg_id_for(bucket);
        find_test_object_key(self, bucket, prefix, |object_pg_id, _| {
            object_pg_id == bucket_pg_id
        })
    }

    fn test_find_object_keys_on_distinct_metadata_pgs(
        &self,
        bucket: &BucketName,
        prefixes: &[&str],
    ) -> Option<Vec<ObjectKey>> {
        let mut selected_pg_ids = Vec::with_capacity(prefixes.len());
        let mut keys = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            let key = find_test_object_key(self, bucket, prefix, |object_pg_id, _| {
                !selected_pg_ids.contains(&object_pg_id)
            })?;
            selected_pg_ids.push(self.test_object_pg_id_for(bucket, &key));
            keys.push(key);
        }
        Some(keys)
    }

    fn test_find_fresh_object_key_with_metadata_pg_after_data_pg(
        &self,
        bucket: &BucketName,
        prefix: &str,
    ) -> Option<ObjectKey> {
        find_test_object_key(self, bucket, prefix, |object_pg_id, key| {
            object_pg_id > self.test_data_pg_id_for(bucket, key, GenerationId::MIN)
        })
    }

    fn test_current_object_has_metadata_pg_after_data_pg(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<bool, ObjectPgActionError> {
        let generation_id = match self.test_get_object_meta(bucket, key)? {
            StoredObject::Live(record) => record.generation_id,
            StoredObject::DeleteMarker(_) => {
                return Err(ObjectPgActionError::InvalidRequest {
                    reason: "topology observation requires a current live object".to_string(),
                });
            }
        };
        Ok(self.test_object_pg_id_for(bucket, key)
            > self.test_data_pg_id_for(bucket, key, generation_id))
    }

    fn test_stream_put_session_crosses_metadata_and_data_pgs(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, ObjectPgActionError> {
        let is_exact_put_session =
            self.test_list_all_stream_uploads()?
                .into_iter()
                .any(|session| {
                    session.bucket == *bucket
                        && session.key == *key
                        && session.session_id == *session_id
                        && session.target == StreamUploadTarget::PutObject
                });
        if !is_exact_put_session {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "topology observation requires the exact PutObject stream session"
                    .to_string(),
            });
        }
        let generation_id = self.test_object_generation_reservation_for(bucket, key, session_id)?;
        Ok(self.test_object_pg_id_for(bucket, key)
            != self.test_data_pg_id_for(bucket, key, generation_id))
    }
}

impl StorageClusterRuntimeMapTopologyTestSupport for StorageClusterRuntimeMapHandle {
    fn test_install_next_epoch_with_object_metadata_pg_peering(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
        install_next_epoch_with_object_metadata_pg_peering(self, bucket, key, || {})
    }
}

fn install_next_epoch_with_object_metadata_pg_peering(
    runtime_handle: &StorageClusterRuntimeMapHandle,
    bucket: &BucketName,
    key: &ObjectKey,
    before_install: impl FnOnce(),
) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
    let current = runtime_handle.current();
    let peering_pg = current.test_object_pg_id_for(bucket, key);
    if peering_pg == current.test_bucket_pg_id_for(bucket) {
        return Err(TestStorageTopologyScenarioError::new(
            "select Peering topology scenario route",
            "object and bucket metadata share one PG",
        ));
    }
    let next_epoch = ClusterEpoch::new(current.cluster_epoch().get() + 1).ok_or_else(|| {
        TestStorageTopologyScenarioError::new(
            "advance topology scenario epoch",
            "cluster epoch overflowed",
        )
    })?;
    let routes = current
        .local_pg_routes()
        .map(|route| {
            control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                if route.pg_id() == PgId::new(peering_pg) {
                    PgState::Peering
                } else {
                    route.state()
                },
            )
        })
        .collect::<Vec<_>>();
    let historical_routes = current
        .local_pg_routes()
        .map(|route| {
            control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let candidate = current
        .test_clone_with_pg_routes(next_epoch, routes, historical_routes)
        .map_err(|error| {
            TestStorageTopologyScenarioError::new(
                "construct Peering topology scenario cluster",
                error,
            )
        })?;
    before_install();
    let installed = runtime_handle
        .test_install_if_current(&current, candidate)
        .map_err(|error| {
            TestStorageTopologyScenarioError::new("publish Peering topology scenario", error)
        })?;
    if !installed {
        return Err(TestStorageTopologyScenarioError::new(
            "publish Peering topology scenario",
            "runtime-map generation changed while the scenario was being constructed",
        ));
    }
    let installed = runtime_handle.current();
    let installed_route = installed
        .local_pg_route(PgId::new(peering_pg))
        .ok_or_else(|| {
            TestStorageTopologyScenarioError::new(
                "load installed Peering topology scenario route",
                "selected object metadata PG is absent",
            )
        })?;
    if installed_route.state() != PgState::Peering {
        return Err(TestStorageTopologyScenarioError::new(
            "validate installed Peering topology scenario route",
            format!("expected Peering, got {:?}", installed_route.state()),
        ));
    }
    Ok(next_epoch)
}

fn find_test_object_key(
    cluster: &StorageCluster,
    bucket: &BucketName,
    prefix: &str,
    mut predicate: impl FnMut(u32, &ObjectKey) -> bool,
) -> Option<ObjectKey> {
    for suffix in 0..10_000 {
        let key = ObjectKey::try_from(format!("{prefix}-{suffix:04}")).ok()?;
        if predicate(cluster.test_object_pg_id_for(bucket, &key), &key) {
            return Some(key);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        AclGrants, BucketObjectLockConfig, BucketObjectOwnership, BucketOwnershipControls,
        BucketVersioningState, CanonicalUserId, CreateBucketConfig, EcShape, NodeId,
        RouteMapValidity,
    };

    fn create_test_bucket(cluster: &StorageCluster, bucket: &BucketName) {
        let owner = CanonicalUserId::from_principal("owner");
        cluster
            .create_bucket_with_config_and_load_info(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: "owner",
                owner_canonical_id: &owner,
                acl_grants: &AclGrants::default(),
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Disabled,
                object_lock: BucketObjectLockConfig::default(),
                ownership_controls: BucketOwnershipControls {
                    object_ownership: BucketObjectOwnership::ObjectWriter,
                },
            })
            .unwrap();
    }

    fn dynamic_test_cluster(root: &std::path::Path) -> Arc<StorageCluster> {
        StorageCluster::open_static_local_nodes(
            root,
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0, 1],
            EcShape { k: 2, m: 1 },
        )
        .unwrap()
        .test_clone_with_dynamic_route_map_validity(
            RouteMapValidity::until_ms(u64::MAX - 1).unwrap(),
        )
        .unwrap()
    }

    fn key_distinct_from_bucket_pg(cluster: &StorageCluster, bucket: &BucketName) -> ObjectKey {
        cluster
            .test_find_object_key_on_metadata_pg_distinct_from_bucket(bucket, "peering")
            .expect("two PGs must provide a key outside the bucket metadata PG")
    }

    fn next_epoch_active_clone(cluster: &StorageCluster) -> Arc<StorageCluster> {
        let next_epoch = ClusterEpoch::new(cluster.cluster_epoch().get() + 1).unwrap();
        let routes = cluster
            .local_pg_routes()
            .map(|route| {
                control_plane::PgRouteSnapshot::reconstructed(
                    next_epoch,
                    route.pg_id(),
                    route.primary_node_id(),
                    route.acting_set().to_vec(),
                    route.state(),
                )
            })
            .collect::<Vec<_>>();
        let historical_routes = cluster
            .local_pg_routes()
            .map(|route| {
                control_plane::PgRouteSnapshot::reconstructed(
                    route.cluster_epoch(),
                    route.pg_id(),
                    route.primary_node_id(),
                    route.acting_set().to_vec(),
                    route.state(),
                )
            })
            .collect::<Vec<_>>();
        cluster
            .test_clone_with_pg_routes(next_epoch, routes, historical_routes)
            .unwrap()
    }

    #[test]
    fn process_local_route_authority_clone_preserves_store_and_retained_route_history() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let initial_epoch = initial.cluster_epoch();
        let retained_pg = initial.local_pg_routes().next().unwrap().pg_id();
        let bucket = BucketName::try_from("process-local-route-clone").unwrap();
        create_test_bucket(&initial, &bucket);
        let current = next_epoch_active_clone(&initial);
        let current_epoch = current.cluster_epoch();
        let clone = crate::test_support::StorageClusterRouteMapTestSupport::test_clone_with_dynamic_route_map_validity(
                current.as_ref(),
                RouteMapValidity::until_ms(u64::MAX - 1).unwrap(),
            )
            .unwrap();
        drop(initial);

        assert_eq!(clone.cluster_epoch(), current_epoch);
        assert_eq!(
            clone.process_local_registry_key(),
            current.process_local_registry_key(),
            "the clone must share the source runtime instead of reopening its stores"
        );
        assert!(clone.head_bucket_info(&bucket).is_ok());
        let retained = clone
            .reconstructed_pg_route_at_epoch(retained_pg, initial_epoch)
            .expect("process-local clone must retain the prior epoch route");
        assert_eq!(retained.cluster_epoch(), initial_epoch);
        assert_eq!(retained.pg_id(), retained_pg);
    }

    #[test]
    fn peering_scenario_publishes_only_through_its_receiver_handle() {
        let tmp = test_util::tempdir();
        let first = dynamic_test_cluster(&tmp.path().join("first"));
        let second = dynamic_test_cluster(&tmp.path().join("second"));
        let first_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&first)).unwrap();
        let second_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&second)).unwrap();
        let bucket = BucketName::try_from("peering-crossed-handle").unwrap();
        let key = key_distinct_from_bucket_pg(&first, &bucket);

        first_handle
            .test_install_next_epoch_with_object_metadata_pg_peering(&bucket, &key)
            .unwrap();

        assert_eq!(
            first_handle.current().cluster_epoch().get(),
            first.cluster_epoch().get() + 1
        );
        assert!(Arc::ptr_eq(&second_handle.current(), &second));
        assert_eq!(
            second_handle.current().cluster_epoch(),
            second.cluster_epoch()
        );
        assert!(second_handle
            .current()
            .local_pg_routes()
            .all(|route| route.state() == PgState::Active));
    }

    #[test]
    fn peering_scenario_rejects_stale_generation_without_publication() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial)).unwrap();
        let bucket = BucketName::try_from("peering-stale-generation").unwrap();
        let key = key_distinct_from_bucket_pg(&initial, &bucket);
        let peering_pg = PgId::new(initial.test_object_pg_id_for(&bucket, &key));
        let replacement = next_epoch_active_clone(&initial);
        let publishing_handle = handle.clone();
        let published_replacement = Arc::clone(&replacement);

        let error =
            install_next_epoch_with_object_metadata_pg_peering(&handle, &bucket, &key, move || {
                publishing_handle.install(published_replacement).unwrap()
            })
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "publish Peering topology scenario: runtime-map generation changed while the scenario was being constructed"
        );
        assert!(Arc::ptr_eq(&handle.current(), &replacement));
        assert_eq!(
            handle.current().local_pg_route(peering_pg).unwrap().state(),
            PgState::Active,
            "the stale Peering candidate must not replace the intervening generation"
        );
    }
}
