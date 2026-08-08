use std::fmt;
use std::sync::Arc;

use super::TestStorageFailure;
use crate::control_plane;
use crate::{
    BucketName, ClusterEpoch, GenerationId, ObjectKey, ObjectPgActionError, PgId, PgState,
    RouteMapValidity, SessionId, StorageCluster, StorageClusterRuntimeMapHandle, StoredObject,
    StreamUploadTarget,
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

    fn test_find_object_keys_on_metadata_pgs_in_scan_order(
        &self,
        bucket: &BucketName,
        prefixes: &[&str],
    ) -> Option<Vec<ObjectKey>>;

    fn test_find_object_key_on_same_metadata_pg_as(
        &self,
        bucket: &BucketName,
        reference: &ObjectKey,
        prefix: &str,
    ) -> Option<ObjectKey>;

    fn test_find_object_keys_on_same_metadata_pg(
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
    ) -> Result<bool, TestStorageFailure>;

    fn test_stream_put_session_crosses_metadata_and_data_pgs(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, TestStorageFailure>;

    fn test_clone_with_stale_current_pg_routes(
        &self,
    ) -> Result<Arc<StorageCluster>, TestStorageTopologyScenarioError>;

    fn test_all_pg_primaries_differ_from(&self, previous: &StorageCluster) -> bool;
}

/// Storage-owned runtime-map transitions for cross-crate behavioral tests.
///
/// The runtime-map handle is the sole topology authority: the scenario derives
/// the current cluster, local stores, routes, and epoch from this exact
/// publication domain.
pub trait StorageClusterRuntimeMapTopologyTestSupport {
    fn test_install_same_epoch_topology_refresh(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError>;

    fn test_install_next_epoch_with_retained_routes(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError>;

    fn test_install_same_epoch_with_changed_primaries(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError>;

    fn test_install_next_epoch_with_changed_primaries(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError>;

    fn test_install_next_epoch_with_object_metadata_pg_peering(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError>;
}

pub(crate) fn current_object_has_metadata_pg_after_data_pg_raw(
    cluster: &StorageCluster,
    bucket: &BucketName,
    key: &ObjectKey,
) -> Result<bool, ObjectPgActionError> {
    let generation_id = match cluster.test_get_object_meta(bucket, key)? {
        StoredObject::Live(record) => record.generation_id,
        StoredObject::DeleteMarker(_) => {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "topology observation requires a current live object".to_string(),
            });
        }
    };
    Ok(cluster.test_object_pg_id_for(bucket, key)
        > cluster.test_data_pg_id_for(bucket, key, generation_id))
}

pub(crate) fn stream_put_session_crosses_metadata_and_data_pgs_raw(
    cluster: &StorageCluster,
    bucket: &BucketName,
    key: &ObjectKey,
    session_id: &SessionId,
) -> Result<bool, ObjectPgActionError> {
    let is_exact_put_session = cluster
        .test_list_all_stream_uploads()?
        .into_iter()
        .any(|session| {
            session.bucket == *bucket
                && session.key == *key
                && session.session_id == *session_id
                && session.target == StreamUploadTarget::PutObject
        });
    if !is_exact_put_session {
        return Err(ObjectPgActionError::InvalidRequest {
            reason: "topology observation requires the exact PutObject stream session".to_string(),
        });
    }
    let generation_id = cluster.test_object_generation_reservation_for(bucket, key, session_id)?;
    Ok(cluster.test_object_pg_id_for(bucket, key)
        != cluster.test_data_pg_id_for(bucket, key, generation_id))
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

    fn test_find_object_keys_on_metadata_pgs_in_scan_order(
        &self,
        bucket: &BucketName,
        prefixes: &[&str],
    ) -> Option<Vec<ObjectKey>> {
        let scan_pg_ids = self.metadata_pg_ids();
        if prefixes.len() > scan_pg_ids.len() {
            return None;
        }
        prefixes
            .iter()
            .zip(scan_pg_ids)
            .map(|(prefix, target_pg_id)| {
                find_test_object_key(self, bucket, prefix, |object_pg_id, _| {
                    object_pg_id == target_pg_id
                })
            })
            .collect()
    }

    fn test_find_object_key_on_same_metadata_pg_as(
        &self,
        bucket: &BucketName,
        reference: &ObjectKey,
        prefix: &str,
    ) -> Option<ObjectKey> {
        let reference_pg_id = self.test_object_pg_id_for(bucket, reference);
        find_test_object_key(self, bucket, prefix, |object_pg_id, _| {
            object_pg_id == reference_pg_id
        })
    }

    fn test_find_object_keys_on_same_metadata_pg(
        &self,
        bucket: &BucketName,
        prefixes: &[&str],
    ) -> Option<Vec<ObjectKey>> {
        let Some((first_prefix, remaining_prefixes)) = prefixes.split_first() else {
            return Some(Vec::new());
        };
        let first = find_test_object_key(self, bucket, first_prefix, |_, _| true)?;
        let mut keys = Vec::with_capacity(prefixes.len());
        keys.push(first.clone());
        for prefix in remaining_prefixes {
            keys.push(self.test_find_object_key_on_same_metadata_pg_as(bucket, &first, prefix)?);
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
    ) -> Result<bool, TestStorageFailure> {
        current_object_has_metadata_pg_after_data_pg_raw(self, bucket, key)
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    fn test_stream_put_session_crosses_metadata_and_data_pgs(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, TestStorageFailure> {
        stream_put_session_crosses_metadata_and_data_pgs_raw(self, bucket, key, session_id)
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    fn test_clone_with_stale_current_pg_routes(
        &self,
    ) -> Result<Arc<StorageCluster>, TestStorageTopologyScenarioError> {
        let next_epoch = next_cluster_epoch(self)?;
        let next_routes = route_snapshots(self, next_epoch, PrimarySelection::Preserve)?;
        let historical_routes = retained_route_snapshots(self);
        let stale_routes = route_snapshots(self, self.cluster_epoch(), PrimarySelection::Preserve)?;
        self.test_clone_with_stale_current_pg_routes_from_snapshots(
            next_epoch,
            next_routes,
            historical_routes,
            stale_routes,
        )
        .map_err(|error| {
            TestStorageTopologyScenarioError::new(
                "construct stale-current-route topology scenario",
                error,
            )
        })
    }

    fn test_all_pg_primaries_differ_from(&self, previous: &StorageCluster) -> bool {
        self.local_pg_routes().all(|route| {
            previous
                .local_pg_route(route.pg_id())
                .is_some_and(|previous_route| {
                    previous_route.primary_node_id() != route.primary_node_id()
                })
        })
    }
}

impl StorageClusterRuntimeMapTopologyTestSupport for StorageClusterRuntimeMapHandle {
    fn test_install_same_epoch_topology_refresh(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
        install_topology_refresh(self, EpochSelection::Preserve, PrimarySelection::Preserve)
    }

    fn test_install_next_epoch_with_retained_routes(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
        install_topology_refresh(self, EpochSelection::Advance, PrimarySelection::Preserve)
    }

    fn test_install_same_epoch_with_changed_primaries(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
        install_topology_refresh(self, EpochSelection::Preserve, PrimarySelection::Change)
    }

    fn test_install_next_epoch_with_changed_primaries(
        &self,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
        install_topology_refresh(self, EpochSelection::Advance, PrimarySelection::Change)
    }

    fn test_install_next_epoch_with_object_metadata_pg_peering(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
        install_next_epoch_with_object_metadata_pg_peering(self, bucket, key, || {})
    }
}

#[derive(Clone, Copy)]
enum EpochSelection {
    Preserve,
    Advance,
}

#[derive(Clone, Copy)]
enum PrimarySelection {
    Preserve,
    Change,
}

fn install_topology_refresh(
    runtime_handle: &StorageClusterRuntimeMapHandle,
    epoch_selection: EpochSelection,
    primary_selection: PrimarySelection,
) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
    let current = runtime_handle.current();
    let target_epoch = match epoch_selection {
        EpochSelection::Preserve => current.cluster_epoch(),
        EpochSelection::Advance => next_cluster_epoch(&current)?,
    };
    let routes = route_snapshots(&current, target_epoch, primary_selection)?;
    let historical_routes = retained_route_snapshots(&current);
    let candidate = current
        .test_clone_with_pg_routes(target_epoch, routes, historical_routes)
        .map_err(|error| {
            TestStorageTopologyScenarioError::new("construct topology refresh", error)
        })?;
    candidate.test_store_route_map_validity(
        RouteMapValidity::until_ms(u64::MAX - 1)
            .expect("maximum finite route-map validity is valid"),
    );
    let installed = runtime_handle
        .test_install_if_current(&current, candidate)
        .map_err(|error| {
            TestStorageTopologyScenarioError::new("publish topology refresh", error)
        })?;
    if !installed {
        return Err(TestStorageTopologyScenarioError::new(
            "publish topology refresh",
            "runtime-map generation changed while the scenario was being constructed",
        ));
    }
    Ok(target_epoch)
}

fn next_cluster_epoch(
    cluster: &StorageCluster,
) -> Result<ClusterEpoch, TestStorageTopologyScenarioError> {
    cluster
        .cluster_epoch()
        .get()
        .checked_add(1)
        .and_then(ClusterEpoch::new)
        .ok_or_else(|| {
            TestStorageTopologyScenarioError::new(
                "advance topology scenario epoch",
                "cluster epoch overflowed",
            )
        })
}

fn route_snapshots(
    cluster: &StorageCluster,
    target_epoch: ClusterEpoch,
    primary_selection: PrimarySelection,
) -> Result<Vec<control_plane::PgRouteSnapshot>, TestStorageTopologyScenarioError> {
    cluster
        .local_pg_routes()
        .map(|route| {
            let primary = match primary_selection {
                PrimarySelection::Preserve => route.primary_node_id(),
                PrimarySelection::Change => route
                    .acting_set()
                    .iter()
                    .copied()
                    .find(|node_id| *node_id != route.primary_node_id())
                    .ok_or_else(|| {
                        TestStorageTopologyScenarioError::new(
                            "select changed-primary topology scenario route",
                            format!("PG {} has no alternate acting-set node", route.pg_id()),
                        )
                    })?,
            };
            Ok(
                control_plane::PgRouteSnapshot::test_reconstructed_with_metadata_read_route(
                    target_epoch,
                    route.pg_id(),
                    primary,
                    route.acting_set().to_vec(),
                    route.state(),
                    route.metadata_read_route(),
                ),
            )
        })
        .collect()
}

fn retained_route_snapshots(cluster: &StorageCluster) -> Vec<control_plane::PgRouteSnapshot> {
    let mut routes = cluster
        .test_historical_pg_routes()
        .cloned()
        .collect::<Vec<_>>();
    routes.extend(
        route_snapshots(cluster, cluster.cluster_epoch(), PrimarySelection::Preserve)
            .expect("preserving current topology cannot fail"),
    );
    routes
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
    let next_epoch = next_cluster_epoch(&current)?;
    let routes = current
        .local_pg_routes()
        .map(|route| {
            control_plane::PgRouteSnapshot::test_reconstructed_with_metadata_read_route(
                next_epoch,
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                if route.pg_id() == PgId::new(peering_pg) {
                    PgState::Peering
                } else {
                    route.state()
                },
                route.metadata_read_route(),
            )
        })
        .collect::<Vec<_>>();
    let historical_routes = retained_route_snapshots(&current);
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
    fn semantic_key_group_selection_pins_same_and_distinct_metadata_placement() {
        let tmp = test_util::tempdir();
        let cluster = dynamic_test_cluster(tmp.path());
        let bucket = BucketName::try_from("semantic-key-placement").unwrap();

        let distinct = cluster
            .test_find_object_keys_on_distinct_metadata_pgs(&bucket, &["a/", "b/"])
            .expect("two-PG topology must provide distinct key placements");
        assert_ne!(
            cluster.test_object_pg_id_for(&bucket, &distinct[0]),
            cluster.test_object_pg_id_for(&bucket, &distinct[1])
        );

        let same = cluster
            .test_find_object_keys_on_same_metadata_pg(&bucket, &["one/", "two/", "three/"])
            .expect("topology must provide keys sharing one metadata PG");
        let same_pg = cluster.test_object_pg_id_for(&bucket, &same[0]);
        assert!(same
            .iter()
            .all(|key| cluster.test_object_pg_id_for(&bucket, key) == same_pg));

        let matched = cluster
            .test_find_object_key_on_same_metadata_pg_as(&bucket, &distinct[1], "matched/")
            .expect("topology must provide a key matching the reference placement");
        assert_eq!(
            cluster.test_object_pg_id_for(&bucket, &matched),
            cluster.test_object_pg_id_for(&bucket, &distinct[1])
        );
        assert!(matched.as_str().starts_with("matched/"));
    }

    #[test]
    fn scan_order_key_selection_follows_sorted_metadata_pg_scan_order() {
        let tmp = test_util::tempdir();
        let cluster = StorageCluster::open_static_local_nodes(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[9, 2, 5],
            EcShape { k: 2, m: 1 },
        )
        .unwrap();
        let bucket = BucketName::try_from("scan-ordered-key-placement").unwrap();

        let keys = cluster
            .test_find_object_keys_on_metadata_pgs_in_scan_order(
                &bucket,
                &["first/", "middle/", "last/"],
            )
            .expect("three-PG topology must provide one key per scan position");
        let selected_pg_ids = keys
            .iter()
            .map(|key| cluster.test_object_pg_id_for(&bucket, key))
            .collect::<Vec<_>>();

        assert_eq!(selected_pg_ids, [2, 5, 9]);
        assert!(keys[0].as_str().starts_with("first/"));
        assert!(keys[1].as_str().starts_with("middle/"));
        assert!(keys[2].as_str().starts_with("last/"));
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

    #[test]
    fn same_epoch_refresh_preserves_certified_peering_read_route() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let peering_pg = initial.local_pg_routes().next().unwrap().pg_id();
        let read_route = crate::control_plane::PgMetadataReadRoute::new(
            initial
                .local_pg_route(peering_pg)
                .unwrap()
                .primary_node_id(),
            crate::control_plane::PgMetadataProof::empty(),
        );
        let routes = initial
            .local_pg_routes()
            .map(|route| {
                control_plane::PgRouteSnapshot::test_reconstructed_with_metadata_read_route(
                    route.cluster_epoch(),
                    route.pg_id(),
                    route.primary_node_id(),
                    route.acting_set().to_vec(),
                    if route.pg_id() == peering_pg {
                        PgState::Peering
                    } else {
                        route.state()
                    },
                    (route.pg_id() == peering_pg).then_some(read_route),
                )
            })
            .collect::<Vec<_>>();
        let peering = initial
            .test_clone_with_pg_routes(
                initial.cluster_epoch(),
                routes,
                retained_route_snapshots(&initial),
            )
            .unwrap();
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&peering)).unwrap();

        handle.test_install_same_epoch_topology_refresh().unwrap();

        let refreshed = handle.current();
        let refreshed_route = refreshed.local_pg_route(peering_pg).unwrap();
        assert_eq!(refreshed_route.state(), PgState::Peering);
        assert_eq!(refreshed_route.metadata_read_route(), Some(read_route));
    }

    #[test]
    fn peering_transition_preserves_all_older_retained_epochs() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let initial_epoch = initial.cluster_epoch();
        let retained_pg = initial.local_pg_routes().next().unwrap().pg_id();
        let current = next_epoch_active_clone(&initial);
        let current_epoch = current.cluster_epoch();
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&current)).unwrap();
        let bucket = BucketName::try_from("peering-retained-history").unwrap();
        let key = key_distinct_from_bucket_pg(&current, &bucket);

        let installed_epoch = handle
            .test_install_next_epoch_with_object_metadata_pg_peering(&bucket, &key)
            .unwrap();

        assert_eq!(installed_epoch.get(), current_epoch.get() + 1);
        let installed = handle.current();
        assert_eq!(
            installed
                .reconstructed_pg_route_at_epoch(retained_pg, initial_epoch)
                .unwrap()
                .cluster_epoch(),
            initial_epoch
        );
        assert_eq!(
            installed
                .reconstructed_pg_route_at_epoch(retained_pg, current_epoch)
                .unwrap()
                .cluster_epoch(),
            current_epoch
        );
    }

    #[test]
    fn next_epoch_scenarios_return_an_error_at_epoch_overflow() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let max_epoch = ClusterEpoch::new(u64::MAX).unwrap();
        let max_epoch_cluster = initial
            .test_clone_with_pg_routes(
                max_epoch,
                route_snapshots(&initial, max_epoch, PrimarySelection::Preserve).unwrap(),
                retained_route_snapshots(&initial),
            )
            .unwrap();
        let handle = StorageClusterRuntimeMapHandle::new(max_epoch_cluster).unwrap();
        let bucket = BucketName::try_from("peering-overflow").unwrap();
        let key = key_distinct_from_bucket_pg(&handle.current(), &bucket);

        let refresh_error = handle
            .test_install_next_epoch_with_retained_routes()
            .unwrap_err();
        assert_eq!(
            refresh_error.to_string(),
            "advance topology scenario epoch: cluster epoch overflowed"
        );
        let peering_error = handle
            .test_install_next_epoch_with_object_metadata_pg_peering(&bucket, &key)
            .unwrap_err();
        assert_eq!(
            peering_error.to_string(),
            "advance topology scenario epoch: cluster epoch overflowed"
        );
    }

    #[test]
    fn topology_refresh_scenarios_preserve_runtime_identity_and_retained_routes() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial)).unwrap();
        let initial_epoch = initial.cluster_epoch();
        let retained_pg = initial.local_pg_routes().next().unwrap().pg_id();
        let initial_registry = initial.process_local_registry_key();

        let same_epoch = handle.test_install_same_epoch_topology_refresh().unwrap();
        let refreshed = handle.current();
        assert_eq!(same_epoch, initial_epoch);
        assert_eq!(refreshed.cluster_epoch(), initial_epoch);
        assert_eq!(refreshed.process_local_registry_key(), initial_registry);
        assert!(!Arc::ptr_eq(&refreshed, &initial));
        assert!(!refreshed.test_all_pg_primaries_differ_from(&initial));

        let next_epoch = handle
            .test_install_next_epoch_with_retained_routes()
            .unwrap();
        let advanced = handle.current();
        assert_eq!(next_epoch.get(), initial_epoch.get() + 1);
        assert_eq!(advanced.process_local_registry_key(), initial_registry);
        let retained = advanced
            .reconstructed_pg_route_at_epoch(retained_pg, initial_epoch)
            .expect("next-epoch refresh must retain the prior route");
        assert_eq!(retained.cluster_epoch(), initial_epoch);
        assert_eq!(retained.pg_id(), retained_pg);
    }

    #[test]
    fn changed_primary_scenarios_move_every_route_without_reopening_storage() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial)).unwrap();
        let initial_registry = initial.process_local_registry_key();

        handle
            .test_install_same_epoch_with_changed_primaries()
            .unwrap();
        let same_epoch = handle.current();
        assert_eq!(same_epoch.cluster_epoch(), initial.cluster_epoch());
        assert_eq!(same_epoch.process_local_registry_key(), initial_registry);
        assert!(same_epoch.test_all_pg_primaries_differ_from(&initial));

        handle
            .test_install_next_epoch_with_changed_primaries()
            .unwrap();
        let next_epoch = handle.current();
        assert_eq!(
            next_epoch.cluster_epoch().get(),
            same_epoch.cluster_epoch().get() + 1
        );
        assert_eq!(next_epoch.process_local_registry_key(), initial_registry);
        assert!(next_epoch.test_all_pg_primaries_differ_from(&same_epoch));
    }

    #[test]
    fn changed_primary_scenario_rejects_a_single_node_acting_set() {
        let tmp = test_util::tempdir();
        let initial = StorageCluster::open_static_local_nodes(
            tmp.path(),
            &[NodeId::new(0)],
            &[0],
            EcShape { k: 1, m: 0 },
        )
        .unwrap()
        .test_clone_with_dynamic_route_map_validity(
            RouteMapValidity::until_ms(u64::MAX - 1).unwrap(),
        )
        .unwrap();
        let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial)).unwrap();

        let error = handle
            .test_install_next_epoch_with_changed_primaries()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("has no alternate acting-set node"),
            "unexpected scenario error: {error}"
        );
        assert!(Arc::ptr_eq(&handle.current(), &initial));
    }

    #[test]
    fn stale_current_route_scenario_preserves_storage_but_crosses_route_epoch() {
        let tmp = test_util::tempdir();
        let initial = dynamic_test_cluster(tmp.path());
        let bucket = BucketName::try_from("stale-current-route-authority").unwrap();
        create_test_bucket(&initial, &bucket);
        let stale = initial.test_clone_with_stale_current_pg_routes().unwrap();

        assert_eq!(
            stale.process_local_registry_key(),
            initial.process_local_registry_key()
        );
        assert_eq!(
            stale.cluster_epoch().get(),
            initial.cluster_epoch().get() + 1
        );
        assert!(stale
            .local_pg_routes()
            .all(|route| route.cluster_epoch() == initial.cluster_epoch()));
        assert!(
            stale.test_route_authority_digest_matches_local_map(),
            "stale routes must be installed before the route-authority digest is minted"
        );
        let error = stale.head_bucket_info(&bucket).unwrap_err();
        assert_eq!(
            error.kind(),
            &crate::BucketSnapshotLoadFailureKind::SlowDown
        );
        assert_eq!(error.diagnostic_cause_label(), "store_topology_failure");
    }
}
