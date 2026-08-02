use super::*;
use crate::node_client::{ObjectPayloadLeaseKind, ObjectPayloadLeaseRoute};
use crate::StorageClusterRouteHandle;

struct PayloadLeaseUnavailableClient {
    inner: Arc<dyn ObjectPayloadLeaseNodeClient>,
}

struct PayloadLeaseUnavailableRoute<'a> {
    inner: Box<dyn ObjectPayloadLeaseRoute + 'a>,
}

impl ObjectPayloadLeaseNodeClient for PayloadLeaseUnavailableClient {
    fn open_object_payload_lease_route(
        &self,
        route_cluster_epoch: ClusterEpoch,
        bucket: &crate::BucketName,
        key: &crate::ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Box<dyn ObjectPayloadLeaseRoute + '_>, StoreError> {
        Ok(Box::new(PayloadLeaseUnavailableRoute {
            inner: self.inner.open_object_payload_lease_route(
                route_cluster_epoch,
                bucket,
                key,
                generation_id,
            )?,
        }))
    }
}

impl ObjectPayloadLeaseRoute for PayloadLeaseUnavailableRoute<'_> {
    fn acquire_object_payload_lease(
        &self,
        _kind: ObjectPayloadLeaseKind,
    ) -> Result<Option<Box<dyn ObjectPayloadLeaseNodeLease>>, StoreError> {
        Err(StoreError::Io {
            context: "connect storage-node object-payload lease RPC endpoint",
            source: std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        })
    }

    fn try_begin_object_payload_reclaim(
        &self,
        authority: &crate::metadata_command::ObjectPayloadReclaimClaimProof,
    ) -> Result<bool, StoreError> {
        self.inner.try_begin_object_payload_reclaim(authority)
    }

    fn object_payload_lease_count(&self) -> Result<usize, StoreError> {
        self.inner.object_payload_lease_count()
    }
}

fn retained_read_with_unavailable_lease_nodes(
    unavailable_node_ids: &[NodeId],
) -> Result<Vec<u8>, StoreError> {
    let tmp = test_util::tempdir();
    let mut map = LocalClusterMap::open(
        tmp.path(),
        &trace_node_ids(),
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    for node_id in unavailable_node_ids {
        let inner = Arc::clone(map.node(*node_id).unwrap().object_payload_lease_client());
        map.replace_object_payload_lease_client_for_tests(
            *node_id,
            Arc::new(PayloadLeaseUnavailableClient { inner }),
        );
    }
    let map = Arc::new(map);
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let payload = b"retained EC read excludes every unleased shard location";
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        payload,
    );

    let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_object_read_route(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let outcome = route
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    let segment = outcome.snapshot().object_segments[0].clone();
    let generation_id = outcome.snapshot().stored.as_live().unwrap().generation_id;
    let (_, _, leased_snapshot) = outcome.into_parts();
    let retained = match route.retain_object_payload_read(leased_snapshot) {
        Ok(retained) => retained,
        Err(error) => {
            assert_eq!(
                cluster.object_payload_lease_holder_node_count(&bucket, &key, generation_id),
                0,
                "failed handoff must release broad and partially acquired narrow leases"
            );
            return Err(error);
        }
    };
    let retained = retained.expect("live object should retain payload authority");

    let unavailable = unavailable_node_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let _read_guard =
        cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(move |location, _| {
            assert!(
                !unavailable.contains(&location.node_id()),
                "retained read must not access an unleased shard location"
            );
            Ok(())
        }));
    let mut bytes = Vec::new();
    retained.read_segment_payload_stored_bytes_into(&segment, &mut bytes)?;
    drop(retained);
    assert_eq!(
        cluster.object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        0,
        "retained read must release every successfully acquired node lease"
    );
    Ok(bytes)
}

#[test]
fn retained_read_reconstructs_from_exact_successfully_leased_node_subset() {
    let bytes =
        retained_read_with_unavailable_lease_nodes(&[NodeId::new(0), NodeId::new(1)]).unwrap();
    assert_eq!(
        bytes,
        b"retained EC read excludes every unleased shard location"
    );
}

#[test]
fn retained_read_rejects_a_leased_node_subset_below_ec_k() {
    let error = retained_read_with_unavailable_lease_nodes(&[
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
    ])
    .unwrap_err();
    assert!(matches!(error, StoreError::NotFound));
}

#[test]
fn retained_object_payload_read_binds_the_complete_logical_segment_layout() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"retained payload",
    );

    let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_object_read_route(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let outcome = route
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    let segment = outcome
        .snapshot()
        .object_segments
        .first()
        .expect("test object should have one segment")
        .clone();
    let generation_id = outcome.snapshot().stored.as_live().unwrap().generation_id;
    let other_bucket = crate::BucketName::try_from("other-bucket".to_string()).unwrap();
    let other_key = crate::ObjectKey::try_from("other-key".to_string()).unwrap();
    let other_generation = GenerationId::new(generation_id.get() + 1).unwrap();
    for (lease_bucket, lease_key, lease_generation) in [
        (&other_bucket, &key, generation_id),
        (&bucket, &other_key, generation_id),
        (&bucket, &key, other_generation),
    ] {
        let error = match cluster.acquire_object_payload_read_lease(
            lease_bucket,
            lease_key,
            lease_generation,
            [&segment],
        ) {
            Ok(_) => panic!("crossed payload segment subject unexpectedly acquired a lease"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::PayloadShardSetMismatch { .. }));
    }
    let (_, snapshot, leased_snapshot) = outcome.into_parts();
    assert!(Arc::ptr_eq(&snapshot, &leased_snapshot.snapshot));
    let mut retained = route
        .retain_object_payload_read(leased_snapshot)
        .unwrap()
        .expect("live object should retain payload authority");
    let mut bytes = Vec::new();
    retained
        .read_segment_payload_stored_bytes_into(&segment, &mut bytes)
        .unwrap();
    assert_eq!(bytes, b"retained payload");
    assert!(retained.covers_complete_object_payload_layout(
        &bucket,
        &key,
        generation_id,
        [&segment]
    ));
    let second = segment.with_test_segment_index(segment.segment_index() + 1);
    retained.segments.push(second.clone());
    assert!(retained.covers_complete_object_payload_layout(
        &bucket,
        &key,
        generation_id,
        [&segment, &second]
    ));
    assert!(!retained.covers_complete_object_payload_layout(
        &bucket,
        &key,
        generation_id,
        [&segment]
    ));

    let crossed = segment.with_test_segment_index(segment.segment_index() + 2);
    assert!(!retained.covers_complete_object_payload_layout(
        &bucket,
        &key,
        generation_id,
        [&crossed, &second]
    ));
    let error = retained
        .read_segment_payload_stored_bytes_into(&crossed, &mut Vec::new())
        .unwrap_err();
    assert!(matches!(error, StoreError::PayloadShardSetMismatch { .. }));
}

#[test]
fn retained_object_payload_read_rejects_an_omitted_snapshot_segment() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"retained payload",
    );
    let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let route = admission
        .active_object_read_route(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let outcome = route
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    let (_, shared_snapshot, mut leased_snapshot) = outcome.into_parts();
    drop(shared_snapshot);
    Arc::make_mut(&mut leased_snapshot.snapshot)
        .object_segments
        .clear();

    let error = match route.retain_object_payload_read(leased_snapshot) {
        Ok(_) => panic!("incomplete payload snapshot unexpectedly retained read authority"),
        Err(error) => error,
    };

    assert!(matches!(error, StoreError::PayloadShardSetMismatch { .. }));
}

#[test]
fn stale_cluster_rejects_an_opaque_payload_segment_before_reading() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"payload",
    );
    let outcome = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::StandardSegments,
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();
    let segment = outcome.snapshot.object_segments.first().unwrap();
    let stale_cluster = StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&map),
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap();

    let error = stale_cluster
        .read_object_payload_segment_stored_bytes_into(segment, &mut Vec::new())
        .unwrap_err();

    assert!(matches!(error, StoreError::StalePayloadOperation { .. }));
}

#[test]
fn retained_object_payload_handoff_rejects_another_version_of_the_same_key() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let older = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [1; 16],
        [1; 16],
        b"older",
    );
    let newer = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [2; 16],
        [2; 16],
        b"newer",
    );
    assert_ne!(older.version_id, newer.version_id);

    let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let older_route = admission
        .active_object_read_route(
            &bucket,
            &key,
            Some(older.version_id),
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let (_, _snapshot, older_handoff) = older_route
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap()
        .into_parts();
    let newer_route = admission
        .active_object_read_route(
            &bucket,
            &key,
            Some(newer.version_id),
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
    let error = match newer_route.retain_object_payload_read(older_handoff) {
        Ok(_) => panic!("another version's payload handoff must be rejected"),
        Err(error) => error,
    };
    assert!(matches!(error, StoreError::PayloadShardSetMismatch { .. }));
}

#[test]
fn object_read_snapshot_fails_closed_while_metadata_pg_is_peering() {
    let tmp = test_util::tempdir();
    let mut map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    drop(cluster);

    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(0))
        .unwrap()
        .state = PgState::Peering;
    let cluster = current_cluster(&map);

    let err = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |_stored| Ok::<_, ()>(()),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));
}

#[test]
fn object_read_snapshot_retries_when_object_changes_after_auth_subject_load() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let original = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    let mutated = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let outcome = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |stored| {
                call_count.set(call_count.get() + 1);
                let generation_id = stored
                    .as_live()
                    .expect("test object should be live")
                    .generation_id;
                if !mutated.replace(true) {
                    assert_eq!(generation_id, original.generation_id);
                    write_committed_direct_segment_for_with_versioning(
                        &cluster,
                        &bucket,
                        &key,
                        crate::BucketVersioningState::Disabled,
                        [2; 16],
                        [2; 16],
                        b"second",
                    );
                }
                Ok::<_, ()>(generation_id)
            },
        )
        .unwrap()
        .unwrap();

    let snapshot_generation = outcome
        .snapshot
        .stored
        .as_live()
        .expect("snapshot should contain the replacement live object")
        .generation_id;
    assert_eq!(call_count.get(), 2);
    assert_ne!(snapshot_generation, original.generation_id);
    assert_eq!(outcome.value, snapshot_generation);
}

#[test]
fn leased_object_read_snapshot_retries_when_subject_changes_before_exact_snapshot() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let original = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    let mutated = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let outcome = cluster
        .load_leased_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            |stored| {
                call_count.set(call_count.get() + 1);
                let generation_id = stored
                    .as_live()
                    .expect("test object should be live")
                    .generation_id;
                if !mutated.replace(true) {
                    assert_eq!(generation_id, original.generation_id);
                    write_committed_direct_segment_for_with_versioning(
                        &cluster,
                        &bucket,
                        &key,
                        crate::BucketVersioningState::Disabled,
                        [2; 16],
                        [2; 16],
                        b"second",
                    );
                }
                Ok::<_, ()>(generation_id)
            },
        )
        .unwrap()
        .unwrap();

    let snapshot_generation = outcome
        .snapshot()
        .stored
        .as_live()
        .expect("snapshot should contain the replacement live object")
        .generation_id;
    assert_eq!(call_count.get(), 2);
    assert_ne!(snapshot_generation, original.generation_id);
    let (value, _snapshot, _leased_snapshot) = outcome.into_parts();
    assert_eq!(value, snapshot_generation);
}

#[test]
fn leased_object_read_snapshot_blocks_reclaim_until_handoff() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let original = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );

    let outcome = cluster
        .load_leased_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();
    let (_, _snapshot, leased_snapshot) = outcome.into_parts();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [2; 16],
        [2; 16],
        b"second",
    );
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, original.generation_id)
        .unwrap());
    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, original.generation_id)
            .unwrap(),
        "broad snapshot lease must block reclaim before the read-handle lease handoff"
    );

    drop(leased_snapshot);
    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, original.generation_id)
            .unwrap(),
        "reclaim should proceed after the broad snapshot lease is released"
    );
}

#[test]
fn object_read_snapshot_retries_when_object_disappears_after_auth_subject_load() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    let deleted = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let err = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |stored| {
                call_count.set(call_count.get() + 1);
                assert!(stored.as_live().is_some());
                if !deleted.replace(true) {
                    let object_pg = map
                        .node(NodeId::new(0))
                        .unwrap()
                        .storage_node()
                        .get_pg(cluster.object_metadata_pg_id(&bucket, &key))
                        .unwrap();
                    crate::PgMetadataStore::delete_object_meta(&*object_pg, &bucket, &key).unwrap();
                }
                Ok::<_, ()>(())
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::ObjectPgActionError::Metadata(crate::MetadataError::ObjectNotFound)
    ));
    assert_eq!(call_count.get(), 1);
}

#[test]
fn object_read_snapshot_stale_retry_uses_time_budget() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"first",
    );
    const STALE_SNAPSHOTS: u8 = 24;
    let call_count = std::cell::Cell::new(0_u8);

    let outcome = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::MetadataOnly,
            |stored| {
                let attempt = call_count.get();
                call_count.set(attempt + 1);
                assert!(stored.as_live().is_some());
                if attempt < STALE_SNAPSHOTS {
                    let mutation_seed = attempt + 2;
                    write_committed_direct_segment_for_with_versioning(
                        &cluster,
                        &bucket,
                        &key,
                        crate::BucketVersioningState::Disabled,
                        [mutation_seed; 16],
                        [mutation_seed; 16],
                        &[mutation_seed],
                    );
                }
                Ok::<_, ()>(())
            },
        )
        .unwrap()
        .unwrap();

    assert!(outcome.snapshot.stored.as_live().is_some());
    assert_eq!(call_count.get(), STALE_SNAPSHOTS + 1);
}

#[test]
fn object_tag_read_retries_when_tags_change_after_auth_subject_load() {
    let tmp = test_util::tempdir();
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &trace_node_ids(),
            &[0],
            SharedStorageNode::DEFAULT_EC_SHAPE,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let written = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"tagged",
    );
    let mutated = std::cell::Cell::new(false);
    let call_count = std::cell::Cell::new(0);

    let tags = cluster
            .get_object_tags_if(&bucket, &key, None, |stored| {
                call_count.set(call_count.get() + 1);
                let live = stored.as_live().expect("test object should be live");
                assert_eq!(live.generation_id, written.generation_id);
                if !mutated.replace(true) {
                    cluster
                        .put_object_tags_if(
                            &bucket,
                            &key,
                            None,
                            "<Tagging><TagSet><Tag><Key>state</Key><Value>new</Value></Tag></TagSet></Tagging>",
                            |stored| {
                                Ok::<_, ()>(
                                    stored.as_live().expect("test object should be live").version_id,
                                )
                            },
                        )
                        .unwrap()
                        .unwrap();
                }
                Ok::<_, ()>(live.version_id)
            })
            .unwrap()
            .unwrap();

    assert_eq!(call_count.get(), 2);
    let expected = crate::tests::object_tags(
        "<Tagging><TagSet><Tag><Key>state</Key><Value>new</Value></Tag></TagSet></Tagging>",
    );
    assert_eq!(
        tags.as_ref().map(crate::SerializedTagSet::tag_set),
        Some(expected.tag_set())
    );
}
