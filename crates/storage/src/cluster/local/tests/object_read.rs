// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::node_client::{ObjectPayloadLeaseKind, ObjectPayloadLeaseRoute};
use crate::{
    BucketFastPathInfo, BucketSnapshotLoadError, ObjectEncryptionSubject, ObjectPgActionError,
    SegmentEncryptionSubject, StorageClusterRouteHandle,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn embedded_peering_metadata_read_rechecks_certified_proof_under_pg_lock() {
    let tmp = test_util::tempdir();
    let node_ids = trace_node_ids();
    let certified_node = node_ids[0];
    let pg_id = PgId::new(0);
    let mut map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[pg_id.get()],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let proof = map
        .node(certified_node)
        .unwrap()
        .test_node()
        .pg_heartbeat_observation(certified_node, pg_id, PgState::Peering)
        .unwrap()
        .metadata_proof;
    let peering_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = peering_epoch;
    map.pg_routes.insert(
        pg_id,
        LocalPgRoute {
            cluster_epoch: peering_epoch,
            pg_id,
            primary_node_id: certified_node,
            acting_set: Arc::from(node_ids),
            state: PgState::Peering,
            metadata_read_route: Some(crate::control_plane::PgMetadataReadRoute::new(
                certified_node,
                proof,
            )),
        },
    );
    let map = Arc::new(map);
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("embedded-peering-proof-bucket");
    let key = crate::tests::object_key("embedded-peering-proof-key");

    assert!(cluster
        .load_existing_live_object(&bucket, &key)
        .unwrap()
        .is_none());

    {
        let pg = map
            .node(certified_node)
            .unwrap()
            .test_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.test_replace_pending_metadata_command_slot(
            &create_bucket_metadata_command(pg_id, 1, bucket.clone()),
            Some(&bucket),
        )
        .unwrap();
    }
    let error = cluster
        .load_existing_live_object(&bucket, &key)
        .unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ObjectReadFailureKind::RetryableConvergence
    );
    assert_eq!(error.diagnostic_cause_label(), "store_topology_failure");
    map.node(certified_node)
        .unwrap()
        .test_node()
        .get_pg(pg_id.get())
        .unwrap()
        .test_clear_pending_metadata_command_slot()
        .unwrap();

    {
        let pg = map
            .node(certified_node)
            .unwrap()
            .test_node()
            .get_pg(pg_id.get())
            .unwrap();
        crate::PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    let error = cluster
        .load_existing_live_object(&bucket, &key)
        .unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ObjectReadFailureKind::RetryableConvergence
    );
    assert_eq!(error.diagnostic_cause_label(), "store_topology_failure");
}

#[test]
fn embedded_peering_metadata_read_authorization_rejects_equal_proof_from_another_pg() {
    let tmp = test_util::tempdir();
    let node_ids = trace_node_ids();
    let certified_node = node_ids[0];
    let first_pg_id = PgId::new(0);
    let second_pg_id = PgId::new(1);
    let mut map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[first_pg_id.get(), second_pg_id.get()],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let first_proof = map
        .node(certified_node)
        .unwrap()
        .test_node()
        .pg_heartbeat_observation(certified_node, first_pg_id, PgState::Peering)
        .unwrap()
        .metadata_proof;
    let second_proof = map
        .node(certified_node)
        .unwrap()
        .test_node()
        .pg_heartbeat_observation(certified_node, second_pg_id, PgState::Peering)
        .unwrap()
        .metadata_proof;
    assert_eq!(
        first_proof, second_proof,
        "empty replicas should exercise equal-proof authorization substitution"
    );

    let peering_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = peering_epoch;
    for (pg_id, proof) in [(first_pg_id, first_proof), (second_pg_id, second_proof)] {
        map.pg_routes.insert(
            pg_id,
            LocalPgRoute {
                cluster_epoch: peering_epoch,
                pg_id,
                primary_node_id: certified_node,
                acting_set: Arc::from(node_ids),
                state: PgState::Peering,
                metadata_read_route: Some(crate::control_plane::PgMetadataReadRoute::new(
                    certified_node,
                    proof,
                )),
            },
        );
    }

    let first_node = map
        .metadata_pg_read_node(peering_epoch, first_pg_id)
        .unwrap();
    let second_node = map
        .metadata_pg_read_node(peering_epoch, second_pg_id)
        .unwrap();
    assert_eq!(first_node.node_id(), second_node.node_id());
    let route = second_node
        .bucket_metadata_client()
        .open_bucket_metadata_read_scan_route(
            peering_epoch,
            BucketPgId::new_for_test(second_pg_id),
            first_node.authorization(),
        )
        .unwrap();
    assert!(matches!(
        route.list_buckets("owner").unwrap_err(),
        BucketSnapshotLoadError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "use metadata read authorization for PG",
        })
    ));
}

struct PayloadLeaseUnavailableClient {
    inner: Arc<dyn ObjectPayloadLeaseNodeClient>,
    failure: PayloadLeaseUnavailableFailure,
    attempts: Option<Arc<AtomicUsize>>,
    recovered: Option<Arc<AtomicBool>>,
}

struct PayloadLeaseUnavailableRoute<'a> {
    inner: Box<dyn ObjectPayloadLeaseRoute + 'a>,
    failure: PayloadLeaseUnavailableFailure,
    attempts: Option<Arc<AtomicUsize>>,
    recovered: Option<Arc<AtomicBool>>,
}

#[derive(Clone, Copy)]
enum PayloadLeaseUnavailableFailure {
    Transport,
    ResourceExhausted,
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
            failure: self.failure,
            attempts: self.attempts.clone(),
            recovered: self.recovered.clone(),
        }))
    }
}

impl ObjectPayloadLeaseRoute for PayloadLeaseUnavailableRoute<'_> {
    fn acquire_object_payload_lease(
        &self,
        kind: ObjectPayloadLeaseKind,
    ) -> Result<Option<Box<dyn ObjectPayloadLeaseNodeLease>>, StoreError> {
        if let Some(attempts) = &self.attempts {
            attempts.fetch_add(1, Ordering::Relaxed);
        }
        if self
            .recovered
            .as_ref()
            .is_some_and(|recovered| recovered.load(Ordering::Relaxed))
        {
            return self.inner.acquire_object_payload_lease(kind);
        }
        match self.failure {
            PayloadLeaseUnavailableFailure::Transport => Err(StoreError::StorageRpc {
                node_id: 0,
                operation: "connect storage-node object-payload lease RPC endpoint",
                failure: crate::storage_rpc::StorageRpcErrorCode::TransportClosed,
                detail: crate::StorageNodeFailureDetail::new("injected unavailable lease node"),
            }),
            PayloadLeaseUnavailableFailure::ResourceExhausted => {
                Err(StoreError::storage_node_resource_exhausted(
                    0,
                    "broad object payload lease acquire",
                ))
            }
        }
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

#[test]
fn opaque_payload_shard_write_attempts_bind_originating_cluster_and_pin_attempt_order() {
    let tmp = test_util::tempdir();
    let unrelated_tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = current_cluster(&map);
    let unrelated_map = Arc::new(
        LocalClusterMap::open(
            unrelated_tmp.path(),
            &node_ids,
            &[0],
            EcShape { k: 2, m: 1 },
        )
        .unwrap(),
    );
    let unrelated_cluster = current_cluster(&unrelated_map);
    let bucket = crate::tests::bucket_name("opaque-shard-write-attempts");
    let key = crate::tests::object_key("committed");
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_hook = Arc::clone(&ordinals);
    let guard = crate::test_support::install_payload_shard_write_attempt_hook(
        &cluster,
        Arc::new(move |attempt| {
            ordinals_for_hook.lock().unwrap().push(attempt);
            Ok(())
        }),
    );

    write_committed_direct_segment_for(&cluster, &bucket, &key, b"committed payload");
    let attempts = guard.finish();

    assert_eq!(attempts.count(), 3);
    assert_eq!(*ordinals.lock().unwrap(), vec![1, 2, 3]);
    assert!(unrelated_cluster
        .load_existing_live_object(&bucket, &key)
        .unwrap()
        .is_none());
    assert!(
        !attempts.all_absent().unwrap(),
        "evidence must inspect its originating cluster rather than an empty same-topology cluster"
    );
}

#[test]
fn opaque_object_segment_faults_bind_the_captured_payload_subject() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("opaque-object-segment-faults");
    let route_key = crate::tests::object_key("route-subject");
    let route_pg = cluster.test_object_pg_id_for(&bucket, &route_key);
    let mut same_pg_keys = (0..10_000)
        .map(|index| crate::tests::object_key(format!("same-pg-subject-{index}")))
        .filter(|key| cluster.test_object_pg_id_for(&bucket, key) == route_pg);
    let checksum_key = same_pg_keys.next().expect("same-PG checksum key");
    let untouched_key = same_pg_keys.next().expect("same-PG untouched key");
    assert_ne!(checksum_key, untouched_key);
    assert_eq!(
        cluster.test_object_pg_id_for(&bucket, &checksum_key),
        route_pg
    );
    assert_eq!(
        cluster.test_object_pg_id_for(&bucket, &untouched_key),
        route_pg
    );

    let route_object =
        write_committed_direct_segment_for(&cluster, &bucket, &route_key, b"route payload");
    let checksum_object = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &checksum_key,
        crate::BucketVersioningState::Enabled,
        [2; 16],
        [42; 16],
        b"checksum payload",
    );
    assert!(checksum_object.version_id.is_versioned());
    let untouched_object = write_committed_direct_segment_for_with_okh(
        &cluster,
        &bucket,
        &untouched_key,
        [43; 16],
        b"untouched payload",
    );
    let route_metadata_before = cluster
        .test_get_object_version(&bucket, &route_key, route_object.version_id)
        .unwrap();
    let checksum_metadata_before = cluster
        .test_get_object_version(&bucket, &checksum_key, checksum_object.version_id)
        .unwrap();
    let untouched_metadata_before = cluster
        .test_get_object_version(&bucket, &untouched_key, untouched_object.version_id)
        .unwrap();
    let route_before = cluster
        .test_capture_object_payload(&bucket, &route_key, route_object.version_id)
        .unwrap();
    let checksum_before = cluster
        .test_capture_object_payload(&bucket, &checksum_key, checksum_object.version_id)
        .unwrap();
    let untouched_before = cluster
        .test_capture_object_payload(&bucket, &untouched_key, untouched_object.version_id)
        .unwrap();

    cluster
        .test_inject_object_payload_first_segment_unknown_data_pg(&route_before)
        .unwrap();
    cluster
        .test_inject_object_payload_first_segment_checksum_mismatch(&checksum_before)
        .unwrap();

    let route_after = cluster
        .test_capture_object_payload(&bucket, &route_key, route_object.version_id)
        .unwrap();
    let checksum_after = cluster
        .test_capture_object_payload(&bucket, &checksum_key, checksum_object.version_id)
        .unwrap();
    let untouched_after = cluster
        .test_capture_object_payload(&bucket, &untouched_key, untouched_object.version_id)
        .unwrap();
    let mut expected_route = route_before.segments().to_vec();
    expected_route[0].data_pg_id = u32::MAX;
    assert_eq!(route_after.segments(), expected_route);
    let mut expected_checksum = checksum_before.segments().to_vec();
    expected_checksum[0].segment_crc64 ^= 1;
    assert_eq!(checksum_after.segments(), expected_checksum);
    assert_eq!(untouched_after.segments(), untouched_before.segments());
    assert_eq!(
        cluster
            .test_get_object_version(&bucket, &route_key, route_object.version_id)
            .unwrap(),
        route_metadata_before
    );
    assert_eq!(
        cluster
            .test_get_object_version(&bucket, &checksum_key, checksum_object.version_id)
            .unwrap(),
        checksum_metadata_before
    );
    assert_eq!(
        cluster
            .test_get_object_version(&bucket, &untouched_key, untouched_object.version_id)
            .unwrap(),
        untouched_metadata_before
    );
}

#[test]
fn opaque_object_segment_fault_rejects_a_replaced_null_generation_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("opaque-object-segment-stale");
    let key = crate::tests::object_key("null-replacement");
    let first = write_committed_direct_segment_for(&cluster, &bucket, &key, b"first payload");
    let stale = cluster
        .test_capture_object_payload(&bucket, &key, first.version_id)
        .unwrap();
    let replacement = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [2; 16],
        [44; 16],
        b"replacement payload",
    );
    let current_before = cluster
        .test_capture_object_payload(&bucket, &key, replacement.version_id)
        .unwrap();

    assert!(matches!(
        cluster
            .test_inject_object_payload_first_segment_checksum_mismatch(&stale)
            .unwrap_err(),
        ObjectPgActionError::Store(StoreError::RouteCapabilitySubjectMismatch {
            operation: "inject exact live object segment fault for test scenario",
        })
    ));

    let current_after = cluster
        .test_capture_object_payload(&bucket, &key, replacement.version_id)
        .unwrap();
    assert_eq!(current_after.segments(), current_before.segments());
}

fn retained_read_with_unavailable_lease_nodes(
    unavailable_node_ids: &[NodeId],
) -> Result<RetainedReadTestOutcome, crate::ObjectReadFailure> {
    retained_read_with_unavailable_lease_nodes_and_failure(
        unavailable_node_ids,
        PayloadLeaseUnavailableFailure::Transport,
    )
}

#[derive(Debug)]
struct RetainedReadTestOutcome {
    bytes: Vec<u8>,
    expected_storage_bytes: Vec<u8>,
    read_shard_indices: Vec<u8>,
}

fn retained_read_with_unavailable_lease_nodes_and_failure(
    unavailable_node_ids: &[NodeId],
    failure: PayloadLeaseUnavailableFailure,
) -> Result<RetainedReadTestOutcome, crate::ObjectReadFailure> {
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
            Arc::new(PayloadLeaseUnavailableClient {
                inner,
                failure,
                attempts: None,
                recovered: None,
            }),
        );
    }
    let map = Arc::new(map);
    let cluster = current_cluster(&map);
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let payload = b"retained EC read excludes every unleased shard location";
    let committed = write_committed_direct_segment_for_with_versioning(
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
    let outcome = match route.load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(())) {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(())) => unreachable!("snapshot callback cannot fail"),
        Err(error) => return Err(error),
    };
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
    let read_shard_indices = Arc::new(Mutex::new(Vec::new()));
    let observed_read_shard_indices = Arc::clone(&read_shard_indices);
    let _read_guard =
        cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(move |location, _| {
            assert!(
                !unavailable.contains(&location.node_id()),
                "retained read must not access an unleased shard location"
            );
            observed_read_shard_indices
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(location.shard_index().get());
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
    let read_shard_indices = read_shard_indices
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    Ok(RetainedReadTestOutcome {
        bytes,
        expected_storage_bytes: committed.encrypted.storage_bytes,
        read_shard_indices,
    })
}

#[test]
fn retained_read_healthy_path_reads_only_the_data_shards() {
    let outcome = retained_read_with_unavailable_lease_nodes(&[]).unwrap();
    assert_eq!(outcome.bytes, outcome.expected_storage_bytes);
    assert_eq!(
        outcome.read_shard_indices,
        (0..SharedStorageNode::DEFAULT_EC_SHAPE.k).collect::<Vec<_>>(),
        "healthy retained reads must not materialize parity shards"
    );
}

#[test]
fn admitted_payload_read_route_mints_exact_segment_encryption_subjects() {
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
    let bucket = crate::tests::bucket_name("payload-read-subject-bucket");
    let key = crate::tests::object_key("payload-read-subject-key");
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"subject-bound payload");

    let handle = StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let mut bucket_snapshot = admission
        .active_bucket_route(&bucket)
        .unwrap()
        .load_bucket_snapshot(crate::BucketSnapshotRequest::default())
        .unwrap();
    let authoritative_incarnation = bucket_snapshot.bucket.bucket_incarnation_generation;
    let substituted_snapshot_incarnation = authoritative_incarnation.checked_add(100).unwrap();
    bucket_snapshot.bucket.bucket_incarnation_generation = substituted_snapshot_incarnation;
    let bucket_incarnation = bucket_snapshot.bucket_incarnation_read_subject();
    assert_eq!(
        bucket_incarnation.bucket_incarnation_generation(),
        authoritative_incarnation,
        "mutating the snapshot DTO must not mint substituted read authority"
    );
    let route = admission
        .active_object_payload_read_route(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            &bucket_incarnation,
        )
        .unwrap();
    let outcome = route
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    let live = outcome.snapshot().stored.as_live().unwrap();
    let segment = &outcome.snapshot().object_segments[0];
    let actual_subject = segment.encryption_subject().unwrap();
    let expected_subject = SegmentEncryptionSubject::standard_object(
        ObjectEncryptionSubject::standard_object(
            authoritative_incarnation,
            &bucket,
            &key,
            live.generation_id,
            &live.encryption,
        ),
        segment.segment_index(),
        segment.size(),
    );
    let substituted_subject = SegmentEncryptionSubject::standard_object(
        ObjectEncryptionSubject::standard_object(
            substituted_snapshot_incarnation,
            &bucket,
            &key,
            live.generation_id,
            &live.encryption,
        ),
        segment.segment_index(),
        segment.size(),
    );
    assert_eq!(
        actual_subject.payload_aead_context(),
        expected_subject.payload_aead_context()
    );
    assert_ne!(
        actual_subject.payload_aead_context(),
        substituted_subject.payload_aead_context()
    );

    let mut fast_path = BucketFastPathInfo::from(&bucket_snapshot);
    let substituted_fast_path_incarnation = authoritative_incarnation.checked_add(200).unwrap();
    fast_path.bucket_incarnation_generation = substituted_fast_path_incarnation;
    let fast_path_subject = fast_path.bucket_incarnation_read_subject();
    assert_eq!(
        fast_path_subject.bucket_incarnation_generation(),
        authoritative_incarnation,
        "mutating the fast-path DTO must not mint substituted read authority"
    );
    let fast_path_outcome = admission
        .active_object_payload_read_route(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            &fast_path_subject,
        )
        .unwrap()
        .load_leased_object_read_snapshot_if(|_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    let fast_path_segment = &fast_path_outcome.snapshot().object_segments[0];
    assert_eq!(
        fast_path_segment
            .encryption_subject()
            .unwrap()
            .payload_aead_context(),
        expected_subject.payload_aead_context()
    );
    assert_ne!(
        fast_path_segment
            .encryption_subject()
            .unwrap()
            .payload_aead_context(),
        SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(
                substituted_fast_path_incarnation,
                &bucket,
                &key,
                live.generation_id,
                &live.encryption,
            ),
            fast_path_segment.segment_index(),
            fast_path_segment.size(),
        )
        .payload_aead_context()
    );

    let other_bucket = crate::tests::bucket_name("payload-read-subject-other-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    admission
        .active_bucket_route(&other_bucket)
        .unwrap()
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: other_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let other_snapshot = admission
        .active_bucket_route(&other_bucket)
        .unwrap()
        .load_bucket_snapshot(crate::BucketSnapshotRequest::default())
        .unwrap();
    let error = match admission.active_object_payload_read_route(
        &bucket,
        &key,
        None,
        crate::ObjectReadSnapshotMode::FullPayloadLayout,
        &other_snapshot.bucket_incarnation_read_subject(),
    ) {
        Ok(_) => panic!("foreign bucket incarnation unexpectedly opened payload read route"),
        Err(error) => error,
    };
    assert!(matches!(
        error.retained_store_error(),
        Some(StoreError::RouteCapabilitySubjectMismatch {
            operation: "derive payload read route from bucket incarnation",
        })
    ));
}

#[test]
fn retained_read_uses_persisted_witness_without_ack_rows_and_identifies_multiple_corrupt_shards() {
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
    let bucket = crate::tests::bucket_name("retained-witness-bucket");
    let key = crate::tests::object_key("retained-witness-key");
    let committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"persisted integrity identifies two independently corrupted data shards",
    );
    let shard_keys = committed
        .written
        .written_shards
        .iter()
        .map(|written| written.key.clone())
        .collect::<Vec<_>>();
    cluster
        .test_delete_metadata_primary_payload_shard_acks(committed.written.data_pg_id, &shard_keys)
        .unwrap();
    let shard_size = committed
        .encrypted
        .storage_bytes
        .len()
        .div_ceil(usize::from(committed.written.ec.k));
    for shard_index in [0_u8, 1] {
        let path = cluster
            .test_payload_shard_file_path(
                committed.written.data_pg_id,
                committed.written.ec,
                &committed.segment_okh,
                committed.generation_id,
                shard_index,
            )
            .unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), shard_size);
        bytes[0] ^= 0x80;
        std::fs::write(path, bytes).unwrap();
    }

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
    assert!(segment.shard_integrity().is_some());
    let (_, _, leased_snapshot) = outcome.into_parts();
    let retained = route
        .retain_object_payload_read(leased_snapshot)
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    retained
        .read_segment_payload_stored_bytes_into(&segment, &mut bytes)
        .unwrap();
    assert_eq!(bytes, committed.encrypted.storage_bytes);

    let repairs = cluster
        .list_placed_segment_shard_repairs(committed.written.data_pg_id)
        .unwrap();
    assert_eq!(repairs.len(), 2);
    assert_eq!(
        repairs
            .iter()
            .map(|repair| repair.work_item.shard_index.get())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1])
    );
    assert!(repairs
        .iter()
        .all(|repair| repair.work_item.shard_integrity.is_some()));

    drop(retained);
    let repair_work = repairs[0].work_item.clone();
    let repaired = cluster
        .repair_placed_segment_payload_shards_if_needed_preserving_repair_rows(&repair_work)
        .expect("witness-backed repair must converge after loss of acknowledgement rows");
    assert_eq!(
        repaired
            .iter()
            .map(|written| written.key.shard_index().get())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1]),
        "repair must replace both shards identified by the durable witness"
    );
    assert!(
        cluster
            .placed_segment_payload_shard_repair_targets_with_integrity(
                repair_work.request,
                repair_work.shard_integrity.as_ref(),
            )
            .expect("witness-backed post-repair verification must succeed")
            .is_empty(),
        "completed witness-backed repair must leave no unhealthy shard targets"
    );
}

#[test]
fn retained_read_reconstructs_from_exact_successfully_leased_node_subset() {
    let outcome =
        retained_read_with_unavailable_lease_nodes(&[NodeId::new(0), NodeId::new(1)]).unwrap();
    assert_eq!(outcome.bytes, outcome.expected_storage_bytes);
}

#[test]
fn retained_read_rejects_a_leased_node_subset_below_ec_k() {
    let error = retained_read_with_unavailable_lease_nodes(&[
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
    ])
    .unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ObjectReadFailureKind::RetryableConvergence
    );
    assert_eq!(
        error.diagnostic_cause_label(),
        "store_rpc_transport_failure"
    );
}

#[test]
fn retained_read_preserves_transport_failure_when_no_lease_node_is_available() {
    let error = retained_read_with_unavailable_lease_nodes(&trace_node_ids()).unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ObjectReadFailureKind::RetryableConvergence
    );
    assert_eq!(
        error.diagnostic_cause_label(),
        "store_rpc_transport_failure"
    );
}

#[test]
fn retained_read_preserves_resource_exhaustion_when_no_lease_node_is_available() {
    let error = retained_read_with_unavailable_lease_nodes_and_failure(
        &trace_node_ids(),
        PayloadLeaseUnavailableFailure::ResourceExhausted,
    )
    .unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ObjectReadFailureKind::ResourceExhausted
    );
    assert_eq!(error.diagnostic_cause_label(), "store_resource_exhausted");
}

#[test]
fn retained_reads_do_not_reconnect_to_a_recently_unreachable_lease_node() {
    let tmp = test_util::tempdir();
    let node_ids = trace_node_ids();
    let unavailable_node = node_ids[0];
    let attempts = Arc::new(AtomicUsize::new(0));
    let recovered = Arc::new(AtomicBool::new(false));
    let mut map = LocalClusterMap::open(
        tmp.path(),
        &node_ids,
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let inner = Arc::clone(
        map.node(unavailable_node)
            .unwrap()
            .object_payload_lease_client(),
    );
    map.replace_object_payload_lease_client_for_tests(
        unavailable_node,
        Arc::new(PayloadLeaseUnavailableClient {
            inner,
            failure: PayloadLeaseUnavailableFailure::Transport,
            attempts: Some(Arc::clone(&attempts)),
            recovered: Some(Arc::clone(&recovered)),
        }),
    );
    let map = Arc::new(map);
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("broad-lease-unavailable-cache");
    let key = crate::tests::object_key("read");
    let payload = b"readable from the two surviving EC shards";
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, payload);
    let read = || {
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
        let (_, _, leased_snapshot) = outcome.into_parts();
        let retained = route
            .retain_object_payload_read(leased_snapshot)
            .unwrap()
            .unwrap();
        let mut bytes = Vec::new();
        retained
            .read_segment_payload_stored_bytes_into(&segment, &mut bytes)
            .unwrap();
        bytes
    };

    let before = attempts.load(Ordering::Relaxed);
    assert_eq!(read(), committed.encrypted.storage_bytes);
    assert_eq!(attempts.load(Ordering::Relaxed), before + 1);
    assert_eq!(read(), committed.encrypted.storage_bytes);
    assert_eq!(attempts.load(Ordering::Relaxed), before + 1);

    recovered.store(true, Ordering::Relaxed);
    std::thread::sleep(OBJECT_PAYLOAD_LEASE_UNAVAILABLE_RETRY + Duration::from_millis(20));
    assert_eq!(read(), committed.encrypted.storage_bytes);
    assert_eq!(attempts.load(Ordering::Relaxed), before + 2);
    assert_eq!(read(), committed.encrypted.storage_bytes);
    assert_eq!(attempts.load(Ordering::Relaxed), before + 3);
}

#[test]
fn unavailable_lease_node_allows_only_one_probe_after_cooldown() {
    let tmp = test_util::tempdir();
    let node_id = NodeId::new(1);
    let map = LocalClusterMap::open(
        tmp.path(),
        &trace_node_ids(),
        &[0],
        SharedStorageNode::DEFAULT_EC_SHAPE,
    )
    .unwrap();
    let node = map.node(node_id).unwrap();
    let health = &map.runtime_state;
    let now = Instant::now();
    let ordinary = health.begin_object_payload_lease_node_attempt(node, now);
    assert!(matches!(ordinary, ObjectPayloadLeaseNodeAttempt::Ordinary));
    health.finish_object_payload_lease_node_attempt(node, ordinary, true, now);
    assert!(matches!(
        health.begin_object_payload_lease_node_attempt(node, now),
        ObjectPayloadLeaseNodeAttempt::Skip
    ));

    let after_cooldown = now + OBJECT_PAYLOAD_LEASE_UNAVAILABLE_RETRY;
    let probe = health.begin_object_payload_lease_node_attempt(node, after_cooldown);
    assert!(matches!(probe, ObjectPayloadLeaseNodeAttempt::Probe(_)));
    assert!(matches!(
        health.begin_object_payload_lease_node_attempt(node, after_cooldown),
        ObjectPayloadLeaseNodeAttempt::Skip
    ));
    health.finish_object_payload_lease_node_attempt(node, probe, false, after_cooldown);
    assert!(matches!(
        health.begin_object_payload_lease_node_attempt(node, after_cooldown),
        ObjectPayloadLeaseNodeAttempt::Ordinary
    ));
}

#[test]
fn broad_lease_refresh_during_old_identity_probe_does_not_suppress_replacement() {
    use crate::control_plane::{
        ControlPlaneHeartbeatRuntimeMapSource, FileControlPlaneStore, NodeHeartbeat,
        SingleAuthorityControlPlane,
    };

    let tmp = test_util::tempdir();
    let node_id = NodeId::new(2);
    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            vec![
                (NodeId::new(1), "/tmp/lease-refresh-node-1.sock".to_owned()),
                (node_id, "/tmp/lease-refresh-node-2.sock".to_owned()),
            ],
            vec![PgId::new(0)],
        )
        .unwrap();
    let now_ms = crate::clock::current_time_millis();
    let initial_runtime_map = authority.snapshot().runtime_map(now_ms).unwrap();
    let original = LocalClusterMap::open_frontend_topology_only_with_runtime_map(
        NodeId::new(1),
        &initial_runtime_map,
        EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let old_node = original.node(node_id).unwrap();
    let old_incarnation = old_node.route_authority_node_incarnation.unwrap();
    let now = Instant::now();
    let ordinary = original
        .runtime_state
        .begin_object_payload_lease_node_attempt(old_node, now);
    original
        .runtime_state
        .finish_object_payload_lease_node_attempt(old_node, ordinary, true, now);
    let after_cooldown = now + OBJECT_PAYLOAD_LEASE_UNAVAILABLE_RETRY;
    let old_probe = original
        .runtime_state
        .begin_object_payload_lease_node_attempt(old_node, after_cooldown);
    assert!(matches!(old_probe, ObjectPayloadLeaseNodeAttempt::Probe(_)));

    authority
        .refresh_node_heartbeat(
            NodeHeartbeat::test_fixture(
                node_id,
                old_incarnation + 1,
                old_node
                    .route_authority_advertised_endpoint
                    .clone()
                    .unwrap(),
                authority.snapshot().cluster_epoch(),
                5_000,
                Default::default(),
                Vec::new(),
            ),
            now_ms + 1,
        )
        .unwrap();
    let refreshed_runtime_map = authority.snapshot().runtime_map(now_ms + 1).unwrap();
    let refreshed = LocalClusterMap::open_runtime_map_with_existing_local_nodes(
        &original,
        &refreshed_runtime_map,
    )
    .unwrap();
    let new_node = refreshed.node(node_id).unwrap();
    assert!(Arc::ptr_eq(
        &original.runtime_state,
        &refreshed.runtime_state
    ));
    assert_eq!(
        new_node.route_authority_node_incarnation,
        Some(old_incarnation + 1)
    );
    assert!(matches!(
        refreshed
            .runtime_state
            .begin_object_payload_lease_node_attempt(new_node, after_cooldown),
        ObjectPayloadLeaseNodeAttempt::Ordinary
    ));

    original
        .runtime_state
        .finish_object_payload_lease_node_attempt(old_node, old_probe, true, after_cooldown);
    assert!(matches!(
        refreshed
            .runtime_state
            .begin_object_payload_lease_node_attempt(new_node, after_cooldown),
        ObjectPayloadLeaseNodeAttempt::Ordinary
    ));

    let current_attempt = refreshed
        .runtime_state
        .begin_object_payload_lease_node_attempt(new_node, after_cooldown);
    refreshed
        .runtime_state
        .finish_object_payload_lease_node_attempt(new_node, current_attempt, true, after_cooldown);
    let mut changed_endpoint =
        refreshed.test_clone_with_route_map_validity(RouteMapValidity::Forever);
    changed_endpoint
        .nodes
        .get_mut(&node_id)
        .unwrap()
        .route_authority_advertised_endpoint = Some("/tmp/replaced-lease-endpoint.sock".to_owned());
    assert!(matches!(
        changed_endpoint
            .runtime_state
            .begin_object_payload_lease_node_attempt(
                changed_endpoint.node(node_id).unwrap(),
                after_cooldown,
            ),
        ObjectPayloadLeaseNodeAttempt::Ordinary
    ));
}

#[test]
fn active_object_payload_read_binds_its_subject_segments_and_lease() {
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
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [1; 16],
        [1; 16],
        b"active payload",
    );

    let snapshot = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap()
        .snapshot;
    let live = snapshot.stored.as_live().unwrap();
    let segment = snapshot.object_segments.first().unwrap().clone();
    let active = cluster
        .acquire_object_payload_read(&bucket, &key, live.generation_id, [&segment])
        .unwrap();
    assert!(active.contains_object_payload_segments(&bucket, &key, live.generation_id, [&segment]));

    let mut bytes = Vec::new();
    active
        .read_segment_payload_stored_bytes_into(&segment, &mut bytes)
        .unwrap();
    assert_eq!(bytes, committed.encrypted.storage_bytes);

    let crossed = segment.with_test_segment_index(segment.segment_index() + 1);
    assert!(!active.contains_object_payload_segments(
        &bucket,
        &key,
        live.generation_id,
        [&crossed]
    ));
    let error = active
        .read_segment_payload_stored_bytes_into(&crossed, &mut Vec::new())
        .unwrap_err();
    assert_eq!(error.kind(), crate::ObjectReadFailureKind::InternalError);
}

#[test]
fn active_object_payload_range_read_maps_only_intersecting_systematic_shards() {
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
    let bucket = crate::tests::bucket_name("systematic-range-bucket");
    let key = crate::tests::object_key("systematic-range-key");
    let payload = (0..257).map(|index| index as u8).collect::<Vec<_>>();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, &payload);

    let snapshot = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap()
        .snapshot;
    let live = snapshot.stored.as_live().unwrap();
    let segment = snapshot.object_segments[0].clone();
    let active = cluster
        .acquire_object_payload_read(&bucket, &key, live.generation_id, [&segment])
        .unwrap();

    let shard_size = committed
        .encrypted
        .storage_bytes
        .len()
        .div_ceil(usize::from(committed.written.ec.k));
    assert!(shard_size > 4);
    let start = shard_size - 3;
    let end = shard_size + 5;
    assert!(end < committed.encrypted.storage_bytes.len());

    let read_indices = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&read_indices);
    let _guard =
        cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(move |location, _| {
            observed
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(location.shard_index().get());
            Ok(())
        }));
    let mut selected = Vec::new();
    let outcome = active
        .read_segment_payload_stored_range_into(
            &segment,
            u64::try_from(start).unwrap()..u64::try_from(end).unwrap(),
            &mut selected,
        )
        .unwrap();

    assert_eq!(
        outcome,
        crate::ObjectPayloadRangeReadOutcome::UnauthenticatedCiphertext
    );
    assert_eq!(selected, committed.encrypted.storage_bytes[start..end]);
    assert_eq!(
        *read_indices
            .lock()
            .unwrap_or_else(|error| error.into_inner()),
        vec![0, 1],
        "the healthy range path must lease and read only intersecting systematic shards"
    );

    let crossed = segment.with_test_segment_index(segment.segment_index() + 1);
    let error = active
        .read_segment_payload_stored_range_into(&crossed, 0..1, &mut Vec::new())
        .unwrap_err();
    assert_eq!(error.kind(), crate::ObjectReadFailureKind::InternalError);

    drop(_guard);
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed_attempts = Arc::clone(&attempts);
    let _failure_guard =
        cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(move |location, _| {
            if observed_attempts.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(StoreError::StorageRpc {
                    node_id: location.node_id().as_u32(),
                    operation: "decode read-handle shard range response",
                    failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                    detail: crate::StorageNodeFailureDetail::new(
                        "injected malformed second systematic range response",
                    ),
                });
            }
            Ok(())
        }));
    selected.extend_from_slice(b"stale output");
    let outcome = active
        .read_segment_payload_stored_range_into(
            &segment,
            u64::try_from(start).unwrap()..u64::try_from(end).unwrap(),
            &mut selected,
        )
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        outcome,
        crate::ObjectPayloadRangeReadOutcome::FullSegmentFallbackRequired
    );
    assert!(
        selected.is_empty(),
        "a later range failure must discard bytes assembled from earlier shards"
    );
}

#[test]
fn active_object_payload_range_read_requests_full_fallback_without_exposing_partial_bytes() {
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
    let bucket = crate::tests::bucket_name("systematic-range-fallback-bucket");
    let key = crate::tests::object_key("systematic-range-fallback-key");
    let committed = write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &key,
        b"selected systematic range falls back to complete witness recovery",
    );

    let snapshot = cluster
        .load_object_read_snapshot_if(
            &bucket,
            &key,
            None,
            crate::ObjectReadSnapshotMode::FullPayloadLayout,
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap()
        .snapshot;
    let live = snapshot.stored.as_live().unwrap();
    let segment = snapshot.object_segments[0].clone();
    let active = cluster
        .acquire_object_payload_read(&bucket, &key, live.generation_id, [&segment])
        .unwrap();

    let shard_path = cluster
        .test_payload_shard_file_path(
            committed.written.data_pg_id,
            committed.written.ec,
            &committed.segment_okh,
            committed.generation_id,
            0,
        )
        .unwrap();
    std::fs::remove_file(shard_path).unwrap();

    let mut selected = b"must be cleared".to_vec();
    let outcome = active
        .read_segment_payload_stored_range_into(&segment, 0..8, &mut selected)
        .unwrap();
    assert_eq!(
        outcome,
        crate::ObjectPayloadRangeReadOutcome::FullSegmentFallbackRequired
    );
    assert!(selected.is_empty());

    let mut recovered = Vec::new();
    active
        .read_segment_payload_stored_bytes_into(&segment, &mut recovered)
        .unwrap();
    assert_eq!(recovered, committed.encrypted.storage_bytes);
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
    let committed = write_committed_direct_segment_for_with_versioning(
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
        let error = match cluster.acquire_object_payload_read_lease_inner(
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
    assert!(leased_snapshot.test_shares_snapshot(&snapshot));
    let mut retained = route
        .retain_object_payload_read(leased_snapshot)
        .unwrap()
        .expect("live object should retain payload authority");
    let mut bytes = Vec::new();
    retained
        .read_segment_payload_stored_bytes_into(&segment, &mut bytes)
        .unwrap();
    assert_eq!(bytes, committed.encrypted.storage_bytes);
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
    let raw_error = retained
        .read_segment_payload_stored_bytes_into_raw(&crossed, &mut Vec::new())
        .unwrap_err();
    assert!(matches!(
        raw_error,
        StoreError::PayloadShardSetMismatch { .. }
    ));
    let error = retained
        .read_segment_payload_stored_bytes_into(&crossed, &mut Vec::new())
        .unwrap_err();
    assert_eq!(error.kind(), crate::ObjectReadFailureKind::InternalError);
    assert_eq!(error.diagnostic_cause_label(), "store_integrity_failure");
    assert_eq!(error.to_string(), "object read failed");
    assert!(std::error::Error::source(&error).is_none());
    let debug = format!("{error:?}");
    assert!(!debug.contains("payload read is outside"));
    assert!(!debug.contains("PayloadShardSetMismatch"));
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
    leased_snapshot.test_clear_object_segments();

    let error = match route.retain_object_payload_read(leased_snapshot) {
        Ok(_) => panic!("incomplete payload snapshot unexpectedly retained read authority"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), crate::ObjectReadFailureKind::InternalError);
    assert_eq!(error.diagnostic_cause_label(), "store_integrity_failure");
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

    let raw_error = stale_cluster
        .read_segment_payload_stored_bytes_at_placement_epoch_into(
            segment.placement_cluster_epoch(),
            segment.stored_bytes_request(),
            &mut Vec::new(),
        )
        .unwrap_err();
    assert!(matches!(
        raw_error,
        StoreError::StalePayloadOperation { .. }
    ));
    let error = stale_cluster
        .read_object_payload_segment_stored_bytes_into(segment, &mut Vec::new())
        .unwrap_err();

    assert_eq!(
        error.kind(),
        crate::ObjectReadFailureKind::RetryableConvergence
    );
    assert_eq!(error.diagnostic_cause_label(), "store_topology_failure");
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
    assert_eq!(error.kind(), crate::ObjectReadFailureKind::InternalError);
    assert_eq!(error.diagnostic_cause_label(), "store_integrity_failure");
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
