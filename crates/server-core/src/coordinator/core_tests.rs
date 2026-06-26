use super::test_helpers::{self, UploadPartRequest};
use super::test_support::*;
use super::test_topology::*;
use super::*;
use crate::conditional::{DeleteCondition, SpecificEtag, WriteCondition};
use crate::coordinator::bucket_handles::{BucketHandleLoader, BucketHandleRequest};
use crate::sse::SSE_CUSTOMER_ALGORITHM;
use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use storage::storage_node_server::{
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeServer,
};
use storage::{
    install_bucket_scoped_test_hooks, BucketScopedTestHooks, ClusterEpoch, LocalClusterMap,
    LocalNodeStoreConfig, LocalPgRoute, LocalUnixStorageNodeClientConfig,
    MetadataCommandApplyTestKind, NodeId, PgId, PgState, PlacedSegmentShardBackfillWorkItem,
    SegmentStoredBytesRequest, ShardScavengerObservationReason, SharedStorageNode, StorageCluster,
    StorageClusterRuntimeMapHandle,
};

const TEST_EVENT_TIMEOUT: Duration = Duration::from_secs(2);
const BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
enum LockWaitEvent {
    Progress,
    UnexpectedBucketLock,
    UnexpectedStorageLoad,
    CompletedEarly,
}

fn setup_direct_coordinator_with_storage_cluster(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_managed_key_provider_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        test_sse_s3_provider(),
    )
    .unwrap()
}

fn setup_coordinator_with_only_shard_repair_worker(
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            ShardRepairSweeper::acquire_shared,
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap()
}

fn setup_coordinator_with_only_shard_backfill_worker(
    storage_handle: StorageClusterRuntimeMapHandle,
    storage_cluster: Arc<StorageCluster>,
) -> Coordinator {
    Coordinator::new_with_shared_caches_and_background_sweeper_factories(
        storage_handle,
        Arc::clone(&storage_cluster),
        shared_caches_for_storage_cluster(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_cluster| Ok(ShardRepairSweeper::disabled(Arc::clone(storage_cluster))),
            ShardBackfillSweeper::acquire_shared,
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap()
}

fn make_private_socket_dir(path: &std::path::Path) {
    std::fs::create_dir_all(path).unwrap();
    let mut perms = std::fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o700);
    std::fs::set_permissions(path, perms).unwrap();
}

fn spawn_storage_node_server_loop(
    server: Arc<StorageNodeServer>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            match server.accept_one() {
                Ok(()) => {}
                Err(error) if stop.load(Ordering::SeqCst) => {
                    let _ = error;
                    break;
                }
                Err(error) => panic!("storage node server failed: {error}"),
            }
        }
    })
}

fn stop_storage_node_server_loops(
    stop: Arc<AtomicBool>,
    socket_paths: &[PathBuf],
    threads: Vec<thread::JoinHandle<()>>,
) {
    stop.store(true, Ordering::SeqCst);
    for socket_path in socket_paths {
        let _ = UnixStream::connect(socket_path);
    }
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn coordinator_storage_node_tracks_runtime_map_handle_install() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();

    assert!(Arc::ptr_eq(&coord.storage_node(), &initial));

    let next_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(next_tmp.path(), &[0, 1]);
    assert!(!Arc::ptr_eq(&coord.storage_node(), &candidate));
    handle.install(Arc::clone(&candidate)).unwrap();

    assert!(Arc::ptr_eq(&coord.storage_node(), &candidate));
}

#[test]
fn shard_backfill_worker_uses_refreshed_runtime_map_handle() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let _coord =
        setup_coordinator_with_only_shard_backfill_worker(handle.clone(), Arc::clone(&initial));

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment_okh = [0xBF; 16];
    let segment_vid = GenerationId::MIN;
    let payload = b"backfill worker must use refreshed runtime map";
    let written_segment = initial
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            segment_vid,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    initial
        .test_register_payload_shard_acks(
            written_segment.data_pg_id,
            &written_segment.written_shards,
        )
        .unwrap();

    let source_epoch = initial.cluster_epoch();
    let desired_epoch = ClusterEpoch::new(source_epoch.get() + 1).unwrap();
    let source_route = initial
        .local_pg_route(PgId::new(written_segment.data_pg_id))
        .expect("test PG should have a local route");
    let historical_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        source_epoch,
        source_route.pg_id(),
        source_route.primary_node_id(),
        source_route.acting_set().to_vec(),
        source_route.state(),
    );
    let ec_shape = initial.default_payload_ec_shape();
    let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                storage::NodeId::new(node_id),
                tmp.path().join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let mut refreshed_map = LocalClusterMap::open_frontend_placeholder_with_configs_and_epoch(
        storage::NodeId::new(0),
        configs,
        &[0],
        ec_shape,
        desired_epoch,
    )
    .unwrap();
    refreshed_map.test_install_historical_pg_routes([historical_route]);
    let refreshed = StorageCluster::from_local_map(Arc::new(refreshed_map)).unwrap();
    handle.install(Arc::clone(&refreshed)).unwrap();

    let work_item = PlacedSegmentShardBackfillWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: written_segment.data_pg_id,
            segment_okh,
            segment_vid,
            stored_size: payload.len(),
            segment_crc64: checksum::crc64::checksum(payload),
            ec: written_segment.ec,
        },
        source_cluster_epoch: source_epoch,
        desired_cluster_epoch: desired_epoch,
    };
    assert!(
        initial
            .backfill_placed_segment_payload_shards_for_work_item(&work_item)
            .is_err(),
        "the stale initial cluster must not be able to reconstruct the desired epoch"
    );
    refreshed
        .record_placed_segment_shard_backfill(&work_item, None)
        .unwrap();

    let start = std::time::Instant::now();
    loop {
        let rows = refreshed
            .list_placed_segment_shard_backfills(written_segment.data_pg_id)
            .unwrap();
        if rows.is_empty() {
            break;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard backfill worker did not complete durable row after runtime-map refresh: {rows:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn shard_backfill_worker_executes_remote_storage_node_work() {
    let tmp = test_util::tempdir();
    let source_node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
    ];
    let desired_node_ids = [
        NodeId::new(0),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
    ];
    let all_node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
        NodeId::new(4),
        NodeId::new(5),
        NodeId::new(6),
    ];
    let ec_shape = SharedStorageNode::DEFAULT_EC_SHAPE;
    let pg_id = PgId::new(0);
    let mut authority = storage::control_plane::SingleAuthorityControlPlane::open(
        storage::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            source_node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join("sockets")
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            vec![pg_id],
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(
            NodeId::new(6),
            storage::control_plane::NodeMembershipState::Active,
        )
        .unwrap();
    authority
        .set_pg_acting_set(pg_id, desired_node_ids.to_vec())
        .unwrap();
    let source_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(pg_id, source_epoch)
        .unwrap();
    let source_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        source_route.cluster_epoch(),
        source_route.pg_id(),
        source_route.primary_node_id(),
        source_route.acting_set().to_vec(),
        PgState::Active,
    );
    let desired_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(pg_id, authority.snapshot().cluster_epoch())
        .unwrap();
    let desired_route = storage::control_plane::PgRouteSnapshot::reconstructed(
        desired_route.cluster_epoch(),
        desired_route.pg_id(),
        desired_route.primary_node_id(),
        desired_route.acting_set().to_vec(),
        PgState::Active,
    );
    assert_eq!(source_route.acting_set(), &source_node_ids);
    assert_eq!(desired_route.acting_set(), &desired_node_ids);

    let configs: Vec<_> = all_node_ids
        .iter()
        .map(|&node_id| {
            LocalNodeStoreConfig::new(
                node_id,
                tmp.path()
                    .join("storage")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.iter().take(source_node_ids.len()).cloned(),
            &[pg_id.get()],
            ec_shape,
            source_route.cluster_epoch(),
            [LocalPgRoute::from(&source_route)],
        )
        .unwrap(),
    );
    let source_cluster = StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    let bucket = trusted_bucket_name("remote-backfill-bucket");
    let key = trusted_object_key("remote-backfill-key");
    let segment_okh = [0xD7; 16];
    let segment_vid = GenerationId::MIN;
    let payload = b"remote storage-node shard backfill worker";
    let written_segment = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            segment_vid,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(
            written_segment.data_pg_id,
            &written_segment.written_shards,
        )
        .unwrap();
    let work_item = PlacedSegmentShardBackfillWorkItem {
        request: SegmentStoredBytesRequest {
            data_pg_id: written_segment.data_pg_id,
            segment_okh,
            segment_vid,
            stored_size: payload.len(),
            segment_crc64: checksum::crc64::checksum(payload),
            ec: written_segment.ec,
        },
        source_cluster_epoch: source_route.cluster_epoch(),
        desired_cluster_epoch: desired_route.cluster_epoch(),
    };
    drop(source_cluster);
    drop(source_map);

    let socket_dir = tmp.path().join("sockets-desired");
    make_private_socket_dir(&socket_dir);
    let mut server_threads = Vec::new();
    let mut wake_socket_paths = Vec::new();
    let stop = Arc::new(AtomicBool::new(false));
    let mut client_configs = Vec::new();
    for config in &configs {
        let socket_path = socket_dir.join(format!("node-{}.sock", config.node_id().as_u32()));
        let server_config = StorageNodeProcessConfig {
            node_id: config.node_id(),
            cluster_epoch: desired_route.cluster_epoch(),
            route_map_valid_until_ms: None,
            data_dir: config.data_dir().to_path_buf(),
            default_ec_shape: ec_shape,
            pg_ids: vec![pg_id.get()],
            socket_path: socket_path.clone(),
            pg_routes: vec![StorageNodePgRoute::from(&desired_route)],
            historical_pg_routes: Vec::new(),
        };
        let server = Arc::new(StorageNodeServer::bind(server_config).unwrap());
        for _ in 0..4 {
            server_threads.push(spawn_storage_node_server_loop(
                Arc::clone(&server),
                Arc::clone(&stop),
            ));
            wake_socket_paths.push(socket_path.clone());
        }
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            config.node_id(),
            socket_path.clone(),
        ));
    }

    let mut desired_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &[pg_id.get()],
        ec_shape,
        desired_route.cluster_epoch(),
        [LocalPgRoute::from(&desired_route)],
    )
    .unwrap();
    desired_map.test_install_historical_pg_routes([source_route.clone()]);
    desired_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let desired_cluster = StorageCluster::from_local_map(Arc::new(desired_map)).unwrap();
    let source_health = desired_cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(&source_route, work_item.request)
        .unwrap();
    assert_eq!(
        source_health.risk,
        storage::PlacedSegmentShardSetRisk::Healthy
    );
    let before = desired_cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(
            &desired_route,
            work_item.request,
        )
        .unwrap();
    assert!(
        matches!(
            before.risk,
            storage::PlacedSegmentShardSetRisk::Unrecoverable
        ),
        "expected unrecoverable desired-route health before backfill, got {before:?}"
    );
    assert!(before
        .shards
        .iter()
        .any(|shard| shard.location.node_id() == NodeId::new(6) && !shard.validation.is_valid()));

    desired_cluster
        .record_placed_segment_shard_backfill(&work_item, None)
        .unwrap();
    super::runtime::run_one_placed_segment_shard_backfill_for_test(
        &desired_cluster,
        "remote-backfill-test",
    );

    let rows = desired_cluster
        .list_placed_segment_shard_backfills(written_segment.data_pg_id)
        .unwrap();
    assert!(rows.is_empty(), "backfill row should complete: {rows:?}");
    let after = desired_cluster
        .placed_segment_payload_shard_health_for_pg_route_snapshot(
            &desired_route,
            work_item.request,
        )
        .unwrap();
    assert_eq!(after.risk, storage::PlacedSegmentShardSetRisk::Healthy);
    assert!(after
        .shards
        .iter()
        .filter(|shard| shard.location.node_id() == NodeId::new(6))
        .all(|shard| shard.validation.is_valid()));

    stop_storage_node_server_loops(stop, &wake_socket_paths, server_threads);
}

#[test]
fn get_object_pins_runtime_map_for_snapshot_and_body() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("bucket".to_string(), "key".to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    assert_eq!(result.body.read_all().unwrap(), b"pinned-runtime-map");
}

#[test]
fn copy_object_pins_runtime_map_for_source_and_destination() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let handle_for_hook = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("bucket".to_string(), "src".to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            handle_for_hook.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    handle.install(initial).unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "dst",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    assert_eq!(result.body.read_all().unwrap(), b"copy-pinned-runtime-map");
}

#[test]
fn object_metadata_pins_runtime_map_after_policy_context_load() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"object-metadata-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_object_metadata_policy_context: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        "<Tagging><TagSet><Tag><Key>pin</Key><Value>metadata</Value></Tag></TagSet></Tagging>",
        test_requester(),
        None,
    )
    .unwrap();

    handle.install(initial).unwrap();
    let tags = get_object_tags_test(&coord, "bucket", "key", None, test_requester(), None)
        .unwrap()
        .unwrap();
    assert!(tags.contains("<Key>pin</Key>"));
    assert!(tags.contains("<Value>metadata</Value>"));
}

#[test]
fn delete_object_pins_runtime_map_after_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"delete-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    handle.install(initial).unwrap();
    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn delete_bucket_pins_runtime_map_after_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    delete_bucket_test(&coord, "bucket").unwrap();

    handle.install(initial).unwrap();
    let err = coord
        .head_bucket(&bucket_request_with_expected_owner(
            "bucket",
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn bucket_subresource_write_pins_runtime_map_after_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let lifecycle = "<LifecycleConfiguration><Rule><ID>pin</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
    put_bucket_lifecycle_test(&coord, "bucket", lifecycle, test_requester(), None).unwrap();

    handle.install(initial).unwrap();
    let stored = coord
        .get_bucket_lifecycle(&bucket_request_with_expected_owner(
            "bucket",
            test_requester(),
            None,
        ))
        .unwrap()
        .unwrap();
    assert!(stored.contains("<ID>pin</ID>"));
}

#[test]
fn bucket_subresource_write_pins_runtime_map_before_authorization() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some("bucket".to_string()),
        after_bucket_mutation_storage_node_capture: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let lifecycle = "<LifecycleConfiguration><Rule><ID>pin-before-auth</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>";
    put_bucket_lifecycle_test(&coord, "bucket", lifecycle, test_requester(), None).unwrap();

    handle.install(initial).unwrap();
    let stored = coord
        .get_bucket_lifecycle(&bucket_request_with_expected_owner(
            "bucket",
            test_requester(),
            None,
        ))
        .unwrap()
        .unwrap();
    assert!(stored.contains("<ID>pin-before-auth</ID>"));
}

#[test]
fn upload_part_copy_pins_runtime_map_after_stream_session_create() {
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some(("bucket".to_string(), "dst".to_string())),
        after_upload_part_copy_stream_session: Some(Arc::new(move || {
            handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            copy_source_range: None,
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap();
}

#[test]
fn put_object_pins_runtime_map_after_bucket_write_reservation() {
    let bucket = "direct-put-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some(bucket.to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
            data: b"direct-put-pinned-runtime-map",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    handle.install(initial).unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"direct-put-pinned-runtime-map"
    );
}

fn install_next_epoch_runtime_map_with_historical_routes(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
) {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                node_root.join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let next_epoch = ClusterEpoch::new(initial.cluster_epoch().get() + 1).unwrap();
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            let route = storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                acting_set.clone(),
                PgState::Active,
            );
            LocalPgRoute::from(&route)
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let mut candidate_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        initial.test_pg_ids(),
        initial.default_payload_ec_shape(),
        next_epoch,
        routes,
    )
    .unwrap();
    candidate_map.test_install_historical_pg_routes(historical_routes);
    let candidate = StorageCluster::from_local_map(Arc::new(candidate_map)).unwrap();
    handle.install(candidate).unwrap();
}

fn install_same_store_next_epoch_runtime_map(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
) {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                node_root.join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let next_epoch = ClusterEpoch::new(initial.cluster_epoch().get() + 1).unwrap();
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            let route = storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                acting_set.clone(),
                PgState::Active,
            );
            LocalPgRoute::from(&route)
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let mut candidate_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        initial.test_pg_ids(),
        initial.default_payload_ec_shape(),
        next_epoch,
        routes,
    )
    .unwrap();
    candidate_map.test_install_historical_pg_routes(historical_routes);
    let candidate = StorageCluster::from_local_map(Arc::new(candidate_map)).unwrap();
    handle.install(candidate).unwrap();
}

fn install_same_store_next_epoch_runtime_map_with_peering_pg(
    handle: &StorageClusterRuntimeMapHandle,
    initial: &Arc<StorageCluster>,
    node_root: &std::path::Path,
    peering_pg: u32,
) -> ClusterEpoch {
    let node_count = u32::from(initial.default_payload_ec_shape().k)
        + u32::from(initial.default_payload_ec_shape().m);
    let configs = (0..node_count)
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                NodeId::new(node_id),
                node_root.join(format!("node-{node_id:04}")),
            )
        })
        .collect::<Vec<_>>();
    let next_epoch = ClusterEpoch::new(initial.cluster_epoch().get() + 1).unwrap();
    let acting_set = (0..node_count).map(NodeId::new).collect::<Vec<_>>();
    let routes = initial
        .test_pg_ids()
        .iter()
        .map(|pg_id| {
            let state = if *pg_id == peering_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            let route = storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                acting_set.clone(),
                state,
            );
            LocalPgRoute::from(&route)
        })
        .collect::<Vec<_>>();
    let historical_routes = initial
        .local_pg_routes()
        .map(|route| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                route.state(),
            )
        })
        .collect::<Vec<_>>();
    let mut candidate_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        initial.test_pg_ids(),
        initial.default_payload_ec_shape(),
        next_epoch,
        routes,
    )
    .unwrap();
    candidate_map.test_install_historical_pg_routes(historical_routes);
    let candidate = StorageCluster::from_local_map(Arc::new(candidate_map)).unwrap();
    handle.install(candidate).unwrap();
    next_epoch
}

fn find_bucket_key_for_metadata_and_data_pg(
    storage_cluster: &StorageCluster,
    metadata_pg_id: u32,
    data_pg_id: u32,
) -> (String, String) {
    for bucket_suffix in 0..1024 {
        let bucket = format!("remote-read-epoch-bucket-{bucket_suffix}");
        let bucket_name = trusted_bucket_name(&bucket);
        if storage_cluster.test_bucket_pg_id_for(&bucket_name) != metadata_pg_id {
            continue;
        }
        for key_suffix in 0..10_000 {
            let key = format!("key-{key_suffix:04}");
            let object_key = trusted_object_key(&key);
            if storage_cluster.test_object_pg_id_for(&bucket_name, &object_key) == metadata_pg_id
                && storage_cluster.test_data_pg_id_for(&bucket_name, &object_key, GenerationId::MIN)
                    == data_pg_id
            {
                return (bucket, key);
            }
        }
    }
    panic!(
        "failed to find bucket/key with bucket and object PG {metadata_pg_id} and data PG {data_pg_id}"
    );
}

fn find_key_for_object_metadata_pg_with_prefix(
    storage_cluster: &StorageCluster,
    bucket: &str,
    metadata_pg_id: u32,
    key_prefix: &str,
) -> String {
    let bucket_name = trusted_bucket_name(bucket);
    for key_suffix in 0..10_000 {
        let key = format!("{key_prefix}{key_suffix:04}");
        let object_key = trusted_object_key(&key);
        if storage_cluster.test_object_pg_id_for(&bucket_name, &object_key) == metadata_pg_id {
            return key;
        }
    }
    panic!("failed to find key with prefix {key_prefix:?} on object metadata PG {metadata_pg_id}");
}

#[test]
fn put_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-before-metadata-apply");

    let bucket = "direct-put-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let put_coord = Arc::clone(&coord);
    let put_thread = thread::spawn(move || {
        let metadata = MetadataBlob::new();
        test_helpers::put_object(
            &put_coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: b"direct-put-crosses-epoch-change",
                metadata: &metadata,
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 1,
        "direct PUT should have applied only the generation-reservation command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let put_result = put_thread.join().unwrap().unwrap();
    assert_eq!(put_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "direct PUT crossing an epoch change should append exactly one commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "direct PUT command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "direct PUT command should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"direct-put-crosses-epoch-change"
    );
}

#[test]
fn overwrite_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("overwrite-object-before-metadata-apply");

    let bucket = "overwrite-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"old-body",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let put_coord = Arc::clone(&coord);
    let put_thread = thread::spawn(move || {
        let metadata = MetadataBlob::new();
        test_helpers::put_object(
            &put_coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: b"new-body",
                metadata: &metadata,
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 1,
        "overwrite should have applied only the generation-reservation command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let put_result = put_thread.join().unwrap().unwrap();
    assert_eq!(put_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "overwrite crossing an epoch change should append exactly one commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "overwrite command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "overwrite command should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"new-body");
}

#[test]
fn copy_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("copy-object-before-metadata-apply");

    let bucket = "copy-object-epoch-change-bucket";
    let src_key = "src";
    let dst_key = "dst";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, src_key, test_requester(), None),
            data: b"copy-crosses-epoch-change",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let dst_object_key = trusted_object_key(dst_key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let pre_commit_kinds = Arc::new(Mutex::new(Vec::new()));
    let hook_bucket = bucket_name.clone();
    let hook_key = dst_object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let pre_commit_kinds_for_hook = Arc::clone(&pre_commit_kinds);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject {
                    gate_for_hook.wait_at(TOKEN);
                } else {
                    let mut kinds = pre_commit_kinds_for_hook.lock().unwrap();
                    if kinds.last() != Some(&context.kind) {
                        kinds.push(context.kind);
                    }
                }
            }
            Ok(())
        }));

    let copy_coord = Arc::clone(&coord);
    let copy_thread = thread::spawn(move || {
        copy_coord.copy_object(&CopyObjectRequest {
            source: copy_source(bucket, src_key, None),
            destination: object_request_with_expected_owner(
                bucket,
                dst_key,
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 3,
        "copy should have applied generation reservation, stream-session create, and segment append before the destination commit gate"
    );
    assert_eq!(
        pre_commit_kinds.lock().unwrap().as_slice(),
        &[
            MetadataCommandApplyTestKind::ReserveObjectGeneration,
            MetadataCommandApplyTestKind::CreateStreamUpload,
            MetadataCommandApplyTestKind::AppendStreamSegment,
        ],
        "copy should apply the expected destination command prefix before the gated commit"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let copy_result = copy_thread.join().unwrap().unwrap();
    assert_eq!(copy_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "copy crossing an epoch change should append exactly one destination commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "copy destination commit should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "copy destination commit should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                dst_key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"copy-crosses-epoch-change"
    );
}

#[test]
fn delete_object_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("delete-object-before-metadata-apply");

    let bucket = "delete-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"delete-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::DeleteObjectVersion
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let delete_coord = Arc::clone(&coord);
    let delete_thread = thread::spawn(move || {
        delete_coord.delete_object(&delete_object_request(
            bucket,
            key,
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "delete should not apply the object-PG command before the pre-apply gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    delete_thread.join().unwrap().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 1,
        "delete crossing an epoch change should append exactly one delete command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "delete command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "delete command should change the object-PG materialized state digest"
    );

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn complete_multipart_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("complete-multipart-before-metadata-apply");

    let bucket = "complete-multipart-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let (upload_id, parts) =
        create_upload_with_parts(&coord, bucket, key, &[(1, b"multipart-crosses-epoch")]);

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitMultipartObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let complete_coord = Arc::clone(&coord);
    let complete_thread = thread::spawn(move || {
        complete_coord.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                key,
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "multipart completion should not apply an object-PG command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let complete_result = complete_thread.join().unwrap().unwrap();
    assert_eq!(complete_result.version_id, VersionId::Null);
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "multipart completion crossing an epoch change should append exactly one commit command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "multipart completion command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "multipart completion command should change the object-PG materialized state digest"
    );

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"multipart-crosses-epoch");
}

#[test]
fn upload_part_finalize_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("upload-part-finalize-before-metadata-apply");

    let bucket = "upload-part-finalize-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let session = begin_stream_part_test(&coord, bucket, key, &upload.upload_id, 1).unwrap();
    let data = b"upload-part-finalize-crosses-epoch-change";
    coord
        .append_plaintext_stream_segment_for_test(bucket, key, &session.session_id, 0, data)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitStreamPart
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let finalize_coord = Arc::clone(&coord);
    let upload_id = upload.upload_id.clone();
    let session_id = session.session_id.clone();
    let finalize_thread = thread::spawn(move || {
        finalize_coord.finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                key,
                &upload_id,
                test_requester(),
                None,
            ),
            session_id: &session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(data),
            total_size: data.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "UploadPart finalization should not apply an object-PG command before the pre-commit gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let part = finalize_thread.join().unwrap().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "UploadPart finalization crossing an epoch change should append exactly one part commit after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "UploadPart finalization command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "UploadPart finalization command should change the object-PG materialized state digest"
    );

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                key,
                &upload.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: part.etag,
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);
}

#[test]
fn upload_part_copy_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("upload-part-copy-before-metadata-apply");

    let bucket = "upload-part-copy-epoch-change-bucket";
    let src_key = "src";
    let dst_key = "dst";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let source_payload = b"upload-part-copy-crosses-epoch-change";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, src_key, test_requester(), None),
            data: source_payload,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, dst_key, test_requester(), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let dst_object_key = trusted_object_key(dst_key);
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let pre_commit_kinds = Arc::new(Mutex::new(Vec::new()));
    let hook_bucket = bucket_name.clone();
    let hook_key = dst_object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let pre_commit_kinds_for_hook = Arc::clone(&pre_commit_kinds);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
            {
                if context.kind == MetadataCommandApplyTestKind::CommitStreamPart {
                    gate_for_hook.wait_at(TOKEN);
                } else {
                    let mut kinds = pre_commit_kinds_for_hook.lock().unwrap();
                    if kinds.last() != Some(&context.kind) {
                        kinds.push(context.kind);
                    }
                }
            }
            Ok(())
        }));

    let copy_coord = Arc::clone(&coord);
    let upload_id = upload.upload_id.clone();
    let copy_thread = thread::spawn(move || {
        copy_coord.upload_part_copy(&UploadPartCopyRequest {
            source: copy_source(bucket, src_key, None),
            upload: multipart_object_request_with_expected_owner(
                bucket,
                dst_key,
                &upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            copy_source_range: None,
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            sse_customer: None,
        })
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index,
        before_object_pg_proof.applied_log_index + 2,
        "UploadPartCopy should have created a destination stream and appended copied data before the part commit gate"
    );
    assert_eq!(
        pre_commit_kinds.lock().unwrap().as_slice(),
        &[
            MetadataCommandApplyTestKind::CreateStreamUpload,
            MetadataCommandApplyTestKind::AppendStreamSegment,
        ],
        "UploadPartCopy should apply the expected destination command prefix before the gated part commit"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    let part = copy_thread.join().unwrap().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &dst_object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "UploadPartCopy crossing an epoch change should append exactly one part commit after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "UploadPartCopy part commit should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "UploadPartCopy part commit should change the object-PG materialized state digest"
    );

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                dst_key,
                &upload.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: part.etag,
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                dst_key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), source_payload);
}

#[test]
fn put_object_tags_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-tags-before-metadata-apply");

    let bucket = "put-tags-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"tagged-across-epoch",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let object_pg = initial.test_object_pg_id_for(&bucket_name, &object_key);
    let primary_node = initial
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::PutObjectMetadata
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let tag_coord = Arc::clone(&coord);
    let tag_thread = thread::spawn(move || {
        put_object_tags_test(
            &tag_coord,
            bucket,
            key,
            None,
            "<Tagging><TagSet><Tag><Key>epoch</Key><Value>changed</Value></Tag></TagSet></Tagging>",
            test_requester(),
            None,
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "tag update should not apply the object-PG metadata command before the pre-apply gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    tag_thread.join().unwrap().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "tag update crossing an epoch change should append exactly one object metadata command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "tag update command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "tag update command should change the object-PG materialized state digest"
    );

    let tags = get_object_tags_test(&coord, bucket, key, None, test_requester(), None).unwrap();
    assert_eq!(
        tags.as_deref(),
        Some(
            "<Tagging><TagSet><Tag><Key>epoch</Key><Value>changed</Value></Tag></TagSet></Tagging>"
        )
    );
}

#[test]
fn put_object_legal_hold_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-legal-hold-before-metadata-apply");

    let bucket = "put-legal-hold-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: test_requester(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"legal-hold-across-epoch",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let object_pg = initial.test_object_pg_id_for(&bucket_name, &object_key);
    let primary_node = initial
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::PutObjectMetadata
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let legal_hold_coord = Arc::clone(&coord);
    let legal_hold_thread = thread::spawn(move || {
        put_object_legal_hold_test(
            &legal_hold_coord,
            bucket,
            key,
            Some(put.version_id),
            LegalHoldStatus::On,
            test_requester(),
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "legal-hold update should not apply the object-PG metadata command before the pre-apply gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    legal_hold_thread.join().unwrap().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "legal-hold update crossing an epoch change should append exactly one object metadata command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "legal-hold update command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "legal-hold update command should change the object-PG materialized state digest"
    );

    let legal_hold =
        get_object_legal_hold_test(&coord, bucket, key, Some(put.version_id), test_requester())
            .unwrap();
    assert_eq!(legal_hold, Some(LegalHoldStatus::On));
}

#[test]
fn put_object_retention_epoch_change_before_metadata_apply_commits_once_on_pinned_route() {
    const TOKEN: DeterministicFaultToken =
        DeterministicFaultToken::new("put-object-retention-before-metadata-apply");

    let bucket = "put-retention-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord = Arc::new(
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap(),
    );
    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: test_requester(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: true,
        })
        .unwrap();
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"retention-across-epoch",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    let object_key = trusted_object_key(key);
    let object_pg = initial.test_object_pg_id_for(&bucket_name, &object_key);
    let primary_node = initial
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let before_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();

    let gate = DeterministicFaultGate::new(TOKEN);
    let hook_bucket = bucket_name.clone();
    let hook_key = object_key.clone();
    let gate_for_hook = Arc::clone(&gate);
    let _hook_guard =
        initial.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::PutObjectMetadata
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                gate_for_hook.wait_at(TOKEN);
            }
            Ok(())
        }));

    let retention = ObjectRetention {
        mode: ObjectLockMode::Governance,
        retain_until_unix_seconds: Coordinator::current_unix_seconds().unwrap() + 3600,
    };
    let retention_coord = Arc::clone(&coord);
    let retention_thread = thread::spawn(move || {
        put_object_retention_test(
            &retention_coord,
            bucket,
            key,
            Some(put.version_id),
            retention,
            false,
            test_requester(),
        )
    });

    gate.wait_until_arrived(TEST_EVENT_TIMEOUT);
    let paused_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        paused_object_pg_proof.applied_log_index, before_object_pg_proof.applied_log_index,
        "retention update should not apply the object-PG metadata command before the pre-apply gate"
    );

    install_next_epoch_runtime_map_with_historical_routes(&handle, &initial, tmp.path());

    gate.release();
    retention_thread.join().unwrap().unwrap();
    let after_object_pg_proof = initial
        .test_object_pg_metadata_proof(&bucket_name, &object_key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof.applied_log_index,
        paused_object_pg_proof.applied_log_index + 1,
        "retention update crossing an epoch change should append exactly one object metadata command after the gate"
    );
    assert_ne!(
        after_object_pg_proof.applied_log_hash, before_object_pg_proof.applied_log_hash,
        "retention update command should change the object-PG command-log hash"
    );
    assert_ne!(
        after_object_pg_proof.state_digest, before_object_pg_proof.state_digest,
        "retention update command should change the object-PG materialized state digest"
    );

    let fetched =
        get_object_retention_test(&coord, bucket, key, Some(put.version_id), test_requester())
            .unwrap();
    assert_eq!(fetched, Some(retention));
}

#[test]
fn get_object_epoch_change_after_read_snapshot_uses_pinned_route() {
    let bucket = "get-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"get-crosses-epoch-change",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let hook_handle = handle.clone();
    let hook_initial = Arc::clone(&initial);
    let hook_node_root = tmp.path().to_path_buf();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            install_next_epoch_runtime_map_with_historical_routes(
                &hook_handle,
                &hook_initial,
                &hook_node_root,
            );
        })),
        ..ReclamationTestHooks::default()
    });

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"get-crosses-epoch-change");
}

#[test]
fn head_object_epoch_change_after_read_snapshot_uses_pinned_route() {
    let bucket = "head-epoch-change-bucket";
    let key = "key";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            data: b"head-crosses-epoch-change",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let hook_handle = handle.clone();
    let hook_initial = Arc::clone(&initial);
    let hook_node_root = tmp.path().to_path_buf();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.to_string())),
        after_object_read_snapshot: Some(Arc::new(move || {
            install_next_epoch_runtime_map_with_historical_routes(
                &hook_handle,
                &hook_initial,
                &hook_node_root,
            );
        })),
        ..ReclamationTestHooks::default()
    });

    let result = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.size, b"head-crosses-epoch-change".len() as u64);
    assert_eq!(result.version_id, VersionId::Null);
}

#[test]
fn get_uses_retained_payload_route_over_unix_after_data_pg_move_and_metadata_reads_stay_available()
{
    let tmp = test_util::tempdir();
    let old_acting_set = vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let moved_acting_set = vec![NodeId::new(3), NodeId::new(4), NodeId::new(5)];
    let node_ids = (0..6).map(NodeId::new).collect::<Vec<_>>();
    let pg_ids = vec![0, 1];
    let metadata_pg_id = 0;
    let moved_data_pg_id = 1;
    let ec_shape = storage::EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let next_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join(format!("remote-read-node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            storage::control_plane::PgRouteSnapshot::reconstructed(
                current_epoch,
                PgId::new(*pg_id),
                NodeId::new(0),
                old_acting_set.clone(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs.clone(),
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    let current_cluster = StorageCluster::from_local_map(Arc::new(current_map)).unwrap();
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&current_cluster));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();

    let (bucket, key) = find_bucket_key_for_metadata_and_data_pg(
        &current_cluster,
        metadata_pg_id,
        moved_data_pg_id,
    );
    coord
        .create_bucket_for_owner("default-owner", &bucket, false)
        .unwrap();
    let payload = b"coordinator remote unix read uses retained placement epoch";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(&bucket, &key, test_requester(), None),
            data: payload,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let bucket_name = trusted_bucket_name(&bucket);
    let object_key = trusted_object_key(&key);
    let segment = current_cluster
        .test_get_object_segments(&bucket_name, &object_key, put.version_id)
        .unwrap()
        .pop()
        .expect("direct PUT should record one segment");
    assert_eq!(segment.data_pg_id, moved_data_pg_id);
    assert_eq!(segment.placement_cluster_epoch, current_epoch);

    let next_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let (primary, acting_set) = if *pg_id == moved_data_pg_id {
                (NodeId::new(3), moved_acting_set.clone())
            } else {
                (NodeId::new(0), old_acting_set.clone())
            };
            storage::control_plane::PgRouteSnapshot::reconstructed(
                next_epoch,
                PgId::new(*pg_id),
                primary,
                acting_set,
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let socket_dir = tmp.path().join("remote-read-sockets");
    make_private_socket_dir(&socket_dir);
    let stop = Arc::new(AtomicBool::new(false));
    let mut server_threads = Vec::new();
    let mut wake_socket_paths = Vec::new();
    let mut client_configs = Vec::new();
    for config in &configs {
        let socket_path = socket_dir.join(format!("node-{}.sock", config.node_id().as_u32()));
        let server_config = StorageNodeProcessConfig {
            node_id: config.node_id(),
            cluster_epoch: next_epoch,
            route_map_valid_until_ms: None,
            data_dir: config.data_dir().to_path_buf(),
            default_ec_shape: ec_shape,
            pg_ids: pg_ids.clone(),
            socket_path: socket_path.clone(),
            pg_routes: next_routes.iter().map(StorageNodePgRoute::from).collect(),
            historical_pg_routes: current_routes
                .iter()
                .map(StorageNodePgRoute::from)
                .collect(),
        };
        let server = Arc::new(StorageNodeServer::bind(server_config).unwrap());
        for _ in 0..4 {
            server_threads.push(spawn_storage_node_server_loop(
                Arc::clone(&server),
                Arc::clone(&stop),
            ));
            wake_socket_paths.push(socket_path.clone());
        }
        client_configs.push(LocalUnixStorageNodeClientConfig::new(
            config.node_id(),
            socket_path,
        ));
    }

    let mut next_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &pg_ids,
        ec_shape,
        next_epoch,
        next_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    next_map.test_install_historical_pg_routes(current_routes);
    next_map
        .install_unix_storage_node_clients(client_configs)
        .unwrap();
    let next_cluster = StorageCluster::from_local_map(Arc::new(next_map)).unwrap();
    handle.install(next_cluster).unwrap();

    let read = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                &bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(read.body.read_all().unwrap(), payload);
    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                &bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, payload.len() as u64);
    let listed = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(&bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    let listed_keys = listed
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(listed_keys, [key.as_str()]);
    assert!(!listed.is_truncated);

    stop_storage_node_server_loops(stop, &wake_socket_paths, server_threads);
}

#[test]
fn list_objects_epoch_change_before_storage_list_uses_pinned_route() {
    let bucket = "list-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    for (key, data) in [("a/1", b"1".as_slice()), ("a/2", b"2"), ("b/1", b"3")] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let candidate_tmp = test_util::tempdir();
    let hook_handle = handle.clone();
    let hook_initial = Arc::clone(&initial);
    let hook_node_root = candidate_tmp.path().to_path_buf();
    let _serial = LIST_OBJECTS_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_list_objects_test_hooks(ListObjectsTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_list: Some(Arc::new(move || {
            install_next_epoch_runtime_map_with_historical_routes(
                &hook_handle,
                &hook_initial,
                &hook_node_root,
            );
        })),
    });

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    let keys = result
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys, ["a/1", "a/2", "b/1"]);
    assert!(!result.is_truncated);
}

#[test]
fn list_objects_continuation_survives_epoch_change_between_pages() {
    let bucket = "list-continuation-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    for key in ["a/1", "a/2", "a/3", "a/4"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: key.as_bytes(),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let first = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: Some("a/"),
            delimiter: None,
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    let first_keys = first
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(first_keys, ["a/1", "a/2"]);
    assert!(first.is_truncated);
    assert_eq!(first.next_continuation_token.as_deref(), Some("a/2"));

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let second = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: Some("a/"),
            delimiter: None,
            continuation_token: first.next_continuation_token.as_deref(),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    let second_keys = second
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(second_keys, ["a/3", "a/4"]);
    assert!(!second.is_truncated);
    assert_eq!(second.next_continuation_token, None);
}

#[test]
fn list_objects_delimiter_continuation_survives_epoch_change_between_pages() {
    let bucket = "list-delimiter-continuation-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let pg_ids = initial.test_pg_ids();
    assert!(
        pg_ids.len() >= 2,
        "test requires at least two object metadata PGs"
    );
    let key_a = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "a/");
    let key_b = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[1], "b/");
    let key_c = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "c/");
    let root_key =
        find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[1], "z-root-");

    for key in [&key_a, &key_b, &key_c, &root_key] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                data: key.as_bytes(),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let first = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert!(first.objects.is_empty());
    assert_eq!(first.common_prefixes, ["a/".to_string(), "b/".to_string()]);
    assert!(first.is_truncated);
    assert_eq!(first.next_continuation_token.as_deref(), Some("b/"));

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let second = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: first.next_continuation_token.as_deref(),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    let second_keys = second
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(second.common_prefixes, ["c/".to_string()]);
    assert_eq!(second_keys, [root_key.as_str()]);
    assert!(!second.is_truncated);
    assert_eq!(second.next_continuation_token, None);
}

#[test]
fn read_and_list_fail_closed_while_object_metadata_pg_is_peering() {
    let bucket = "object-peering-read-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&coord, bucket, "peering-key");
    let peering_pg = object_pg_id(&coord, bucket, &key);
    let bucket_pg = bucket_pg_id(&coord, bucket);
    assert_ne!(
        peering_pg, bucket_pg,
        "test must keep the bucket PG active while the object metadata PG peers"
    );

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"must-not-be-served-from-peering-pg",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let current_epoch = install_same_store_next_epoch_runtime_map_with_peering_pg(
        &handle,
        &initial,
        tmp.path(),
        peering_pg,
    );
    assert_eq!(
        handle
            .current()
            .local_pg_route(PgId::new(peering_pg))
            .unwrap()
            .state(),
        PgState::Peering
    );

    let assert_pg_not_active = |operation: &str, error: ServerError| {
        assert!(
            matches!(
                error,
                ServerError::Store(storage::StoreError::PgNotActive {
                    pg_id,
                    cluster_epoch,
                    state: PgState::Peering,
                }) if pg_id == peering_pg && cluster_epoch == current_epoch
            ),
            "{operation} should fail closed on the Peering object metadata PG, got {error:?}"
        );
    };

    let get_error = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_pg_not_active("GET", get_error);

    let head_error = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_pg_not_active("HEAD", head_error);

    let list_error = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert_pg_not_active("LIST", list_error);

    let version_list_error = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert_pg_not_active("LIST versions", version_list_error);
}

#[test]
fn large_put_object_pins_runtime_map_after_stream_session_create() {
    let bucket = "large-put-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
        bucket: Some(bucket.to_string()),
        after_loaded: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..BucketWriteHandleTestHooks::default()
    });

    let metadata = MetadataBlob::new();
    let data = vec![b'x'; INTERNAL_SEGMENT_SIZE + 1];
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "large", test_requester(), None),
            data: &data,
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    handle.install(initial).unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "large",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);
}

#[test]
fn streaming_upload_part_pins_runtime_map_after_session_create() {
    let bucket = "stream-part-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let storage_node = coord.storage_node_for_request();
    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let session = {
        let _hook_guard = install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some(bucket.to_string()),
            after_loaded: Some(Arc::new(move || {
                hook_handle.install(Arc::clone(&candidate)).unwrap();
            })),
            ..BucketWriteHandleTestHooks::default()
        });

        coord
            .begin_stream_part_with_storage_node(
                &storage_node,
                &BeginStreamPartRequest {
                    upload: multipart_object_request_with_expected_owner(
                        bucket,
                        "key",
                        &upload.upload_id,
                        test_requester(),
                        None,
                    ),
                    part_number: 1,
                    policy_context: PutObjectPolicyContext::default(),
                    sse_customer: None,
                },
            )
            .unwrap()
    };
    let data = b"streaming-upload-part-pinned-runtime-map";
    coord
        .append_stream_part_data_with_storage_node(
            &storage_node,
            &AppendStreamPartRequest {
                bucket: BucketName::new(bucket).unwrap(),
                key: ObjectKey::new("key").unwrap(),
                upload_id: &upload.upload_id,
                session_id: &session.session_id,
                part_number: 1,
                segment_index: 0,
                data,
                sse_customer: None,
            },
        )
        .unwrap();
    let crc64 = checksum::crc64::checksum(data);
    let part = coord
        .finalize_stream_part_with_storage_node(
            &storage_node,
            FinalizeStreamPartRequest {
                upload: multipart_object_request_with_expected_owner(
                    bucket,
                    "key",
                    &upload.upload_id,
                    test_requester(),
                    None,
                ),
                session_id: &session.session_id,
                part_number: 1,
                crc64,
                total_size: data.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            },
        )
        .unwrap();

    handle.install(initial).unwrap();
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: part.etag,
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);
}

#[test]
fn complete_multipart_upload_pins_runtime_map_between_snapshot_and_commit() {
    let bucket = "complete-multipart-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(
        &coord,
        bucket,
        "key",
        &[(1, b"complete-multipart-pinned-runtime-map")],
    );
    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), "key".to_string())),
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    handle.install(initial).unwrap();
    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        result.body.read_all().unwrap(),
        b"complete-multipart-pinned-runtime-map"
    );
}

#[test]
fn abort_multipart_upload_pins_runtime_map_after_auth_lookup() {
    let bucket = "abort-multipart-pinned-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), "key".to_string())),
        after_abort_multipart_auth_lookup: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            bucket,
            "key",
            &upload.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    handle.install(initial).unwrap();
    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 1000,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected pinned abort to remove upload from original map, got {err:?}"
    );
}

#[test]
fn abort_multipart_upload_pins_runtime_map_after_bucket_summary() {
    let bucket = "abort-multipart-bucket-summary-pinned";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let candidate_tmp = test_util::tempdir();
    let candidate = open_test_storage_cluster(candidate_tmp.path(), &[0, 1]);
    let hook_handle = handle.clone();
    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), "key".to_string())),
        after_abort_multipart_bucket_summary: Some(Arc::new(move || {
            hook_handle.install(Arc::clone(&candidate)).unwrap();
        })),
        ..ReclamationTestHooks::default()
    });

    coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            bucket,
            "key",
            &upload.upload_id,
            test_requester(),
            None,
        ))
        .unwrap();

    handle.install(initial).unwrap();
    let err = coord
        .list_parts(&ListPartsRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number_marker: None,
            max_parts: 1000,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::NoSuchUpload { .. }),
        "expected pinned abort to remove upload from original map, got {err:?}"
    );
}

fn install_bucket_command_log_conflict_hook(
    storage_cluster: &Arc<StorageCluster>,
    bucket: &BucketName,
    kind: MetadataCommandApplyTestKind,
) -> storage::MetadataCommandApplyContextTestHookGuard {
    let bucket_pg = storage_cluster.test_bucket_pg_id_for(bucket);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(bucket_pg))
        .unwrap()
        .primary_node_id();
    let hook_bucket = bucket.clone();
    storage_cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(
        move |context| {
            if context.kind == kind
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.is_none()
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: bucket_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        },
    ))
}

fn install_object_command_log_conflict_hook(
    storage_cluster: &Arc<StorageCluster>,
    bucket: &BucketName,
    key: &ObjectKey,
    kind: MetadataCommandApplyTestKind,
) -> storage::MetadataCommandApplyContextTestHookGuard {
    let object_pg = storage_cluster.test_object_pg_id_for(bucket, key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    storage_cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(
        move |context| {
            if context.kind == kind
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        },
    ))
}

fn delete_bucket_metadata_or_accept_reclaim_worker_finalize(
    storage_cluster: &StorageCluster,
    bucket: &BucketName,
) {
    match storage_cluster.test_delete_bucket_metadata(bucket) {
        Ok(()) => {}
        Err(storage::BucketWriteDrainError::Metadata(storage::MetadataError::BucketNotFound {
            ..
        })) => {}
        Err(err) => panic!("failed to delete test bucket metadata: {err:?}"),
    }
}

#[test]
fn lock_mutex_unpoisoned_recovers_after_panic() {
    let lock = Mutex::new(vec![1usize]);
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = lock.lock().unwrap();
        panic!("poison mutex");
    }));

    lock_mutex_unpoisoned(&lock).push(2);
    assert_eq!(*lock_mutex_unpoisoned(&lock), vec![1, 2]);
}

#[test]
fn rwlock_helpers_recover_after_panic() {
    let lock = RwLock::new(HashMap::from([("bucket".to_string(), 1usize)]));
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut guard = lock.write().unwrap();
        guard.insert("poisoned".to_string(), 2);
        panic!("poison rwlock");
    }));

    write_rwlock_unpoisoned(&lock).insert("ok".to_string(), 3);
    let guard = read_rwlock_unpoisoned(&lock);
    assert_eq!(guard.get("bucket"), Some(&1));
    assert_eq!(guard.get("poisoned"), Some(&2));
    assert_eq!(guard.get("ok"), Some(&3));
}

#[test]
fn object_pg_command_contention_maps_to_operation_aborted() {
    let bucket = trusted_bucket_name("contention-bucket");
    let key = trusted_object_key("contention-key");

    fn assert_maps_to_operation_aborted(error: storage::ObjectPgActionError) {
        assert!(matches!(
            Coordinator::map_object_pg_action_error(error),
            ServerError::OperationAborted
        ));
    }

    fn assert_read_snapshot_maps_to_operation_aborted(
        bucket: &BucketName,
        key: &ObjectKey,
        error: storage::ObjectPgActionError,
    ) {
        assert!(matches!(
            Coordinator::map_object_read_snapshot_error(bucket, key, None, true, error),
            ServerError::OperationAborted
        ));
    }

    let epoch = storage::ClusterEpoch::INITIAL;
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Store(
        storage::StoreError::MetadataCommandLogConflict {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: epoch,
            log_index: 3,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandLogConflict {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: epoch,
            log_index: 3,
        }),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Store(
        storage::StoreError::MetadataCommandPendingConflict {
            pg_id: 2,
            cluster_epoch: epoch,
            existing_log_index: 3,
            candidate_log_index: 4,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandPendingConflict {
            pg_id: 2,
            cluster_epoch: epoch,
            existing_log_index: 3,
            candidate_log_index: 4,
        }),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Store(
        storage::StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Store(storage::StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        }),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Metadata(
        storage::MetadataError::ObjectGenerationReservationConflict {
            reservation_id: "reservation".to_string(),
            generation_id: 5,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Metadata(
            storage::MetadataError::ObjectGenerationReservationConflict {
                reservation_id: "reservation".to_string(),
                generation_id: 5,
            },
        ),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Metadata(
        storage::MetadataError::ObjectVersionReservationConflict {
            version_id: storage::VersionId::from_u64(7),
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Metadata(
            storage::MetadataError::ObjectVersionReservationConflict {
                version_id: storage::VersionId::from_u64(7),
            },
        ),
    );
    assert_maps_to_operation_aborted(storage::ObjectPgActionError::Metadata(
        storage::MetadataError::StaleObjectWriteCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            write_sequence: 11,
            generation_id: None,
        },
    ));
    assert_read_snapshot_maps_to_operation_aborted(
        &bucket,
        &key,
        storage::ObjectPgActionError::Metadata(storage::MetadataError::StaleObjectWriteCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            write_sequence: 12,
            generation_id: Some(13),
        }),
    );
}

#[test]
fn stale_bucket_metadata_command_maps_to_operation_aborted() {
    let bucket = trusted_bucket_name("stale-bucket-command");
    let error = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::StaleBucketMetadataCommand {
            name: bucket,
            bucket_execution_generation: 7,
        },
    );
    assert!(matches!(
        Coordinator::map_bucket_snapshot_load_error(error),
        ServerError::OperationAborted
    ));

    let bucket = trusted_bucket_name("stale-bucket-handle-command");
    let error = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::StaleBucketMetadataCommand {
            name: bucket,
            bucket_execution_generation: 8,
        },
    );
    assert!(matches!(
        BucketHandleLoader::map_bucket_snapshot_error(error),
        ServerError::OperationAborted
    ));
}

#[test]
fn bucket_snapshot_object_reservation_conflicts_map_to_operation_aborted() {
    let version_conflict = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::ObjectVersionReservationConflict {
            version_id: storage::VersionId::from_u64(9),
        },
    );
    assert!(matches!(
        Coordinator::map_bucket_snapshot_load_error(version_conflict),
        ServerError::OperationAborted
    ));

    let generation_conflict = storage::BucketSnapshotLoadError::Metadata(
        storage::MetadataError::ObjectGenerationReservationConflict {
            reservation_id: "reservation".to_string(),
            generation_id: 17,
        },
    );
    assert!(matches!(
        BucketHandleLoader::map_bucket_snapshot_error(generation_conflict),
        ServerError::OperationAborted
    ));
}

#[test]
fn bucket_write_drain_contention_maps_to_operation_aborted() {
    let bucket = trusted_bucket_name("bucket-write-drain-contention");
    let epoch = storage::ClusterEpoch::INITIAL;
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            storage::StoreError::MetadataCommandLogConflict {
                node_id: 1,
                pg_id: 2,
                cluster_epoch: epoch,
                log_index: 3,
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            storage::StoreError::MetadataCommandPendingConflict {
                pg_id: 2,
                cluster_epoch: epoch,
                existing_log_index: 3,
                candidate_log_index: 4,
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            storage::StoreError::MetadataCommandContention {
                context: "pending bucket command displaced during cleanup",
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Metadata(
            storage::MetadataError::StaleBucketMetadataCommand {
                name: bucket,
                bucket_execution_generation: 5,
            },
        )),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Metadata(
            storage::MetadataError::ObjectVersionReservationConflict {
                version_id: storage::VersionId::from_u64(11),
            },
        )),
        ServerError::OperationAborted
    ));
}

#[test]
fn storage_rpc_resource_exhaustion_maps_to_slow_down() {
    fn resource_exhausted() -> storage::StoreError {
        storage::StoreError::StorageRpcResourceExhausted {
            node_id: 1,
            operation: "test operation",
            message: "active read handles exceed limit".to_string(),
        }
    }

    fn nested_resource_exhausted() -> storage::StoreError {
        storage::StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            source: Box::new(resource_exhausted()),
        }
    }

    fn nested_delete_in_progress() -> storage::StoreError {
        storage::StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            source: Box::new(storage::StoreError::StorageRpcShardDeleteInProgress {
                node_id: 1,
                operation: "shard delete",
                message: "shard delete fenced by read handle".to_string(),
            }),
        }
    }

    assert!(matches!(
        super::map_store_error(nested_resource_exhausted()),
        ServerError::SlowDown
    ));
    assert!(matches!(
        super::map_store_error(nested_delete_in_progress()),
        ServerError::Store(storage::StoreError::ShardStore { source, .. })
            if matches!(*source, storage::StoreError::StorageRpcShardDeleteInProgress { .. })
    ));
    assert!(matches!(
        super::map_store_error(storage::StoreError::MetadataCommandContention {
            context: "pending command displaced during cleanup",
        }),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        super::map_store_error(storage::StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: storage::ClusterEpoch::INITIAL,
            source: Box::new(storage::StoreError::MetadataCommandContention {
                context: "pending command displaced during cleanup",
            }),
        }),
        ServerError::OperationAborted
    ));
    assert!(matches!(
        Coordinator::map_object_pg_action_error(storage::ObjectPgActionError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        Coordinator::map_object_pg_action_error(storage::ObjectPgActionError::Store(
            nested_resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        Coordinator::map_bucket_snapshot_load_error(storage::BucketSnapshotLoadError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        BucketHandleLoader::map_bucket_snapshot_error(storage::BucketSnapshotLoadError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
    assert!(matches!(
        Coordinator::map_bucket_write_drain_error(storage::BucketWriteDrainError::Store(
            resource_exhausted(),
        )),
        ServerError::SlowDown
    ));
}

#[test]
fn get_object_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"payload-read-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected shard-read overload".to_string(),
            })
        },
    ));

    let err = match coord.get_object(&GetObjectRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        cond: NO_READ,
    }) {
        Ok(result) => result.body.read_all().unwrap_err(),
        Err(err) => err,
    };

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected payload read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn get_object_range_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"payload-range-read-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected range shard-read overload".to_string(),
            })
        },
    ));

    let err = match coord.get_object_range(&GetObjectRangeRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        range: ByteRange::Range { start: 0, end: 6 },
        cond: NO_READ,
    }) {
        Ok(result) => result.body.read_all().unwrap_err(),
        Err(err) => err,
    };

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected range payload read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn get_object_part_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let part = vec![0xAB; MIN_PART];
    let (upload_id, complete_parts) =
        create_upload_with_parts(&coord, "bucket", "key", &[(1, part.as_slice())]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &complete_parts,
            sse_customer: None,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
        })
        .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected multipart-part shard-read overload".to_string(),
            })
        },
    ));

    let err = match coord.get_object_part(&GetObjectPartRequest {
        sse_customer: None,
        object: object_version_request_with_expected_owner(
            "bucket",
            "key",
            None,
            test_requester(),
            None,
        ),
        part_number: 1,
        cond: NO_READ,
    }) {
        Ok(result) => result.body.read_all().unwrap_err(),
        Err(err) => err,
    };

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected multipart part payload read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn copy_object_source_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected copy source overload".to_string(),
            })
        },
    ));

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected copy source read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn upload_part_copy_source_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-source-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected upload-part-copy source overload".to_string(),
            })
        },
    ));

    let err = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            copy_source_range: None,
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected upload-part-copy source read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn upload_part_copy_range_source_payload_read_resource_exhaustion_maps_to_slow_down() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-range-source-overload",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let _hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(Arc::new(
        |location, _shard_key| {
            Err(storage::StoreError::StorageRpcResourceExhausted {
                node_id: location.node_id().as_u32(),
                operation: "read payload shard",
                message: "test injected upload-part-copy range source overload".to_string(),
            })
        },
    ));

    let err = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            copy_source_range: Some((2, 12)),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();

    assert!(
        matches!(err, ServerError::SlowDown),
        "expected ranged upload-part-copy source read overload to map to SlowDown, got {err:?}"
    );
}

#[test]
fn phase_10_6_remote_frontend_worker_mode_enables_routed_workers() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);

    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_with_background_worker_mode(
            storage_cluster,
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::remote_frontend_phase_10_6(),
        )
        .unwrap();

    assert_eq!(
        coord.background_worker_mode_for_test(),
        BackgroundWorkerMode {
            object_reclaim_and_bucket_finalize: true,
            lifecycle: true,
            shard_scavenger: true,
            shard_repair: true,
            shard_backfill: true,
            stream_session: true,
        }
    );
}

#[test]
fn put_object_effective_policy_context_derives_explicit_sse_s3() {
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    let request = PutObjectRequest {
        object: object_request("bucket", "key", test_requester()),
        data: b"body",
        metadata: &metadata,
        system_metadata: &system_metadata,
        tags: None,
        cond: NO_WRITE,
        acl: PutObjectWriteAcl::None,
        policy_context: PutObjectPolicyContext::default(),
        object_lock: ObjectLockState::default(),
        encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
    };

    assert_eq!(
        request
            .effective_policy_context()
            .unwrap()
            .managed_encryption,
        Some(ManagedEncryptionAlgorithm::Aes256)
    );
}

#[test]
fn put_object_effective_policy_context_overrides_conflicting_encryption_fields() {
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    let sse_customer = test_sse_customer_request();
    let request = PutObjectRequest {
        object: object_request("bucket", "key", test_requester()),
        data: b"body",
        metadata: &metadata,
        system_metadata: &system_metadata,
        tags: None,
        cond: NO_WRITE,
        acl: PutObjectWriteAcl::None,
        policy_context: PutObjectPolicyContext::default()
            .with_managed_encryption(Some(ManagedEncryptionAlgorithm::Aes256)),
        object_lock: ObjectLockState::default(),
        encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
    };

    let policy_context = request.effective_policy_context().unwrap();
    assert_eq!(policy_context.managed_encryption, None);
    assert_eq!(
        policy_context.sse_customer_algorithm,
        Some(SSE_CUSTOMER_ALGORITHM)
    );
}

#[test]
fn create_multipart_effective_policy_context_derives_explicit_sse_s3() {
    let metadata = MetadataBlob::default();
    let system_metadata = SystemMetadata::default();
    let request = CreateMultipartUploadRequest {
        object: object_request("bucket", "key", test_requester()),
        metadata: &metadata,
        system_metadata: &system_metadata,
        tags: None,
        checksum: None,
        acl: PutObjectWriteAcl::None,
        policy_context: PutObjectPolicyContext::default(),
        object_lock: ObjectLockState::default(),
        encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
    };

    assert_eq!(
        request
            .effective_policy_context()
            .unwrap()
            .managed_encryption,
        Some(ManagedEncryptionAlgorithm::Aes256)
    );
}

#[test]
fn begin_stream_put_effective_policy_context_uses_request_encryption() {
    let sse_customer = test_sse_customer_request();
    let cleared = WriteEncryptionRequest::none().with_policy_context(
        PutObjectPolicyContext::default()
            .with_managed_encryption(Some(ManagedEncryptionAlgorithm::Aes256))
            .with_sse_customer_algorithm(Some("AES256"))
            .with_default_canned_acl(PutObjectWriteAcl::None.policy_condition_value()),
    );
    assert_eq!(cleared.managed_encryption, None);
    assert_eq!(cleared.sse_customer_algorithm, None);

    let sse_c = WriteEncryptionRequest::sse_customer(&sse_customer).with_policy_context(
        PutObjectPolicyContext::default()
            .with_default_canned_acl(PutObjectWriteAcl::None.policy_condition_value()),
    );
    assert_eq!(sse_c.managed_encryption, None);
    assert_eq!(sse_c.sse_customer_algorithm, Some(SSE_CUSTOMER_ALGORITHM));
}

#[test]
fn put_object_persists_explicit_object_owner_identity() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("custom-object-owner");
    let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
    let requester = Requester::authenticated(owner.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    coord
        .put_object(&PutObjectRequest {
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    let live = coord
        .storage_node()
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let live = live.into_live().expect("expected live object");
    assert_eq!(live.owner.principal, owner.principal());
    assert_eq!(live.owner.canonical_id, owner_canonical_id);
}

#[test]
fn direct_put_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let metadata = MetadataBlob::new();
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"first-write",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected direct PUT command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn direct_put_generation_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectGeneration,
    );

    let metadata = MetadataBlob::new();
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"generation-reservation-conflict",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected direct PUT generation reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn direct_put_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    coord
        .put_bucket_versioning(&PutBucketVersioningRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            state: BucketVersioningState::Enabled,
        })
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );

    let metadata = MetadataBlob::new();
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"version-reservation-conflict",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected direct PUT version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn create_bucket_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::CreateBucket,
    );

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: bucket,
            requester: test_requester(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CreateBucket command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_begin_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CreateStreamUpload
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let err = begin_stream_put_test(&coord, "bucket", "key").unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT begin command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_begin_generation_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectGeneration,
    );

    let err = begin_stream_put_test(&coord, "bucket", "key").unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT begin generation reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_finalize_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let metadata = MetadataBlob::new();
    let write_encryption = coord
        .load_stream_put_write_encryption(&bucket, &key, &session_id, None)
        .unwrap();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b""),
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: write_encryption.as_ref(),
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT finalize command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_put_finalize_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );

    let metadata = MetadataBlob::new();
    let write_encryption = coord
        .load_stream_put_write_encryption(&bucket, &key, &session_id, None)
        .unwrap();
    let err = coord
        .finalize_stream_put(&FinalizeStreamPutRequest {
            object: object_request("bucket", "key", test_requester()),
            session_id: &session_id,
            crc64: checksum::crc64::checksum(b""),
            total_size: 0,
            metadata_blob: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            write_encryption: write_encryption.as_ref(),
            tags: None,
            cond: &WriteCondition::default(),
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            requested_object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream PUT finalize version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn copy_object_destination_create_stream_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CreateStreamUpload,
    );

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CopyObject destination stream create conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
    let leaked_sessions = storage_cluster.list_stream_upload_sessions_best_effort();
    assert!(
        leaked_sessions.is_empty(),
        "failed CopyObject destination stream create must not leave stream uploads: {leaked_sessions:?}"
    );
}

#[test]
fn copy_object_destination_finalize_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitDirectPutObject,
    );

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CopyObject destination finalize conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn copy_object_failure_retries_destination_stream_abort_cleanup() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"copy-source",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let append_failures = Arc::new(AtomicUsize::new(1));
    let abort_failures = Arc::new(AtomicUsize::new(2));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_append_failures = Arc::clone(&append_failures);
    let hook_abort_failures = Arc::clone(&abort_failures);
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.bucket.as_ref() != Some(&hook_bucket)
                || context.key.as_ref() != Some(&hook_key)
                || context.node_id != primary_node
            {
                return Ok(());
            }
            if context.kind == MetadataCommandApplyTestKind::AppendStreamSegment
                && hook_append_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            if context.kind == MetadataCommandApplyTestKind::AbortStreamUpload
                && hook_abort_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let err = coord
        .copy_object(&CopyObjectRequest {
            source: copy_source("bucket", "src", None),
            destination: object_request_with_expected_owner(
                "bucket",
                "dst",
                test_requester(),
                None,
            ),
            dst_condition: NO_WRITE,
            directive: MetadataDirective::Copy,
            website_redirect_location: None,
            tagging: TaggingDirective::Copy,
            acl: NO_PUT_OBJECT_ACL.into(),
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            destination_encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CopyObject append conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
    assert_eq!(
        append_failures.load(Ordering::SeqCst),
        0,
        "test must inject one CopyObject append conflict"
    );
    assert_eq!(
        abort_failures.load(Ordering::SeqCst),
        0,
        "CopyObject cleanup should retry transient abort conflicts"
    );
    let leaked_sessions = storage_cluster.list_stream_upload_sessions_best_effort();
    assert!(
        leaked_sessions.is_empty(),
        "failed CopyObject must not leave stream uploads after retrying abort cleanup: {leaked_sessions:?}"
    );
}

#[test]
fn stream_append_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AppendStreamSegment,
    );

    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session_id, 0, b"chunk")
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream append command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn stream_abort_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AbortStreamUpload,
    );

    let err = coord
        .abort_stream_put("bucket", "key", &session_id)
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected stream abort command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_versioning_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketVersioning,
    );

    let err = put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketVersioning command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_acl_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketAcl,
    );

    let err =
        put_bucket_canned_acl_test(&coord, "bucket", BucketAcl::Private, test_requester(), None)
            .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketAcl command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_property_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketProperty,
    );

    let err = put_bucket_public_access_block_test(
        &coord,
        "bucket",
        "<PublicAccessBlockConfiguration><BlockPublicPolicy>true</BlockPublicPolicy></PublicAccessBlockConfiguration>",
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketProperty command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_bucket_subresource_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::PutBucketSubresource,
    );

    let cors = "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    let err = coord
        .put_bucket_cors(&put_bucket_config_request_with_expected_owner(
            "bucket",
            cors,
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutBucketSubresource command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn put_object_metadata_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"tag-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::PutObjectMetadata,
    );

    let err = put_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        "foo=bar",
        test_requester(),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected PutObjectMetadata command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_object_version_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"delete-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::DeleteObjectVersion
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected delete object command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_objects_entry_maps_command_log_conflict_to_operation_aborted_error() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"delete-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::DeleteObjectVersion,
    );

    let entries = [DeleteEntry {
        key,
        version_id: None,
        cond: DeleteCondition::None,
    }];
    let result = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap();
    assert!(
        result.deleted.is_empty(),
        "failed delete entry must not be reported as deleted: {result:?}"
    );
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].key, "key");
    assert_eq!(result.errors[0].code, "OperationAborted");
    drop(hook_guard);
}

#[test]
fn delete_object_marker_insert_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::InsertDeleteMarker
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
            {
                return Err(storage::StoreError::MetadataCommandLogConflict {
                    node_id: primary_node.as_u32(),
                    pg_id: object_pg,
                    cluster_epoch: storage::ClusterEpoch::INITIAL,
                    log_index: 1,
                });
            }
            Ok(())
        }),
    );

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected delete marker command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_object_marker_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );

    let err = coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected delete marker version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn create_multipart_upload_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CreateMultipartUpload,
    );

    let metadata = MetadataBlob::new();
    let err = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CreateMultipartUpload command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn complete_multipart_upload_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitMultipartObject,
    );

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CompleteMultipartUpload command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn complete_multipart_upload_version_reservation_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::ReserveObjectVersion,
    );

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected CompleteMultipartUpload version reservation conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn upload_part_append_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let session = begin_stream_part_test(&coord, "bucket", "key", &create.upload_id, 1).unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AppendStreamSegment,
    );

    let err = coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session.session_id, 0, b"part")
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected UploadPart append command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn upload_part_finalize_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();
    let session = begin_stream_part_test(&coord, "bucket", "key", &create.upload_id, 1).unwrap();
    let data = b"part";
    coord
        .append_plaintext_stream_segment_for_test("bucket", "key", &session.session_id, 0, data)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitStreamPart,
    );

    let err = coord
        .finalize_stream_part(FinalizeStreamPartRequest {
            upload: multipart_object_request("bucket", "key", &create.upload_id, test_requester()),
            session_id: &session.session_id,
            part_number: 1,
            crc64: checksum::crc64::checksum(data),
            total_size: data.len() as u64,
            claimed_checksum: None,
            computed_checksum: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected UploadPart finalize command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn upload_part_copy_destination_finalize_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "src", test_requester(), None),
            data: b"upload-part-copy-source",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "dst", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("dst");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::CommitStreamPart,
    );

    let err = coord
        .upload_part_copy(&UploadPartCopyRequest {
            source: copy_source("bucket", "src", None),
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "dst",
                &upload.upload_id,
                test_requester(),
                None,
            ),
            part_number: 1,
            copy_source_range: None,
            policy_context: PutObjectPolicyContext::default(),
            source_sse_customer: None,
            sse_customer: None,
        })
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected UploadPartCopy destination commit conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn abort_multipart_upload_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, _parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AbortMultipartUpload,
    );

    let err = coord
        .abort_multipart_upload(&multipart_object_request_with_expected_owner(
            "bucket",
            "key",
            &upload_id,
            test_requester(),
            None,
        ))
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected AbortMultipartUpload command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn delete_bucket_begin_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_bucket_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        MetadataCommandApplyTestKind::MarkBucketDeleting,
    );

    let err = delete_bucket_test(&coord, "bucket").unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected DeleteBucket begin command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn bucket_delete_finalizer_request_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1")]);
    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::DeleteCompletedMultipartUpload,
    );

    let err = coord
        .read_runtime()
        .try_finalize_bucket_delete_for(&bucket)
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected bucket delete finalizer command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn lifecycle_current_expiry_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        "<LifecycleConfiguration><Rule><ID>expire</ID><Filter><Prefix/></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
        test_requester(),
        None,
    )
    .unwrap();

    let metadata = MetadataBlob::new();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"expire-me",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let version_id = coord
        .storage_node()
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .as_live()
        .unwrap()
        .version_id;
    let bucket_incarnation_generation = coord
        .storage_node()
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_incarnation_generation;
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::DeleteObjectVersion,
    );

    let err = coord
        .read_runtime()
        .expire_current_object_if_due(
            &bucket,
            &key,
            version_id,
            bucket_incarnation_generation,
            u64::MAX,
        )
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected lifecycle current expiry command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn lifecycle_abort_multipart_maps_command_log_conflict_to_operation_aborted() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_lifecycle_test(
        &coord,
        "bucket",
        "<LifecycleConfiguration><Rule><ID>abort-mpu</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>",
        test_requester(),
        None,
    )
    .unwrap();

    let metadata = MetadataBlob::new();
    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request("bucket", "logs/app", test_requester()),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("logs/app");
    let bucket_incarnation_generation = coord
        .storage_node()
        .head_bucket_info(&bucket)
        .unwrap()
        .bucket_incarnation_generation;
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let hook_guard = install_object_command_log_conflict_hook(
        &storage_cluster,
        &bucket,
        &key,
        MetadataCommandApplyTestKind::AbortMultipartUpload,
    );

    let err = coord
        .read_runtime()
        .abort_multipart_upload_if_due(
            &bucket,
            &key,
            &create.upload_id,
            bucket_incarnation_generation,
            u64::MAX,
        )
        .unwrap_err();
    assert!(
        matches!(err, ServerError::OperationAborted),
        "expected lifecycle multipart abort command conflict to map to OperationAborted, got {err:?}"
    );
    drop(hook_guard);
}

#[test]
fn direct_put_retry_converges_pending_partial_metadata_command() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let object_pg = storage_cluster.test_object_pg_id_for(&bucket, &key);
    let primary_node = storage_cluster
        .local_pg_route(PgId::new(object_pg))
        .unwrap()
        .primary_node_id();
    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = storage_cluster.test_install_before_metadata_command_apply_context_hook(
        Arc::new(move |context| {
            if context.kind == MetadataCommandApplyTestKind::CommitDirectPutObject
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && context.node_id == primary_node
                && fail_once_hook.swap(false, Ordering::SeqCst)
            {
                return Err(storage::StoreError::Io {
                    context: "injected coordinator direct put metadata command apply failure",
                    source: std::io::Error::other(
                        "injected coordinator direct put metadata command apply failure",
                    ),
                });
            }
            Ok(())
        }),
    );

    let metadata = MetadataBlob::new();
    let first_err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"first-write",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(
        matches!(
            first_err,
            ServerError::Store(storage::StoreError::Io {
                context: "injected coordinator direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected direct PUT command failure, got {first_err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));
    drop(hook_guard);

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"retry-write",
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let get = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(get.body.read_all().unwrap(), b"retry-write");
}

#[test]
fn delete_marker_persists_explicit_owner_identity() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("custom-delete-owner");
    let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
    let requester = Requester::authenticated(owner.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        requester.clone(),
        None,
    )
    .unwrap();
    coord
        .put_object(&PutObjectRequest {
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            data: b"hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
        })
        .unwrap();

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            requester.clone(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let marker = coord
        .storage_node()
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let marker = match marker {
        StoredObject::DeleteMarker(marker) => marker,
        other => panic!("expected delete marker, got {other:?}"),
    };
    assert_eq!(marker.owner.principal, owner.principal());
    assert_eq!(marker.owner.canonical_id, owner_canonical_id);
}

#[test]
fn multipart_upload_and_complete_persist_explicit_owner_identity() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner_canonical_id = CanonicalUserId::from_principal("custom-mpu-owner");
    let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
    let requester = Requester::authenticated(owner.clone());
    let expected_owner =
        OwnerIdentity::new(owner.principal().to_string(), owner_canonical_id.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", requester.clone(), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let upload_record = coord
        .storage_node()
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
        )
        .unwrap();
    assert_eq!(upload_record.initiator, Some(expected_owner.clone()));
    assert_eq!(upload_record.owner, expected_owner);

    test_helpers::upload_part(
        &coord,
        &UploadPartRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                requester.clone(),
                None,
            ),
            part_number: 1,
            data: b"multipart-data",
            claimed_checksum: None,

            sse_customer: None,
        },
    )
    .unwrap();

    coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &upload.upload_id,
                requester.clone(),
                None,
            ),
            parts: &[CompletePart {
                part_number: 1,
                etag: format_etag(checksum::crc64::checksum(b"multipart-data")),
                checksum: None,
            }],
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        })
        .unwrap();

    let live = coord
        .storage_node()
        .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
        .unwrap();
    let live = live.into_live().expect("expected completed object");
    assert_eq!(live.owner.principal, owner.principal());
    assert_eq!(live.owner.canonical_id, owner_canonical_id);
}

#[test]
fn create_multipart_upload_bucket_owner_preferred_promotes_bucket_owner_with_full_control_acl() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("bucket-owner-canonical"),
        "Bucket Owner",
    );
    let writer = AccountIdentity::new(
        "writer-a",
        CanonicalUserId::from_principal("writer-canonical"),
        "Writer",
    );

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(bucket_owner.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();
    put_bucket_canned_acl_test(
        &coord,
        "bucket",
        BucketAcl::PublicReadWrite,
        Requester::authenticated(bucket_owner.clone()),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(&coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>",
            Requester::authenticated(bucket_owner.clone()), None)
        .unwrap();

    let upload = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                Requester::authenticated(writer.clone()),
                None,
            ),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: PutObjectAcl::BucketOwnerFullControl.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let upload_record = coord
        .storage_node()
        .test_get_multipart_upload(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("key"),
            &upload.upload_id,
        )
        .unwrap();
    assert_eq!(
        upload_record.initiator,
        Some(OwnerIdentity::new(
            writer.principal().to_string(),
            writer.canonical_user_id().clone(),
        ))
    );
    assert_eq!(
        upload_record.owner,
        OwnerIdentity::new(
            bucket_owner.principal().to_string(),
            bucket_owner.canonical_user_id().clone(),
        )
    );
}

#[test]
fn create_bucket_idempotent_create_does_not_overwrite_ownership_controls() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketAlreadyOwnedByYou));

    let controls = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        controls,
        BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::ObjectWriter,
        }
    );
}

#[test]
fn create_bucket_rejects_public_read_with_owner_enforced() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Canned(BucketAcl::PublicRead),
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithObjectOwnership
    ));
}

#[test]
fn create_bucket_rejects_public_read_with_object_writer() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Canned(BucketAcl::PublicRead),
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithBlockPublicAccessError
    ));
}

#[test]
fn create_bucket_allows_default_private_with_owner_enforced() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let controls = get_bucket_ownership_controls_test(
        &coord,
        "bucket",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        controls,
        BucketOwnershipControls {
            object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
        }
    );
}

#[test]
fn get_bucket_acl_bucket_owner_enforced_allows_same_account_owner_view() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-acl-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/reader",
        CanonicalUserId::from_principal("bucket-same-account-acl-canonical"),
        "Same Account Reader",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let same_account_requester = Requester::authenticated_owner_account_admin(same_account_user);

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();

    let boe_acl = get_bucket_acl_test(&coord, "bucket", same_account_requester, None).unwrap();
    assert_eq!(
        boe_acl.owner_canonical_id,
        bucket_owner.canonical_user_id().clone()
    );
    assert_eq!(boe_acl.acl_grants.iter().count(), 1);
    assert!(boe_acl.acl_grants.iter().any(|grant| {
        grant
            == &AclGrant::new(
                AclGrantee::CanonicalUser(bucket_owner.canonical_user_id().clone()),
                AclPermission::FullControl,
            )
    }));
}

#[test]
fn authorize_get_bucket_acl_bucket_owner_enforced_allows_same_account_owner_view() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        CanonicalUserId::from_principal("bucket-owner-acl-canonical"),
        "Bucket Owner",
    );
    let same_account_user = AccountIdentity::new(
        "arn:aws:iam::111122223333:user/reader",
        CanonicalUserId::from_principal("bucket-same-account-acl-canonical"),
        "Same Account Reader",
    );
    let owner_requester = Requester::authenticated(bucket_owner.clone());
    let same_account_requester = Requester::authenticated_owner_account_admin(same_account_user);

    create_bucket_for_owner_with_flags(
        &coord,
        bucket_owner.principal(),
        bucket_owner.canonical_user_id(),
        "bucket",
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester,
        None,
    )
    .unwrap();

    let authorized = coord
        .authorize_get_bucket_acl(&bucket_request_with_expected_owner(
            "bucket",
            same_account_requester,
            None,
        ))
        .unwrap();
    assert_eq!(
        authorized.result.owner_canonical_id,
        bucket_owner.canonical_user_id().clone()
    );
    assert_eq!(authorized.result.acl_grants.iter().count(), 1);
    assert!(authorized.result.acl_grants.iter().any(|grant| {
        grant
            == &AclGrant::new(
                AclGrantee::CanonicalUser(bucket_owner.canonical_user_id().clone()),
                AclPermission::FullControl,
            )
    }));
}

#[test]
fn create_bucket_rejects_explicit_private_with_owner_enforced() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: test_helpers::requester("owner-a"),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Canned(BucketAcl::Private),
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap_err();
    assert!(matches!(
        err,
        ServerError::InvalidBucketAclWithObjectOwnership
    ));
}

#[test]
fn create_bucket_persists_explicit_grants() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let owner = AccountIdentity::new(
        "owner-a",
        CanonicalUserId::from_principal("owner-create-grants-canonical"),
        "Owner A",
    );
    let writer = AccountIdentity::new(
        "writer-a",
        CanonicalUserId::from_principal("writer-create-grants-canonical"),
        "Writer A",
    );
    let owner_requester = Requester::authenticated(owner.clone());
    let writer_requester = Requester::authenticated(writer.clone());

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: owner_requester.clone(),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::Grants(AclGrants::new(vec![
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::Read,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::Write,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::ReadAcp,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::WriteAcp,
                ),
                AclGrant::new(
                    AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                    AclPermission::FullControl,
                ),
            ])),
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let acl = get_bucket_acl_test(&coord, "bucket", owner_requester.clone(), None).unwrap();
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::Read,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::Write,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::ReadAcp,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::WriteAcp,
    ));
    assert!(grants_contain(
        &acl.acl_grants,
        &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
        AclPermission::FullControl,
    ));
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", writer_requester, None),
            data: b"granted-write",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
}

#[test]
fn list_buckets_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let names: Vec<String> = coord
        .list_buckets(&ListBucketsRequest {
            requester: test_helpers::requester("default-owner"),
        })
        .unwrap()
        .into_iter()
        .map(|b| b.name.into_string())
        .collect();
    assert_eq!(names, vec![bucket]);
}

#[test]
fn list_objects_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let resp = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert!(resp.objects.is_empty());
}

#[test]
fn list_object_versions_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let resp = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert!(resp.versions.is_empty());
}

#[test]
fn list_object_versions_clamps_oversized_max_keys() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    for index in 0..1005 {
        let key = format!("key-{index:04}");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let resp = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 5000,
            requested_max_keys: Some(5000),
        })
        .unwrap();

    assert_eq!(resp.versions.len(), 1000);
    assert!(resp.is_truncated);
    assert_eq!(resp.next_key_marker.as_deref(), Some("key-0999"));
    assert_eq!(resp.next_version_id_marker, Some(VersionId::from_u64(1)));
}

#[test]
fn list_object_versions_paginates_across_pgs() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let key_a = find_key_with_object_pg_distinct_from(&coord, "bucket", "a", &[]);
    let pg_a = object_pg_id(&coord, "bucket", &key_a);
    let key_b = find_key_with_object_pg_distinct_from(&coord, "bucket", "b", &[pg_a]);
    let pg_b = object_pg_id(&coord, "bucket", &key_b);
    let key_c = find_key_with_object_pg_distinct_from(&coord, "bucket", "c", &[pg_a, pg_b]);

    let older_a = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key_a, test_requester(), None),
            data: b"older-a",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let newer_a = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key_a, test_requester(), None),
            data: b"newer-a",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key_b, test_requester(), None),
            data: b"value-b",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key_c, test_requester(), None),
            data: b"value-c",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let first_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(first_page.versions.len(), 2);
    assert_eq!(first_page.versions[0].key, key_a);
    assert_eq!(first_page.versions[0].version_id, newer_a.version_id);
    assert_eq!(first_page.versions[1].key, key_a);
    assert_eq!(first_page.versions[1].version_id, older_a.version_id);
    assert!(first_page.versions[0].is_latest);
    assert!(!first_page.versions[1].is_latest);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker.as_deref(), Some(key_a.as_str()));
    assert_eq!(first_page.next_version_id_marker, Some(older_a.version_id));

    let second_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: first_page.next_key_marker.as_deref(),
            version_id_marker: first_page.next_version_id_marker,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(second_page.versions.len(), 2);
    assert_eq!(second_page.versions[0].key, key_b);
    assert_eq!(second_page.versions[0].version_id, VersionId::from_u64(1));
    assert!(second_page.versions[0].is_latest);
    assert_eq!(second_page.versions[1].key, key_c);
    assert_eq!(second_page.versions[1].version_id, VersionId::from_u64(1));
    assert!(second_page.versions[1].is_latest);
    assert!(!second_page.is_truncated);
    assert_eq!(second_page.next_key_marker, None);
    assert_eq!(second_page.next_version_id_marker, None);
}

#[test]
fn list_object_versions_continuation_survives_epoch_change_between_pages() {
    let bucket = "version-continuation-epoch-change-bucket";
    let tmp = test_util::tempdir();
    let initial = open_test_storage_cluster(tmp.path(), &[0, 1]);
    let handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial));
    let coord =
        Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
            handle.clone(),
            "us-east-1".to_string(),
            None,
            test_sse_s3_provider(),
            BackgroundWorkerMode::none(),
        )
        .unwrap();
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        bucket,
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    let pg_ids = initial.test_pg_ids();
    assert!(
        pg_ids.len() >= 2,
        "test requires at least two object metadata PGs"
    );
    let key_a = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[0], "a/");
    let key_b = find_key_for_object_metadata_pg_with_prefix(&initial, bucket, pg_ids[1], "b/");

    let older_a = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key_a, test_requester(), None),
            data: b"older-a",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let newer_a = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key_a, test_requester(), None),
            data: b"newer-a",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key_b, test_requester(), None),
            data: b"value-b",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let first_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(first_page.versions.len(), 2);
    assert_eq!(first_page.versions[0].key, key_a);
    assert_eq!(first_page.versions[0].version_id, newer_a.version_id);
    assert_eq!(first_page.versions[1].key, key_a);
    assert_eq!(first_page.versions[1].version_id, older_a.version_id);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker.as_deref(), Some(key_a.as_str()));
    assert_eq!(first_page.next_version_id_marker, Some(older_a.version_id));

    install_same_store_next_epoch_runtime_map(&handle, &initial, tmp.path());

    let second_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            delimiter: None,
            key_marker: first_page.next_key_marker.as_deref(),
            version_id_marker: first_page.next_version_id_marker,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(second_page.versions.len(), 1);
    assert_eq!(second_page.versions[0].key, key_b);
    assert_eq!(second_page.versions[0].version_id, VersionId::from_u64(1));
    assert!(second_page.versions[0].is_latest);
    assert!(!second_page.is_truncated);
    assert_eq!(second_page.next_key_marker, None);
    assert_eq!(second_page.next_version_id_marker, None);
}

#[test]
fn list_object_versions_with_delimiter_returns_common_prefixes() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    for key in ["dir/a", "dir/b", "z.txt"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let result = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: None,
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();

    assert_eq!(result.common_prefixes, vec!["dir/".to_string()]);
    assert_eq!(result.versions.len(), 1);
    assert_eq!(result.versions[0].key, "z.txt");
    assert!(!result.is_truncated);
    assert_eq!(result.next_key_marker, None);
    assert_eq!(result.next_version_id_marker, None);
}

#[test]
fn list_object_versions_delimiter_paginates_common_prefixes() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    for key in ["dir/a", "dir/b", "z.txt"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let first_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: None,
            version_id_marker: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(first_page.versions.is_empty());
    assert_eq!(first_page.common_prefixes, vec!["dir/".to_string()]);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker.as_deref(), Some("dir/"));
    assert_eq!(first_page.next_version_id_marker, None);

    let second_page = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: first_page.next_key_marker.as_deref(),
            version_id_marker: first_page.next_version_id_marker,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(second_page.common_prefixes.is_empty());
    assert_eq!(second_page.versions.len(), 1);
    assert_eq!(second_page.versions[0].key, "z.txt");
    assert!(!second_page.is_truncated);
    assert_eq!(second_page.next_key_marker, None);
    assert_eq!(second_page.next_version_id_marker, None);
}

#[test]
fn list_object_versions_delimiter_filters_common_prefix_at_or_before_key_marker() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let metadata = MetadataBlob::new();
    let system_metadata = SystemMetadata::EMPTY;

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_versioning_test(
        &coord,
        "bucket",
        BucketVersioningState::Enabled,
        test_requester(),
        None,
    )
    .unwrap();

    for key in ["allowed/again", "allowed/versioned", "z.txt"] {
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", key, test_requester(), None),
                data: b"value",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let result = coord
        .list_object_versions(&ListObjectVersionsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            key_marker: Some("allowed/again"),
            version_id_marker: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();

    assert!(result.common_prefixes.is_empty());
    assert_eq!(result.versions.len(), 1);
    assert_eq!(result.versions[0].key, "z.txt");
    assert!(!result.is_truncated);
}

#[test]
fn list_multipart_uploads_with_sparse_pg_topology() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0, 2, 5]);
    let coord = setup_same_process_coordinator_with_storage_cluster(storage_cluster);
    let bucket = "bucket-sparse";
    let key = "key-sparse";
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, key, test_requester(), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,
            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    let resp = coord
        .list_multipart_uploads(&ListMultipartUploadsRequest {
            bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .unwrap();
    assert_eq!(resp.uploads.len(), 1);
    assert_eq!(resp.uploads[0].key, key);
}

#[test]
fn delete_nonempty_bucket_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = delete_bucket_test(&coord, "bucket").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotEmpty));
}

#[test]
fn put_object_persists_tags_in_initial_write() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.tags.as_deref(), Some(tags_xml));
}

#[test]
fn put_object_with_tags_allows_same_account_owner_account() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let same_account_canonical_id = CanonicalUserId::from_principal("111122223333");
    let bucket_owner = AccountIdentity::new(
        "arn:aws:iam::111122223333:root",
        same_account_canonical_id.clone(),
        "Bucket Owner",
    );
    let same_account_account_principal = AccountIdentity::new(
        "111122223333",
        same_account_canonical_id,
        "Same Account Owner Principal",
    );

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name("bucket"),
            requester: Requester::authenticated(bucket_owner.clone()),
            namespace: BucketNamespace::Global,
            acl: CreateBucketAcl::DefaultPrivate,
            ownership: BucketObjectOwnership::ObjectWriter,
            object_lock_enabled: false,
        })
        .unwrap();

    let tags_xml =
        "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "key",
                Requester::authenticated_owner_account_admin(same_account_account_principal),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: Some(tags_xml),
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let tags = get_object_tags_test(
        &coord,
        "bucket",
        "key",
        None,
        Requester::authenticated(bucket_owner),
        None,
    )
    .unwrap();
    assert_eq!(tags.as_deref(), Some(tags_xml));
}

#[test]
fn put_object_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-put-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    let writer = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _write_handle_serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        ..BucketScopedTestHooks::default()
    });
    let _write_handle_hook_guard =
        install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some(bucket.to_string()),
            after_loaded: Some(Arc::new(move || {
                let _ = event_tx.send(LockWaitEvent::Progress);
            })),
            ..BucketWriteHandleTestHooks::default()
        });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = test_helpers::put_object(
            &writer,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        );
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "put_object should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn create_multipart_upload_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-create-mpu-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    let creator = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _write_handle_serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        ..BucketScopedTestHooks::default()
    });
    let _write_handle_hook_guard =
        install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some(bucket.to_string()),
            after_loaded: Some(Arc::new(move || {
                let _ = event_tx.send(LockWaitEvent::Progress);
            })),
            ..BucketWriteHandleTestHooks::default()
        });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let metadata = MetadataBlob::new();
        let res = creator.create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner(bucket, "key", test_requester(), None),
            metadata: &metadata,
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        });
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "create_multipart_upload should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn delete_bucket_waits_for_bucket_write_handle_action() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-waits-handle";
    let coord = Arc::new(setup_coordinator_with_pg_count(tmp.path(), 1));
    let requester = test_requester();

    coord
        .create_bucket(&CreateBucketRequest {
            name: trusted_bucket_name(bucket),
            requester: requester.clone(),
            acl: CreateBucketAcl::DefaultPrivate,
            namespace: BucketNamespace::Global,
            ownership: BucketObjectOwnership::BucketOwnerEnforced,
            object_lock_enabled: false,
        })
        .unwrap();

    let _serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (drain_wait_tx, drain_wait_rx) = mpsc::channel();
    let (delete_tx, delete_rx) = mpsc::channel();
    let _hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_write_drain_wait: Some(Arc::new(move || {
            let _ = drain_wait_tx.send(());
        })),
        ..BucketScopedTestHooks::default()
    });

    let write_coord = Arc::clone(&coord);
    let write_request = object_request(bucket, "key", requester.clone());
    let write_thread = thread::spawn(move || {
        write_coord.with_bucket_write_handle_for(
            &write_request,
            BucketHandleRequest::new(),
            |_bucket| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, ServerError>(())
            },
        )
    });

    started_rx.recv().unwrap();

    let delete_coord = Arc::clone(&coord);
    let delete_request = BucketRequest {
        name: trusted_bucket_name(bucket),
        requester: requester.clone(),
        expected_bucket_owner: None,
    };
    let delete_thread = thread::spawn(move || {
        let result = delete_coord.delete_bucket(&delete_request);
        delete_tx.send(result).unwrap();
    });

    drain_wait_rx.recv().unwrap();
    assert!(matches!(
        delete_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    release_tx.send(()).unwrap();

    write_thread.join().unwrap().unwrap();
    delete_thread.join().unwrap();
    delete_rx.recv().unwrap().unwrap();
}

#[test]
fn delete_bucket_authorizes_idempotent_retry_while_deleting() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-idempotent-auth";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();

    delete_bucket_test(&coord, bucket)
        .expect("idempotent DeleteBucket retry should authorize while bucket delete drain exists");
}

#[test]
fn delete_bucket_stale_raw_authorization_does_not_delete_recreated_bucket() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-stale-auth-recreate";
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("attacker-owner", bucket, false)
        .unwrap();

    let bucket_name = trusted_bucket_name(bucket);
    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();
    let stale_authorized = coord
        .authorize_delete_bucket(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("attacker-owner"),
            None,
        ))
        .expect("idempotent retry should authorize against the deleting bucket incarnation");

    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);
    coord
        .create_bucket_for_owner("victim-owner", bucket, false)
        .unwrap();
    let recreated = storage_cluster.head_bucket_info(&bucket_name).unwrap();
    assert_eq!(recreated.owner_principal, "victim-owner");
    assert_eq!(recreated.state, storage::BucketState::Active);
    assert_ne!(
        recreated.bucket_incarnation_generation, stale_authorized.bucket_incarnation_generation,
        "recreated bucket must be a distinct incarnation"
    );

    let err = storage_cluster
        .begin_bucket_delete_if_current(
            &stale_authorized.name,
            stale_authorized.bucket_execution_generation,
            stale_authorized.bucket_incarnation_generation,
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            storage::BucketWriteDrainError::Store(
                storage::StoreError::MetadataCommandContention { .. }
            )
        ),
        "stale authorization should return retryable contention, got {err:?}"
    );

    let still_active = storage_cluster.head_bucket_info(&bucket_name).unwrap();
    assert_eq!(still_active.owner_principal, "victim-owner");
    assert_eq!(still_active.state, storage::BucketState::Active);
    assert_eq!(
        still_active.bucket_incarnation_generation,
        recreated.bucket_incarnation_generation
    );
}

#[test]
fn delete_bucket_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    let deleter = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        after_begin_bucket_delete_drain: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        ..BucketScopedTestHooks::default()
    });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = delete_bucket_test(&deleter, bucket);
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "delete_bucket should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn head_object_lazily_populates_bucket_fast_path_for_boe_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count(tmp.path(), METADATA_FANOUT_TEST_PG_COUNT);
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        "bucket",
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_requester(),
        None,
    )
    .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&coord, "bucket", "head-fast");
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord.remove_bucket_fast_path(&trusted_bucket_name("bucket"));
    assert!(coord
        .get_bucket_fast_path(&trusted_bucket_name("bucket"))
        .is_none());

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, 4);

    let cached = coord
        .get_bucket_fast_path(&trusted_bucket_name("bucket"))
        .expect("head_object should populate BOE bucket fast path");
    assert_eq!(cached.name.as_str(), "bucket");
    assert_eq!(cached.state, BucketState::Active);
}

#[test]
fn head_object_waits_for_bucket_pg_when_non_boe_bucket_fast_path_is_warm() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-fast-no-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            panic!("non-BOE head_object should not use fast bucket path");
        })),
    });
    let bucket_pg = storage_cluster
        .test_lock_bucket_pg(&trusted_bucket_name(bucket))
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = reader.head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        });
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(
        rx.try_recv().is_err(),
        "head_object returned before bucket pg released"
    );
    drop(bucket_pg);
    let head = rx
        .recv()
        .expect("head_object should complete after bucket pg released")
        .unwrap();
    assert_eq!(head.size, 4);
    handle.join().unwrap();
}

#[test]
fn head_object_uses_validated_boe_fast_path_when_warm() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-boe-fast-no-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_requester(),
        None,
    )
    .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-boe-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_load = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx_load.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
    });
    let head = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert_eq!(head.size, 4);
}

#[test]
fn head_object_uses_validated_boe_policy_and_abac_tags_fast_path_when_warm() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-policy-abac-fast";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    admin
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    admin
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-abac-fast/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-policy-abac-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                &key,
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let cached = reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("head_object should populate policy/tag fast path");
    assert!(matches!(
        cached.policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_load = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx_load.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
    });
    let head = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert_eq!(head.size, 4);

    admin
        .put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner(
                    bucket,
                    test_helpers::requester("111122223333"),
                    None,
                ),
                account_id: "111122223333",
            },
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            request_tags: &[],
        })
        .unwrap();
    assert!(admin
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());
}

#[test]
fn head_object_fast_path_denies_with_non_matching_boe_abac_bucket_tags() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-policy-abac-fast-deny";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    admin
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    admin
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(
                bucket,
                test_helpers::requester("111122223333"),
                None,
            ),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-abac-fast-deny/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-policy-abac-fast-deny");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                &key,
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let cached = reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("head_object should populate policy/tag fast path");
    assert!(matches!(
        cached.policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_load = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx_load.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn head_object_reloads_after_boe_policy_mutation_rebuilds_fast_path() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-head-policy-cold-fallback";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader_after_reload =
        setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_requester(),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-cold-fallback/*"}]}"#,
        test_requester(),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "head-policy-cold");
    let key_after_reload = key.clone();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert!(reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());

    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-head-policy-cold-fallback/*"}]}"#,
        test_requester(),
        None,
    )
    .unwrap();
    let cached = reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("policy mutation should leave cached entry in place");
    let raw = storage_cluster
        .test_head_bucket_raw(&trusted_bucket_name(bucket))
        .expect("policy mutation should leave bucket metadata readable");
    assert!(
        raw.bucket_execution_generation > cached.bucket_execution_generation,
        "bucket execution generation should advance on policy mutation"
    );
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&trusted_bucket_name(bucket)),
        Some(false),
        "same-process policy mutation should immediately mark the cached BOE entry stale"
    );

    {
        let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let saw_storage_load = Arc::new(AtomicBool::new(false));
        let saw_fast_path = Arc::new(AtomicBool::new(false));
        let saw_storage_load_hook = Arc::clone(&saw_storage_load);
        let saw_fast_path_hook = Arc::clone(&saw_fast_path);
        let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
            bucket: Some(bucket.to_string()),
            before_storage_load: Some(Arc::new(move || {
                saw_storage_load_hook.store(true, Ordering::SeqCst);
            })),
            after_policy_fast_path_hit: Some(Arc::new(move || {
                saw_fast_path_hook.store(true, Ordering::SeqCst);
            })),
        });
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = reader.head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    bucket,
                    &key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            });
            tx.send(res).unwrap();
        });
        let head = rx
            .recv_timeout(TEST_EVENT_TIMEOUT)
            .expect("head_object should complete after storage reload")
            .unwrap();
        assert!(
            saw_storage_load.load(Ordering::SeqCst),
            "first read after policy mutation should reload from storage"
        );
        assert_eq!(head.size, 4);
        handle.join().unwrap();
    }

    {
        let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let saw_storage_load = Arc::new(AtomicBool::new(false));
        let saw_fast_path = Arc::new(AtomicBool::new(false));
        let saw_storage_load_hook = Arc::clone(&saw_storage_load);
        let saw_fast_path_hook = Arc::clone(&saw_fast_path);
        let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
            bucket: Some(bucket.to_string()),
            before_storage_load: Some(Arc::new(move || {
                saw_storage_load_hook.store(true, Ordering::SeqCst);
            })),
            after_policy_fast_path_hit: Some(Arc::new(move || {
                saw_fast_path_hook.store(true, Ordering::SeqCst);
            })),
        });
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = reader_after_reload.head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    bucket,
                    &key_after_reload,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            });
            tx.send(res).unwrap();
        });
        let head = rx
            .recv_timeout(TEST_EVENT_TIMEOUT)
            .expect("head_object should complete from rebuilt fast path")
            .unwrap();
        assert!(
            !saw_storage_load.load(Ordering::SeqCst),
            "rebuilt BOE entry should not reload from storage on the next read"
        );
        assert!(
            saw_fast_path.load(Ordering::SeqCst),
            "rebuilt BOE entry should serve the next read from the fast path"
        );
        assert_eq!(head.size, 4);
        handle.join().unwrap();
    }
}

#[test]
fn production_storage_cluster_constructors_share_bucket_fast_path_cache_across_coordinators() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-prod-shared-cache";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-prod-shared-cache/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert!(matches!(
        reader
            .get_bucket_fast_path(&trusted_bucket_name(bucket))
            .expect("reader should warm shared fast path")
            .policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));

    admin
        .delete_bucket_policy(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap();

    assert!(reader
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn head_object_validates_independent_fast_path_before_stale_policy_allow() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-tighten";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-tighten/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    writer
        .delete_bucket_policy(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("111122223333"),
            None,
        ))
        .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn head_object_validates_independent_fast_path_before_stale_policy_deny() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-loosen";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    put_bucket_policy_test(
        &writer,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-loosen/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn head_object_validates_independent_fast_path_before_stale_abac_tags() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-tags";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let owner_account = "111122223333";
    let owner_requester = test_helpers::requester(owner_account);

    admin
        .create_bucket_for_owner(owner_account, bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    admin
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    admin
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            enabled: true,
        })
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-tags/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "cross-process-tags");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, owner_requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );
    let cached = reader
        .get_bucket_fast_path(&bucket_name)
        .expect("reader should warm shared fast path");
    assert!(matches!(
        cached.policy,
        storage::BucketFastPathPolicy::Loaded(_)
    ));
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    writer
        .put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner(bucket, owner_requester, None),
                account_id: owner_account,
            },
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            request_tags: &[],
        })
        .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn get_object_validates_independent_fast_path_before_stale_ownership_controls() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-ownership";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let owner_canonical_id = CanonicalUserId::from_principal("owner-a");

    create_bucket_for_owner_with_flags(
        &admin,
        "owner-a",
        &owner_canonical_id,
        bucket,
        false,
        false,
        false,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"writer-a"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-ownership/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("writer-a"),
                None,
            ),
            data: b"writer-owned",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"owner-a"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-ownership/*"}]}"#,
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("owner-a"),
        None,
    )
    .unwrap();

    let warm = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(warm.body.read_all().unwrap(), b"writer-owned");
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    writer
        .delete_bucket_ownership_controls(&bucket_request_with_expected_owner(
            bucket,
            test_helpers::requester("owner-a"),
            None,
        ))
        .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("owner-a"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn get_object_validates_independent_fast_path_before_stale_public_access_block() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-pab";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let writer =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let owner_requester = test_helpers::requester("111122223333");

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                owner_requester.clone(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-pab/*"},{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-pab/*"}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();

    reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    put_bucket_public_access_block_test(
        &writer,
        bucket,
        "<PublicAccessBlockConfiguration><BlockPublicAcls>false</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
        owner_requester,
        None,
    )
    .unwrap();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "writer-side cache hints must not touch an independent reader cache"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn head_object_bypasses_fast_path_when_identity_validation_fails() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-identity-load-failure";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let coord = setup_process_isolated_cache_coordinator_with_storage_cluster(storage_cluster);
    let owner_requester = test_helpers::requester("111122223333");

    coord
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-identity-load-failure/*"}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", owner_requester, None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _identity_load_error_guard =
        install_bucket_fast_path_identity_load_error_test_hook(bucket.to_string());
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, 4);
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(
        event_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        None,
        "failed identity validation should remove the cached fast-path entry"
    );
}

#[test]
fn parsed_policy_cache_bypasses_fast_path_when_identity_validation_fails() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-parsed-policy-identity-load-failure";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let coord = setup_process_isolated_cache_coordinator_with_storage_cluster(storage_cluster);
    let owner_requester = test_helpers::requester("111122223333");

    coord
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &coord,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-parsed-policy-identity-load-failure/*"}]}"#,
        owner_requester.clone(),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, "key", owner_requester, None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    let bucket_summary = coord.unchecked_active_bucket_summary(bucket).unwrap();
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );
    assert!(
        coord
            .get_bucket_fast_path(&bucket_name)
            .expect("head_object should populate BOE fast-path policy")
            .bucket_policy_present
    );

    let _identity_load_error_guard =
        install_bucket_fast_path_identity_load_error_test_hook(bucket.to_string());
    let parsed_policy = coord.cached_bucket_policy(&bucket_summary).unwrap();

    assert!(
        parsed_policy.is_some(),
        "loaded bucket policy fallback should still parse after the cached policy proof fails"
    );
    assert_eq!(
        coord.bucket_fast_path_is_fresh_for_test(&bucket_name),
        None,
        "failed parsed-policy identity validation should remove the cached fast-path entry"
    );
}

#[test]
fn head_object_rejects_old_incarnation_fast_path_after_delete_recreate() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-cross-process-recreate";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader =
        setup_process_isolated_cache_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-recreate/*"},{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket-fast-path-cross-process-recreate"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let warm_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(warm_err, ServerError::ObjectNotFound { .. }),
        "unexpected warm error: {warm_err:?}"
    );
    let bucket_name = trusted_bucket_name(bucket);
    let cached_identity = reader
        .get_bucket_fast_path(&bucket_name)
        .expect("BOE read should warm cache")
        .identity();
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();
    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);
    let recreated_owner = CanonicalUserId::from_principal("777788889999");
    storage_cluster
        .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
            name: bucket,
            owner_principal: "777788889999",
            owner_canonical_id: &recreated_owner,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: storage::BucketOwnershipControls {
                object_ownership: storage::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let recreated = storage_cluster
        .load_bucket_fast_path_identity(&bucket_name)
        .unwrap()
        .expect("recreated bucket should have a fast-path identity");
    assert_ne!(
        recreated.bucket_incarnation_generation, cached_identity.bucket_incarnation_generation,
        "delete/recreate must change the bucket incarnation used by cache validation"
    );
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "isolated reader cache should not receive writer-side invalidation"
    );

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });

    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(
        event_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(err, ServerError::AccessDenied));
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        None,
        "old-incarnation cache entry should be removed after request-time validation"
    );
}

#[test]
fn bucket_fast_path_watcher_survives_first_cluster_handle_drop() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-first-handle-drop";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-watch-first-handle-drop/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    drop(admin);
    storage_cluster
        .delete_bucket_subresource_and_load_info(
            &bucket_name,
            storage::BucketSubresourceKind::Policy,
        )
        .unwrap();

    let start = std::time::Instant::now();
    while reader.bucket_fast_path_is_fresh_for_test(&bucket_name) != Some(false) {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher stopped after first cluster handle was dropped"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn bucket_fast_path_watcher_observes_direct_storage_policy_mutation() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-direct-policy";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket-fast-path-watch-direct-policy/*"}]}"#,
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                bucket,
                "key",
                test_helpers::requester("111122223333"),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let bucket_name = trusted_bucket_name(bucket);
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    storage_cluster
        .delete_bucket_subresource_and_load_info(
            &bucket_name,
            storage::BucketSubresourceKind::Policy,
        )
        .unwrap();

    let start = std::time::Instant::now();
    while reader.bucket_fast_path_is_fresh_for_test(&bucket_name) != Some(false) {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher did not observe direct storage policy mutation"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "key",
                None,
                test_helpers::requester("444455556666"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_fast_path_watcher_observes_direct_storage_delete_recreate() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-direct-recreate";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();
    let warm_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(warm_err, ServerError::ObjectNotFound { .. }),
        "unexpected warm error: {warm_err:?}"
    );
    let bucket_name = trusted_bucket_name(bucket);
    let cached_generation = reader
        .get_bucket_fast_path(&bucket_name)
        .expect("BOE read should warm cache")
        .bucket_execution_generation;
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true)
    );

    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();
    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);
    let recreated_owner = CanonicalUserId::from_principal("777788889999");
    storage_cluster
        .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
            name: bucket,
            owner_principal: "777788889999",
            owner_canonical_id: &recreated_owner,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: storage::BucketOwnershipControls {
                object_ownership: storage::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let recreated = storage_cluster.test_head_bucket_raw(&bucket_name).unwrap();
    assert!(
        recreated.bucket_execution_generation > cached_generation,
        "delete/recreate must advance authoritative bucket execution generation"
    );

    let start = std::time::Instant::now();
    while reader.bucket_fast_path_is_fresh_for_test(&bucket_name) == Some(true) {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher did not invalidate after direct storage delete/recreate"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    let err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(matches!(err, ServerError::AccessDenied));
}

#[test]
fn bucket_fast_path_watcher_recovers_after_observing_missing_bucket_before_recreate() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-fast-path-watch-delete-then-recreate";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let reader = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("111122223333", bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &admin,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        test_helpers::requester("111122223333"),
        None,
    )
    .unwrap();

    let warm_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(warm_err, ServerError::ObjectNotFound { .. }),
        "unexpected warm error: {warm_err:?}"
    );
    let bucket_name = trusted_bucket_name(bucket);
    assert!(
        reader.get_bucket_fast_path(&bucket_name).is_some(),
        "BOE read should warm cache"
    );

    storage_cluster.begin_bucket_delete(&bucket_name).unwrap();
    delete_bucket_metadata_or_accept_reclaim_worker_finalize(&storage_cluster, &bucket_name);

    let start = std::time::Instant::now();
    while reader.get_bucket_fast_path(&bucket_name).is_some() {
        assert!(
            start.elapsed() < BUCKET_FAST_PATH_WATCH_TEST_TIMEOUT,
            "bucket fast path watcher did not remove cache entry after direct delete"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }

    let recreated_owner = CanonicalUserId::from_principal("111122223333");
    storage_cluster
        .create_bucket_with_config_and_load_info(&storage::CreateBucketConfig {
            name: bucket,
            owner_principal: "111122223333",
            owner_canonical_id: &recreated_owner,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
            ownership_controls: storage::BucketOwnershipControls {
                object_ownership: storage::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    storage_cluster
        .put_bucket_ownership_controls_and_load_info(
            &bucket_name,
            BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
            },
        )
        .unwrap();

    let reload_err = reader
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                "missing-key",
                None,
                test_helpers::requester("111122223333"),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(
        matches!(reload_err, ServerError::ObjectNotFound { .. }),
        "unexpected reload error: {reload_err:?}"
    );
    assert_eq!(
        reader.bucket_fast_path_is_fresh_for_test(&bucket_name),
        Some(true),
        "recreated bucket should repopulate a fresh BOE fast-path entry"
    );
}

#[test]
fn put_bucket_tags_invalidates_warm_fast_path_tags() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-put-tags-invalidates-fast-path";
    let coord = setup_coordinator_with_pg_count(tmp.path(), METADATA_FANOUT_TEST_PG_COUNT);
    let owner_account = "111122223333";
    let owner_requester = test_helpers::requester(owner_account);

    coord
        .create_bucket_for_owner(owner_account, bucket, false)
        .unwrap();
    put_bucket_ownership_controls_test(
        &coord,
        bucket,
        "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
        owner_requester.clone(),
        None,
    )
    .unwrap();
    coord
        .put_bucket_tags(&PutBucketConfigRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>public</Value></Tag></TagSet></Tagging>",
        })
        .unwrap();
    coord
        .put_bucket_abac(&PutBucketAbacRequest {
            bucket: bucket_request_with_expected_owner(bucket, owner_requester.clone(), None),
            enabled: true,
        })
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&coord, bucket, "put-tags-invalidates");
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, owner_requester.clone(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                owner_requester.clone(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let cached = coord
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .expect("bucket fast path should be populated");
    assert!(matches!(
        cached.tags,
        storage::BucketFastPathTags::Loaded(_)
    ));

    coord
        .put_bucket_tags_for_tag_resource(&PutBucketTagControlRequest {
            control: BucketTagControlRequest {
                bucket: bucket_request_with_expected_owner(bucket, owner_requester, None),
                account_id: owner_account,
            },
            config:
                "<Tagging><TagSet><Tag><Key>security</Key><Value>private</Value></Tag></TagSet></Tagging>",
            request_tags: &[],
        })
        .unwrap();

    assert!(coord
        .get_bucket_fast_path(&trusted_bucket_name(bucket))
        .is_some());

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel::<LockWaitEvent>();
    let event_tx_fast_path = event_tx.clone();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            let _ = event_tx_fast_path.send(LockWaitEvent::UnexpectedStorageLoad);
        })),
    });
    coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_helpers::requester(owner_account),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
}

#[test]
fn delete_object_falls_back_to_storage_load_when_bucket_fast_path_is_acl_free() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-delete-fast-no-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    let deleter = setup_same_process_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    let key = find_key_with_object_pg_ne_bucket_pg(&admin, bucket, "delete-fast");
    test_helpers::put_object(
        &admin,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(bucket, &key, test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    deleter.remove_bucket_fast_path(&trusted_bucket_name(bucket));
    admin
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();

    let _serial = BUCKET_POLICY_LOAD_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let _hook_guard = install_bucket_policy_load_test_hooks(BucketPolicyLoadTestHooks {
        bucket: Some(bucket.to_string()),
        before_storage_load: Some(Arc::new(move || {
            let _ = event_tx.send(LockWaitEvent::Progress);
        })),
        after_policy_fast_path_hit: Some(Arc::new(move || {
            panic!("delete_object should not use ACL-free fast bucket path");
        })),
    });
    let bucket_pg = storage_cluster
        .test_lock_bucket_pg(&trusted_bucket_name(bucket))
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let key_for_delete = key.clone();
    let handle = thread::spawn(move || {
        let res = deleter.delete_object(&delete_object_request(
            bucket,
            &key_for_delete,
            None,
            test_requester(),
            false,
            NO_DELETE,
        ));
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    assert!(
        rx.try_recv().is_err(),
        "delete_object returned before bucket pg released"
    );
    drop(bucket_pg);
    let deleted = rx
        .recv()
        .expect("delete_object should complete after bucket pg released")
        .unwrap();
    assert!(!deleted.delete_marker);
    assert!(matches!(
        admin.get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                bucket,
                &key,
                None,
                test_requester(),
                None
            ),
            cond: NO_READ,
        }),
        Err(ServerError::ObjectNotFound { .. })
    ));
    handle.join().unwrap();
}

#[test]
fn complete_multipart_upload_does_not_wait_for_bucket_lock() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-complete-no-lock";
    let pg_ids: Vec<u32> = (0..4).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    let completer = setup_same_process_coordinator_with_storage_cluster_without_background_sweepers(
        Arc::clone(&storage_cluster),
    );
    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();

    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, "key", &[(1, b"part")]);

    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let _write_handle_serial = BUCKET_WRITE_HANDLE_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let guard = storage_cluster.test_lock_bucket(&trusted_bucket_name(bucket));
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_lock = event_tx.clone();
    let _storage_hook_guard = install_bucket_scoped_test_hooks(BucketScopedTestHooks {
        target: Some(trusted_bucket_name(bucket)),
        before_bucket_lock_acquire: Some(Arc::new(move || {
            let _ = event_tx_lock.send(LockWaitEvent::UnexpectedBucketLock);
        })),
        ..BucketScopedTestHooks::default()
    });
    let _write_handle_hook_guard =
        install_bucket_write_handle_test_hooks(BucketWriteHandleTestHooks {
            bucket: Some(bucket.to_string()),
            after_loaded: Some(Arc::new(move || {
                let _ = event_tx.send(LockWaitEvent::Progress);
            })),
            ..BucketWriteHandleTestHooks::default()
        });
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                "key",
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        });
        tx.send(res).unwrap();
    });

    assert_eq!(event_rx.recv().unwrap(), LockWaitEvent::Progress);
    drop(guard);
    let res = rx.recv().unwrap();
    assert!(
        res.is_ok(),
        "complete_multipart_upload should succeed without waiting on bucket lock: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn complete_multipart_upload_does_not_deadlock_when_bucket_policy_shares_pg() {
    let tmp = test_util::tempdir();
    let bucket = "bucket-complete-same-pg";
    let pg_ids: Vec<u32> = (0..METADATA_FANOUT_TEST_PG_COUNT).collect();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &pg_ids);
    let admin = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );
    let completer = setup_same_process_coordinator_with_storage_cluster_without_lifecycle_sweeper(
        Arc::clone(&storage_cluster),
    );

    admin
        .create_bucket_for_owner("default-owner", bucket, false)
        .unwrap();
    put_bucket_policy_test(
        &admin,
        bucket,
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"default-owner"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket-complete-same-pg/*"}]}"#,
        test_requester(),
        None,
    )
    .unwrap();

    let key = find_key_with_object_pg_eq_bucket_pg(&admin, bucket, "same-pg");

    let (upload_id, parts) = create_upload_with_parts(&admin, bucket, &key, &[(1, b"part")]);

    let _serial = RECLAMATION_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let (event_tx, event_rx) = mpsc::channel();
    let event_tx_hook = event_tx.clone();
    let _hook_guard = install_reclamation_test_hooks(ReclamationTestHooks {
        target: Some((bucket.to_string(), key.clone())),
        probe_multipart_complete_auth_lookup: true,
        after_multipart_complete_pre_commit: Some(Arc::new(move || {
            let _ = event_tx_hook.send(LockWaitEvent::Progress);
        })),
        ..ReclamationTestHooks::default()
    });
    let (tx, rx) = mpsc::channel();
    let event_tx_complete = event_tx.clone();
    let key_for_complete = key.clone();
    let handle = thread::spawn(move || {
        let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                bucket,
                &key_for_complete,
                &upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,
            expected_object_size: None,
            cond: &WriteCondition::default(),
            sse_customer: None,
        });
        let _ = event_tx_complete.send(LockWaitEvent::CompletedEarly);
        tx.send(res).unwrap();
    });

    let event = event_rx.recv().unwrap();
    let res = rx
        .recv()
        .expect("complete_multipart_upload should not deadlock on bucket policy lookup");
    assert_eq!(
        event,
        LockWaitEvent::Progress,
        "complete_multipart_upload returned before the expected progress point: {res:?}"
    );
    assert!(
        res.is_ok(),
        "complete_multipart_upload should succeed when bucket policy shares the metadata PG: {res:?}"
    );
    handle.join().unwrap();
}

#[test]
fn delete_bucket_rejects_active_stream_put_session() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
    let err = delete_bucket_test(&coord, "bucket").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotEmpty));

    coord
        .abort_stream_put("bucket", "key", &session_id)
        .unwrap();
    delete_bucket_test(&coord, "bucket").unwrap();
    wait_until_bucket_gone(&coord, "bucket");
}

#[test]
fn put_get_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [("Content-Type", "text/plain")];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "hello.txt",
                test_requester(),
                None,
            ),
            data: b"Hello, world!",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert!(!result.etag.is_empty());

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "hello.txt",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"Hello, world!");
    assert_eq!(obj.size, 13);
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("text/plain")
    );
}

#[test]
fn put_get_with_metadata() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let headers = [
        ("Content-Type", "application/json"),
        ("X-Amz-Meta-Author", "alice"),
        ("X-Amz-Meta-Version", "42"),
    ];
    let metadata = MetadataBlob::from_headers(&headers).unwrap();
    let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
            data: b"{}",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"{}");
    assert_eq!(
        obj.system_metadata.content_type().map(|v| v.as_str()),
        Some("application/json")
    );
    assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
    assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("42"));
}

#[test]
fn head_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let metadata = MetadataBlob::from_headers(&[("Content-Type", "text/plain")]).unwrap();
    let system_metadata = SystemMetadata::from_headers(&[("Content-Type", "text/plain")]).unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(head.size, 4);
    assert_eq!(
        head.system_metadata.content_type().map(|v| v.as_str()),
        Some("text/plain")
    );
}

#[test]
fn overwrite_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"v2");
}

#[test]
fn empty_object() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "empty", test_requester(), None),
            data: b"",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let segments = coord
        .storage_node()
        .test_get_object_segments(
            &trusted_bucket_name("bucket"),
            &trusted_object_key("empty"),
            put.version_id,
        )
        .unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].size, 0);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "empty",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"");
    assert_eq!(obj.size, 0);
}

#[test]
fn delete_object_then_get_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn delete_object_eventually_reclaims_simple_shards() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"simple-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let (generation_id, ec, data_pg_id, okh, segment_vid) = {
        match coord
            .storage_node()
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => {
                let segments = coord
                    .storage_node()
                    .test_get_object_segments(
                        &trusted_bucket_name("bucket"),
                        &trusted_object_key("key"),
                        record.version_id,
                    )
                    .unwrap();
                let segment = segments
                    .first()
                    .expect("direct put should store one segment");
                (
                    record.generation_id,
                    record.ec,
                    segment.data_pg_id,
                    segment.segment_okh,
                    segment.segment_vid,
                )
            }
            other @ StoredObject::DeleteMarker(_) => {
                panic!("expected live object, got {other:?}")
            }
        }
    };

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    reclaim_object_payload(&coord, "bucket", "key", generation_id);
    assert_shard_set_deleted(&coord, data_pg_id, &okh, segment_vid, ec);
}

#[test]
fn shard_scavenger_audit_records_file_without_row_observations() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count_without_background_sweepers(tmp.path(), 1);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let reservation_id = storage::SessionId::try_from("77".repeat(16)).unwrap();
    let generation_id = coord
        .storage_node()
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let written = coord
        .storage_node()
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &[0xe7; 16],
            b"background shard scavenger audit candidate",
        )
        .unwrap();

    coord
        .storage_node()
        .audit_shard_storage_for_scavenger()
        .unwrap();
    let observations = coord
        .storage_node()
        .test_list_shard_scavenger_observations(written.data_pg_id)
        .unwrap();
    assert!(
        written.written_shards.iter().all(|shard| {
            observations.iter().any(|observation| {
                observation.reason == ShardScavengerObservationReason::FileWithoutShardRow
                    && observation.resolved_at.is_none()
                    && observation.key.data_pg_id == written.data_pg_id
                    && observation.key.shard_key == shard.key
            })
        }),
        "shard scavenger audit did not record file-without-row observations; observations={observations:?}"
    );
}

#[test]
fn read_discovered_corrupt_shard_queues_background_repair_without_inline_rewrite() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_with_pg_count_without_background_sweepers(tmp.path(), 1);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"foreground read should not rewrite corrupt shard inline";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_shard_index = 0;
    let corrupt_path = shard_file_path(&coord, "bucket", "key", corrupt_shard_index);
    let original_bytes = std::fs::read(&corrupt_path).unwrap();
    corrupt_shard_on_disk(&coord, "bucket", "key", corrupt_shard_index);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();
    assert_ne!(corrupt_bytes, original_bytes);

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);

    assert_eq!(
        std::fs::read(&corrupt_path).unwrap(),
        corrupt_bytes,
        "foreground read recovery must not rewrite the damaged shard inline"
    );
    let repairs = coord
        .storage_node()
        .list_placed_segment_shard_repairs(segment.data_pg_id)
        .unwrap();
    assert_eq!(repairs.len(), 1);
    let repair = &repairs[0].work_item;
    assert_eq!(repair.request.data_pg_id, segment.data_pg_id);
    assert_eq!(repair.request.segment_okh, segment.segment_okh);
    assert_eq!(repair.request.segment_vid, segment.segment_vid);
    assert_eq!(repair.request.segment_crc64, segment.segment_crc64);
    assert_eq!(repair.shard_index.get(), corrupt_shard_index);
    assert_eq!(
        coord
            .storage_node()
            .try_take_placed_segment_shard_repair_work(),
        Some(*repair),
        "successful read recovery should leave a background repair wake hint"
    );
}

#[test]
fn shard_repair_worker_retries_after_transient_shard_read_error() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        Arc::clone(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_cluster| Ok(ShardRepairSweeper::disabled(Arc::clone(storage_cluster))),
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap();

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"background shard repair retries transient read failure";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_shard_index = 0;
    let corrupt_path = shard_file_path(&coord, "bucket", "key", corrupt_shard_index);
    corrupt_shard_on_disk(&coord, "bucket", "key", corrupt_shard_index);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);

    let fail_once = Arc::new(AtomicBool::new(true));
    let failure_injected = Arc::new(AtomicBool::new(false));
    let hook_fail_once = Arc::clone(&fail_once);
    let hook_failure_injected = Arc::clone(&failure_injected);
    let _read_hook_guard = storage_cluster.test_install_before_placed_payload_shard_read_hook(
        Arc::new(move |location, _shard_key| {
            if hook_fail_once.swap(false, Ordering::SeqCst) {
                hook_failure_injected.store(true, Ordering::SeqCst);
                return Err(storage::StoreError::StorageRpcResourceExhausted {
                    node_id: location.node_id().as_u32(),
                    operation: "repair read payload shard",
                    message: "test injected transient shard repair read failure".to_string(),
                });
            }
            Ok(())
        }),
    );
    let _worker = setup_coordinator_with_only_shard_repair_worker(Arc::clone(&storage_cluster));

    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs.iter().any(|repair| {
            repair.last_error.as_deref().is_some_and(|error| {
                error.contains("test injected transient shard repair read failure")
            })
        }) {
            break;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard repair worker did not record transient failure; repairs={repairs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(failure_injected.load(Ordering::SeqCst));

    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs.is_empty() {
            let repaired_bytes = std::fs::read(&corrupt_path).unwrap();
            assert_ne!(
                repaired_bytes, corrupt_bytes,
                "retry should rewrite the corrupt shard after transient failure"
            );
            return;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT + Duration::from_secs(2),
            "shard repair worker did not retry and drain row; repairs={repairs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn shard_repair_worker_records_unrecoverable_repair_without_partial_write() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let coord = Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        Arc::clone(&storage_cluster),
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            |storage_cluster| Ok(ShardRepairSweeper::disabled(Arc::clone(storage_cluster))),
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap();

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"background shard repair fails closed when too many shards are unavailable";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_path = shard_file_path(&coord, "bucket", "key", 0);
    corrupt_shard_on_disk(&coord, "bucket", "key", 0);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);

    let missing_paths = [
        shard_file_path(&coord, "bucket", "key", 1),
        shard_file_path(&coord, "bucket", "key", 2),
    ];
    for path in &missing_paths {
        std::fs::remove_file(path).unwrap();
    }

    let _worker = setup_coordinator_with_only_shard_repair_worker(Arc::clone(&storage_cluster));
    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs
            .iter()
            .any(|repair| repair.last_error.as_deref().is_some())
        {
            assert_eq!(repairs.len(), 1);
            assert_eq!(repairs[0].work_item.shard_index.get(), 0);
            assert_eq!(std::fs::read(&corrupt_path).unwrap(), corrupt_bytes);
            for path in &missing_paths {
                assert!(
                    !path.exists(),
                    "unrecoverable repair should not recreate any shard from an insufficient EC set"
                );
            }
            return;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard repair worker did not record unrecoverable repair failure; repairs={repairs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn shard_repair_worker_repairs_read_discovered_corrupt_shard() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let _hook_serial = SHARD_REPAIR_WORKER_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster(tmp.path(), &[0]);
    let target_registry_key = storage_cluster.process_local_registry_key();
    let (idle_tx, idle_rx) = mpsc::channel();
    let _hook_guard = install_shard_repair_worker_test_hooks(ShardRepairWorkerTestHooks {
        target_registry_key: Some(target_registry_key),
        after_idle_timeout: Some(Arc::new(move || {
            let _ = idle_tx.send(());
        })),
    });
    let coord = Coordinator::new_with_background_sweeper_factories_for_storage_cluster(
        storage_cluster,
        "us-east-1".to_string(),
        None,
        Some(test_sse_s3_provider()),
        (
            false,
            |_, _| Ok(LifecycleSweeper::disabled()),
            |_| Ok(ShardScavengerSweeper::disabled()),
            ShardRepairSweeper::acquire_shared,
            |_| Ok(ShardBackfillSweeper::disabled()),
            |_| Ok(StreamSessionSweeper::disabled()),
        ),
    )
    .unwrap();
    idle_rx
        .recv_timeout(TEST_EVENT_TIMEOUT)
        .expect("shard repair worker did not reach the idle timeout path");

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"background shard repair worker payload";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, VersionId::Null)
        .unwrap()
        .pop()
        .expect("put object should create one segment");
    let corrupt_shard_index = 0;
    let corrupt_path = shard_file_path(&coord, "bucket", "key", corrupt_shard_index);
    corrupt_shard_on_disk(&coord, "bucket", "key", corrupt_shard_index);
    let corrupt_bytes = std::fs::read(&corrupt_path).unwrap();

    let result = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), data);

    let start = std::time::Instant::now();
    loop {
        let repairs = coord
            .storage_node()
            .list_placed_segment_shard_repairs(segment.data_pg_id)
            .unwrap();
        if repairs.is_empty() {
            let repaired = coord
                .get_object(&GetObjectRequest {
                    sse_customer: None,
                    object: object_version_request_with_expected_owner(
                        "bucket",
                        "key",
                        None,
                        test_requester(),
                        None,
                    ),
                    cond: NO_READ,
                })
                .unwrap();
            assert_eq!(repaired.body.read_all().unwrap(), data);
            let repaired_bytes = std::fs::read(&corrupt_path).unwrap();
            assert_ne!(
                repaired_bytes, corrupt_bytes,
                "repair should rewrite the corrupt shard file"
            );
            return;
        }
        assert!(
            start.elapsed() < TEST_EVENT_TIMEOUT,
            "shard repair worker did not repair and drain row; repairs={repairs:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn reclaim_object_payload_delete_failure_keeps_retryable_reclaim_record() {
    let _storage_serial = STORAGE_TEST_HOOK_SERIAL
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap();
    let tmp = test_util::tempdir();
    let coord = setup_coordinator_without_reclaim_sweeper(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"simple-data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,
            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let (generation_id, ec, data_pg_id, okh, segment_vid) = {
        match coord
            .storage_node()
            .test_get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::Live(record) => {
                let segments = coord
                    .storage_node()
                    .test_get_object_segments(
                        &trusted_bucket_name("bucket"),
                        &trusted_object_key("key"),
                        record.version_id,
                    )
                    .unwrap();
                let segment = segments
                    .first()
                    .expect("direct put should store one segment");
                (
                    record.generation_id,
                    record.ec,
                    segment.data_pg_id,
                    segment.segment_okh,
                    segment.segment_vid,
                )
            }
            other @ StoredObject::DeleteMarker(_) => {
                panic!("expected live object, got {other:?}")
            }
        }
    };

    let failing_key = ShardKey::new(&okh, segment_vid.get(), 0);
    let placed_cleanup_guard = coord
        .storage_node()
        .test_install_before_placed_payload_shard_delete_hook(Arc::new(move |shard_key| {
            if shard_key == &failing_key {
                return Err(storage::StoreError::Io {
                    context: "injected reclaim placed delete failure",
                    source: std::io::Error::other("injected reclaim placed delete failure"),
                });
            }
            Ok(())
        }));

    coord
        .delete_object(&delete_object_request(
            "bucket",
            "key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();

    let err = coord
        .read_runtime()
        .try_reclaim_object_payload("bucket", "key", generation_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            ServerError::Store(storage::StoreError::Io {
                context: "injected reclaim placed delete failure",
                ..
            })
        ),
        "expected injected reclaim delete failure, got {err:?}"
    );
    for shard_index in 0..ec.k + ec.m {
        let shard_key = ShardKey::new(&okh, segment_vid.get(), shard_index);
        assert!(
            coord
                .storage_node()
                .test_shard_exists(data_pg_id, &shard_key)
                .unwrap(),
            "failed reclaim should keep ack metadata {shard_index} retryable"
        );
        assert!(
            coord
                .storage_node()
                .test_payload_shard_file_exists(data_pg_id, ec, &okh, segment_vid, shard_index)
                .unwrap(),
            "failed reclaim should keep placed shard {shard_index} retryable"
        );
    }

    drop(placed_cleanup_guard);
    reclaim_object_payload(&coord, "bucket", "key", generation_id);
    assert_shard_set_deleted(&coord, data_pg_id, &okh, segment_vid, ec);
}

#[test]
fn delete_nonexistent_object_is_ok() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    // Should not error
    coord
        .delete_object(&delete_object_request(
            "bucket",
            "no-such-key",
            None,
            test_requester(),
            false,
            NO_DELETE,
        ))
        .unwrap();
}

#[test]
fn list_objects() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
            data: b"1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "a/2", test_requester(), None),
            data: b"2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "b/1", test_requester(), None),
            data: b"3",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 3);
    // Should be sorted
    assert_eq!(result.objects[0].key, "a/1");
    assert_eq!(result.objects[1].key, "a/2");
    assert_eq!(result.objects[2].key, "b/1");
}

#[test]
fn list_objects_with_prefix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/cat.jpg",
                test_requester(),
                None,
            ),
            data: b"cat",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/dog.jpg",
                test_requester(),
                None,
            ),
            data: b"dog",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "docs/readme.md",
                test_requester(),
                None,
            ),
            data: b"md",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some("photos/"),
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 2);
}

#[test]
fn list_objects_with_delimiter() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/cat.jpg",
                test_requester(),
                None,
            ),
            data: b"cat",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/dog.jpg",
                test_requester(),
                None,
            ),
            data: b"dog",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "docs/readme.md",
                test_requester(),
                None,
            ),
            data: b"md",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "root.txt",
                test_requester(),
                None,
            ),
            data: b"root",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].key, "root.txt");
    assert!(result.common_prefixes.contains(&"photos/".to_string()));
    assert!(result.common_prefixes.contains(&"docs/".to_string()));
}

#[test]
fn put_get_object_trailing_slash_key() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "folder/", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "folder/",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"data");
    assert_eq!(obj.size, 4);
}

// ── Disk manipulation helpers for EC tests ────────────────────────

/// Compute shard file path on disk for a given object and shard index.
fn shard_file_path(coord: &Coordinator, bucket: &str, key: &str, shard_index: u8) -> PathBuf {
    let (data_pg_id, okh, generation_id, ec) = {
        let bucket_name = trusted_bucket_name(bucket);
        let object_key = trusted_object_key(key);
        let record = coord
            .storage_node()
            .test_get_object_meta(&bucket_name, &object_key)
            .unwrap();
        let segments = coord
            .storage_node()
            .test_get_object_segments(&bucket_name, &object_key, record.version_id())
            .unwrap();
        if let Some(segment) = segments.first() {
            (
                segment.data_pg_id,
                segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            )
        } else {
            let live = record.as_live().expect("expected live object");
            (
                object_data_pg_id(
                    coord,
                    bucket_name.as_str(),
                    object_key.as_str(),
                    live.generation_id,
                ),
                object_key_hash(bucket_name.as_str(), object_key.as_str()),
                live.generation_id,
                live.ec,
            )
        }
    };
    coord
        .storage_node()
        .test_payload_shard_file_path(data_pg_id, ec, &okh, generation_id, shard_index)
        .unwrap()
}

/// Delete a specific shard file from disk.
fn delete_shard_on_disk(coord: &Coordinator, bucket: &str, key: &str, shard_index: u8) {
    let path = shard_file_path(coord, bucket, key, shard_index);
    std::fs::remove_file(&path).unwrap_or_else(|e| {
        panic!(
            "failed to delete shard {shard_index} at {}: {e}",
            path.display()
        )
    });
}

/// Corrupt a specific shard file on disk (flip first byte).
/// PgStore's read_shard will detect CRC mismatch.
fn corrupt_shard_on_disk(coord: &Coordinator, bucket: &str, key: &str, shard_index: u8) {
    let path = shard_file_path(coord, bucket, key, shard_index);
    let mut data = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "failed to read shard {shard_index} at {}: {e}",
            path.display()
        )
    });
    assert!(!data.is_empty(), "shard file is empty");
    data[0] ^= 0xFF;
    std::fs::write(&path, &data).unwrap();
}

// ── EC fault injection tests ────────────────────────────────────

#[test]
fn ec_reconstruction_after_shard_loss() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"This data should survive shard loss!";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "resilient",
                test_requester(),
                None,
            ),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete one data shard using the helper
    delete_shard_on_disk(&coord, "bucket", "resilient", 0);

    // Get should still succeed via EC reconstruction
    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "resilient",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_drop_one_data_shard_get() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC single shard loss test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj1", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    delete_shard_on_disk(&coord, "bucket", "obj1", 0);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj1",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_degraded_read_reuses_reconstruction_scratch() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = vec![5u8; INTERNAL_SEGMENT_SIZE];
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "obj-reconstruct",
                test_requester(),
                None,
            ),
            data: &data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    delete_shard_on_disk(&coord, "bucket", "obj-reconstruct", 0);

    assert_eq!(coord.payload_buffer_pool.allocation_count(), 0);
    let ec = coord.storage_node().default_payload_ec_shape();
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);

    let first = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj-reconstruct",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(first.body.read_all().unwrap(), data);
    assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);

    let second = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj-reconstruct",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(second.body.read_all().unwrap(), data);
    assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);
    assert_eq!(coord.storage_node().test_ec_scratch_allocation_count(ec), 1);
}

#[test]
fn ec_drop_m_shards_at_limit() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC m-shard loss limit test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj2", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete 2 data shards (indices 0 and 1)
    delete_shard_on_disk(&coord, "bucket", "obj2", 0);
    delete_shard_on_disk(&coord, "bucket", "obj2", 1);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj2",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_drop_m_plus_one_shards_fails() {
    // Config: k=4, m=2. Dropping m+1=3 shards should fail.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC m+1 shard loss test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj3", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete 3 shards (indices 0, 1, 2)
    delete_shard_on_disk(&coord, "bucket", "obj3", 0);
    delete_shard_on_disk(&coord, "bucket", "obj3", 1);
    delete_shard_on_disk(&coord, "bucket", "obj3", 2);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj3",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    let err = obj.body.read_all().unwrap_err();
    assert!(
        matches!(err, ServerError::Store(storage::StoreError::NotFound)),
        "unrecoverable payload loss must not be reported as NoSuchKey: {err:?}"
    );
    assert_eq!(err.http_status(), 500);
    assert_eq!(err.s3_error_code(), "InternalError");
}

#[test]
fn ec_corrupt_one_data_shard_recovery() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC corruption recovery test data";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj4", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    corrupt_shard_on_disk(&coord, "bucket", "obj4", 0);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj4",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_range_get_with_missing_shard() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"Hello, World! Range test with EC recovery";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj5", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("obj5");
    let generation_id = coord
        .storage_node()
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .into_live()
        .expect("put object should create a live object")
        .generation_id;
    let segment = coord
        .storage_node()
        .test_get_object_segments(&bucket, &key, put.version_id)
        .unwrap()
        .pop()
        .expect("put object should create one object segment");
    let expected_selected_nodes = coord
        .storage_node()
        .segment_payload_shard_locations(
            segment.data_pg_id,
            storage::EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        )
        .unwrap()
        .into_iter()
        .map(|location| location.node_id())
        .collect::<BTreeSet<_>>()
        .len();

    // Delete shard 0 (covers the beginning of the data)
    delete_shard_on_disk(&coord, "bucket", "obj5", 0);

    // Range get should still succeed via EC reconstruction
    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj5",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 4 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        coord
            .storage_node()
            .object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        expected_selected_nodes,
        "degraded EC range read should hold handles for the selected recovery shard-owner set"
    );
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
    assert_eq!(
        coord
            .storage_node()
            .object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        0,
        "degraded EC range read should release shard-owner handles after body consumption"
    );
}

#[test]
fn ec_drop_parity_shard_data_still_works() {
    if !backend_supports_parity_recovery() {
        return;
    }
    // Delete parity shard (index k=4). Only data shards needed for normal read.
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC parity shard drop test";
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj6", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // Delete first parity shard (index 4, since k=4)
    delete_shard_on_disk(&coord, "bucket", "obj6", 4);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj6",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);
}

#[test]
fn ec_healthy_read_skips_corrupt_parity_shards() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC healthy read should skip parity shards";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj7", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let segment = {
        let segments = coord
            .storage_node()
            .test_get_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("obj7"),
                put.version_id,
            )
            .unwrap();
        assert_eq!(segments.len(), 1);
        segments[0].clone()
    };

    corrupt_shard_on_disk(&coord, "bucket", "obj7", 4);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj7",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);

    let parity_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), 4);
    assert!(
        coord
            .storage_node()
            .test_shard_exists(segment.data_pg_id, &parity_key)
            .unwrap(),
        "healthy-path read should not touch parity shard 4"
    );
}

#[test]
fn ec_reconstruction_stops_after_first_needed_parity_shard() {
    if !backend_supports_parity_recovery() {
        return;
    }
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let data = b"EC reconstruction should stop after first needed parity";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "obj8", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let segment = {
        let segments = coord
            .storage_node()
            .test_get_object_segments(
                &trusted_bucket_name("bucket"),
                &trusted_object_key("obj8"),
                put.version_id,
            )
            .unwrap();
        assert_eq!(segments.len(), 1);
        segments[0].clone()
    };

    delete_shard_on_disk(&coord, "bucket", "obj8", 0);
    corrupt_shard_on_disk(&coord, "bucket", "obj8", 5);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "obj8",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), data);

    let parity_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), 5);
    assert!(
        coord
            .storage_node()
            .test_shard_exists(segment.data_pg_id, &parity_key)
            .unwrap(),
        "reconstruction should stop once enough shards are present"
    );
}

#[test]
fn put_to_nonexistent_bucket_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "no-such-bucket",
                "key",
                test_requester(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn get_nonexistent_object_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let err = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "no-such-key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::ObjectNotFound { .. }));
}

#[test]
fn etag_consistency() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.etag, obj.etag);

    let head = coord
        .head_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.etag, head.etag);
}

#[test]
fn list_objects_delimiter_with_continuation() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
            data: b"1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "a/2", test_requester(), None),
            data: b"2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "b/1", test_requester(), None),
            data: b"3",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "c/1", test_requester(), None),
            data: b"4",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "root.txt",
                test_requester(),
                None,
            ),
            data: b"5",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // First page: max_keys=2 with delimiter
    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(
        result.objects.len() + result.common_prefixes.len(),
        2,
        "should return exactly 2 entries (objects + prefixes)"
    );
    assert!(result.is_truncated);
    assert!(result.next_continuation_token.is_some());

    // Second page using continuation token
    let token = result.next_continuation_token.unwrap();
    let result2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: Some(&token),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert!(
        !result2.objects.is_empty() || !result2.common_prefixes.is_empty(),
        "continuation page should have entries"
    );
}

#[test]
fn list_objects_delimiter_continuation_skips_large_common_prefix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    for i in 0..1500 {
        let key = format!("dir/file-{i:04}.txt");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "z.txt", test_requester(), None),
            data: b"z",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page1.objects.is_empty());
    assert_eq!(page1.common_prefixes, vec!["dir/".to_string()]);
    assert!(page1.is_truncated);

    let token = page1
        .next_continuation_token
        .as_deref()
        .expect("first page should return a continuation token")
        .to_string();
    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert_eq!(page2.common_prefixes, Vec::<String>::new());
    assert_eq!(page2.objects.len(), 1);
    assert_eq!(page2.objects[0].key, "z.txt");
    assert!(!page2.is_truncated);
}

#[test]
fn list_objects_delimiter_with_no_upper_bound_common_prefix_is_final_page() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let delimiter = "\u{10ffff}";

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "a", test_requester(), None),
            data: b"a",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                &format!("{delimiter}child"),
                test_requester(),
                None,
            ),
            data: b"b",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert_eq!(page1.objects.len(), 1);
    assert_eq!(page1.objects[0].key, "a");
    assert!(page1.common_prefixes.is_empty());
    assert!(page1.is_truncated);

    let token = page1
        .next_continuation_token
        .as_deref()
        .expect("first page should return a continuation token")
        .to_string();
    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page2.objects.is_empty());
    assert_eq!(page2.common_prefixes, vec![delimiter.to_string()]);
    assert!(!page2.is_truncated);
    assert!(page2.next_continuation_token.is_none());
}

#[test]
fn list_objects_delimiter_continuation_with_boundary_token_does_not_panic() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let delimiter = "\x7f";
    let token = format!("{}{}", "a".repeat(1023), delimiter);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "z", test_requester(), None),
            data: b"z",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert_eq!(page2.common_prefixes, Vec::<String>::new());
    assert_eq!(page2.objects.len(), 1);
    assert_eq!(page2.objects[0].key, "z");
    assert!(!page2.is_truncated);
    assert!(page2.next_continuation_token.is_none());
}

#[test]
fn list_objects_delimiter_common_prefix_boundary_falls_back_without_error() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    let delimiter = "\x7f";
    let common_prefix = format!("{}{}", "a".repeat(1023), delimiter);

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                &common_prefix,
                test_requester(),
                None,
            ),
            data: b"prefix",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "z", test_requester(), None),
            data: b"z",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: None,
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page1.objects.is_empty());
    assert_eq!(page1.common_prefixes, vec![common_prefix.clone()]);
    assert!(page1.is_truncated);

    let token = page1
        .next_continuation_token
        .as_deref()
        .expect("first page should return a continuation token")
        .to_string();
    assert_eq!(token, common_prefix);

    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some(delimiter),
            continuation_token: Some(&token),
            max_keys: 1,
            requested_max_keys: Some(1),
        })
        .unwrap();
    assert!(page2.common_prefixes.is_empty());
    assert_eq!(page2.objects.len(), 1);
    assert_eq!(page2.objects[0].key, "z");
    assert!(!page2.is_truncated);
    assert!(page2.next_continuation_token.is_none());
}

#[test]
fn list_objects_max_keys_counts_prefixes() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    // Create many prefixed objects to ensure common_prefixes count toward max_keys
    for i in 0..10 {
        let key = format!("dir{i}/file.txt");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 3,
            requested_max_keys: Some(3),
        })
        .unwrap();
    // With delimiter "/", all entries become common prefixes
    assert_eq!(result.common_prefixes.len(), 3);
    assert!(result.is_truncated);
}

#[test]
fn put_object_to_nonexistent_bucket_no_orphaned_shards() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    // Don't create bucket — put should fail at bucket check before writing shards
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("no-bucket", "key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn delete_nonexistent_bucket_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = delete_bucket_test(&coord, "no-such-bucket").unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn list_objects_no_delimiter_truncated() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    for i in 0..5 {
        let key = format!("key-{i:02}");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    // Request fewer than available
    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 3,
            requested_max_keys: Some(3),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 3);
    assert!(result.is_truncated);
    assert!(result.next_continuation_token.is_some());
}

#[test]
fn list_objects_no_delimiter_with_continuation() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    for i in 0..5 {
        let key = format!("key-{i:02}");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    // First page
    let page1 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(page1.objects.len(), 2);
    assert!(page1.is_truncated);
    let token = page1.next_continuation_token.as_ref().unwrap();

    // Second page using continuation token
    let page2 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: Some(token),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(page2.objects.len(), 2);
    assert!(page2.is_truncated);
    let token2 = page2.next_continuation_token.as_ref().unwrap();

    // Third page — should get remainder
    let page3 = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: Some(token2),
            max_keys: 2,
            requested_max_keys: Some(2),
        })
        .unwrap();
    assert_eq!(page3.objects.len(), 1);
    assert!(!page3.is_truncated);
    assert!(page3.next_continuation_token.is_none());
}

#[test]
fn list_objects_prefix_with_delimiter() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/2024/jan.jpg",
                test_requester(),
                None,
            ),
            data: b"j",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/2024/feb.jpg",
                test_requester(),
                None,
            ),
            data: b"f",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/2025/mar.jpg",
                test_requester(),
                None,
            ),
            data: b"m",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "photos/top.jpg",
                test_requester(),
                None,
            ),
            data: b"t",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // List with prefix "photos/" and delimiter "/"
    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: Some("photos/"),
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    // top.jpg is a direct child, 2024/ and 2025/ are common prefixes
    assert_eq!(result.objects.len(), 1);
    assert_eq!(result.objects[0].key, "photos/top.jpg");
    assert_eq!(result.common_prefixes.len(), 2);
    assert!(result.common_prefixes.contains(&"photos/2024/".to_string()));
    assert!(result.common_prefixes.contains(&"photos/2025/".to_string()));
    assert!(!result.is_truncated);
}

#[test]
fn list_objects_not_truncated_no_token() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner(
                "bucket",
                "only-one",
                test_requester(),
                None,
            ),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap();
    assert_eq!(result.objects.len(), 1);
    assert!(!result.is_truncated);
    assert!(result.next_continuation_token.is_none());
}

#[test]
fn list_objects_max_keys_zero() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key1", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 0,
            requested_max_keys: Some(0),
        })
        .unwrap();
    assert!(result.objects.is_empty());
    assert!(result.common_prefixes.is_empty());
    assert!(!result.is_truncated);
    assert!(result.next_continuation_token.is_none());
}

#[test]
fn list_objects_max_keys_zero_with_delimiter() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let result = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            prefix: None,
            delimiter: Some("/"),
            continuation_token: None,
            max_keys: 0,
            requested_max_keys: Some(0),
        })
        .unwrap();
    assert!(result.objects.is_empty());
    assert!(result.common_prefixes.is_empty());
    assert!(!result.is_truncated);
}

#[test]
fn list_objects_nonexistent_bucket_fails() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner("no-bucket", test_requester(), None),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn list_objects_nonexistent_bucket_for_non_owner_still_returns_bucket_not_found() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let err = coord
        .list_objects_v2(&ListObjectsV2Request {
            bucket: bucket_request_with_expected_owner(
                "no-bucket",
                test_helpers::requester("other-user"),
                None,
            ),
            prefix: None,
            delimiter: None,
            continuation_token: None,
            max_keys: 1000,
            requested_max_keys: Some(1000),
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn delete_objects_batch() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key1", test_requester(), None),
            data: b"data1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key2", test_requester(), None),
            data: b"data2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let entries = vec![
        DeleteEntry {
            key: trusted_object_key("key1"),
            version_id: None,
            cond: DeleteCondition::None,
        },
        DeleteEntry {
            key: trusted_object_key("key2"),
            version_id: None,
            cond: DeleteCondition::None,
        },
        // key3 doesn't exist — should still succeed (idempotent)
        DeleteEntry {
            key: trusted_object_key("key3"),
            version_id: None,
            cond: DeleteCondition::None,
        },
    ];

    let result = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap();
    assert_eq!(result.deleted.len(), 3);
    assert!(result.errors.is_empty());

    // Verify objects are actually gone
    assert!(coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key1",
                None,
                test_requester(),
                None
            ),
            cond: NO_READ,
        })
        .is_err());
    assert!(coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key2",
                None,
                test_requester(),
                None
            ),
            cond: NO_READ,
        })
        .is_err());
}

#[test]
fn delete_objects_nonexistent_bucket() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    let entries = vec![DeleteEntry {
        key: trusted_object_key("key1"),
        version_id: None,
        cond: DeleteCondition::None,
    }];

    let err = coord
        .delete_objects(&DeleteObjectsRequest {
            bucket: bucket_request_with_expected_owner("no-bucket", test_requester(), None),
            entries: &entries,
            bypass_governance: false,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::BucketNotFound { .. }));
}

#[test]
fn max_object_size_constant() {
    // Verify the constant matches AWS S3 single PUT limit (5 GiB).
    assert_eq!(MAX_OBJECT_SIZE, 5 * 1024 * 1024 * 1024);
}

#[test]
fn max_parts_constant() {
    assert_eq!(MAX_PARTS, 10_000);
}

#[test]
fn complete_multipart_too_many_parts() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let create = coord
        .create_multipart_upload(&CreateMultipartUploadRequest {
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            checksum: None,

            acl: NO_PUT_OBJECT_ACL.into(),
            encryption: WriteEncryptionRequest::none(),
            object_lock: ObjectLockState::default(),
            policy_context: PutObjectPolicyContext::default(),
        })
        .unwrap();

    // Build a part list with MAX_PARTS + 1 entries.
    let parts: Vec<_> = (1..=MAX_PARTS as u32 + 1)
        .map(|n| CompletePart {
            part_number: n,
            etag: "dummy".to_string(),
            checksum: None,
        })
        .collect();

    let err = coord
        .complete_multipart_upload(&CompleteMultipartUploadRequest {
            upload: multipart_object_request_with_expected_owner(
                "bucket",
                "key",
                &create.upload_id,
                test_requester(),
                None,
            ),
            parts: &parts,
            claimed_checksum: None,

            expected_object_size: None,

            cond: &WriteCondition::default(),

            sse_customer: None,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRequest { .. }));
}

// ── shard planning unit tests ──────────────────────────────────────

#[test]
fn compute_shard_size_exact_multiple() {
    // 100 bytes, k=4 → no padding needed → 25 per shard
    assert_eq!(compute_shard_size(100, 4), 25);
}

#[test]
fn compute_shard_size_needs_padding() {
    // 101 bytes, k=4 → pad to 104 → 26 per shard
    assert_eq!(compute_shard_size(101, 4), 26);
}

#[test]
fn compute_shard_size_small() {
    // 1 byte, k=4 → pad to 4 → 1 per shard
    assert_eq!(compute_shard_size(1, 4), 1);
}

#[test]
fn compute_shard_size_zero() {
    // 0 bytes, k=4 → 0 per shard
    assert_eq!(compute_shard_size(0, 4), 0);
}

#[test]
fn shards_for_byte_range_single_shard() {
    // shard_size=25, range [0,24] → shard 0
    assert_eq!(shards_for_byte_range(0, 24, 25, 4), vec![0]);
}

#[test]
fn shards_for_byte_range_spans_two() {
    // shard_size=25, range [20,30] → shards 0,1
    assert_eq!(shards_for_byte_range(20, 30, 25, 4), vec![0, 1]);
}

#[test]
fn shards_for_byte_range_all_shards() {
    // shard_size=25, range [0,99] → shards 0,1,2,3
    assert_eq!(shards_for_byte_range(0, 99, 25, 4), vec![0, 1, 2, 3]);
}

#[test]
fn shards_for_byte_range_last_shard_only() {
    // shard_size=25, range [75,99] → shard 3
    assert_eq!(shards_for_byte_range(75, 99, 25, 4), vec![3]);
}

#[test]
fn shards_for_byte_range_clamped_to_k() {
    // end falls past last shard → clamp to k-1
    assert_eq!(shards_for_byte_range(75, 200, 25, 4), vec![3]);
}

#[test]
fn shards_for_byte_range_zero_shard_size() {
    let empty: Vec<usize> = vec![];
    assert_eq!(shards_for_byte_range(0, 10, 0, 4), empty);
}

// ── range GET tests ────────────────────────────────────────────────

#[test]
fn get_object_range_basic() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"Hello, World!",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=0-4 → "Hello"
    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 4 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
    assert_eq!(result.range_start, 0);
    assert_eq!(result.range_end, 4);
    assert_eq!(result.size, 13);
}

#[test]
fn get_object_range_holds_payload_lease_on_selected_shard_nodes() {
    let tmp = test_util::tempdir();
    let storage_cluster = open_test_storage_cluster_with_ec_shape(
        tmp.path(),
        &[0, 1, 2, 3],
        storage::EcShape { k: 2, m: 1 },
    );
    let coord = setup_direct_coordinator_with_storage_cluster(Arc::clone(&storage_cluster));
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    let bucket = trusted_bucket_name("bucket");
    let key = trusted_object_key("key");
    let data = b"read handles should only pin selected shard owners";
    let put = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data,
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let generation_id = storage_cluster
        .test_get_object_meta(&bucket, &key)
        .unwrap()
        .into_live()
        .expect("put object should create a live object")
        .generation_id;
    let segment = storage_cluster
        .test_get_object_segments(&bucket, &key, put.version_id)
        .unwrap()
        .pop()
        .expect("direct put should create one object segment");
    let expected_selected_nodes = storage_cluster
        .segment_payload_shard_locations(
            segment.data_pg_id,
            storage::EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &segment.segment_okh,
            segment.segment_vid,
        )
        .unwrap()
        .into_iter()
        .map(|location| location.node_id())
        .collect::<BTreeSet<_>>()
        .len();

    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range { start: 0, end: 4 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(
        storage_cluster.object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        expected_selected_nodes,
        "range read should hold payload leases only on selected shard-owner nodes"
    );
    assert_eq!(result.body.read_all().unwrap(), b"read ");
    assert_eq!(
        storage_cluster.object_payload_lease_holder_node_count(&bucket, &key, generation_id),
        0,
        "read handle drop should release selected shard-owner payload leases"
    );
}

#[test]
fn get_object_range_suffix() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"Hello, World!",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=-6 → "World!"  (last 6 bytes)
    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Suffix { length: 6 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"World!");
    assert_eq!(result.range_start, 7);
    assert_eq!(result.range_end, 12);
}

#[test]
fn get_object_range_from_start() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"Hello, World!",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=7- → "World!"
    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::FromStart { start: 7 },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"World!");
}

#[test]
fn get_object_range_unsatisfiable() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"Hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=100- → unsatisfiable
    let err = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::FromStart { start: 100 },
            cond: NO_READ,
        })
        .unwrap_err();
    assert!(matches!(err, ServerError::InvalidRange { total_size: 5 }));
}

#[test]
fn get_object_range_clamps_end() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());

    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"Hello",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    // bytes=0-99999 on 5-byte object → clamp to 0-4
    let result = coord
        .get_object_range(&GetObjectRangeRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            range: ByteRange::Range {
                start: 0,
                end: 99999,
            },
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(result.body.read_all().unwrap(), b"Hello");
    assert_eq!(result.range_start, 0);
    assert_eq!(result.range_end, 4);
}

// ── Conditional request integration tests ────────────────────────

#[test]
fn put_if_none_match_star_creates() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let cond = WriteCondition::IfNoneMatchStar;
    let result = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "new-key", test_requester(), None),
            data: b"data",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert!(!result.etag.is_empty());
}

#[test]
fn put_if_none_match_star_rejects_overwrite() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = WriteCondition::IfNoneMatchStar;
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed));
}

#[test]
fn put_if_match_updates() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let r1 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag.clone()).unwrap());
    let r2 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    assert_ne!(r1.etag, r2.etag);

    let obj = coord
        .get_object(&GetObjectRequest {
            sse_customer: None,
            object: object_version_request_with_expected_owner(
                "bucket",
                "key",
                None,
                test_requester(),
                None,
            ),
            cond: NO_READ,
        })
        .unwrap();
    assert_eq!(obj.body.read_all().unwrap(), b"v2");
}

#[test]
fn put_if_match_stale_etag_rejected() {
    let tmp = test_util::tempdir();
    let coord = setup_coordinator(tmp.path());
    coord
        .create_bucket_for_owner("default-owner", "bucket", false)
        .unwrap();

    let r1 = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v1",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();
    // Overwrite so etag changes
    test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v2",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: NO_WRITE,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap();

    let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag).unwrap());
    let err = test_helpers::put_object(
        &coord,
        &PutObjectRequest {
            encryption: WriteEncryptionRequest::none(),
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
            data: b"v3",
            metadata: &MetadataBlob::new(),
            system_metadata: &SystemMetadata::EMPTY,
            tags: None,
            cond: &cond,

            acl: NO_PUT_OBJECT_ACL.into(),
        },
    )
    .unwrap_err();
    assert!(matches!(err, ServerError::PreconditionFailed));
}
