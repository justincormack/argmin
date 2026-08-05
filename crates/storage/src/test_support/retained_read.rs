use std::fmt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::control_plane::PgRouteSnapshot;
use crate::storage_node_server::{
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts, StorageNodeServer,
};
use crate::{
    BucketName, ClusterEpoch, EcShape, GenerationId, LocalClusterMap, LocalNodeStoreConfig,
    LocalPgRoute, LocalUnixStorageNodeClientConfig, NodeId, ObjectKey, PgId, PgState,
    RouteMapValidity, StorageCluster, StorageClusterRouteHandle, StorageClusterRuntimeMapHandle,
    TestObjectPayloadSnapshot, VersionId,
};

/// Failure while constructing or advancing an opaque retained-read topology scenario.
#[derive(Debug)]
pub struct TestRetainedReadPgMoveScenarioError {
    context: &'static str,
    detail: String,
}

impl TestRetainedReadPgMoveScenarioError {
    fn new(context: &'static str, detail: impl fmt::Display) -> Self {
        Self {
            context,
            detail: detail.to_string(),
        }
    }
}

impl fmt::Display for TestRetainedReadPgMoveScenarioError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.context, self.detail)
    }
}

impl std::error::Error for TestRetainedReadPgMoveScenarioError {}

type ScenarioResult<T> = Result<T, TestRetainedReadPgMoveScenarioError>;

fn long_lived_route_map_validity() -> RouteMapValidity {
    RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_add(3_600_000))
        .expect("one-hour test route-map validity must be in the future")
}

fn make_private_socket_dir(path: &Path) -> ScenarioResult<()> {
    std::fs::create_dir_all(path).map_err(|error| {
        TestRetainedReadPgMoveScenarioError::new("create storage-node socket directory", error)
    })?;
    let mut permissions = std::fs::metadata(path)
        .map_err(|error| {
            TestRetainedReadPgMoveScenarioError::new("inspect storage-node socket directory", error)
        })?
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions).map_err(|error| {
        TestRetainedReadPgMoveScenarioError::new("protect storage-node socket directory", error)
    })
}

struct TestUnixStorageNodeRuntime {
    stop: Arc<AtomicBool>,
    wake_socket_paths: Vec<PathBuf>,
    threads: Vec<JoinHandle<Result<(), String>>>,
    servers: Vec<Arc<StorageNodeServer>>,
    client_configs: Vec<LocalUnixStorageNodeClientConfig>,
}

impl TestUnixStorageNodeRuntime {
    fn start(
        socket_dir: &Path,
        configs: &[LocalNodeStoreConfig],
        pg_ids: &[u32],
        ec_shape: EcShape,
        cluster_epoch: ClusterEpoch,
        routes: &[PgRouteSnapshot],
        route_map_validity: RouteMapValidity,
    ) -> ScenarioResult<Self> {
        make_private_socket_dir(socket_dir)?;
        let mut runtime = Self {
            stop: Arc::new(AtomicBool::new(false)),
            wake_socket_paths: Vec::new(),
            threads: Vec::new(),
            servers: Vec::new(),
            client_configs: Vec::new(),
        };
        for config in configs {
            let socket_path = socket_dir.join(format!("node-{}.sock", config.node_id().as_u32()));
            let server_config = StorageNodeProcessConfig::new(StorageNodeProcessConfigParts {
                node_id: config.node_id(),
                cluster_epoch,
                route_map_validity,
                data_dir: config.data_dir().to_path_buf(),
                default_ec_shape: ec_shape,
                pg_ids: pg_ids.to_vec(),
                socket_path: socket_path.clone(),
                pg_routes: routes.iter().map(StorageNodePgRoute::from).collect(),
                historical_pg_routes: Vec::new(),
                pending_metadata_command_recoveries: Vec::new(),
            })
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "construct storage-node runtime configuration",
                    error,
                )
            })?;
            let server = Arc::new(StorageNodeServer::bind(server_config).map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new("bind storage-node test server", error)
            })?);
            for _ in 0..4 {
                let thread_server = Arc::clone(&server);
                let thread_stop = Arc::clone(&runtime.stop);
                runtime.threads.push(thread::spawn(move || {
                    while !thread_stop.load(Ordering::Acquire) {
                        match thread_server.accept_one() {
                            Ok(()) => {}
                            Err(error) if thread_stop.load(Ordering::Acquire) => {
                                let _ = error;
                                break;
                            }
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                    Ok(())
                }));
                runtime.wake_socket_paths.push(socket_path.clone());
            }
            runtime.servers.push(server);
            runtime
                .client_configs
                .push(LocalUnixStorageNodeClientConfig::new(
                    config.node_id(),
                    socket_path,
                ));
        }
        Ok(runtime)
    }

    fn stop(&mut self) -> ScenarioResult<()> {
        self.stop.store(true, Ordering::Release);
        for socket_path in &self.wake_socket_paths {
            let _ = UnixStream::connect(socket_path);
        }
        let mut first_failure = None;
        for thread in self.threads.drain(..) {
            match thread.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_failure.get_or_insert(error);
                }
                Err(_) => {
                    first_failure.get_or_insert_with(|| "storage-node thread panicked".into());
                }
            };
        }
        if let Some(error) = first_failure {
            return Err(TestRetainedReadPgMoveScenarioError::new(
                "run storage-node test server",
                error,
            ));
        }
        Ok(())
    }
}

impl Drop for TestUnixStorageNodeRuntime {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// Opaque storage-owned topology scenario for a retained body whose data PG
/// moves after the response is created but before its first payload read.
///
/// The caller controls only the production-visible operation ordering. Storage
/// owns the PG selection, Unix node runtime, route history, placement canary,
/// corruption target, and runtime-map transition.
pub struct TestRetainedReadPgMoveScenario {
    initial_cluster: Arc<StorageCluster>,
    runtime_handle: StorageClusterRuntimeMapHandle,
    route_handle: StorageClusterRouteHandle,
    bucket: BucketName,
    key: ObjectKey,
    configs: Vec<LocalNodeStoreConfig>,
    pg_ids: Vec<u32>,
    ec_shape: EcShape,
    current_epoch: ClusterEpoch,
    next_epoch: ClusterEpoch,
    current_routes: Vec<PgRouteSnapshot>,
    next_routes: Vec<PgRouteSnapshot>,
    socket_dir: PathBuf,
    payload_snapshot: Option<TestObjectPayloadSnapshot>,
    unix_runtime: Option<TestUnixStorageNodeRuntime>,
}

impl fmt::Debug for TestRetainedReadPgMoveScenario {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestRetainedReadPgMoveScenario")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("prepared", &self.payload_snapshot.is_some())
            .finish_non_exhaustive()
    }
}

impl TestRetainedReadPgMoveScenario {
    pub fn new(root: &Path) -> ScenarioResult<Self> {
        let old_acting_set = vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let moved_acting_set = vec![NodeId::new(3), NodeId::new(4), NodeId::new(5)];
        let node_ids = (0..6).map(NodeId::new).collect::<Vec<_>>();
        let pg_ids = vec![0, 1];
        let metadata_pg_id = 0;
        let moved_data_pg_id = 1;
        let ec_shape = EcShape { k: 2, m: 1 };
        let current_epoch = ClusterEpoch::INITIAL;
        let next_epoch = ClusterEpoch::new(current_epoch.get() + 1).ok_or_else(|| {
            TestRetainedReadPgMoveScenarioError::new(
                "construct successor cluster epoch",
                "epoch overflow",
            )
        })?;
        let configs = node_ids
            .iter()
            .map(|node_id| {
                LocalNodeStoreConfig::new(
                    *node_id,
                    root.join(format!("retained-read-node-{:04}", node_id.as_u32())),
                )
            })
            .collect::<Vec<_>>();
        let current_routes = pg_ids
            .iter()
            .map(|pg_id| {
                PgRouteSnapshot::reconstructed(
                    current_epoch,
                    PgId::new(*pg_id),
                    NodeId::new(0),
                    old_acting_set.clone(),
                    PgState::Active,
                )
            })
            .collect::<Vec<_>>();
        let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.clone(),
            &pg_ids,
            ec_shape,
            current_epoch,
            current_routes.iter().map(LocalPgRoute::from),
        )
        .map_err(|error| {
            TestRetainedReadPgMoveScenarioError::new(
                "construct initial retained-read topology",
                error,
            )
        })?;
        current_map.test_set_route_map_validity(long_lived_route_map_validity());
        let initial_cluster =
            StorageCluster::test_from_local_map_with_epoch(Arc::new(current_map), current_epoch)
                .map_err(|error| {
                    TestRetainedReadPgMoveScenarioError::new(
                        "construct initial retained-read cluster",
                        error,
                    )
                })?;
        let runtime_handle = StorageClusterRuntimeMapHandle::new(Arc::clone(&initial_cluster))
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "construct retained-read runtime-map handle",
                    error,
                )
            })?;
        let route_handle = runtime_handle.route_handle();
        let (bucket, key) =
            Self::select_bucket_and_key(&initial_cluster, metadata_pg_id, moved_data_pg_id)?;
        let next_routes = pg_ids
            .iter()
            .map(|pg_id| {
                let (primary, acting_set) = if *pg_id == moved_data_pg_id {
                    (NodeId::new(3), moved_acting_set.clone())
                } else {
                    (NodeId::new(0), old_acting_set.clone())
                };
                PgRouteSnapshot::reconstructed(
                    next_epoch,
                    PgId::new(*pg_id),
                    primary,
                    acting_set,
                    PgState::Active,
                )
            })
            .collect();

        Ok(Self {
            initial_cluster,
            runtime_handle,
            route_handle,
            bucket,
            key,
            configs,
            pg_ids,
            ec_shape,
            current_epoch,
            next_epoch,
            current_routes,
            next_routes,
            socket_dir: root.join("retained-read-sockets"),
            payload_snapshot: None,
            unix_runtime: None,
        })
    }

    fn select_bucket_and_key(
        cluster: &StorageCluster,
        metadata_pg_id: u32,
        data_pg_id: u32,
    ) -> ScenarioResult<(BucketName, ObjectKey)> {
        for bucket_suffix in 0..1024 {
            let bucket =
                BucketName::try_from(format!("retained-read-epoch-bucket-{bucket_suffix}"))
                    .map_err(|error| {
                        TestRetainedReadPgMoveScenarioError::new(
                            "construct retained-read bucket name",
                            error,
                        )
                    })?;
            if cluster.test_bucket_pg_id_for(&bucket) != metadata_pg_id {
                continue;
            }
            for key_suffix in 0..10_000 {
                let key = ObjectKey::try_from(format!("key-{key_suffix:04}"))
                    .expect("generated retained-read key must be valid");
                if cluster.test_object_pg_id_for(&bucket, &key) == metadata_pg_id
                    && cluster.test_data_pg_id_for(&bucket, &key, GenerationId::MIN) == data_pg_id
                {
                    return Ok((bucket, key));
                }
            }
        }
        Err(TestRetainedReadPgMoveScenarioError::new(
            "select retained-read placement",
            "no bucket/key mapped to the required metadata and data PGs",
        ))
    }

    pub fn route_handle(&self) -> StorageClusterRouteHandle {
        self.route_handle.clone()
    }

    pub fn bucket(&self) -> &str {
        self.bucket.as_str()
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    /// Captures and validates the newly written payload, starts the Unix storage
    /// nodes, and replaces the embedded frontend map with an equivalent Unix map.
    pub fn prepare_after_put(&mut self, version_id: VersionId) -> ScenarioResult<()> {
        if self.payload_snapshot.is_some() || self.unix_runtime.is_some() {
            return Err(TestRetainedReadPgMoveScenarioError::new(
                "prepare retained-read topology",
                "scenario was already prepared",
            ));
        }
        let snapshot = self
            .initial_cluster
            .test_capture_object_payload(&self.bucket, &self.key, version_id)
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new("capture retained-read payload", error)
            })?;
        if snapshot.segment_count() != 1 {
            return Err(TestRetainedReadPgMoveScenarioError::new(
                "validate retained-read payload layout",
                format!(
                    "expected one direct-PUT segment, found {}",
                    snapshot.segment_count()
                ),
            ));
        }
        let uses_current_placement = self
            .initial_cluster
            .test_object_payload_snapshot_uses_current_placement(&snapshot)
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "validate retained-read payload placement",
                    error,
                )
            })?;
        if !uses_current_placement {
            return Err(TestRetainedReadPgMoveScenarioError::new(
                "validate retained-read payload placement",
                "payload does not use the selected data PG and original placement epoch",
            ));
        }

        let unix_runtime = TestUnixStorageNodeRuntime::start(
            &self.socket_dir,
            &self.configs,
            &self.pg_ids,
            self.ec_shape,
            self.current_epoch,
            &self.current_routes,
            long_lived_route_map_validity(),
        )?;
        let mut current_unix_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            self.configs.clone(),
            &self.pg_ids,
            self.ec_shape,
            self.current_epoch,
            self.current_routes.iter().map(LocalPgRoute::from),
        )
        .map_err(|error| {
            TestRetainedReadPgMoveScenarioError::new(
                "construct current Unix retained-read topology",
                error,
            )
        })?;
        current_unix_map
            .install_unix_storage_node_clients(unix_runtime.client_configs.clone())
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "install current Unix retained-read clients",
                    error,
                )
            })?;
        current_unix_map.test_set_route_map_validity(long_lived_route_map_validity());
        let current_unix_cluster = StorageCluster::test_from_local_map_with_epoch(
            Arc::new(current_unix_map),
            self.current_epoch,
        )
        .map_err(|error| {
            TestRetainedReadPgMoveScenarioError::new(
                "construct current Unix retained-read cluster",
                error,
            )
        })?;
        self.runtime_handle
            .install(current_unix_cluster)
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "publish current Unix retained-read topology",
                    error,
                )
            })?;
        self.payload_snapshot = Some(snapshot);
        self.unix_runtime = Some(unix_runtime);
        Ok(())
    }

    /// Corrupts one owner-selected shard, advances the storage nodes and
    /// frontend to the moved data-PG topology, and retains the old exact route.
    pub fn corrupt_and_advance_after_body_created(&mut self) -> ScenarioResult<()> {
        let snapshot = self.payload_snapshot.as_ref().ok_or_else(|| {
            TestRetainedReadPgMoveScenarioError::new(
                "advance retained-read topology",
                "scenario has not been prepared",
            )
        })?;
        let unix_runtime = self.unix_runtime.as_ref().ok_or_else(|| {
            TestRetainedReadPgMoveScenarioError::new(
                "advance retained-read topology",
                "Unix storage-node runtime is not running",
            )
        })?;
        self.initial_cluster
            .test_inject_object_payload_shard_corruption(snapshot, 0, 0)
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "inject retained-read shard corruption",
                    error,
                )
            })?;

        let route_map_validity = long_lived_route_map_validity();
        for (server, config) in unix_runtime.servers.iter().zip(&self.configs) {
            let socket_path = self
                .socket_dir
                .join(format!("node-{}.sock", config.node_id().as_u32()));
            let next_config = StorageNodeProcessConfig::new(StorageNodeProcessConfigParts {
                node_id: config.node_id(),
                cluster_epoch: self.next_epoch,
                route_map_validity,
                data_dir: config.data_dir().to_path_buf(),
                default_ec_shape: self.ec_shape,
                pg_ids: self.pg_ids.clone(),
                socket_path,
                pg_routes: self
                    .next_routes
                    .iter()
                    .map(StorageNodePgRoute::from)
                    .collect(),
                historical_pg_routes: self
                    .current_routes
                    .iter()
                    .map(StorageNodePgRoute::from)
                    .collect(),
                pending_metadata_command_recoveries: Vec::new(),
            })
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "construct successor storage-node configuration",
                    error,
                )
            })?;
            server
                .install_control_plane_runtime_config(next_config)
                .map_err(|error| {
                    TestRetainedReadPgMoveScenarioError::new(
                        "install successor storage-node configuration",
                        error,
                    )
                })?;
        }

        let mut next_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            self.configs.clone(),
            &self.pg_ids,
            self.ec_shape,
            self.next_epoch,
            self.next_routes.iter().map(LocalPgRoute::from),
        )
        .map_err(|error| {
            TestRetainedReadPgMoveScenarioError::new(
                "construct successor retained-read topology",
                error,
            )
        })?;
        next_map.test_install_historical_pg_routes(self.current_routes.clone());
        next_map
            .install_unix_storage_node_clients(unix_runtime.client_configs.clone())
            .map_err(|error| {
                TestRetainedReadPgMoveScenarioError::new(
                    "install successor Unix retained-read clients",
                    error,
                )
            })?;
        next_map.test_set_route_map_validity(route_map_validity);
        let next_cluster =
            StorageCluster::test_from_local_map_with_epoch(Arc::new(next_map), self.next_epoch)
                .map_err(|error| {
                    TestRetainedReadPgMoveScenarioError::new(
                        "construct successor retained-read cluster",
                        error,
                    )
                })?;
        self.runtime_handle.install(next_cluster).map_err(|error| {
            TestRetainedReadPgMoveScenarioError::new(
                "publish successor retained-read topology",
                error,
            )
        })
    }

    pub fn finish(mut self) -> ScenarioResult<()> {
        if let Some(mut runtime) = self.unix_runtime.take() {
            runtime.stop()?;
        }
        Ok(())
    }
}
