// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodeProcessConfig {
    pub(crate) node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) route_map_validity: RouteMapValidity,
    pub(crate) data_dir: PathBuf,
    pub(crate) default_ec_shape: EcShape,
    pub(crate) pg_ids: Vec<u32>,
    pub(crate) socket_path: PathBuf,
    pub(crate) pg_routes: Vec<StorageNodePgRoute>,
    pub(crate) historical_pg_routes: Vec<StorageNodePgRoute>,
    pub(crate) pending_metadata_command_recoveries: Vec<(PgId, PendingMetadataCommandRecovery)>,
}
#[derive(Debug, Clone)]
pub struct StorageNodeProcessConfigParts {
    pub node_id: NodeId,
    pub cluster_epoch: ClusterEpoch,
    pub route_map_validity: RouteMapValidity,
    pub data_dir: PathBuf,
    pub default_ec_shape: EcShape,
    pub pg_ids: Vec<u32>,
    pub socket_path: PathBuf,
    pub pg_routes: Vec<StorageNodePgRoute>,
    pub historical_pg_routes: Vec<StorageNodePgRoute>,
    pub pending_metadata_command_recoveries: Vec<(PgId, PendingMetadataCommandRecovery)>,
}

#[derive(Debug, Clone)]
pub struct StorageNodeControlPlaneRefresh {
    lease: HeartbeatLease,
    runtime_map: ClusterRuntimeMapSnapshot,
    next_config: StorageNodeProcessConfig,
}

impl StorageNodeControlPlaneRefresh {
    #[must_use]
    pub fn lease(&self) -> &HeartbeatLease {
        &self.lease
    }

    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn next_config(&self) -> &StorageNodeProcessConfig {
        &self.next_config
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        HeartbeatLease,
        ClusterRuntimeMapSnapshot,
        StorageNodeProcessConfig,
    ) {
        (self.lease, self.runtime_map, self.next_config)
    }
}

impl StorageNodeProcessConfig {
    const CONTROL_PLANE_RUNTIME_CONFIG_FILE: &'static str = "control-plane-runtime-config-v1";
    const CONTROL_PLANE_RUNTIME_CONFIG_MAGIC_PREFIX: &'static str =
        "argmin-storage-node-runtime-config-v";
    const CONTROL_PLANE_RUNTIME_CONFIG_VERSION: u16 = 5;

    pub fn new(parts: StorageNodeProcessConfigParts) -> Result<Self, StorageNodeServerError> {
        let config = Self {
            node_id: parts.node_id,
            cluster_epoch: parts.cluster_epoch,
            route_map_validity: parts.route_map_validity,
            data_dir: parts.data_dir,
            default_ec_shape: parts.default_ec_shape,
            pg_ids: parts.pg_ids,
            socket_path: parts.socket_path,
            pg_routes: parts.pg_routes,
            historical_pg_routes: parts.historical_pg_routes,
            pending_metadata_command_recoveries: parts.pending_metadata_command_recoveries,
        };
        validate_process_config_route_table(&config)?;
        Ok(config)
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn route_map_validity(&self) -> RouteMapValidity {
        self.route_map_validity
    }

    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    #[must_use]
    pub fn default_ec_shape(&self) -> EcShape {
        self.default_ec_shape
    }

    #[must_use]
    pub fn pg_ids(&self) -> &[u32] {
        &self.pg_ids
    }

    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    #[must_use]
    pub fn pg_routes(&self) -> &[StorageNodePgRoute] {
        &self.pg_routes
    }

    #[must_use]
    pub fn historical_pg_routes(&self) -> &[StorageNodePgRoute] {
        &self.historical_pg_routes
    }

    /// Return the opaque durable identity for this standalone storage-node
    /// route configuration.
    pub fn standalone_route_identity(
        &self,
    ) -> Result<crate::StandaloneRouteIdentity, crate::StandaloneRouteIdentityError> {
        if self.route_map_validity != RouteMapValidity::Forever {
            return Err(crate::StandaloneRouteIdentityError::DynamicAuthority);
        }
        if !self.pending_metadata_command_recoveries.is_empty() {
            return Err(crate::StandaloneRouteIdentityError::InvalidIdentity {
                reason: "standalone storage-node route contains dynamic recovery state",
            });
        }

        let mut hasher = ChecksumHasher::new(ChecksumAlgorithm::Sha256);
        standalone_storage_node_digest_bytes(
            &mut hasher,
            b"argmin/standalone-storage-node-route/v1",
        );
        standalone_storage_node_digest_u32(&mut hasher, self.node_id.as_u32());
        standalone_storage_node_digest_u64(&mut hasher, self.cluster_epoch.get());
        standalone_storage_node_digest_bytes(&mut hasher, self.data_dir.as_os_str().as_bytes());
        standalone_storage_node_digest_u8(&mut hasher, self.default_ec_shape.k);
        standalone_storage_node_digest_u8(&mut hasher, self.default_ec_shape.m);
        standalone_storage_node_digest_bytes(&mut hasher, self.socket_path.as_os_str().as_bytes());
        standalone_storage_node_digest_len(&mut hasher, self.pg_ids.len());
        for pg_id in &self.pg_ids {
            standalone_storage_node_digest_u32(&mut hasher, *pg_id);
        }
        standalone_storage_node_digest_routes(&mut hasher, &self.pg_routes);
        standalone_storage_node_digest_routes(&mut hasher, &self.historical_pg_routes);
        Ok(crate::StandaloneRouteIdentity(
            hasher
                .finalize()
                .bytes()
                .try_into()
                .expect("SHA-256 standalone storage-node route identity contains 32 bytes"),
        ))
    }

    pub(crate) fn from_runtime_map(
        node_id: NodeId,
        data_dir: impl Into<PathBuf>,
        default_ec_shape: EcShape,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<Self, StorageNodeServerError> {
        let node = runtime_map
            .nodes()
            .iter()
            .find(|node| node.node_id() == node_id)
            .ok_or(StorageNodeServerError::RuntimeMapNodeNotFound {
                node_id: node_id.as_u32(),
                cluster_epoch: runtime_map.cluster_epoch(),
            })?;
        let pg_routes: Vec<StorageNodePgRoute> = runtime_map
            .pg_routes()
            .iter()
            .map(StorageNodePgRoute::from)
            .collect();
        let historical_pg_routes: Vec<StorageNodePgRoute> = runtime_map
            .historical_pg_routes()
            .iter()
            .map(StorageNodePgRoute::from)
            .collect();
        let pending_metadata_command_recoveries = runtime_map
            .pg_routes()
            .iter()
            .filter_map(|route| {
                route
                    .pending_metadata_command_recovery()
                    .map(|recovery| (route.pg_id(), recovery))
            })
            .collect();
        let pg_ids: Vec<u32> = pg_routes.iter().map(|route| route.pg_id).collect();
        validate_pg_ids(&pg_ids)?;
        validate_pg_routes(&pg_ids, &pg_routes)?;

        runtime_map
            .bind_process_local_lease_at(
                crate::clock::current_time_millis(),
                crate::clock::monotonic_time_millis(),
            )
            .map_err(|error| StorageNodeServerError::RouteMapLeaseBinding {
                message: error.to_string(),
            })?;
        Ok(Self {
            node_id,
            cluster_epoch: runtime_map.cluster_epoch(),
            route_map_validity: runtime_map.validity(),
            data_dir: data_dir.into(),
            default_ec_shape,
            pg_ids,
            socket_path: PathBuf::from(node.endpoint()),
            pg_routes,
            historical_pg_routes,
            pending_metadata_command_recoveries,
        })
    }

    pub(crate) fn from_runtime_map_refresh(
        current: &StorageNodeProcessConfig,
        runtime_map: &ClusterRuntimeMapSnapshot,
        history_reference_summary: crate::PgClusterMapHistoryReferenceSummary,
    ) -> Result<Self, StorageNodeServerError> {
        let protected_historical_route_keys = protected_route_keys_for_refresh(runtime_map);
        let mut next = Self::from_runtime_map(
            current.node_id,
            current.data_dir.clone(),
            current.default_ec_shape,
            runtime_map,
        )?;

        let mut historical_pg_routes = BTreeMap::new();
        for route in &current.historical_pg_routes {
            if route.cluster_epoch < next.cluster_epoch {
                historical_pg_routes.insert((route.cluster_epoch, route.pg_id), route.clone());
            }
        }
        if current.cluster_epoch < next.cluster_epoch {
            for route in &current.pg_routes {
                historical_pg_routes.insert((route.cluster_epoch, route.pg_id), route.clone());
            }
        }
        for route in &next.historical_pg_routes {
            if route.cluster_epoch < next.cluster_epoch {
                historical_pg_routes.insert((route.cluster_epoch, route.pg_id), route.clone());
            }
        }
        next.historical_pg_routes = prune_refresh_historical_pg_routes(
            historical_pg_routes,
            current.cluster_epoch,
            history_reference_summary,
            &protected_historical_route_keys,
        );
        Ok(next)
    }

    pub fn route_map_valid_until_ms(&self) -> Option<u64> {
        self.route_map_validity.valid_until_ms()
    }

    fn only_extends_route_map_validity_from(&self, current: &Self) -> bool {
        let (Some(current_deadline), Some(candidate_deadline)) = (
            current.route_map_valid_until_ms(),
            self.route_map_valid_until_ms(),
        ) else {
            return false;
        };
        candidate_deadline >= current_deadline
            && self.node_id == current.node_id
            && self.cluster_epoch == current.cluster_epoch
            && self.data_dir == current.data_dir
            && self.default_ec_shape == current.default_ec_shape
            && self.pg_ids == current.pg_ids
            && self.socket_path == current.socket_path
            && self.pg_routes == current.pg_routes
            && self.historical_pg_routes == current.historical_pg_routes
            && self.pending_metadata_command_recoveries
                == current.pending_metadata_command_recoveries
    }

    pub fn is_route_map_valid_at(&self, now_ms: u64) -> bool {
        self.route_map_validity.is_valid_at(now_ms)
    }

    pub fn require_route_map_valid_at(&self, now_ms: u64) -> Result<(), StorageNodeServerError> {
        match self.route_map_validity {
            RouteMapValidity::Until(valid_until_ms) if valid_until_ms.get() <= now_ms => {
                Err(StorageNodeServerError::RouteMapExpired {
                    cluster_epoch: self.cluster_epoch,
                    valid_until_ms: valid_until_ms.get(),
                    now_ms,
                })
            }
            _ => Ok(()),
        }
    }

    pub fn validate_runtime_refresh_from(
        &self,
        current: &StorageNodeProcessConfig,
    ) -> Result<(), StorageNodeServerError> {
        if self.node_id != current.node_id {
            return Err(StorageNodeServerError::RuntimeRefreshNodeChanged {
                current: current.node_id.as_u32(),
                candidate: self.node_id.as_u32(),
            });
        }
        if self.data_dir != current.data_dir {
            return Err(StorageNodeServerError::RuntimeRefreshDataDirChanged {
                current: current.data_dir.clone(),
                candidate: self.data_dir.clone(),
            });
        }
        if self.default_ec_shape != current.default_ec_shape {
            return Err(StorageNodeServerError::RuntimeRefreshEcShapeChanged {
                current: current.default_ec_shape,
                candidate: self.default_ec_shape,
            });
        }
        if self.socket_path != current.socket_path {
            return Err(StorageNodeServerError::RuntimeRefreshSocketPathChanged {
                current: current.socket_path.clone(),
                candidate: self.socket_path.clone(),
            });
        }
        Ok(())
    }

    pub(crate) fn load_control_plane_runtime_config(
        data_dir: impl AsRef<Path>,
        node_id: NodeId,
        default_ec_shape: EcShape,
        socket_path: impl AsRef<Path>,
    ) -> Result<Option<Self>, StorageNodeServerError> {
        let data_dir = data_dir.as_ref();
        let path = control_plane_runtime_config_path(data_dir);
        let Some(raw) = read_control_plane_runtime_config(&path)? else {
            return Ok(None);
        };
        let config = decode_control_plane_runtime_config(
            &path,
            data_dir.to_path_buf(),
            default_ec_shape,
            &raw,
        )?;
        if config.node_id != node_id {
            return Err(StorageNodeServerError::RuntimeConfigInvalid {
                path,
                message: format!(
                    "runtime config node {} does not match expected node {}",
                    config.node_id.as_u32(),
                    node_id.as_u32()
                ),
            });
        }
        if config.socket_path != socket_path.as_ref() {
            return Err(StorageNodeServerError::RuntimeConfigInvalid {
                path,
                message: format!(
                    "runtime config socket path {:?} does not match expected {:?}",
                    config.socket_path,
                    socket_path.as_ref()
                ),
            });
        }
        validate_process_config_route_table(&config)?;
        Ok(Some(config))
    }

    pub(crate) fn persist_control_plane_runtime_config(
        &self,
    ) -> Result<(), StorageNodeServerError> {
        self.stage_control_plane_runtime_config()?.publish()
    }

    fn stage_control_plane_runtime_config(
        &self,
    ) -> Result<StagedControlPlaneRuntimeConfig, StorageNodeServerError> {
        self.stage_control_plane_runtime_config_with_post_write(|| {})
    }

    fn stage_control_plane_runtime_config_with_post_write<F>(
        &self,
        post_write: F,
    ) -> Result<StagedControlPlaneRuntimeConfig, StorageNodeServerError>
    where
        F: FnOnce(),
    {
        prepare_private_data_dir(&self.data_dir).map_err(|source| {
            StorageNodeServerError::RuntimeConfigWrite {
                path: self.data_dir.clone(),
                source,
            }
        })?;
        let path = control_plane_runtime_config_path(&self.data_dir);
        let tmp_path = path.with_extension("tmp");
        let contents = encode_control_plane_runtime_config(self);
        if contents.len() > CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES {
            return Err(runtime_config_invalid(
                &path,
                format!(
                    "encoded runtime config exceeds {CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES} bytes"
                ),
            ));
        }
        let mut tmp_file = create_control_plane_runtime_config_staging_file(&tmp_path)?;
        tmp_file.write_all(contents.as_bytes()).map_err(|source| {
            StorageNodeServerError::RuntimeConfigWrite {
                path: tmp_path.clone(),
                source,
            }
        })?;
        drop(tmp_file);
        post_write();
        Ok(StagedControlPlaneRuntimeConfig {
            tmp_path,
            path,
            published: false,
        })
    }

    pub(crate) fn control_plane_heartbeat(
        &self,
        node: &SharedStorageNode,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
    ) -> Result<NodeHeartbeat, StorageNodeServerError> {
        for route in &self.pg_routes {
            if route.cluster_epoch != self.cluster_epoch {
                return Err(StorageNodeServerError::RouteEpochMismatch {
                    pg_id: route.pg_id,
                    route_epoch: route.cluster_epoch,
                    config_epoch: self.cluster_epoch,
                });
            }
        }
        let endpoint =
            self.socket_path
                .to_str()
                .ok_or_else(|| StorageNodeServerError::SocketPathNotUtf8 {
                    path: self.socket_path.clone(),
                })?;
        node.control_plane_heartbeat(
            self.node_id,
            node_incarnation,
            endpoint,
            self.cluster_epoch,
            requested_lease_duration_ms,
            self.pg_routes
                .iter()
                .filter(|route| route.acting_set.contains(&self.node_id))
                .map(|route| (PgId::new(route.pg_id), route.state)),
        )
        .map_err(StorageNodeServerError::from)
    }
}

fn standalone_storage_node_digest_len(hasher: &mut ChecksumHasher, len: usize) {
    standalone_storage_node_digest_u64(
        hasher,
        u64::try_from(len).expect("standalone route collection length fits u64"),
    );
}

fn standalone_storage_node_digest_bytes(hasher: &mut ChecksumHasher, bytes: &[u8]) {
    standalone_storage_node_digest_len(hasher, bytes.len());
    hasher.update(bytes);
}

fn standalone_storage_node_digest_u64(hasher: &mut ChecksumHasher, value: u64) {
    hasher.update(&value.to_be_bytes());
}

fn standalone_storage_node_digest_u32(hasher: &mut ChecksumHasher, value: u32) {
    hasher.update(&value.to_be_bytes());
}

fn standalone_storage_node_digest_u8(hasher: &mut ChecksumHasher, value: u8) {
    hasher.update(&[value]);
}

fn standalone_storage_node_digest_routes(
    hasher: &mut ChecksumHasher,
    routes: &[StorageNodePgRoute],
) {
    let mut routes = routes.iter().collect::<Vec<_>>();
    routes.sort_by_key(|route| (route.cluster_epoch, route.pg_id));
    standalone_storage_node_digest_len(hasher, routes.len());
    for route in routes {
        standalone_storage_node_digest_u32(hasher, route.pg_id);
        standalone_storage_node_digest_u64(hasher, route.cluster_epoch.get());
        standalone_storage_node_digest_u8(
            hasher,
            match route.state {
                PgState::Active => 1,
                PgState::Peering => 2,
                PgState::Degraded => 3,
                PgState::Backfilling => 4,
                PgState::Inconsistent => 5,
            },
        );
        standalone_storage_node_digest_u32(hasher, route.primary_node_id.as_u32());
        standalone_storage_node_digest_len(hasher, route.acting_set.len());
        for node_id in &route.acting_set {
            standalone_storage_node_digest_u32(hasher, node_id.as_u32());
        }
    }
}

struct StagedControlPlaneRuntimeConfig {
    tmp_path: PathBuf,
    path: PathBuf,
    published: bool,
}

impl StagedControlPlaneRuntimeConfig {
    fn publish(mut self) -> Result<(), StorageNodeServerError> {
        fs::rename(&self.tmp_path, &self.path).map_err(|source| {
            StorageNodeServerError::RuntimeConfigWrite {
                path: self.path.clone(),
                source,
            }
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for StagedControlPlaneRuntimeConfig {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

fn bind_storage_node_route_map_lease(
    validity: RouteMapValidity,
) -> Result<Option<BoundRouteMapLease>, StorageNodeServerError> {
    bind_storage_node_route_map_lease_at(
        validity,
        crate::clock::current_time_millis(),
        crate::clock::monotonic_time_millis(),
        crate::clock::clock_health_time_millis(),
    )
}

fn bind_storage_node_route_map_lease_at(
    validity: RouteMapValidity,
    local_wall_ms: u64,
    local_monotonic_ms: u64,
    local_health_ms: Option<u64>,
) -> Result<Option<BoundRouteMapLease>, StorageNodeServerError> {
    let Some(valid_until_ms) = validity.valid_until_ms() else {
        return Ok(None);
    };
    validate_process_lease_clock(
        local_wall_ms,
        local_health_ms,
        CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
    )
    .map_err(|error| StorageNodeServerError::RouteMapLeaseBinding {
        message: error.to_string(),
    })?;
    BoundRouteMapLease::bind(
        local_wall_ms,
        valid_until_ms,
        local_wall_ms,
        local_monotonic_ms,
        CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
    )
    .map(Some)
    .map_err(|error| StorageNodeServerError::RouteMapLeaseBinding {
        message: error.to_string(),
    })
}

fn prune_refresh_historical_pg_routes(
    historical_pg_routes: BTreeMap<(ClusterEpoch, u32), StorageNodePgRoute>,
    fallback_floor: ClusterEpoch,
    history_reference_summary: crate::PgClusterMapHistoryReferenceSummary,
    protected_historical_route_keys: &BTreeSet<(ClusterEpoch, u32)>,
) -> Vec<StorageNodePgRoute> {
    let referenced_floor = history_reference_summary
        .oldest_required_epoch()
        .filter(|epoch| *epoch < fallback_floor);
    let retention_floor = referenced_floor.unwrap_or(fallback_floor);
    let mut retained = BTreeMap::new();
    let mut predecessor_by_pg = BTreeMap::<u32, StorageNodePgRoute>::new();
    for ((epoch, pg_id), route) in historical_pg_routes {
        if epoch >= retention_floor || protected_historical_route_keys.contains(&(epoch, pg_id)) {
            retained.insert((epoch, pg_id), route);
        } else if referenced_floor.is_some() {
            predecessor_by_pg.insert(pg_id, route);
        }
    }
    for route in predecessor_by_pg.into_values() {
        retained.insert((route.cluster_epoch, route.pg_id), route);
    }
    retained.into_values().collect()
}

fn protected_route_keys_for_refresh(
    runtime_map: &ClusterRuntimeMapSnapshot,
) -> BTreeSet<(ClusterEpoch, u32)> {
    let mut protected: BTreeSet<_> = runtime_map
        .historical_pg_routes()
        .iter()
        .map(|route| (route.cluster_epoch(), route.pg_id().get()))
        .collect();
    protected.extend(
        runtime_map
            .pg_routes()
            .iter()
            .chain(runtime_map.historical_pg_routes())
            .flat_map(|route| {
                [
                    route
                        .peering_metadata_transfer_destination_epoch()
                        .map(|destination_epoch| (destination_epoch, route.pg_id().get())),
                    route
                        .peering_metadata_transfer_source_route_epoch()
                        .map(|source_epoch| (source_epoch, route.pg_id().get())),
                    route
                        .pending_metadata_command_recovery()
                        .map(|recovery| (recovery.pending().cluster_epoch(), route.pg_id().get())),
                ]
                .into_iter()
                .flatten()
            }),
    );
    protected
}

fn control_plane_runtime_config_path(data_dir: &Path) -> PathBuf {
    data_dir.join(StorageNodeProcessConfig::CONTROL_PLANE_RUNTIME_CONFIG_FILE)
}

fn create_control_plane_runtime_config_staging_file(
    path: &Path,
) -> Result<File, StorageNodeServerError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            fs::remove_file(path).map_err(|source| StorageNodeServerError::RuntimeConfigWrite {
                path: path.to_path_buf(),
                source,
            })?;
        }
        Ok(_) => {
            return Err(StorageNodeServerError::RuntimeConfigWrite {
                path: path.to_path_buf(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "runtime config staging path is not a regular file",
                ),
            });
        }
        Err(source) if source.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(StorageNodeServerError::RuntimeConfigWrite {
                path: path.to_path_buf(),
                source,
            });
        }
    }

    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options
        .open(path)
        .map_err(|source| StorageNodeServerError::RuntimeConfigWrite {
            path: path.to_path_buf(),
            source,
        })
}

fn read_control_plane_runtime_config(
    path: &Path,
) -> Result<Option<String>, StorageNodeServerError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    let file = match options.open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(StorageNodeServerError::RuntimeConfigRead {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let metadata = file
        .metadata()
        .map_err(|source| StorageNodeServerError::RuntimeConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(runtime_config_invalid(
            path,
            "runtime config is not a regular file",
        ));
    }
    let max_bytes = u64::try_from(CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES)
        .expect("runtime config byte bound fits u64");
    if metadata.len() > max_bytes {
        return Err(runtime_config_invalid(
            path,
            format!("runtime config exceeds {CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES} bytes"),
        ));
    }
    let initial_capacity =
        usize::try_from(metadata.len()).expect("bounded runtime config file length fits usize");
    let mut bytes = Vec::with_capacity(initial_capacity);
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| StorageNodeServerError::RuntimeConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() > CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES {
        return Err(runtime_config_invalid(
            path,
            format!("runtime config exceeds {CONTROL_PLANE_RUNTIME_CONFIG_MAX_BYTES} bytes"),
        ));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| runtime_config_invalid(path, "runtime config is not UTF-8"))
}

fn validate_process_config_matches_persisted_runtime_config(
    config: &StorageNodeProcessConfig,
) -> Result<(), StorageNodeServerError> {
    let persisted = StorageNodeProcessConfig::load_control_plane_runtime_config(
        &config.data_dir,
        config.node_id,
        config.default_ec_shape,
        &config.socket_path,
    )?;
    if persisted
        .as_ref()
        .is_some_and(|persisted| persisted != config)
    {
        return Err(StorageNodeServerError::PersistedRuntimeConfigMismatch {
            path: control_plane_runtime_config_path(&config.data_dir),
        });
    }
    Ok(())
}

fn encode_control_plane_runtime_config(config: &StorageNodeProcessConfig) -> String {
    let mut out = String::new();
    out.push_str(StorageNodeProcessConfig::CONTROL_PLANE_RUNTIME_CONFIG_MAGIC_PREFIX);
    out.push_str(
        &StorageNodeProcessConfig::CONTROL_PLANE_RUNTIME_CONFIG_VERSION.to_string(),
    );
    out.push('\n');
    out.push_str(&format!("node_id {}\n", config.node_id.as_u32()));
    out.push_str(&format!("cluster_epoch {}\n", config.cluster_epoch.get()));
    match config.route_map_validity {
        RouteMapValidity::Forever => out.push_str("route_map_validity forever\n"),
        RouteMapValidity::Until(valid_until_ms) => {
            out.push_str(&format!(
                "route_map_validity until {}\n",
                valid_until_ms.get()
            ));
        }
    }
    out.push_str(&format!(
        "ec_shape {} {}\n",
        config.default_ec_shape.k, config.default_ec_shape.m
    ));
    out.push_str(&format!(
        "socket_path {}\n",
        hex_encode_path(&config.socket_path)
    ));
    out.push_str(&format!("pg_ids {}\n", config.pg_ids.len()));
    for pg_id in &config.pg_ids {
        out.push_str(&format!("{pg_id}\n"));
    }
    encode_storage_node_routes(&mut out, "pg_routes", &config.pg_routes);
    encode_storage_node_routes(
        &mut out,
        "historical_pg_routes",
        &config.historical_pg_routes,
    );
    out.push_str(&format!(
        "pending_metadata_command_recoveries {}\n",
        config.pending_metadata_command_recoveries.len()
    ));
    for (pg_id, recovery) in &config.pending_metadata_command_recoveries {
        out.push_str(&format!(
            "{} {} {} {} {}\n",
            pg_id.get(),
            recovery.reporting_node_id().as_u32(),
            recovery.pending().cluster_epoch().get(),
            recovery.pending().log_index(),
            recovery.pending().command_checksum()
        ));
    }
    out
}

fn encode_storage_node_routes(out: &mut String, label: &str, routes: &[StorageNodePgRoute]) {
    out.push_str(&format!("{label} {}\n", routes.len()));
    for route in routes {
        out.push_str(&format!(
            "{} {} {} {} {} {} {} {} {} {} {} {}",
            route.pg_id,
            route.cluster_epoch.get(),
            pg_state_code(route.state),
            route.primary_node_id.as_u32(),
            route
                .metadata_transfer_destination_epoch
                .map_or_else(|| "-".to_owned(), |epoch| epoch.get().to_string()),
            route.metadata_read_route.map_or_else(
                || "-".to_owned(),
                |read| read.node_id().as_u32().to_string()
            ),
            route.metadata_read_route.map_or_else(
                || "-".to_owned(),
                |read| read.proof().applied_log_index.to_string()
            ),
            route.metadata_read_route.map_or_else(
                || "-".to_owned(),
                |read| read.proof().applied_log_hash.encoding_version().to_string()
            ),
            route.metadata_read_route.map_or_else(
                || "-".to_owned(),
                |read| read.proof().applied_log_hash.value().to_string()
            ),
            route.metadata_read_route.map_or_else(
                || "-".to_owned(),
                |read| read.proof().state_digest.encoding_version().to_string()
            ),
            route.metadata_read_route.map_or_else(
                || "-".to_owned(),
                |read| read.proof().state_digest.value().to_string()
            ),
            route.acting_set.len()
        ));
        for node_id in &route.acting_set {
            out.push_str(&format!(" {}", node_id.as_u32()));
        }
        out.push('\n');
    }
}

fn decode_control_plane_runtime_config(
    path: &Path,
    data_dir: PathBuf,
    default_ec_shape: EcShape,
    raw: &str,
) -> Result<StorageNodeProcessConfig, StorageNodeServerError> {
    let mut lines = raw.lines();
    let magic = lines
        .next()
        .ok_or_else(|| runtime_config_invalid(path, "empty config"))?;
    validate_control_plane_runtime_config_magic(path, magic)?;
    let node_id = NodeId::new(parse_labeled_u32(path, lines.next(), "node_id")?);
    let cluster_epoch = parse_labeled_cluster_epoch(path, lines.next(), "cluster_epoch")?;
    let route_map_validity = parse_labeled_route_map_validity(path, lines.next())?;
    let stored_ec_shape = parse_labeled_ec_shape(path, lines.next(), "ec_shape")?;
    if stored_ec_shape != default_ec_shape {
        return Err(runtime_config_invalid(
            path,
            format!(
                "stored EC shape {:?} does not match expected {:?}",
                stored_ec_shape, default_ec_shape
            ),
        ));
    }
    let socket_path = parse_labeled_hex_path(path, lines.next(), "socket_path")?;
    let pg_id_count = parse_labeled_usize(path, lines.next(), "pg_ids")?;
    validate_runtime_config_count(path, "pg_ids", pg_id_count, lines.clone().count())?;
    let mut pg_ids = Vec::new();
    for _ in 0..pg_id_count {
        let line = lines
            .next()
            .ok_or_else(|| runtime_config_invalid(path, "missing PG id"))?;
        pg_ids.push(parse_u32_field(path, line, "PG id")?);
    }
    let pg_routes = decode_storage_node_routes(path, &mut lines, "pg_routes")?;
    let historical_pg_routes =
        decode_storage_node_routes(path, &mut lines, "historical_pg_routes")?;
    let recovery_count =
        parse_labeled_usize(path, lines.next(), "pending_metadata_command_recoveries")?;
    validate_runtime_config_count(
        path,
        "pending_metadata_command_recoveries",
        recovery_count,
        lines.clone().count(),
    )?;
    let mut pending_metadata_command_recoveries = Vec::new();
    for _ in 0..recovery_count {
        let line = lines.next().ok_or_else(|| {
            runtime_config_invalid(path, "missing pending metadata command recovery")
        })?;
        let mut fields = line.split_whitespace();
        let invalid = "invalid pending metadata command recovery";
        let pg_id = next_runtime_config_field(path, &mut fields, invalid)?;
        let reporting_node_id = next_runtime_config_field(path, &mut fields, invalid)?;
        let cluster_epoch = next_runtime_config_field(path, &mut fields, invalid)?;
        let log_index = next_runtime_config_field(path, &mut fields, invalid)?;
        let command_checksum = next_runtime_config_field(path, &mut fields, invalid)?;
        if fields.next().is_some() {
            return Err(runtime_config_invalid(
                path,
                "invalid pending metadata command recovery",
            ));
        }
        pending_metadata_command_recoveries.push((
            PgId::new(parse_u32_field(path, pg_id, "recovery PG id")?),
            PendingMetadataCommandRecovery::new(
                NodeId::new(parse_u32_field(
                    path,
                    reporting_node_id,
                    "recovery reporting node",
                )?),
                PendingMetadataCommandObservation::new(
                    parse_cluster_epoch_field(path, cluster_epoch, "recovery command epoch")?,
                    std::num::NonZeroU64::new(parse_u64_field(
                        path,
                        log_index,
                        "recovery command log index",
                    )?)
                    .ok_or_else(|| {
                        runtime_config_invalid(path, "recovery command log index must be nonzero")
                    })?,
                    parse_u64_field(path, command_checksum, "recovery command checksum")?,
                ),
            ),
        ));
    }
    if lines.next().is_some() {
        return Err(runtime_config_invalid(path, "trailing data"));
    }
    Ok(StorageNodeProcessConfig {
        node_id,
        cluster_epoch,
        route_map_validity,
        data_dir,
        default_ec_shape,
        pg_ids,
        socket_path,
        pg_routes,
        historical_pg_routes,
        pending_metadata_command_recoveries,
    })
}

fn decode_storage_node_routes<'a>(
    path: &Path,
    lines: &mut std::str::Lines<'a>,
    label: &str,
) -> Result<Vec<StorageNodePgRoute>, StorageNodeServerError> {
    let count = parse_labeled_usize(path, lines.next(), label)?;
    validate_runtime_config_count(path, label, count, lines.clone().count())?;
    let mut routes = Vec::new();
    for _ in 0..count {
        let line = lines
            .next()
            .ok_or_else(|| runtime_config_invalid(path, format!("missing {label} route")))?;
        let mut fields = line.split_whitespace();
        let too_few = format!("{label} route has too few fields");
        let pg_id = next_runtime_config_field(path, &mut fields, &too_few)?;
        let cluster_epoch = next_runtime_config_field(path, &mut fields, &too_few)?;
        let state = next_runtime_config_field(path, &mut fields, &too_few)?;
        let primary_node_id = next_runtime_config_field(path, &mut fields, &too_few)?;
        let metadata_transfer_destination_epoch =
            next_runtime_config_field(path, &mut fields, &too_few)?;
        let metadata_read_node_id = next_runtime_config_field(path, &mut fields, &too_few)?;
        let metadata_read_log_index = next_runtime_config_field(path, &mut fields, &too_few)?;
        let metadata_read_log_hash_version =
            next_runtime_config_field(path, &mut fields, &too_few)?;
        let metadata_read_log_hash = next_runtime_config_field(path, &mut fields, &too_few)?;
        let metadata_read_state_digest_version =
            next_runtime_config_field(path, &mut fields, &too_few)?;
        let metadata_read_state_digest = next_runtime_config_field(path, &mut fields, &too_few)?;
        let acting_len = next_runtime_config_field(path, &mut fields, &too_few)?;
        let acting_len = parse_usize_field(path, acting_len, "acting set length")?;
        let mut acting_set = Vec::new();
        for field in fields {
            if acting_set.len() == acting_len {
                return Err(runtime_config_invalid(
                    path,
                    format!("{label} route acting set length mismatch"),
                ));
            }
            acting_set.push(NodeId::new(parse_u32_field(
                path,
                field,
                "acting set node",
            )?));
        }
        if acting_set.len() != acting_len {
            return Err(runtime_config_invalid(
                path,
                format!("{label} route acting set length mismatch"),
            ));
        }
        routes.push(StorageNodePgRoute {
            pg_id: parse_u32_field(path, pg_id, "route PG id")?,
            cluster_epoch: parse_cluster_epoch_field(path, cluster_epoch, "route cluster epoch")?,
            state: pg_state_from_code(parse_u8_field(path, state, "route PG state")?)
                .ok_or_else(|| runtime_config_invalid(path, "invalid route PG state"))?,
            primary_node_id: NodeId::new(parse_u32_field(path, primary_node_id, "primary node")?),
            metadata_transfer_destination_epoch: if metadata_transfer_destination_epoch == "-" {
                None
            } else {
                Some(parse_cluster_epoch_field(
                    path,
                    metadata_transfer_destination_epoch,
                    "metadata transfer destination epoch",
                )?)
            },
            metadata_read_route: match (
                metadata_read_node_id,
                metadata_read_log_index,
                metadata_read_log_hash_version,
                metadata_read_log_hash,
                metadata_read_state_digest_version,
                metadata_read_state_digest,
            ) {
                ("-", "-", "-", "-", "-", "-") => None,
                fields if fields.0 == "-"
                    || fields.1 == "-"
                    || fields.2 == "-"
                    || fields.3 == "-"
                    || fields.4 == "-"
                    || fields.5 == "-" =>
                {
                    return Err(runtime_config_invalid(
                        path,
                        format!("{label} route has an incomplete metadata read route"),
                    ));
                }
                (
                    node_id,
                    log_index,
                    log_hash_version,
                    log_hash,
                    state_digest_version,
                    state_digest,
                ) => Some(PgMetadataReadRoute::new(
                    NodeId::new(parse_u32_field(path, node_id, "metadata read node")?),
                    crate::control_plane::PgMetadataProof {
                        applied_log_index: parse_u64_field(
                            path,
                            log_index,
                            "metadata read log index",
                        )?,
                        applied_log_hash: crate::control_plane::MetadataCommandLogHash::from_encoded_parts(
                            parse_u8_field(path, log_hash_version, "metadata read log hash version")?,
                            parse_u64_field(path, log_hash, "metadata read log hash")?,
                        )
                        .map_err(|error| runtime_config_invalid(path, error.to_string()))?,
                        state_digest: crate::control_plane::CanonicalStateDigest::from_encoded_parts(
                            parse_u8_field(path, state_digest_version, "metadata read state digest version")?,
                            parse_u64_field(path, state_digest, "metadata read state digest")?,
                        )
                        .map_err(|error| runtime_config_invalid(path, error.to_string()))?,
                    },
                )),
            },
            acting_set,
        });
    }
    Ok(routes)
}

fn next_runtime_config_field<'a>(
    path: &Path,
    fields: &mut std::str::SplitWhitespace<'a>,
    missing_message: &str,
) -> Result<&'a str, StorageNodeServerError> {
    fields
        .next()
        .ok_or_else(|| runtime_config_invalid(path, missing_message))
}

fn validate_runtime_config_count(
    path: &Path,
    label: &str,
    count: usize,
    remaining_lines: usize,
) -> Result<(), StorageNodeServerError> {
    if count > remaining_lines {
        return Err(runtime_config_invalid(
            path,
            format!("{label} count exceeds remaining runtime config records"),
        ));
    }
    Ok(())
}

fn parse_labeled_u32(
    path: &Path,
    line: Option<&str>,
    label: &str,
) -> Result<u32, StorageNodeServerError> {
    parse_labeled_field(path, line, label, |path, value| {
        parse_u32_field(path, value, label)
    })
}

fn parse_labeled_usize(
    path: &Path,
    line: Option<&str>,
    label: &str,
) -> Result<usize, StorageNodeServerError> {
    parse_labeled_field(path, line, label, |path, value| {
        parse_usize_field(path, value, label)
    })
}

fn parse_labeled_cluster_epoch(
    path: &Path,
    line: Option<&str>,
    label: &str,
) -> Result<ClusterEpoch, StorageNodeServerError> {
    parse_labeled_field(path, line, label, |path, value| {
        parse_cluster_epoch_field(path, value, label)
    })
}

fn parse_labeled_ec_shape(
    path: &Path,
    line: Option<&str>,
    label: &str,
) -> Result<EcShape, StorageNodeServerError> {
    let line = line.ok_or_else(|| runtime_config_invalid(path, format!("missing {label}")))?;
    let fields: Vec<_> = line.split_whitespace().collect();
    if fields.len() != 3 || fields[0] != label {
        return Err(runtime_config_invalid(
            path,
            format!("invalid {label} line"),
        ));
    }
    Ok(EcShape {
        k: parse_u8_field(path, fields[1], "EC data shards")?,
        m: parse_u8_field(path, fields[2], "EC parity shards")?,
    })
}

fn parse_labeled_hex_path(
    path: &Path,
    line: Option<&str>,
    label: &str,
) -> Result<PathBuf, StorageNodeServerError> {
    parse_labeled_field(path, line, label, |path, value| {
        decode_hex_path(path, value)
    })
}

fn parse_labeled_route_map_validity(
    path: &Path,
    line: Option<&str>,
) -> Result<RouteMapValidity, StorageNodeServerError> {
    let line = line.ok_or_else(|| runtime_config_invalid(path, "missing route_map_validity"))?;
    let fields: Vec<_> = line.split_whitespace().collect();
    match fields.as_slice() {
        ["route_map_validity", "forever"] => Ok(RouteMapValidity::Forever),
        ["route_map_validity", "until", valid_until_ms] => {
            let valid_until_ms = parse_u64_field(path, valid_until_ms, "route_map_validity")?;
            RouteMapValidity::until_ms(valid_until_ms).ok_or_else(|| {
                runtime_config_invalid(
                    path,
                    "route_map_validity until value uses reserved unbounded sentinel",
                )
            })
        }
        _ => Err(runtime_config_invalid(
            path,
            "invalid route_map_validity line",
        )),
    }
}

fn parse_labeled_field<T>(
    path: &Path,
    line: Option<&str>,
    label: &str,
    parse: impl FnOnce(&Path, &str) -> Result<T, StorageNodeServerError>,
) -> Result<T, StorageNodeServerError> {
    let line = line.ok_or_else(|| runtime_config_invalid(path, format!("missing {label}")))?;
    let mut fields = line.split_whitespace();
    if fields.next() != Some(label) {
        return Err(runtime_config_invalid(
            path,
            format!("invalid {label} line"),
        ));
    }
    let value = fields
        .next()
        .ok_or_else(|| runtime_config_invalid(path, format!("missing {label} value")))?;
    if fields.next().is_some() {
        return Err(runtime_config_invalid(
            path,
            format!("extra {label} fields"),
        ));
    }
    parse(path, value)
}

fn parse_u8_field(path: &Path, value: &str, field: &str) -> Result<u8, StorageNodeServerError> {
    value
        .parse::<u8>()
        .map_err(|_| runtime_config_invalid(path, format!("invalid {field}")))
}

fn parse_u32_field(path: &Path, value: &str, field: &str) -> Result<u32, StorageNodeServerError> {
    value
        .parse::<u32>()
        .map_err(|_| runtime_config_invalid(path, format!("invalid {field}")))
}

fn parse_u64_field(path: &Path, value: &str, field: &str) -> Result<u64, StorageNodeServerError> {
    value
        .parse::<u64>()
        .map_err(|_| runtime_config_invalid(path, format!("invalid {field}")))
}

fn parse_usize_field(
    path: &Path,
    value: &str,
    field: &str,
) -> Result<usize, StorageNodeServerError> {
    value
        .parse::<usize>()
        .map_err(|_| runtime_config_invalid(path, format!("invalid {field}")))
}

fn parse_cluster_epoch_field(
    path: &Path,
    value: &str,
    field: &str,
) -> Result<ClusterEpoch, StorageNodeServerError> {
    ClusterEpoch::new(parse_u64_field(path, value, field)?)
        .ok_or_else(|| runtime_config_invalid(path, format!("{field} must not be zero")))
}

fn runtime_config_invalid(path: &Path, message: impl Into<String>) -> StorageNodeServerError {
    StorageNodeServerError::RuntimeConfigInvalid {
        path: path.to_path_buf(),
        message: message.into(),
    }
}

fn validate_control_plane_runtime_config_magic(
    path: &Path,
    magic: &str,
) -> Result<(), StorageNodeServerError> {
    let Some(version_text) =
        magic.strip_prefix(StorageNodeProcessConfig::CONTROL_PLANE_RUNTIME_CONFIG_MAGIC_PREFIX)
    else {
        return Err(StorageNodeServerError::RuntimeConfigUnknownMagic {
            path: path.to_path_buf(),
        });
    };
    let Some(version) = version_text
        .parse::<u16>()
        .ok()
        .filter(|version| version.to_string() == version_text)
    else {
        return Err(StorageNodeServerError::RuntimeConfigUnknownMagic {
            path: path.to_path_buf(),
        });
    };
    if version != StorageNodeProcessConfig::CONTROL_PLANE_RUNTIME_CONFIG_VERSION {
        return Err(StorageNodeServerError::RuntimeConfigUnsupportedVersion {
            path: path.to_path_buf(),
            actual: version,
        });
    }
    Ok(())
}

fn pg_state_code(state: PgState) -> u8 {
    match state {
        PgState::Active => 1,
        PgState::Peering => 2,
        PgState::Degraded => 3,
        PgState::Backfilling => 4,
        PgState::Inconsistent => 5,
    }
}

fn pg_state_from_code(code: u8) -> Option<PgState> {
    match code {
        1 => Some(PgState::Active),
        2 => Some(PgState::Peering),
        3 => Some(PgState::Degraded),
        4 => Some(PgState::Backfilling),
        5 => Some(PgState::Inconsistent),
        _ => None,
    }
}

fn hex_encode_path(path: &Path) -> String {
    hex_encode(path.as_os_str().as_bytes())
}

fn decode_hex_path(path: &Path, value: &str) -> Result<PathBuf, StorageNodeServerError> {
    let bytes = hex_decode(path, value)?;
    let os = std::ffi::OsString::from_vec(bytes);
    Ok(PathBuf::from(os))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(path: &Path, value: &str) -> Result<Vec<u8>, StorageNodeServerError> {
    if !value.len().is_multiple_of(2) {
        return Err(runtime_config_invalid(path, "hex field has odd length"));
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let high =
            hex_value(pair[0]).ok_or_else(|| runtime_config_invalid(path, "invalid hex field"))?;
        let low =
            hex_value(pair[1]).ok_or_else(|| runtime_config_invalid(path, "invalid hex field"))?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageNodePgRoute {
    pub pg_id: u32,
    pub cluster_epoch: ClusterEpoch,
    pub state: PgState,
    pub primary_node_id: NodeId,
    pub metadata_transfer_destination_epoch: Option<ClusterEpoch>,
    pub metadata_read_route: Option<PgMetadataReadRoute>,
    pub acting_set: Vec<NodeId>,
}

impl From<&PgRouteSnapshot> for StorageNodePgRoute {
    fn from(route: &PgRouteSnapshot) -> Self {
        Self {
            pg_id: route.pg_id().get(),
            cluster_epoch: route.cluster_epoch(),
            state: route.state(),
            primary_node_id: route.primary_node_id(),
            metadata_transfer_destination_epoch: route
                .peering_metadata_transfer_destination_epoch(),
            metadata_read_route: route.metadata_read_route(),
            acting_set: route.acting_set().to_vec(),
        }
    }
}

fn storage_node_rpc_trace_context(node_id: NodeId, request_id: u64) -> observability::TraceContext {
    observability::TraceContext::from_ids(observability::TraceContextIds {
        trace_id: format!("storage-node-{}-rpc-{}", node_id.as_u32(), request_id),
        request_id: format!("storage-node-{}-rpc-{}", node_id.as_u32(), request_id),
    })
}

fn emit_storage_node_metadata_command_log_conflict(
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
    command_kind: Option<&'static str>,
) {
    maybe_print_storage_node_metadata_command_conflict_diagnostic(
        "log_conflict",
        node_id,
        pg_id,
        cluster_epoch,
        log_index,
        command_kind,
    );
    let _ = observability::emit_metadata_command_conflict(
        "storage",
        observability::MetadataCommandConflictSummary {
            node_id: Some(node_id),
            pg_id,
            cluster_epoch: cluster_epoch.get(),
            log_index: Some(log_index),
            kind: "log_conflict",
            command_kind,
        },
    );
}

fn metadata_command_log_conflict_kind_from_pg(
    pg: &PgStore,
    node_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
) -> Option<&'static str> {
    pg.metadata_command_log_entry_command_kind_name(cluster_epoch, log_index)
        .ok()
        .flatten()
        .or_else(|| {
            pg.pending_metadata_command_envelope(node_id, cluster_epoch)
                .ok()
                .flatten()
                .and_then(|command| {
                    (command.id().log_index().get() == log_index)
                        .then(|| command.payload().kind_name())
                })
        })
}

fn emit_storage_node_metadata_command_log_conflict_for_pg(
    pg: &PgStore,
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
    command_kind: Option<&'static str>,
) {
    emit_storage_node_metadata_command_log_conflict(
        node_id,
        pg_id,
        cluster_epoch,
        log_index,
        command_kind.or_else(|| {
            metadata_command_log_conflict_kind_from_pg(pg, node_id, cluster_epoch, log_index)
        }),
    );
}

fn emit_storage_node_metadata_command_pending_conflict(
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    candidate_log_index: u64,
    command_kind: Option<&'static str>,
) {
    maybe_print_storage_node_metadata_command_conflict_diagnostic(
        "pending_slot_conflict",
        node_id,
        pg_id,
        cluster_epoch,
        candidate_log_index,
        command_kind,
    );
    let _ = observability::emit_metadata_command_conflict(
        "storage",
        observability::MetadataCommandConflictSummary {
            node_id: Some(node_id),
            pg_id,
            cluster_epoch: cluster_epoch.get(),
            log_index: Some(candidate_log_index),
            kind: "pending_slot_conflict",
            command_kind,
        },
    );
}

fn maybe_print_storage_node_metadata_command_conflict_diagnostic(
    kind: &'static str,
    node_id: u32,
    pg_id: u32,
    cluster_epoch: ClusterEpoch,
    log_index: u64,
    command_kind: Option<&'static str>,
) {
    if std::env::var_os("ARGMIN_METADATA_COMMAND_CONFLICT_DIAGNOSTICS").is_none() {
        return;
    }
    eprintln!(
        "metadata command conflict source=storage-node kind={kind} node_id={node_id} pg_id={pg_id} cluster_epoch={} log_index={log_index} command_kind={}",
        cluster_epoch.get(),
        command_kind.unwrap_or("unknown")
    );
}

fn maybe_emit_storage_rpc_error(node_id: NodeId, kind: StorageRpcMessageKind, payload: &[u8]) {
    if !matches!(payload.first(), Some(1)) {
        return;
    }
    let Ok(Err(error)) = crate::storage_rpc::decode_storage_rpc_response_payload(payload) else {
        return;
    };
    let rpc_kind = format!("{kind:?}");
    let error_code = format!("{:?}", error.code.wire_code());
    let _ = observability::emit_storage_rpc_error(
        "storage",
        observability::StorageRpcErrorSummary {
            node_id: node_id.as_u32(),
            rpc_kind: &rpc_kind,
            error_code: &error_code,
            message: &error.message,
        },
    );
}

/// Opaque retained implementation diagnostic for invalid initialized PG state.
///
/// The storage crate can preserve the underlying cause for internal diagnosis
/// without exposing its database or filesystem error taxonomy to callers.
pub struct StorageNodeStateDiagnostic {
    _implementation_error: StorageNodeStateImplementationError,
}

impl StorageNodeStateDiagnostic {
    fn from_store(implementation_error: StoreError) -> Self {
        Self {
            _implementation_error: StorageNodeStateImplementationError::Store {
                _error: implementation_error,
            },
        }
    }

    fn from_io(implementation_error: io::Error) -> Self {
        Self {
            _implementation_error: StorageNodeStateImplementationError::Io {
                _error: implementation_error,
            },
        }
    }
}

enum StorageNodeStateImplementationError {
    Store { _error: StoreError },
    Io { _error: io::Error },
}

impl std::fmt::Debug for StorageNodeStateDiagnostic {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageNodeStateDiagnostic")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageNodeServerError {
    #[error("storage-node PG set must not be empty")]
    EmptyPgSet,
    #[error("duplicate storage-node PG id {pg_id}")]
    DuplicatePgId { pg_id: u32 },
    #[error("duplicate storage-node PG route {pg_id}")]
    DuplicatePgRoute { pg_id: u32 },
    #[error("storage-node PG {pg_id} is missing a route")]
    MissingPgRoute { pg_id: u32 },
    #[error("storage-node PG route {pg_id} is not configured for this node")]
    RoutePgNotConfigured { pg_id: u32 },
    #[error("storage-node PG route {pg_id} is inconsistent across static config")]
    InconsistentPgRoute { pg_id: u32 },
    #[error(
        "storage-node PG route {pg_id} has cluster epoch {route_epoch}, expected config epoch {config_epoch}"
    )]
    RouteEpochMismatch {
        pg_id: u32,
        route_epoch: ClusterEpoch,
        config_epoch: ClusterEpoch,
    },
    #[error("invalid pending metadata command recovery for PG {pg_id}: {reason}")]
    InvalidPendingMetadataCommandRecovery { pg_id: u32, reason: String },
    #[error("storage-node PG route {pg_id} primary node {primary_node_id} is not in acting set")]
    RoutePrimaryNotInActingSet { pg_id: u32, primary_node_id: u32 },
    #[error("storage node {node_id} is absent from runtime map for cluster epoch {cluster_epoch}")]
    RuntimeMapNodeNotFound {
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },
    #[error("storage-node runtime route-map lease could not bind to the process clock: {message}")]
    RouteMapLeaseBinding { message: String },
    #[error(
        "storage-node route map for cluster epoch {cluster_epoch} expired at {valid_until_ms}ms, now {now_ms}ms"
    )]
    RouteMapExpired {
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
    },
    #[error("storage-node runtime refresh changed node id from {current} to {candidate}")]
    RuntimeRefreshNodeChanged { current: u32, candidate: u32 },
    #[error(
        "storage-node runtime refresh changed data directory from {current:?} to {candidate:?}"
    )]
    RuntimeRefreshDataDirChanged {
        current: PathBuf,
        candidate: PathBuf,
    },
    #[error("storage-node runtime refresh changed EC shape from {current:?} to {candidate:?}")]
    RuntimeRefreshEcShapeChanged {
        current: EcShape,
        candidate: EcShape,
    },
    #[error("storage-node runtime refresh changed socket path from {current:?} to {candidate:?}")]
    RuntimeRefreshSocketPathChanged {
        current: PathBuf,
        candidate: PathBuf,
    },
    #[error(
        "configured storage-node socket path {configured:?} does not match control-plane endpoint {runtime_map:?} for node {node_id}"
    )]
    BootstrapSocketPathMismatch {
        configured: PathBuf,
        runtime_map: PathBuf,
        node_id: u32,
    },
    #[error(
        "storage-node runtime refresh changed opened PG set from {current:?} to {candidate:?}"
    )]
    RuntimeRefreshPgSetChanged {
        current: Vec<u32>,
        candidate: Vec<u32>,
    },
    #[error(
        "storage-node runtime refresh attempted epoch downgrade from {current} to {candidate}"
    )]
    RuntimeRefreshEpochDowngrade {
        current: ClusterEpoch,
        candidate: ClusterEpoch,
    },
    #[error("storage-node runtime refresh for epoch {candidate} has unbounded route-map validity")]
    RuntimeRefreshUnboundedRouteMapValidity { candidate: ClusterEpoch },
    #[error(
        "storage-node control-plane heartbeat lease duration {requested_ms}ms must be at least {minimum_ms}ms to cover the {skew_budget_ms}ms clock-skew fence and a positive operational renewal margin"
    )]
    ControlPlaneRefreshLoopLeaseTooShort {
        requested_ms: u64,
        minimum_ms: u64,
        skew_budget_ms: u64,
    },
    #[error("read storage-node control-plane runtime config {path:?}")]
    RuntimeConfigRead {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("write storage-node control-plane runtime config {path:?}")]
    RuntimeConfigWrite {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid storage-node control-plane runtime config {path:?}: {message}")]
    RuntimeConfigInvalid { path: PathBuf, message: String },
    #[error("storage-node control-plane runtime config {path:?} has unknown magic")]
    RuntimeConfigUnknownMagic { path: PathBuf },
    #[error(
        "storage-node control-plane runtime config {path:?} has unsupported version {actual}"
    )]
    RuntimeConfigUnsupportedVersion { path: PathBuf, actual: u16 },
    #[error(
        "storage-node process configuration does not match persisted control-plane runtime config {path:?}"
    )]
    PersistedRuntimeConfigMismatch { path: PathBuf },
    #[error("spawn storage-node control-plane refresh loop")]
    ControlPlaneRefreshLoopSpawn {
        #[source]
        source: io::Error,
    },
    #[error("metadata-transfer staging store failed to open: {message}")]
    MetadataTransferStaging { message: String },
    #[error("metadata-transfer staging outbox is not configured for this storage node")]
    MetadataTransferStagingNotConfigured,
    #[error("spawn metadata-transfer staging evidence outbox")]
    MetadataTransferStagingOutboxSpawn {
        #[source]
        source: io::Error,
    },
    #[error("duplicate storage-node id {id}")]
    DuplicateNodeId { id: u32 },
    #[error(
        "storage node {duplicate_node_id} shares data directory {data_dir:?} with storage node {first_node_id}"
    )]
    DuplicateDataDir {
        first_node_id: u32,
        duplicate_node_id: u32,
        data_dir: PathBuf,
    },
    #[error(
        "storage node {duplicate_node_id} shares Unix socket path {socket_path:?} with storage node {first_node_id}"
    )]
    DuplicateSocketPath {
        first_node_id: u32,
        duplicate_node_id: u32,
        socket_path: PathBuf,
    },
    #[error("storage-node socket path {path:?} has no parent directory")]
    SocketPathMissingParent { path: PathBuf },
    #[error("storage-node socket path {path:?} must be absolute")]
    SocketPathNotAbsolute { path: PathBuf },
    #[error("storage-node socket path {path:?} is not valid UTF-8")]
    SocketPathNotUtf8 { path: PathBuf },
    #[error("storage-node socket path {path:?} has no file name")]
    SocketPathMissingFileName { path: PathBuf },
    #[error("storage-node socket directory {path:?} must be private; mode is {mode:#o}")]
    SocketDirectoryNotPrivate { path: PathBuf, mode: u32 },
    #[error("storage-node socket path {path:?} already exists")]
    SocketPathExists { path: PathBuf },
    #[error("storage-node data directory {path:?} is already locked")]
    DataDirAlreadyLocked { path: PathBuf },
    #[error("storage-node data directory lock {path:?} is not a regular file")]
    DataDirLockNotRegularFile { path: PathBuf },
    #[error(
        "storage-node data directory lock for {locked_path:?} cannot be used with {config_path:?}"
    )]
    DataDirLockPathMismatch {
        locked_path: PathBuf,
        config_path: PathBuf,
    },
    #[error("storage-node data directory lock identity changed for {path:?}")]
    DataDirLockIdentityChanged { path: PathBuf },
    #[error("storage-node data directory inspection failed for {path:?}: {source}")]
    DataDirInspectionIo {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("storage-node PG {pg_id} initialized durable state is invalid")]
    InitializedPgStateInvalid {
        pg_id: u32,
        diagnostic: StorageNodeStateDiagnostic,
    },
    #[error("storage-node initialized durable state is invalid")]
    InitializedStorageNodeStateInvalid {
        diagnostic: StorageNodeStateDiagnostic,
    },
    #[error("storage-node PG {pg_id} has incomplete authoritative payload inventory")]
    InitializedPgPayloadIncomplete { pg_id: u32 },
    #[error("storage-node I/O error during {context} for {path:?}: {source}")]
    Io {
        context: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("storage-node RPC listener error during {context} for {endpoint}: {source}")]
    RpcListenerIo {
        context: &'static str,
        endpoint: String,
        #[source]
        source: io::Error,
    },
    #[error("storage-node RPC listener set must not be empty")]
    EmptyRpcListenerSet,
    #[error("storage-node TCP RPC listener {bind_addr} requires storage RPC authentication")]
    TcpRpcListenerRequiresAuthentication { bind_addr: SocketAddr },
    #[error("invalid storage-node incarnation {value:?} in {path:?}")]
    InvalidNodeIncarnation { path: PathBuf, value: String },
    #[error("storage-node incarnation counter overflowed in {path:?}")]
    NodeIncarnationOverflow { path: PathBuf },
    #[error("failed to open storage node: {0}")]
    Store(StoreFailure),
    #[error("storage RPC stream error: {message}")]
    RpcStream { message: String },
    #[error("storage RPC response payload error: {message}")]
    ResponsePayload { message: String },
    #[error("storage-node active session limit {limit} is exhausted")]
    TooManyActiveSessions { limit: usize },
    #[error("control-plane heartbeat failed: {0}")]
    ControlPlane(#[from] ControlPlaneError),
}

impl From<StoreError> for StorageNodeServerError {
    fn from(error: StoreError) -> Self {
        Self::Store(error.into())
    }
}

#[cfg(test)]
impl StorageNodeServerError {
    fn retained_store_error(&self) -> Option<&StoreError> {
        match self {
            Self::Store(failure) => failure.retained_store_error(),
            _ => None,
        }
    }
}

pub fn validate_storage_node_process_configs(
    configs: &[StorageNodeProcessConfig],
) -> Result<(), StorageNodeServerError> {
    let mut node_ids = BTreeMap::<u32, ()>::new();
    let mut data_dirs = BTreeMap::<PathBuf, NodeId>::new();
    let mut socket_paths = BTreeMap::<PathBuf, NodeId>::new();
    let mut pg_routes = BTreeMap::<u32, StorageNodePgRoute>::new();
    for config in configs {
        if node_ids.insert(config.node_id.as_u32(), ()).is_some() {
            return Err(StorageNodeServerError::DuplicateNodeId {
                id: config.node_id.as_u32(),
            });
        }
        validate_pg_ids(&config.pg_ids)?;
        validate_pg_routes(&config.pg_ids, &config.pg_routes)?;
        let data_dir = canonicalize_existing_or_parent(&config.data_dir, "data directory")?;
        if let Some(first_node_id) = data_dirs.insert(data_dir.clone(), config.node_id) {
            return Err(StorageNodeServerError::DuplicateDataDir {
                first_node_id: first_node_id.as_u32(),
                duplicate_node_id: config.node_id.as_u32(),
                data_dir,
            });
        }
        let socket_path = canonical_socket_path(&config.socket_path)?;
        if let Some(first_node_id) = socket_paths.insert(socket_path.clone(), config.node_id) {
            return Err(StorageNodeServerError::DuplicateSocketPath {
                first_node_id: first_node_id.as_u32(),
                duplicate_node_id: config.node_id.as_u32(),
                socket_path,
            });
        }
        for route in &config.pg_routes {
            match pg_routes.get(&route.pg_id) {
                Some(existing) if existing != route => {
                    return Err(StorageNodeServerError::InconsistentPgRoute { pg_id: route.pg_id })
                }
                Some(_) => {}
                None => {
                    pg_routes.insert(route.pg_id, route.clone());
                }
            }
        }
    }
    Ok(())
}

fn validate_runtime_config_install(
    current: &StorageNodeProcessConfig,
    candidate: &StorageNodeProcessConfig,
) -> Result<(), StorageNodeServerError> {
    candidate.validate_runtime_refresh_from(current)?;
    if candidate.pg_ids != current.pg_ids {
        return Err(StorageNodeServerError::RuntimeRefreshPgSetChanged {
            current: current.pg_ids.clone(),
            candidate: candidate.pg_ids.clone(),
        });
    }
    if candidate.cluster_epoch < current.cluster_epoch {
        return Err(StorageNodeServerError::RuntimeRefreshEpochDowngrade {
            current: current.cluster_epoch,
            candidate: candidate.cluster_epoch,
        });
    }
    if candidate.route_map_valid_until_ms().is_none() {
        return Err(
            StorageNodeServerError::RuntimeRefreshUnboundedRouteMapValidity {
                candidate: candidate.cluster_epoch,
            },
        );
    }
    Ok(())
}
