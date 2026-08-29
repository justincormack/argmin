// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

const METADATA_TRANSFER_STAGING_MAX_ENTRIES: usize = 256;
const METADATA_TRANSFER_STAGING_MAX_TOTAL_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Clone)]
struct StorageNodeRuntimeRouteState {
    config: Arc<StorageNodeProcessConfig>,
    route_map_lease: Option<BoundRouteMapLease>,
}
#[derive(Clone)]
pub struct StorageNodeRpcListenerConfig {
    inner: StorageNodeRpcListenerConfigInner,
}

#[derive(Clone)]
enum StorageNodeRpcListenerConfigInner {
    Unix {
        socket_path: PathBuf,
    },
    Tcp {
        bind_addr: SocketAddr,
        tls_server_config: Arc<rustls::ServerConfig>,
    },
}

impl StorageNodeRpcListenerConfig {
    #[must_use]
    pub fn unix(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            inner: StorageNodeRpcListenerConfigInner::Unix {
                socket_path: socket_path.into(),
            },
        }
    }

    pub fn tls_tcp(
        bind_addr: SocketAddr,
        certified_key: Arc<rustls::sign::CertifiedKey>,
    ) -> io::Result<Self> {
        Ok(Self::tls_tcp_with_config(
            bind_addr,
            storage_rpc_tls_server_config(certified_key)?,
        ))
    }

    fn tls_tcp_with_config(
        bind_addr: SocketAddr,
        tls_server_config: Arc<rustls::ServerConfig>,
    ) -> Self {
        Self {
            inner: StorageNodeRpcListenerConfigInner::Tcp {
                bind_addr,
                tls_server_config,
            },
        }
    }

    #[must_use]
    pub fn is_tls_tcp(&self) -> bool {
        matches!(&self.inner, StorageNodeRpcListenerConfigInner::Tcp { .. })
    }
}

impl std::fmt::Debug for StorageNodeRpcListenerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            StorageNodeRpcListenerConfigInner::Unix { socket_path } => formatter
                .debug_struct("StorageNodeRpcListenerConfig::Unix")
                .field("socket_path", socket_path)
                .finish(),
            StorageNodeRpcListenerConfigInner::Tcp { bind_addr, .. } => formatter
                .debug_struct("StorageNodeRpcListenerConfig::Tcp")
                .field("bind_addr", bind_addr)
                .field("tls", &true)
                .finish(),
        }
    }
}

enum StorageNodeRpcListener {
    Unix {
        listener: UnixListener,
        socket_path: PathBuf,
    },
    Tcp {
        listener: TcpListener,
        bind_addr: SocketAddr,
        tls_server_config: Arc<rustls::ServerConfig>,
    },
}

impl Drop for StorageNodeRpcListener {
    fn drop(&mut self) {
        if let Self::Unix { socket_path, .. } = self {
            let _ = fs::remove_file(socket_path);
        }
    }
}

enum AcceptedStorageNodeRpcStream {
    Unix(std::os::unix::net::UnixStream),
    Tcp {
        stream: TcpStream,
        tls_server_config: Arc<rustls::ServerConfig>,
    },
}

impl StorageNodeRpcListener {
    fn raw_fd(&self) -> std::os::fd::RawFd {
        match self {
            Self::Unix { listener, .. } => listener.as_raw_fd(),
            Self::Tcp { listener, .. } => listener.as_raw_fd(),
        }
    }

    fn endpoint(&self) -> String {
        match self {
            Self::Unix { socket_path, .. } => socket_path.to_string_lossy().into_owned(),
            Self::Tcp { bind_addr, .. } => bind_addr.to_string(),
        }
    }

    fn accept(&self) -> io::Result<AcceptedStorageNodeRpcStream> {
        match self {
            Self::Unix { listener, .. } => listener
                .accept()
                .map(|(stream, _)| AcceptedStorageNodeRpcStream::Unix(stream)),
            Self::Tcp {
                listener,
                tls_server_config,
                ..
            } => listener
                .accept()
                .map(|(stream, _)| AcceptedStorageNodeRpcStream::Tcp {
                    stream,
                    tls_server_config: Arc::clone(tls_server_config),
                }),
        }
    }
}

pub struct StorageNodeServer {
    runtime_route_state: Arc<RwLock<StorageNodeRuntimeRouteState>>,
    runtime_config_install_lock: Mutex<()>,
    route_admission: StorageNodeRouteAdmissionGate,
    _data_dir_lock: StorageNodeDataDirLock,
    control_plane_incarnation_lock: Mutex<()>,
    _node: Arc<SharedStorageNode>,
    metadata_transfer_staging_store: Option<Arc<MetadataTransferStagingStore>>,
    listeners: Vec<StorageNodeRpcListener>,
    read_handles: Arc<Mutex<StorageNodeReadHandleState>>,
    active_sessions: Arc<StorageNodeActiveSessions>,
    metadata_command_locks: StorageNodeMetadataCommandLocks,
    rpc_auth: Option<Arc<StorageRpcServerAuthConfig>>,
    #[cfg(test)]
    runtime_config_stage_test_hook: Mutex<Option<RuntimeConfigStageTestHook>>,
    #[cfg(test)]
    runtime_route_capture_test_hook: Arc<Mutex<Option<RuntimeConfigStageTestHook>>>,
    #[cfg(test)]
    runtime_route_before_publish_lock_test_hook: Mutex<Option<RuntimeConfigStageTestHook>>,
    #[cfg(test)]
    runtime_route_after_publish_lock_test_hook: Mutex<Option<RuntimeConfigStageTestHook>>,
    #[cfg(test)]
    response_envelope_test_hook: Arc<Mutex<Option<StorageRpcResponseEnvelopeTestHook>>>,
    #[cfg(test)]
    response_frame_test_hook: Arc<Mutex<Option<StorageRpcResponseFrameTestHook>>>,
    #[cfg(test)]
    metadata_command_before_commit_test_hook:
        Arc<Mutex<Option<MetadataCommandBeforeCommitTestHook>>>,
    #[cfg(test)]
    metadata_checkpoint_rows_captured_test_hook:
        Arc<Mutex<Option<MetadataCheckpointRowsCapturedTestHook>>>,
    #[cfg(test)]
    control_plane_heartbeat_scan_test_hook: Mutex<Option<RuntimeConfigStageTestHook>>,
}

#[cfg(test)]
type RuntimeConfigStageTestHook = Arc<dyn Fn() + Send + Sync + 'static>;

#[derive(Debug)]
pub(crate) struct StorageNodeDataDirGuard {
    data_dir: PathBuf,
    lock: StorageNodeDataDirLock,
}

impl StorageNodeDataDirGuard {
    pub(crate) fn acquire(data_dir: &Path) -> Result<Self, StorageNodeServerError> {
        let lock = StorageNodeDataDirLock::acquire(data_dir)?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            lock,
        })
    }

    fn into_lock_for(
        self,
        config_data_dir: &Path,
    ) -> Result<StorageNodeDataDirLock, StorageNodeServerError> {
        if self.data_dir != config_data_dir {
            return Err(StorageNodeServerError::DataDirLockPathMismatch {
                locked_path: self.data_dir,
                config_path: config_data_dir.to_path_buf(),
            });
        }
        Ok(self.lock)
    }
}

/// Opaque startup boundary for a control-plane-managed storage-node process.
///
/// This keeps raw node opening, metadata-command recovery, and startup
/// heartbeat inspection inside the storage-node implementation. Application
/// code can inspect startup heartbeats, then consumes the bootstrap to obtain
/// the one-shot [`PreparedStorageNodeServer`] needed to bind.
pub struct StorageNodeBootstrap {
    node: SharedStorageNode,
    node_id: NodeId,
    node_incarnation: u64,
    data_dir: PathBuf,
    default_ec_shape: EcShape,
    configured_socket_path: PathBuf,
    startup_runtime_config: Option<StorageNodeProcessConfig>,
    data_dir_guard: StorageNodeDataDirGuard,
}

/// Exclusive production storage-node ownership retained during initialization.
///
/// The caller must keep this guard until every outer identity and durability
/// marker covering the initialized PG state has been published.
#[derive(Debug)]
#[must_use = "storage-node initialization must remain locked through identity publication"]
pub struct StorageNodeStateInitializationGuard {
    data_dir_guard: StorageNodeDataDirGuard,
}

impl StorageNodeStateInitializationGuard {
    pub fn acquire(data_dir: &Path) -> Result<Self, StorageNodeServerError> {
        Ok(Self {
            data_dir_guard: StorageNodeDataDirGuard::acquire(data_dir)?,
        })
    }

    /// Snapshot data-directory entry names excluding the exact held lock.
    ///
    /// This does not classify any other entry by ownership or initialization
    /// phase. It lets an outer deployment initializer inspect its own markers
    /// without learning storage's private lock filename.
    pub fn data_dir_entry_names_excluding_held_lock(
        &self,
    ) -> Result<Vec<std::ffi::OsString>, StorageNodeServerError> {
        self.data_dir_guard
            .lock
            .entry_names_excluding_held_lock(&self.data_dir_guard.data_dir)
    }

    fn data_dir(&self) -> &Path {
        &self.data_dir_guard.data_dir
    }
}

/// Semantic inspection of a configured storage node's durable PG state.
///
/// Physical PG paths, metadata-store representation, and shard-tree layout are
/// deliberately owned by this module and are not exposed to process startup.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct StorageNodeStateInspection {
    incomplete_payload_pg_ids: Vec<u32>,
}

impl StorageNodeStateInspection {
    #[must_use]
    pub fn incomplete_payload_pg_ids(&self) -> &[u32] {
        &self.incomplete_payload_pg_ids
    }
}

/// Inspect every configured PG without creating or migrating durable state.
pub fn inspect_initialized_storage_node_state(
    data_dir: &Path,
    pg_ids: &[u32],
    expected_durable_identity: &[u8],
) -> Result<StorageNodeStateInspection, StorageNodeServerError> {
    validate_pg_ids(pg_ids)?;
    let mut incomplete_payload_pg_ids = Vec::new();
    for &pg_id in pg_ids {
        let pg_dir = storage_node_pg_dir(data_dir, pg_id);
        verify_pg_durable_identity(&pg_dir, pg_id, expected_durable_identity)
            .map_err(|source| invalid_initialized_pg_state(pg_id, source))?;
        let inventory = inspect_pg_shard_inventory(&pg_dir, pg_id)
            .map_err(|source| invalid_initialized_pg_state(pg_id, source))?;
        if !inventory.authoritative_inventory_is_complete() {
            incomplete_payload_pg_ids.push(pg_id);
        }
    }
    Ok(StorageNodeStateInspection {
        incomplete_payload_pg_ids,
    })
}

/// Initialize the durable PG state owned by one storage-node process.
///
/// This is the explicit first-initialization boundary used before publishing
/// an outer deployment identity. It creates every configured PG with the
/// production storage-node engine and runs the same metadata-command recovery
/// checks used by ordinary storage-node startup. `initialization_guard` must be
/// retained until the caller durably publishes that outer identity.
pub fn initialize_storage_node_state(
    initialization_guard: &StorageNodeStateInitializationGuard,
    node_id: NodeId,
    pg_ids: &[u32],
    default_ec_shape: EcShape,
    initial_cluster_epoch: ClusterEpoch,
    durable_identity: &[u8],
) -> Result<(), StorageNodeServerError> {
    let node = SharedStorageNode::open_with_default_ec_shape_and_epoch(
        initialization_guard.data_dir(),
        pg_ids,
        default_ec_shape,
        initial_cluster_epoch,
    )
    .map_err(invalid_initialized_storage_node_state)?;
    node.recover_pg_metadata_command_state(node_id)
        .map_err(invalid_initialized_storage_node_state)?;
    drop(node);

    for &pg_id in pg_ids {
        let pg_dir = storage_node_pg_dir(initialization_guard.data_dir(), pg_id);
        initialize_pg_durable_identity(&pg_dir, pg_id, durable_identity)
            .map_err(|source| invalid_initialized_pg_state(pg_id, source))?;
    }
    let inspection = inspect_initialized_storage_node_state(
        initialization_guard.data_dir(),
        pg_ids,
        durable_identity,
    )?;
    if let Some(&pg_id) = inspection.incomplete_payload_pg_ids().first() {
        return Err(StorageNodeServerError::InitializedPgPayloadIncomplete { pg_id });
    }
    for &pg_id in pg_ids {
        let pg_dir = storage_node_pg_dir(initialization_guard.data_dir(), pg_id);
        sync_initialized_pg_store_layout(&pg_dir, pg_id)
            .map_err(|source| invalid_initialized_pg_state(pg_id, source))?;
    }
    File::open(initialization_guard.data_dir())
        .and_then(|directory| directory.sync_all())
        .map_err(invalid_initialized_storage_node_state_io)
}

fn storage_node_pg_dir(data_dir: &Path, pg_id: u32) -> PathBuf {
    data_dir.join(format!("pg-{pg_id:04}"))
}

fn invalid_initialized_pg_state(pg_id: u32, source: StoreError) -> StorageNodeServerError {
    StorageNodeServerError::InitializedPgStateInvalid {
        pg_id,
        diagnostic: StorageNodeStateDiagnostic::from_store(source),
    }
}

fn invalid_initialized_storage_node_state(source: StoreError) -> StorageNodeServerError {
    StorageNodeServerError::InitializedStorageNodeStateInvalid {
        diagnostic: StorageNodeStateDiagnostic::from_store(source),
    }
}

fn invalid_initialized_storage_node_state_io(source: io::Error) -> StorageNodeServerError {
    StorageNodeServerError::InitializedStorageNodeStateInvalid {
        diagnostic: StorageNodeStateDiagnostic::from_io(source),
    }
}

impl StorageNodeBootstrap {
    pub fn open_control_plane_managed(
        node_id: NodeId,
        data_dir: impl Into<PathBuf>,
        pg_ids: &[u32],
        default_ec_shape: EcShape,
        configured_socket_path: impl Into<PathBuf>,
    ) -> Result<Self, StorageNodeServerError> {
        let data_dir = data_dir.into();
        let configured_socket_path = configured_socket_path.into();
        let data_dir_guard = StorageNodeDataDirGuard::acquire(&data_dir)?;
        let startup_runtime_config = StorageNodeProcessConfig::load_control_plane_runtime_config(
            &data_dir,
            node_id,
            default_ec_shape,
            &configured_socket_path,
        )?;
        let node_incarnation = advance_storage_node_incarnation(&data_dir)?;
        let node =
            SharedStorageNode::open_with_default_ec_shape(&data_dir, pg_ids, default_ec_shape)?;
        node.recover_pg_metadata_command_state(node_id)?;
        Ok(Self {
            node,
            node_id,
            node_incarnation,
            data_dir,
            default_ec_shape,
            configured_socket_path,
            startup_runtime_config,
            data_dir_guard,
        })
    }

    #[must_use]
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    pub fn control_plane_heartbeat(
        &self,
        requested_lease_duration_ms: u64,
    ) -> Result<NodeHeartbeat, StorageNodeServerError> {
        match self.startup_runtime_config.as_ref() {
            Some(runtime_config) => runtime_config.control_plane_heartbeat(
                &self.node,
                self.node_incarnation,
                requested_lease_duration_ms,
            ),
            None => {
                let endpoint = self.configured_socket_path.to_str().ok_or_else(|| {
                    StorageNodeServerError::SocketPathNotUtf8 {
                        path: self.configured_socket_path.clone(),
                    }
                })?;
                self.node
                    .control_plane_heartbeat(
                        self.node_id,
                        self.node_incarnation,
                        endpoint,
                        ClusterEpoch::INITIAL,
                        requested_lease_duration_ms,
                        std::iter::empty(),
                    )
                    .map_err(StorageNodeServerError::from)
            }
        }
    }

    pub fn prepare(
        self,
        runtime_map: &ClusterRuntimeMapSnapshot,
    ) -> Result<PreparedStorageNodeServer, StorageNodeServerError> {
        let config = match self.startup_runtime_config.as_ref() {
            Some(runtime_config) => {
                let history_reference_summary =
                    self.node.cluster_map_history_reference_summary()?;
                StorageNodeProcessConfig::from_runtime_map_refresh(
                    runtime_config,
                    runtime_map,
                    history_reference_summary,
                )
            }
            None => StorageNodeProcessConfig::from_runtime_map(
                self.node_id,
                self.data_dir.clone(),
                self.default_ec_shape,
                runtime_map,
            ),
        }?;
        if config.socket_path() != self.configured_socket_path {
            return Err(StorageNodeServerError::BootstrapSocketPathMismatch {
                configured: self.configured_socket_path.clone(),
                runtime_map: config.socket_path().to_path_buf(),
                node_id: self.node_id.as_u32(),
            });
        }
        config.persist_control_plane_runtime_config()?;
        Ok(PreparedStorageNodeServer::with_data_dir_guard(
            config,
            self.data_dir_guard,
        ))
    }
}

/// One-shot storage-node bind input.
///
/// A control-plane bootstrap returns this only after durably persisting the
/// exact contained configuration. Its private data-directory guard prevents a
/// second bootstrap from replacing that configuration before [`Self::bind`]
/// consumes both values together.
pub struct PreparedStorageNodeServer {
    config: StorageNodeProcessConfig,
    data_dir_guard: Option<StorageNodeDataDirGuard>,
    rpc_auth: Option<Arc<StorageRpcServerAuthConfig>>,
    rpc_listeners: Option<Vec<StorageNodeRpcListenerConfig>>,
    metadata_transfer_staging_node_incarnation: Option<u64>,
}

impl PreparedStorageNodeServer {
    #[must_use]
    pub fn new(config: StorageNodeProcessConfig) -> Self {
        Self {
            config,
            data_dir_guard: None,
            rpc_auth: None,
            rpc_listeners: None,
            metadata_transfer_staging_node_incarnation: None,
        }
    }

    fn with_data_dir_guard(
        config: StorageNodeProcessConfig,
        data_dir_guard: StorageNodeDataDirGuard,
    ) -> Self {
        Self {
            config,
            data_dir_guard: Some(data_dir_guard),
            rpc_auth: None,
            rpc_listeners: None,
            metadata_transfer_staging_node_incarnation: None,
        }
    }

    #[must_use]
    pub fn with_rpc_auth(mut self, rpc_auth: StorageRpcServerAuthConfig) -> Self {
        self.rpc_auth = Some(Arc::new(rpc_auth));
        self
    }

    #[must_use]
    pub fn with_rpc_listeners(mut self, listeners: Vec<StorageNodeRpcListenerConfig>) -> Self {
        self.rpc_listeners = Some(listeners);
        self
    }

    #[must_use]
    pub fn with_metadata_transfer_staging_node_incarnation(
        mut self,
        node_incarnation: u64,
    ) -> Self {
        self.metadata_transfer_staging_node_incarnation = Some(node_incarnation);
        self
    }

    #[must_use]
    pub fn config(&self) -> &StorageNodeProcessConfig {
        &self.config
    }

    pub fn bind(self) -> Result<StorageNodeServer, StorageNodeServerError> {
        match self.data_dir_guard {
            Some(data_dir_guard) => StorageNodeServer::bind_with_data_dir_guard(
                self.config,
                data_dir_guard,
                self.rpc_auth,
                self.rpc_listeners,
                self.metadata_transfer_staging_node_incarnation,
            ),
            None => StorageNodeServer::bind_with_rpc_auth(
                self.config,
                self.rpc_auth,
                self.rpc_listeners,
                self.metadata_transfer_staging_node_incarnation,
            ),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageNodeControlPlaneRefreshLoopStatus {
    pub scan_publication_attempts: u64,
    pub scan_publication_successes: u64,
    pub scan_publication_failures: u64,
    pub last_scan_publication_lease: Option<HeartbeatLease>,
    pub last_scan_publication_error: Option<String>,
    pub authority_renewal_attempts: u64,
    pub authority_renewal_successes: u64,
    pub authority_renewal_failures: u64,
    pub last_authority_renewal_lease: Option<HeartbeatLease>,
    pub last_authority_renewal_error: Option<String>,
}

pub struct StorageNodeControlPlaneRefreshLoop {
    stop: Arc<(Mutex<bool>, Condvar)>,
    status: Arc<Mutex<StorageNodeControlPlaneRefreshLoopStatus>>,
    handles: Vec<JoinHandle<()>>,
}

struct StorageNodeHeartbeatSubmissionState<S> {
    control_plane: S,
    latest_submitted: Option<NodeHeartbeat>,
    last_submission_started_at: Option<Instant>,
}

pub const STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_USABLE_LEASE_MS: u64 = 1_000;
pub const STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS: u64 =
    CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_USABLE_LEASE_MS;
pub const STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MAX_INTERVAL_MS: u64 = 1_000;

pub fn storage_node_control_plane_heartbeat_interval(
    node_id: NodeId,
    requested_lease_duration_ms: u64,
    completed_attempts: u64,
) -> Result<Duration, StorageNodeServerError> {
    if requested_lease_duration_ms < STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS {
        return Err(
            StorageNodeServerError::ControlPlaneRefreshLoopLeaseTooShort {
                requested_ms: requested_lease_duration_ms,
                minimum_ms: STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
                skew_budget_ms: CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            },
        );
    }

    let usable_lease_duration_ms = requested_lease_duration_ms
        .checked_sub(CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS)
        .expect("validated heartbeat lease exceeds the clock-skew budget");
    let maximum_interval_ms =
        (usable_lease_duration_ms / 3).min(STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MAX_INTERVAL_MS);
    let jitter_seed = u64::from(node_id.as_u32())
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(completed_attempts.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    let jitter_percent = 75 + jitter_seed % 26;
    let interval_ms = maximum_interval_ms
        .saturating_mul(jitter_percent)
        .checked_div(100)
        .unwrap_or_default()
        .max(1);
    Ok(Duration::from_millis(interval_ms))
}

fn wait_for_storage_node_control_plane_worker(
    stop: &Arc<(Mutex<bool>, Condvar)>,
    interval: Duration,
) -> bool {
    let (lock, cvar) = &**stop;
    let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if *stopped {
        return true;
    }
    let (stopped, _) = cvar
        .wait_timeout(stopped, interval)
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *stopped
}

fn record_storage_node_control_plane_scan_publication_result(
    status: &Mutex<StorageNodeControlPlaneRefreshLoopStatus>,
    node_id: NodeId,
    result: &Result<HeartbeatLease, StorageNodeServerError>,
) {
    let mut status = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.scan_publication_attempts += 1;
    match result {
        Ok(lease) => {
            status.scan_publication_successes += 1;
            status.last_scan_publication_lease = Some(lease.clone());
            status.last_scan_publication_error = None;
        }
        Err(error) => {
            let error = error.to_string();
            if status.last_scan_publication_error.as_deref() != Some(error.as_str()) {
                eprintln!(
                    "storage-node {} control-plane heartbeat scan/publication failed: {error}",
                    node_id.as_u32()
                );
            }
            status.scan_publication_failures += 1;
            status.last_scan_publication_error = Some(error);
        }
    }
}

fn record_storage_node_control_plane_authority_renewal_result(
    status: &Mutex<StorageNodeControlPlaneRefreshLoopStatus>,
    node_id: NodeId,
    result: &Result<HeartbeatLease, StorageNodeServerError>,
) {
    let mut status = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    status.authority_renewal_attempts += 1;
    match result {
        Ok(lease) => {
            status.authority_renewal_successes += 1;
            status.last_authority_renewal_lease = Some(lease.clone());
            status.last_authority_renewal_error = None;
        }
        Err(error) => {
            let error = error.to_string();
            if status.last_authority_renewal_error.as_deref() != Some(error.as_str()) {
                eprintln!(
                    "storage-node {} control-plane authority renewal failed: {error}",
                    node_id.as_u32()
                );
            }
            status.authority_renewal_failures += 1;
            status.last_authority_renewal_error = Some(error);
        }
    }
}

impl StorageNodeControlPlaneRefreshLoop {
    pub fn status(&self) -> StorageNodeControlPlaneRefreshLoopStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn stop(&mut self) {
        {
            let (lock, cvar) = &*self.stop;
            let mut stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *stopped = true;
            cvar.notify_all();
        }
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for StorageNodeControlPlaneRefreshLoop {
    fn drop(&mut self) {
        self.stop();
    }
}

fn encode_metadata_command_checkpoint_success_response(
    operation: &'static str,
    payload: &[u8],
    max_payload_len: usize,
) -> Result<Vec<u8>, crate::storage_rpc::StorageRpcPayloadError> {
    let response = encode_storage_rpc_success_response(payload);
    if response.len() <= max_payload_len {
        return Ok(response);
    }
    encode_storage_rpc_error_response(&StorageRpcErrorResponse {
        code: StorageRpcErrorCode::ResourceExhausted,
        message: format!(
            "{operation} response is too large: {} bytes exceeds storage RPC payload limit {} bytes",
            response.len(),
            max_payload_len
        ),
    })
}

fn metadata_command_checkpoint_candidates_for_frame(
    rows: Vec<MetadataCommandCheckpointCandidateRow>,
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    limit: usize,
    max_payload_len: usize,
) -> Vec<MetadataCommandCheckpoint> {
    if limit == 0 {
        return Vec::new();
    }
    let mut checkpoints = Vec::new();
    let candidate_limit = rows.len();
    let candidates = decode_metadata_command_checkpoint_candidate_rows(
        rows,
        cluster_epoch,
        pg_id,
        candidate_limit,
    );
    for candidate in candidates {
        let mut next_checkpoints = checkpoints.clone();
        next_checkpoints.push(candidate.clone());
        let payload = match encode_metadata_command_checkpoint_candidates_response(
            &StorageRpcMetadataCommandCheckpointCandidatesResponse {
                checkpoints: next_checkpoints,
            },
        ) {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        if encode_storage_rpc_success_response(&payload).len() <= max_payload_len {
            checkpoints.push(candidate);
            if checkpoints.len() == limit {
                return checkpoints;
            }
        }
    }
    checkpoints
}

fn bind_storage_node_rpc_listeners(
    configs: Vec<StorageNodeRpcListenerConfig>,
    rpc_auth: Option<&StorageRpcServerAuthConfig>,
) -> Result<Vec<StorageNodeRpcListener>, StorageNodeServerError> {
    if configs.is_empty() {
        return Err(StorageNodeServerError::EmptyRpcListenerSet);
    }
    let mut listeners = Vec::with_capacity(configs.len());
    for config in configs {
        match config.inner {
            StorageNodeRpcListenerConfigInner::Unix { socket_path } => {
                validate_socket_directory(&socket_path)?;
                cleanup_stale_socket_path(&socket_path)?;
                let listener = UnixListener::bind(&socket_path).map_err(|source| {
                    StorageNodeServerError::RpcListenerIo {
                        context: "bind Unix storage-node RPC listener",
                        endpoint: socket_path.to_string_lossy().into_owned(),
                        source,
                    }
                })?;
                listeners.push(StorageNodeRpcListener::Unix {
                    listener,
                    socket_path,
                });
            }
            StorageNodeRpcListenerConfigInner::Tcp {
                bind_addr,
                tls_server_config,
            } => {
                if rpc_auth.is_none() {
                    return Err(
                        StorageNodeServerError::TcpRpcListenerRequiresAuthentication { bind_addr },
                    );
                }
                if let Err(source) = validate_storage_rpc_tls_server_config(&tls_server_config) {
                    return Err(StorageNodeServerError::RpcListenerIo {
                        context: "validate TLS storage-node RPC listener",
                        endpoint: bind_addr.to_string(),
                        source,
                    });
                }
                let listener = TcpListener::bind(bind_addr).map_err(|source| {
                    StorageNodeServerError::RpcListenerIo {
                        context: "bind TCP storage-node RPC listener",
                        endpoint: bind_addr.to_string(),
                        source,
                    }
                })?;
                let bind_addr = listener.local_addr().map_err(|source| {
                    StorageNodeServerError::RpcListenerIo {
                        context: "inspect TCP storage-node RPC listener",
                        endpoint: bind_addr.to_string(),
                        source,
                    }
                })?;
                listeners.push(StorageNodeRpcListener::Tcp {
                    listener,
                    bind_addr,
                    tls_server_config,
                });
            }
        }
    }
    Ok(listeners)
}

impl StorageNodeServer {
    pub fn bind(config: StorageNodeProcessConfig) -> Result<Self, StorageNodeServerError> {
        Self::bind_with_rpc_auth(config, None, None, None)
    }

    #[cfg(test)]
    pub(crate) fn test_storage_node(&self) -> Arc<SharedStorageNode> {
        Arc::clone(&self._node)
    }

    fn bind_with_rpc_auth(
        config: StorageNodeProcessConfig,
        rpc_auth: Option<Arc<StorageRpcServerAuthConfig>>,
        rpc_listeners: Option<Vec<StorageNodeRpcListenerConfig>>,
        metadata_transfer_staging_node_incarnation: Option<u64>,
    ) -> Result<Self, StorageNodeServerError> {
        let data_dir_guard = StorageNodeDataDirGuard::acquire(&config.data_dir)?;
        Self::bind_with_data_dir_guard(
            config,
            data_dir_guard,
            rpc_auth,
            rpc_listeners,
            metadata_transfer_staging_node_incarnation,
        )
    }

    fn bind_with_data_dir_guard(
        config: StorageNodeProcessConfig,
        data_dir_guard: StorageNodeDataDirGuard,
        rpc_auth: Option<Arc<StorageRpcServerAuthConfig>>,
        rpc_listeners: Option<Vec<StorageNodeRpcListenerConfig>>,
        metadata_transfer_staging_node_incarnation: Option<u64>,
    ) -> Result<Self, StorageNodeServerError> {
        validate_process_config_route_table(&config)?;
        let data_dir_lock = data_dir_guard.into_lock_for(&config.data_dir)?;
        validate_process_config_matches_persisted_runtime_config(&config)?;
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )?;
        node.recover_pg_metadata_command_state(config.node_id)?;
        let metadata_transfer_staging_store = metadata_transfer_staging_node_incarnation
            .map(|node_incarnation| {
                let endpoint = config.socket_path.to_str().ok_or_else(|| {
                    StorageNodeServerError::SocketPathNotUtf8 {
                        path: config.socket_path.clone(),
                    }
                })?;
                let identity = MetadataTransferStagingNodeIdentity::new(
                    config.node_id,
                    node_incarnation,
                    endpoint.to_owned(),
                )
                .map_err(|error| StorageNodeServerError::MetadataTransferStaging {
                    message: error.to_string(),
                })?;
                let limits = MetadataTransferStagingLimits::new(
                    METADATA_TRANSFER_STAGING_MAX_ENTRIES,
                    METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES,
                    METADATA_TRANSFER_STAGING_MAX_TOTAL_BYTES,
                )
                .map_err(|error| StorageNodeServerError::MetadataTransferStaging {
                    message: error.to_string(),
                })?;
                MetadataTransferStagingStore::open(&config.data_dir, identity, limits)
                    .map(Arc::new)
                    .map_err(|error| StorageNodeServerError::MetadataTransferStaging {
                        message: error.to_string(),
                    })
            })
            .transpose()?;
        let rpc_listeners = rpc_listeners
            .unwrap_or_else(|| vec![StorageNodeRpcListenerConfig::unix(&config.socket_path)]);
        let listeners = bind_storage_node_rpc_listeners(rpc_listeners, rpc_auth.as_deref())?;
        let route_map_lease = bind_storage_node_route_map_lease(config.route_map_validity)?;
        Ok(Self {
            runtime_route_state: Arc::new(RwLock::new(StorageNodeRuntimeRouteState {
                config: Arc::new(config),
                route_map_lease,
            })),
            runtime_config_install_lock: Mutex::new(()),
            route_admission: StorageNodeRouteAdmissionGate::default(),
            _data_dir_lock: data_dir_lock,
            control_plane_incarnation_lock: Mutex::new(()),
            _node: Arc::new(node),
            metadata_transfer_staging_store,
            listeners,
            read_handles: Arc::new(Mutex::new(StorageNodeReadHandleState::default())),
            active_sessions: Arc::new(StorageNodeActiveSessions::default()),
            metadata_command_locks: StorageNodeMetadataCommandLocks::default(),
            rpc_auth,
            #[cfg(test)]
            runtime_config_stage_test_hook: Mutex::new(None),
            #[cfg(test)]
            runtime_route_capture_test_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            runtime_route_before_publish_lock_test_hook: Mutex::new(None),
            #[cfg(test)]
            runtime_route_after_publish_lock_test_hook: Mutex::new(None),
            #[cfg(test)]
            response_envelope_test_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            response_frame_test_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            metadata_command_before_commit_test_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            metadata_checkpoint_rows_captured_test_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            control_plane_heartbeat_scan_test_hook: Mutex::new(None),
        })
    }

    pub fn advance_control_plane_node_incarnation(&self) -> Result<u64, StorageNodeServerError> {
        let _guard = self
            .control_plane_incarnation_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        advance_storage_node_incarnation(&self.config_snapshot().data_dir)
    }

    pub fn spawn_metadata_transfer_staging_outbox<F, E>(
        &self,
        control_plane: ControlPlaneStorageNodeClient,
        authority_now_ms: F,
        on_fatal: E,
    ) -> Result<StorageNodeMetadataTransferStagingOutbox, StorageNodeServerError>
    where
        F: Fn() -> u64 + Send + 'static,
        E: Fn(String) + Send + 'static,
    {
        let store = self
            .metadata_transfer_staging_store
            .as_ref()
            .ok_or(StorageNodeServerError::MetadataTransferStagingNotConfigured)?;
        StorageNodeMetadataTransferStagingOutbox::spawn(
            Arc::clone(store),
            control_plane,
            authority_now_ms,
            on_fatal,
        )
        .map_err(|source| StorageNodeServerError::MetadataTransferStagingOutboxSpawn { source })
    }

    pub fn accept_one(&self) -> Result<(), StorageNodeServerError> {
        let session_guard = self.acquire_session();
        let (accepted, endpoint) = self.accept_rpc_stream()?;
        let mut stream = Self::prepare_accepted_rpc_stream(
            accepted,
            &endpoint,
            storage_node_rpc_io_timeout(self.rpc_auth.as_deref()),
        )?;
        let mut handler = self.connection_handler();
        handler.handle_session(&mut stream, session_guard)
    }

    #[cfg(test)]
    pub(crate) fn socket_path_for_test(&self) -> PathBuf {
        self.config_snapshot().socket_path
    }

    #[cfg(test)]
    pub(crate) fn tcp_listener_addr_for_test(&self) -> SocketAddr {
        self.listeners
            .iter()
            .find_map(|listener| match listener {
                StorageNodeRpcListener::Tcp { bind_addr, .. } => Some(*bind_addr),
                StorageNodeRpcListener::Unix { .. } => None,
            })
            .expect("test server has a TCP listener")
    }

    #[cfg(test)]
    pub(crate) fn serve_until_stop_for_test(
        &self,
        stop: &std::sync::atomic::AtomicBool,
    ) -> Result<(), StorageNodeServerError> {
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            if let Err(err) = self.accept_and_spawn() {
                if stop.load(std::sync::atomic::Ordering::Acquire) {
                    return Ok(());
                }
                return Err(err);
            }
        }
        Ok(())
    }

    pub fn serve_forever(&self) -> Result<(), StorageNodeServerError> {
        loop {
            self.accept_and_spawn()?;
        }
    }

    pub fn control_plane_heartbeat(
        &self,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
    ) -> Result<NodeHeartbeat, StorageNodeServerError> {
        self.config_snapshot().control_plane_heartbeat(
            &self._node,
            node_incarnation,
            requested_lease_duration_ms,
        )
    }

    pub fn heartbeat_control_plane(
        &self,
        control_plane: &mut impl ControlPlaneHeartbeatSink,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, StorageNodeServerError> {
        let heartbeat =
            self.control_plane_heartbeat(node_incarnation, requested_lease_duration_ms)?;
        control_plane
            .submit_node_heartbeat(heartbeat, authority_now_ms)
            .map_err(StorageNodeServerError::from)
    }

    pub fn refresh_control_plane_runtime_map(
        &self,
        control_plane: &mut impl ControlPlaneHeartbeatRuntimeMapSource,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        authority_now_ms: u64,
    ) -> Result<StorageNodeControlPlaneRefresh, StorageNodeServerError> {
        let heartbeat =
            self.control_plane_heartbeat(node_incarnation, requested_lease_duration_ms)?;
        self.refresh_control_plane_runtime_map_with_heartbeat(
            control_plane,
            heartbeat,
            authority_now_ms,
        )
    }

    fn refresh_control_plane_runtime_map_with_heartbeat(
        &self,
        control_plane: &mut impl ControlPlaneHeartbeatRuntimeMapSource,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<StorageNodeControlPlaneRefresh, StorageNodeServerError> {
        let history_reference_summary = heartbeat.cluster_map_history_route_references.summary();
        let refresh = control_plane
            .refresh_node_heartbeat(heartbeat, authority_now_ms)
            .map_err(StorageNodeServerError::from)?;
        let (lease, runtime_map) = refresh.into_parts();
        let current_config = self.config_snapshot();
        let next_config = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current_config,
            &runtime_map,
            history_reference_summary,
        )?;
        next_config.validate_runtime_refresh_from(&current_config)?;
        Ok(StorageNodeControlPlaneRefresh {
            lease,
            runtime_map,
            next_config,
        })
    }

    fn submit_serialized_control_plane_heartbeat<S, F>(
        &self,
        submission: &Mutex<StorageNodeHeartbeatSubmissionState<S>>,
        authority_now_ms: &Mutex<F>,
        heartbeat: NodeHeartbeat,
    ) -> Result<HeartbeatLease, StorageNodeServerError>
    where
        S: ControlPlaneHeartbeatRuntimeMapSource,
        F: Fn() -> u64,
    {
        let history_reference_summary = heartbeat.cluster_map_history_route_references.summary();
        let (lease, runtime_map) = {
            let mut submission = submission
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Publish sender order before dispatch. If the response is lost
            // after the authority accepts this report, no concurrent renewal
            // may submit the older report and regress its PG observations.
            submission.latest_submitted = Some(heartbeat.clone());
            submission.last_submission_started_at = Some(Instant::now());
            let authority_now_ms = {
                let authority_now_ms = authority_now_ms
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                authority_now_ms()
            };
            submission
                .control_plane
                .refresh_node_heartbeat(heartbeat, authority_now_ms)
                .map_err(StorageNodeServerError::from)?
                .into_parts()
        };
        // Every local topology operation may wait behind route publication.
        // Keep it outside heartbeat submission so the renewal worker can
        // preserve the authority lease from the accepted complete report.
        let current_config = self.config_snapshot();
        let next_config = StorageNodeProcessConfig::from_runtime_map_refresh(
            &current_config,
            &runtime_map,
            history_reference_summary,
        )?;
        next_config.validate_runtime_refresh_from(&current_config)?;
        let refresh = StorageNodeControlPlaneRefresh {
            lease,
            runtime_map,
            next_config,
        };
        self.install_control_plane_refresh(refresh)
    }

    fn renew_latest_serialized_control_plane_heartbeat<S, F>(
        submission: &Mutex<StorageNodeHeartbeatSubmissionState<S>>,
        authority_now_ms: &Mutex<F>,
        minimum_interval: Duration,
    ) -> Option<Result<HeartbeatLease, StorageNodeServerError>>
    where
        S: ControlPlaneHeartbeatRuntimeMapSource,
        F: Fn() -> u64,
    {
        let result = {
            let mut submission = submission
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if submission
                .last_submission_started_at
                .is_some_and(|started| started.elapsed() < minimum_interval)
            {
                return None;
            }
            let heartbeat = submission.latest_submitted.clone()?;
            submission.last_submission_started_at = Some(Instant::now());
            let authority_now_ms = {
                let authority_now_ms = authority_now_ms
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                authority_now_ms()
            };
            submission
                .control_plane
                .refresh_node_heartbeat(heartbeat, authority_now_ms)
                .map(|refresh| refresh.into_parts().0)
                .map_err(StorageNodeServerError::from)
        };
        // The complete-scan worker is the sole runtime-map publisher. A
        // cached report carries no new PG evidence and renews only the
        // authority's node lease, so it cannot be delayed by route draining.
        Some(result)
    }

    pub fn refresh_and_install_control_plane_runtime_map(
        &self,
        control_plane: &mut impl ControlPlaneHeartbeatRuntimeMapSource,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, StorageNodeServerError> {
        let refresh = self.refresh_control_plane_runtime_map(
            control_plane,
            node_incarnation,
            requested_lease_duration_ms,
            authority_now_ms,
        )?;
        self.install_control_plane_refresh(refresh)
    }

    pub fn spawn_control_plane_refresh_loop<S, F>(
        self: Arc<Self>,
        control_plane: S,
        node_incarnation: u64,
        requested_lease_duration_ms: u64,
        authority_now_ms: F,
    ) -> Result<StorageNodeControlPlaneRefreshLoop, StorageNodeServerError>
    where
        S: ControlPlaneHeartbeatRuntimeMapSource + Send + 'static,
        F: Fn() -> u64 + Send + 'static,
    {
        let node_id = self.config_snapshot().node_id;
        storage_node_control_plane_heartbeat_interval(node_id, requested_lease_duration_ms, 0)?;

        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let status = Arc::new(Mutex::new(
            StorageNodeControlPlaneRefreshLoopStatus::default(),
        ));
        let submission = Arc::new(Mutex::new(StorageNodeHeartbeatSubmissionState {
            control_plane,
            latest_submitted: None,
            last_submission_started_at: None,
        }));
        let authority_now_ms = Arc::new(Mutex::new(authority_now_ms));

        let renewal_stop = Arc::clone(&stop);
        let renewal_status = Arc::clone(&status);
        let renewal_submission = Arc::clone(&submission);
        let renewal_authority_now_ms = Arc::clone(&authority_now_ms);
        let renewal_handle = thread::Builder::new()
            .name(format!(
                "argmin-storage-node-{}-control-plane-lease-renewal",
                node_id.as_u32()
            ))
            .spawn(move || {
                let mut completed_attempts = 0_u64;
                loop {
                    let refresh_interval = storage_node_control_plane_heartbeat_interval(
                        node_id,
                        requested_lease_duration_ms,
                        completed_attempts,
                    )
                    .expect("validated heartbeat schedule remains valid");
                    if wait_for_storage_node_control_plane_worker(&renewal_stop, refresh_interval) {
                        break;
                    }
                    if let Some(result) =
                        Self::renew_latest_serialized_control_plane_heartbeat(
                            &renewal_submission,
                            &renewal_authority_now_ms,
                            refresh_interval,
                        )
                    {
                        record_storage_node_control_plane_authority_renewal_result(
                            &renewal_status,
                            node_id,
                            &result,
                        );
                        completed_attempts = completed_attempts.saturating_add(1);
                    }
                }
            })
            .map_err(|source| StorageNodeServerError::ControlPlaneRefreshLoopSpawn { source })?;

        let scan_stop = Arc::clone(&stop);
        let scan_status = Arc::clone(&status);
        let scan_submission = Arc::clone(&submission);
        let scan_authority_now_ms = Arc::clone(&authority_now_ms);
        let scan_server = self;
        let scan_handle = thread::Builder::new()
            .name(format!(
                "argmin-storage-node-{}-control-plane-heartbeat-scan",
                node_id.as_u32()
            ))
            .spawn(move || {
                let mut completed_attempts = 0_u64;
                loop {
                    #[cfg(test)]
                    scan_server.run_control_plane_heartbeat_scan_test_hook();
                    let result = scan_server
                        .control_plane_heartbeat(
                            node_incarnation,
                            requested_lease_duration_ms,
                        )
                        .and_then(|heartbeat| {
                            scan_server.submit_serialized_control_plane_heartbeat(
                                &scan_submission,
                                &scan_authority_now_ms,
                                heartbeat,
                            )
                        });
                    record_storage_node_control_plane_scan_publication_result(
                        &scan_status,
                        node_id,
                        &result,
                    );
                    completed_attempts = completed_attempts.saturating_add(1);
                    let refresh_interval = storage_node_control_plane_heartbeat_interval(
                        node_id,
                        requested_lease_duration_ms,
                        completed_attempts,
                    )
                    .expect("validated heartbeat schedule remains valid");
                    if wait_for_storage_node_control_plane_worker(&scan_stop, refresh_interval) {
                        break;
                    }
                }
            });
        let scan_handle = match scan_handle {
            Ok(handle) => handle,
            Err(source) => {
                {
                    let (lock, cvar) = &*stop;
                    let mut stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    *stopped = true;
                    cvar.notify_all();
                }
                let _ = renewal_handle.join();
                return Err(StorageNodeServerError::ControlPlaneRefreshLoopSpawn { source });
            }
        };

        Ok(StorageNodeControlPlaneRefreshLoop {
            stop,
            status,
            handles: vec![renewal_handle, scan_handle],
        })
    }

    pub fn install_control_plane_refresh(
        &self,
        refresh: StorageNodeControlPlaneRefresh,
    ) -> Result<HeartbeatLease, StorageNodeServerError> {
        let (lease, _runtime_map, next_config) = refresh.into_parts();
        self.install_control_plane_runtime_config(next_config)?;
        Ok(lease)
    }

    pub fn install_control_plane_runtime_config(
        &self,
        next_config: StorageNodeProcessConfig,
    ) -> Result<(), StorageNodeServerError> {
        let _install_guard = self
            .runtime_config_install_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        validate_process_config_route_table(&next_config)?;
        let next_route_map_lease =
            bind_storage_node_route_map_lease(next_config.route_map_validity)?;
        let current_config = self.config_snapshot_arc();
        validate_runtime_config_install(&current_config, &next_config)?;

        // Lease renewal is volatile serving authority. Keep the last complete
        // topology as the restart checkpoint; bootstrap refreshes it before bind.
        if next_config.only_extends_route_map_validity_from(&current_config) {
            #[cfg(test)]
            if let Some(hook) = self
                .runtime_route_before_publish_lock_test_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                hook();
            }
            let mut current_state = self
                .runtime_route_state
                .write()
                .unwrap_or_else(|e| e.into_inner());
            #[cfg(test)]
            if let Some(hook) = self
                .runtime_route_after_publish_lock_test_hook
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                hook();
            }
            validate_runtime_config_install(&current_state.config, &next_config)?;
            if next_config.only_extends_route_map_validity_from(&current_state.config) {
                *current_state = StorageNodeRuntimeRouteState {
                    config: Arc::new(next_config),
                    route_map_lease: next_route_map_lease,
                };
                return Ok(());
            }
        }

        #[cfg(test)]
        let stage_test_hook = self
            .runtime_config_stage_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        #[cfg(test)]
        let staged_config =
            next_config.stage_control_plane_runtime_config_with_post_write(|| {
                if let Some(hook) = stage_test_hook {
                    hook();
                }
            })?;
        #[cfg(not(test))]
        let staged_config = next_config.stage_control_plane_runtime_config()?;

        let _transition = self.route_admission.begin_transition();
        #[cfg(test)]
        if let Some(hook) = self
            .runtime_route_before_publish_lock_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            hook();
        }
        let mut current_state = self
            .runtime_route_state
            .write()
            .unwrap_or_else(|e| e.into_inner());
        #[cfg(test)]
        if let Some(hook) = self
            .runtime_route_after_publish_lock_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            hook();
        }
        validate_runtime_config_install(&current_state.config, &next_config)?;
        staged_config.publish()?;
        *current_state = StorageNodeRuntimeRouteState {
            config: Arc::new(next_config),
            route_map_lease: next_route_map_lease,
        };
        Ok(())
    }

    fn accept_and_spawn(&self) -> Result<(), StorageNodeServerError> {
        let session_guard = self.acquire_session();
        let (accepted, endpoint) = self.accept_rpc_stream()?;
        let mut handler = self.connection_handler();
        let io_timeout = storage_node_rpc_io_timeout(self.rpc_auth.as_deref());
        thread::spawn(move || {
            let mut stream =
                match Self::prepare_accepted_rpc_stream(accepted, &endpoint, io_timeout) {
                    Ok(stream) => stream,
                    Err(error) => {
                        report_storage_node_connection_failure(&error);
                        return;
                    }
                };
            if let Err(error) = handler.handle_session(&mut stream, session_guard) {
                report_storage_node_connection_failure(&error);
            }
        });
        Ok(())
    }

    fn accept_rpc_stream(
        &self,
    ) -> Result<(AcceptedStorageNodeRpcStream, String), StorageNodeServerError> {
        let listener_index = if self.listeners.len() == 1 {
            0
        } else {
            let mut poll_fds = self
                .listeners
                .iter()
                .map(|listener| libc::pollfd {
                    fd: listener.raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                })
                .collect::<Vec<_>>();
            loop {
                // SAFETY: `poll_fds` owns a valid contiguous array for the duration of the call.
                let result = unsafe {
                    libc::poll(
                        poll_fds.as_mut_ptr(),
                        poll_fds
                            .len()
                            .try_into()
                            .expect("listener count fits nfds_t"),
                        -1,
                    )
                };
                if result > 0 {
                    break poll_fds
                        .iter()
                        .position(|fd| fd.revents & libc::POLLIN != 0)
                        .ok_or_else(|| StorageNodeServerError::RpcListenerIo {
                            context: "poll storage-node RPC listeners",
                            endpoint: "listener set".to_string(),
                            source: io::Error::other(
                                "storage-node RPC listener poll returned without a readable listener",
                            ),
                        })?;
                }
                let source = io::Error::last_os_error();
                if source.kind() != io::ErrorKind::Interrupted {
                    return Err(StorageNodeServerError::RpcListenerIo {
                        context: "poll storage-node RPC listeners",
                        endpoint: "listener set".to_string(),
                        source,
                    });
                }
            }
        };
        let listener = &self.listeners[listener_index];
        let endpoint = listener.endpoint();
        let stream = listener
            .accept()
            .map_err(|source| StorageNodeServerError::RpcListenerIo {
                context: "accept storage-node RPC connection",
                endpoint: endpoint.clone(),
                source,
            })?;
        Ok((stream, endpoint))
    }

    fn prepare_accepted_rpc_stream(
        stream: AcceptedStorageNodeRpcStream,
        endpoint: &str,
        io_timeout: Duration,
    ) -> Result<BoxStorageRpcStream, StorageNodeServerError> {
        let deadline = Instant::now().checked_add(io_timeout).ok_or_else(|| {
            StorageNodeServerError::RpcListenerIo {
                context: "set storage-node RPC connection deadline",
                endpoint: endpoint.to_string(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "storage-node RPC connection deadline overflowed",
                ),
            }
        })?;
        match stream {
            AcceptedStorageNodeRpcStream::Unix(stream) => accepted_unix_stream(stream, deadline),
            AcceptedStorageNodeRpcStream::Tcp {
                stream,
                tls_server_config,
            } => accepted_tls_tcp_stream(stream, tls_server_config, deadline),
        }
        .map_err(|source| StorageNodeServerError::RpcListenerIo {
            context: "prepare storage-node RPC connection",
            endpoint: endpoint.to_string(),
            source,
        })
    }

    fn connection_handler(&self) -> StorageNodeConnectionHandler {
        let runtime_route = self.runtime_route_snapshot();
        StorageNodeConnectionHandler {
            config: runtime_route.config,
            route_map_lease: runtime_route.route_map_lease,
            runtime_route_source: Arc::clone(&self.runtime_route_state),
            route_admission: self.route_admission.clone(),
            node: Arc::clone(&self._node),
            metadata_transfer_staging_store: self.metadata_transfer_staging_store.clone(),
            read_handles: Arc::clone(&self.read_handles),
            metadata_command_locks: self.metadata_command_locks.clone(),
            rpc_auth: self.rpc_auth.clone(),
            #[cfg(test)]
            runtime_route_capture_test_hook: Arc::clone(&self.runtime_route_capture_test_hook),
            #[cfg(test)]
            response_envelope_test_hook: Arc::clone(&self.response_envelope_test_hook),
            #[cfg(test)]
            response_frame_test_hook: Arc::clone(&self.response_frame_test_hook),
            #[cfg(test)]
            metadata_command_before_commit_test_hook: Arc::clone(
                &self.metadata_command_before_commit_test_hook,
            ),
            #[cfg(test)]
            metadata_checkpoint_rows_captured_test_hook: Arc::clone(
                &self.metadata_checkpoint_rows_captured_test_hook,
            ),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_response_envelope_test_hook(
        &self,
        hook: StorageRpcResponseEnvelopeTestHook,
    ) {
        *self
            .response_envelope_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn set_response_frame_test_hook(&self, hook: StorageRpcResponseFrameTestHook) {
        *self
            .response_frame_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn set_metadata_command_before_commit_test_hook(
        &self,
        hook: MetadataCommandBeforeCommitTestHook,
    ) {
        *self
            .metadata_command_before_commit_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn set_metadata_checkpoint_rows_captured_test_hook(
        &self,
        hook: MetadataCheckpointRowsCapturedTestHook,
    ) {
        *self
            .metadata_checkpoint_rows_captured_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    pub(crate) fn set_control_plane_heartbeat_scan_test_hook(
        &self,
        hook: RuntimeConfigStageTestHook,
    ) {
        *self
            .control_plane_heartbeat_scan_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn run_control_plane_heartbeat_scan_test_hook(&self) {
        let hook = self
            .control_plane_heartbeat_scan_test_hook
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook();
        }
    }

    fn config_snapshot(&self) -> StorageNodeProcessConfig {
        self.config_snapshot_arc().as_ref().clone()
    }

    fn config_snapshot_arc(&self) -> Arc<StorageNodeProcessConfig> {
        self.runtime_route_state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .config
            .clone()
    }

    fn runtime_route_snapshot(&self) -> StorageNodeRuntimeRouteState {
        self.runtime_route_state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn acquire_session(&self) -> StorageNodeActiveSessionGuard {
        let limit = self
            .rpc_auth
            .as_deref()
            .map(|auth| auth.transport_limits().max_connections())
            .unwrap_or(STORAGE_NODE_MAX_ACTIVE_SESSIONS);
        self.active_sessions.acquire(limit)
    }

    #[cfg(test)]
    pub(crate) fn read_handle_count(&self, location: ShardLocation) -> usize {
        self.read_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .count(location)
    }

    #[cfg(test)]
    pub(crate) fn suppress_metadata_command_lock_wait_stderr(
        &self,
    ) -> SuppressMetadataCommandLockWaitStderr {
        self.metadata_command_locks.suppress_lock_wait_stderr()
    }
}
