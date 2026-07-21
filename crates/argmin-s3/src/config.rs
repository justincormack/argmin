use auth::SecretKey;
use ec::EcConfig;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use storage::control_plane::{
    ControlPlaneRpcFrameTransport, InitialClusterTopologyCertificate, MAX_HEARTBEAT_LEASE_MS,
};
use storage::control_plane_raft::ControlPlaneRaftPeerFrameTransport;
use storage::control_plane_raft::ControlPlaneRaftPeerTransportLimits;
use storage::storage_node_server::STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS;
use storage::LocalUnixStorageNodeClientConfig;
use storage::{NodeId, PgId};

const LOCAL_DEBUG_ENDPOINT_COMPILED_IN: bool = cfg!(any(test, feature = "local-debug-endpoints"));
const MAX_LOCAL_NODE_COUNT: u32 = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessRole {
    LegacyLocal,
    Frontend,
    StorageNode,
    Combined,
    ControlPlane,
}

impl ProcessRole {
    pub(crate) fn has_storage_node(self) -> bool {
        matches!(self, Self::StorageNode | Self::Combined)
    }

    fn has_frontend(self) -> bool {
        matches!(self, Self::LegacyLocal | Self::Frontend | Self::Combined)
    }

    fn has_control_plane(self) -> bool {
        matches!(self, Self::ControlPlane)
    }

    pub(crate) fn uses_remote_frontend_routing(self) -> bool {
        matches!(self, Self::Frontend | Self::Combined)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredStorageNodeSocket {
    pub(crate) node_id: u32,
    pub(crate) socket_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredControlPlaneRaftPeerSocket {
    pub(crate) node_id: u64,
    pub(crate) socket_path: String,
}

#[derive(Clone)]
pub(crate) enum ConfiguredControlPlaneRaftPeerListener {
    Unix {
        endpoint_id: String,
        socket_path: String,
        max_connections: usize,
        io_timeout: Duration,
    },
    Tcp {
        endpoint_id: String,
        bind_addr: String,
        tls_server_config: Arc<rustls::ServerConfig>,
        max_connections: usize,
        io_timeout: Duration,
    },
}

impl fmt::Debug for ConfiguredControlPlaneRaftPeerListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix {
                endpoint_id,
                socket_path,
                max_connections,
                io_timeout,
            } => f
                .debug_struct("ConfiguredControlPlaneRaftPeerListener::Unix")
                .field("endpoint_id", endpoint_id)
                .field("socket_path", socket_path)
                .field("max_connections", max_connections)
                .field("io_timeout", io_timeout)
                .finish(),
            Self::Tcp {
                endpoint_id,
                bind_addr,
                max_connections,
                io_timeout,
                ..
            } => f
                .debug_struct("ConfiguredControlPlaneRaftPeerListener::Tcp")
                .field("endpoint_id", endpoint_id)
                .field("bind_addr", bind_addr)
                .field("tls", &true)
                .field("max_connections", max_connections)
                .field("io_timeout", io_timeout)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum ConfiguredControlPlaneRpcListener {
    Unix {
        endpoint_id: String,
        socket_path: String,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    },
    Tcp {
        endpoint_id: String,
        bind_addr: String,
        tls_server_config: Arc<rustls::ServerConfig>,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    },
}

impl fmt::Debug for ConfiguredControlPlaneRpcListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix {
                endpoint_id,
                socket_path,
                max_connections,
                max_frame_bytes,
                io_timeout,
            } => f
                .debug_struct("ConfiguredControlPlaneRpcListener::Unix")
                .field("endpoint_id", endpoint_id)
                .field("socket_path", socket_path)
                .field("max_connections", max_connections)
                .field("max_frame_bytes", max_frame_bytes)
                .field("io_timeout", io_timeout)
                .finish(),
            Self::Tcp {
                endpoint_id,
                bind_addr,
                max_connections,
                max_frame_bytes,
                io_timeout,
                ..
            } => f
                .debug_struct("ConfiguredControlPlaneRpcListener::Tcp")
                .field("endpoint_id", endpoint_id)
                .field("bind_addr", bind_addr)
                .field("tls", &true)
                .field("max_connections", max_connections)
                .field("max_frame_bytes", max_frame_bytes)
                .field("io_timeout", io_timeout)
                .finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredControlPlaneRaftAuthCredential {
    pub(crate) node_id: u64,
    pub(crate) credential_id: String,
    pub(crate) credential_version: u64,
    pub(crate) secret: BinarySecretConfigValue,
}

impl fmt::Debug for ConfiguredControlPlaneRaftAuthCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfiguredControlPlaneRaftAuthCredential")
            .field("node_id", &self.node_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &self.secret)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredControlPlaneStorageAuthCredential {
    pub(crate) node_id: u32,
    pub(crate) credential_id: String,
    pub(crate) credential_version: u64,
    pub(crate) secret: BinarySecretConfigValue,
}

impl fmt::Debug for ConfiguredControlPlaneStorageAuthCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfiguredControlPlaneStorageAuthCredential")
            .field("node_id", &self.node_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &self.secret)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredControlPlaneFrontendAuthCredential {
    pub(crate) instance_id: String,
    pub(crate) credential_id: String,
    pub(crate) credential_version: u64,
    pub(crate) secret: BinarySecretConfigValue,
}

impl fmt::Debug for ConfiguredControlPlaneFrontendAuthCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfiguredControlPlaneFrontendAuthCredential")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &self.secret)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredControlPlaneAdminAuthCredential {
    pub(crate) instance_id: String,
    pub(crate) credential_id: String,
    pub(crate) credential_version: u64,
    pub(crate) secret: BinarySecretConfigValue,
}

impl fmt::Debug for ConfiguredControlPlaneAdminAuthCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfiguredControlPlaneAdminAuthCredential")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &self.secret)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredControlPlaneFrontendRuntimeMapAuth {
    pub(crate) cluster_id: String,
    pub(crate) instance_id: String,
    pub(crate) credentials: Vec<ConfiguredControlPlaneFrontendAuthCredential>,
}

impl ConfiguredControlPlaneFrontendRuntimeMapAuth {
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup<F: Fn(&str) -> Option<String>>(get: F) -> Result<Option<Self>, String> {
        let credentials = parse_control_plane_frontend_auth_credentials(get(
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
        ))?;
        if credentials.is_empty() {
            return Ok(None);
        }
        let cluster_id = get("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID").ok_or_else(|| {
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS requires ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"
                .to_string()
        })?;
        if cluster_id.is_empty() || !cluster_id.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(
                "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID must be non-empty and contain only printable non-space ASCII"
                    .to_string(),
            );
        }
        let instance_id = get("ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID").ok_or_else(|| {
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID is required when ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS is set"
                .to_string()
        })?;
        validate_auth_instance_id(
            &instance_id,
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
        )?;
        if !credentials
            .iter()
            .any(|credential| credential.instance_id == instance_id)
        {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS must include local frontend instance id {instance_id}"
            ));
        }
        Ok(Some(Self {
            cluster_id,
            instance_id,
            credentials,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredControlPlaneAdminCommandAuth {
    pub(crate) cluster_id: String,
    pub(crate) instance_id: String,
    pub(crate) credentials: Vec<ConfiguredControlPlaneAdminAuthCredential>,
}

impl ConfiguredControlPlaneAdminCommandAuth {
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup<F: Fn(&str) -> Option<String>>(get: F) -> Result<Option<Self>, String> {
        let credentials = parse_control_plane_admin_auth_credentials(get(
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
        ))?;
        if credentials.is_empty() {
            return Ok(None);
        }
        let cluster_id = get("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID").ok_or_else(|| {
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS requires ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"
                .to_string()
        })?;
        if cluster_id.is_empty() || !cluster_id.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(
                "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID must be non-empty and contain only printable non-space ASCII"
                    .to_string(),
            );
        }
        let instance_id = get("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID").ok_or_else(|| {
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID is required when ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS is set"
                .to_string()
        })?;
        validate_auth_instance_id(&instance_id, "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID")?;
        if !credentials
            .iter()
            .any(|credential| credential.instance_id == instance_id)
        {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS must include local admin instance id {instance_id}"
            ));
        }
        Ok(Some(Self {
            cluster_id,
            instance_id,
            credentials,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConfiguredCredentialProfile {
    Standard,
    OwnerAccountAdmin,
}

#[derive(Debug, Clone)]
pub(crate) struct ConfiguredCredential {
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: SecretKey,
    pub(crate) account_id: String,
    pub(crate) principal: String,
    pub(crate) display_name: String,
    pub(crate) authorization_profile: ConfiguredCredentialProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredStaticClusterIdentity {
    pub(crate) cluster_id: String,
    pub(crate) topology_generation: u64,
    pub(crate) topology_digest: String,
    pub(crate) process_id: String,
    pub(crate) process_identity_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredStaticInitialClusterMap {
    pub(crate) topology: InitialClusterTopologyCertificate,
    pub(crate) pg_acting_sets: Vec<(PgId, Vec<NodeId>)>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SecretConfigValue(String);

impl SecretConfigValue {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&observability::redacted("config_secret"), f)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct BinarySecretConfigValue(Vec<u8>);

impl BinarySecretConfigValue {
    pub(crate) fn from_utf8(value: String) -> Self {
        Self(value.into_bytes())
    }

    pub(crate) fn from_bytes(value: Vec<u8>) -> Self {
        Self(value)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for BinarySecretConfigValue {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

impl fmt::Debug for BinarySecretConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&observability::redacted("config_binary_secret"), f)
    }
}

/// Resolved server configuration, loaded from one supported configuration mode.
/// Configuration for the S3 server.
#[derive(Debug, Clone)]
pub(crate) struct ServerConfig {
    pub(crate) process_role: ProcessRole,
    pub(crate) listen_addr: String,
    pub(crate) tls_cert_path: Option<String>,
    pub(crate) tls_key_path: Option<String>,
    pub(crate) data_dir: String,
    pub(crate) pg_count: u32,
    pub(crate) storage_node_ids: Vec<u32>,
    pub(crate) storage_node_id: Option<u32>,
    pub(crate) storage_node_data_dir: Option<String>,
    pub(crate) static_cluster_identity: Option<ConfiguredStaticClusterIdentity>,
    pub(crate) static_initial_cluster_map: Option<ConfiguredStaticInitialClusterMap>,
    pub(crate) storage_node_socket_path: Option<String>,
    pub(crate) storage_node_sockets: Vec<ConfiguredStorageNodeSocket>,
    pub(crate) storage_node_rpc_admission_limit: usize,
    pub(crate) storage_node_rpc_admission_wait_timeout: Duration,
    pub(crate) storage_node_rpc_control_admission_wait_timeout: Duration,
    pub(crate) control_plane_state_path: Option<String>,
    pub(crate) control_plane_socket_path: Option<String>,
    pub(crate) control_plane_clock_recovery_socket_path: Option<String>,
    pub(crate) control_plane_client_socket_paths: Vec<String>,
    pub(crate) control_plane_rpc_listeners: Vec<ConfiguredControlPlaneRpcListener>,
    pub(crate) control_plane_clock_recovery_rpc_listeners: Vec<ConfiguredControlPlaneRpcListener>,
    pub(crate) control_plane_rpc_client_endpoints: Vec<String>,
    pub(crate) control_plane_clock_recovery_rpc_client_endpoints: Vec<String>,
    pub(crate) control_plane_rpc_frame_transport: Option<Arc<dyn ControlPlaneRpcFrameTransport>>,
    pub(crate) control_plane_clock_recovery_rpc_frame_transport:
        Option<Arc<dyn ControlPlaneRpcFrameTransport>>,
    pub(crate) control_plane_auth_cluster_id: Option<String>,
    pub(crate) control_plane_storage_auth_credentials:
        Vec<ConfiguredControlPlaneStorageAuthCredential>,
    pub(crate) control_plane_storage_auth_signing_credential: Option<(String, u64)>,
    pub(crate) control_plane_frontend_auth_instance_id: Option<String>,
    pub(crate) control_plane_frontend_auth_credentials:
        Vec<ConfiguredControlPlaneFrontendAuthCredential>,
    pub(crate) control_plane_frontend_auth_signing_credential: Option<(String, u64)>,
    pub(crate) control_plane_admin_auth_instance_id: Option<String>,
    pub(crate) control_plane_admin_auth_credentials: Vec<ConfiguredControlPlaneAdminAuthCredential>,
    pub(crate) control_plane_experimental_raft: bool,
    pub(crate) control_plane_raft_cluster_name: Option<String>,
    pub(crate) control_plane_raft_node_id: Option<u64>,
    pub(crate) control_plane_raft_peer_socket_path: Option<String>,
    pub(crate) control_plane_raft_peer_sockets: Vec<ConfiguredControlPlaneRaftPeerSocket>,
    pub(crate) control_plane_raft_peer_listeners: Vec<ConfiguredControlPlaneRaftPeerListener>,
    pub(crate) control_plane_raft_peer_frame_transport:
        Option<Arc<dyn ControlPlaneRaftPeerFrameTransport>>,
    pub(crate) control_plane_raft_peer_transport_limits: ControlPlaneRaftPeerTransportLimits,
    pub(crate) control_plane_raft_peer_max_connections: usize,
    pub(crate) control_plane_raft_peer_connect_timeout: Duration,
    pub(crate) control_plane_raft_peer_io_timeout: Duration,
    pub(crate) control_plane_raft_auth_credentials: Vec<ConfiguredControlPlaneRaftAuthCredential>,
    pub(crate) control_plane_raft_auth_signing_credential: Option<(String, u64)>,
    pub(crate) control_plane_lease_scan_interval: Duration,
    pub(crate) control_plane_frontend_refresh_interval: Duration,
    pub(crate) control_plane_heartbeat_lease_duration: Duration,
    pub(crate) storage_cluster_epoch: u64,
    pub(crate) storage_pg_ids: Vec<u32>,
    pub(crate) ec_k: u8,
    pub(crate) ec_m: u8,
    pub(crate) account_id: String,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: SecretKey,
    pub(crate) uat_credentials: Vec<ConfiguredCredential>,
    pub(crate) host_id: Option<String>,
    pub(crate) sse_c_validator_key_b64: Option<SecretConfigValue>,
    pub(crate) sse_s3_wrapping_key_b64: SecretConfigValue,
    pub(crate) region: String,
    pub(crate) workers: u32,
    pub(crate) max_connections: u32,
    pub(crate) max_inflight_requests: u32,
    pub(crate) stream_read_chunk_size: usize,
    pub(crate) panic_on_500: bool,
    pub(crate) abort_on_500: bool,
    pub(crate) local_debug_endpoint: bool,
}

impl ServerConfig {
    /// Resolve configuration values from an environment-style lookup.
    ///
    /// Required: `ARGMIN_ACCOUNT_ID`, `ARGMIN_ACCESS_KEY_ID`,
    /// `ARGMIN_SECRET_ACCESS_KEY`
    /// Optional (with defaults):
    ///   `ARGMIN_HOST_ID` (random stable-for-process host ID)
    ///   `ARGMIN_LISTEN_ADDR` (127.0.0.1:9000)
    ///   `ARGMIN_TLS_CERT_PATH` / `ARGMIN_TLS_KEY_PATH` (unset)
    ///   `ARGMIN_DATA_DIR` (./data)
    ///   `ARGMIN_PG_COUNT` (16)
    ///   `ARGMIN_STORAGE_CLUSTER_EPOCH` (1)
    ///   `ARGMIN_STORAGE_PG_IDS` (all PGs in `0..ARGMIN_PG_COUNT`)
    ///   `ARGMIN_STORAGE_NODE_SOCKETS` (`node_id=/absolute/socket,...`, required for static frontend/combined routing, optional initial control-plane bootstrap membership)
    ///   `ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT` (1024)
    ///   `ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS` (250)
    ///   `ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS` (1000)
    ///   `ARGMIN_CONTROL_PLANE_STATE_PATH` (required for control-plane role)
    ///   `ARGMIN_CONTROL_PLANE_SOCKET_PATH` (required for control-plane role, optional dynamic route source for frontend/storage roles)
    ///   `ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS` (comma-separated absolute Unix sockets used by frontend/storage/admin clients for replicated-authority routing)
    ///   `ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID` (required when control-plane internal auth credentials are configured)
    ///   `ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS` (`node_id=credential_id:version:secret,...`, optional authenticated storage-node heartbeat refresh)
    ///   `ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID` (required for frontend roles when frontend control-plane auth credentials are configured)
    ///   `ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS` (`instance_id=credential_id:version:secret,...`, optional authenticated frontend runtime-map reads)
    ///   `ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID` (optional local admin signing principal)
    ///   `ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS` (`instance_id=credential_id:version:secret,...`, required for control-plane role when any Unix control-plane auth is configured)
    ///   `ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT` (false)
    ///   `ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME` (optional experimental Raft cluster identity)
    ///   `ARGMIN_CONTROL_PLANE_RAFT_NODE_ID` (1 when experimental Raft is enabled)
    ///   `ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH` (optional local experimental Raft peer socket)
    ///   `ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS` (`node_id=/absolute/socket,...`, optional experimental Raft peer map)
    ///   `ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS` (`node_id=credential_id:version:secret,...`, required for multi-node experimental Raft peer auth)
    ///   `ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS` (250)
    ///   `ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS` (250)
    ///   `ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS` (2000)
    ///   `ARGMIN_EC_K` (4)
    ///   `ARGMIN_EC_M` (2)
    ///   `ARGMIN_LOCAL_NODE_COUNT` (`ARGMIN_EC_K + ARGMIN_EC_M`)
    ///   `ARGMIN_REGION` (us-east-1)
    ///   `ARGMIN_WORKERS` (4)
    ///   `ARGMIN_MAX_CONNECTIONS` (512)
    ///   `ARGMIN_MAX_INFLIGHT_REQUESTS` (32)
    ///   `ARGMIN_STREAM_READ_CHUNK_SIZE` (8388608)
    ///   `ARGMIN_PANIC_ON_500` (false)
    ///   `ARGMIN_ABORT_ON_500` (false)
    ///   `ARGMIN_LOCAL_DEBUG_ENDPOINT` (false, requires test or
    ///   local-debug-endpoints build and loopback listen addr)
    ///
    /// UAT-only optional credentials for running `s3-tests` against the
    /// standalone binary:
    ///   `ARGMIN_UAT_ALT_ACCOUNT_ID`
    ///   `ARGMIN_UAT_ALT_ACCESS_KEY_ID`
    ///   `ARGMIN_UAT_ALT_SECRET_ACCESS_KEY`
    ///   `ARGMIN_UAT_SECOND_ACCESS_KEY_ID`
    ///   `ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY`
    ///   `ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID`
    ///   `ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY`
    /// Build configuration from an arbitrary key-lookup function.
    /// Used by the environment/static-manifest loader and directly by tests.
    pub(crate) fn from_lookup<F: Fn(&str) -> Option<String>>(get: F) -> Result<Self, String> {
        let process_role = match get("ARGMIN_PROCESS_ROLE") {
            Some(value) => parse_process_role(&value)?,
            None => ProcessRole::LegacyLocal,
        };
        let (
            account_id,
            access_key_id,
            secret_access_key,
            uat_credentials,
            sse_s3_wrapping_key_b64,
        ) = if process_role.has_frontend() {
            let account_id = get("ARGMIN_ACCOUNT_ID")
                .ok_or_else(|| "ARGMIN_ACCOUNT_ID is required".to_string())?;
            if account_id.len() != 12 || !account_id.bytes().all(|b| b.is_ascii_digit()) {
                return Err("ARGMIN_ACCOUNT_ID must be a 12-digit AWS account ID".to_string());
            }
            let access_key_id = get("ARGMIN_ACCESS_KEY_ID")
                .ok_or_else(|| "ARGMIN_ACCESS_KEY_ID is required".to_string())?;
            let secret_access_key = get("ARGMIN_SECRET_ACCESS_KEY")
                .ok_or_else(|| "ARGMIN_SECRET_ACCESS_KEY is required".to_string())?;
            let uat_credentials = read_uat_credentials(&get, &account_id)?;
            reject_duplicate_access_keys(&access_key_id, &uat_credentials)?;
            reject_reserved_session_access_keys(&access_key_id, &uat_credentials)?;
            let sse_s3_wrapping_key_b64 = get("ARGMIN_SSE_S3_WRAPPING_KEY")
                .ok_or_else(|| "ARGMIN_SSE_S3_WRAPPING_KEY is required".to_string())?;
            (
                account_id,
                access_key_id,
                SecretKey::new(secret_access_key),
                uat_credentials,
                SecretConfigValue::new(sse_s3_wrapping_key_b64),
            )
        } else {
            (
                String::new(),
                String::new(),
                SecretKey::new(String::new()),
                Vec::new(),
                SecretConfigValue::new(String::new()),
            )
        };
        let host_id = get("ARGMIN_HOST_ID");
        let sse_c_validator_key_b64 = get("ARGMIN_SSE_C_VALIDATOR_KEY").map(SecretConfigValue::new);
        let listen_addr = get("ARGMIN_LISTEN_ADDR").unwrap_or_else(|| "127.0.0.1:9000".to_string());
        let tls_cert_path = get("ARGMIN_TLS_CERT_PATH");
        let tls_key_path = get("ARGMIN_TLS_KEY_PATH");
        let data_dir = get("ARGMIN_DATA_DIR").unwrap_or_else(|| "./data".to_string());
        let storage_node_data_dir = get("ARGMIN_STORAGE_NODE_DATA_DIR");
        let storage_node_socket_path = get("ARGMIN_STORAGE_NODE_SOCKET_PATH");
        let storage_cluster_epoch: u64 = get("ARGMIN_STORAGE_CLUSTER_EPOCH")
            .unwrap_or_else(|| "1".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_STORAGE_CLUSTER_EPOCH: {e}"))?;
        let storage_node_id = get("ARGMIN_STORAGE_NODE_ID")
            .map(|value| {
                value
                    .parse()
                    .map_err(|e| format!("invalid ARGMIN_STORAGE_NODE_ID: {e}"))
            })
            .transpose()?;
        let pg_count: u32 = get("ARGMIN_PG_COUNT")
            .unwrap_or_else(|| "16".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_PG_COUNT: {e}"))?;
        let ec_k: u8 = get("ARGMIN_EC_K")
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_K: {e}"))?;
        let ec_m: u8 = get("ARGMIN_EC_M")
            .unwrap_or_else(|| "2".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_EC_M: {e}"))?;
        let ec_config = EcConfig::new(ec_k, ec_m).map_err(|e| format!("invalid EC config: {e}"))?;
        let local_node_count: u32 = match get("ARGMIN_LOCAL_NODE_COUNT") {
            Some(value) => value
                .parse()
                .map_err(|e| format!("invalid ARGMIN_LOCAL_NODE_COUNT: {e}"))?,
            None => u32::try_from(ec_config.total_shards())
                .map_err(|_| "ARGMIN_LOCAL_NODE_COUNT default is too large".to_string())?,
        };
        if local_node_count == 0 {
            return Err("ARGMIN_LOCAL_NODE_COUNT must be > 0".to_string());
        }
        if local_node_count > MAX_LOCAL_NODE_COUNT {
            return Err(format!(
                "ARGMIN_LOCAL_NODE_COUNT must be <= {MAX_LOCAL_NODE_COUNT}"
            ));
        }
        let storage_node_ids: Vec<u32> = (0..local_node_count).collect();
        let region = get("ARGMIN_REGION").unwrap_or_else(|| "us-east-1".to_string());
        let workers: u32 = get("ARGMIN_WORKERS")
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_WORKERS: {e}"))?;
        let max_connections: u32 = get("ARGMIN_MAX_CONNECTIONS")
            .unwrap_or_else(|| "512".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_MAX_CONNECTIONS: {e}"))?;
        let max_inflight_requests: u32 = get("ARGMIN_MAX_INFLIGHT_REQUESTS")
            .unwrap_or_else(|| "32".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_MAX_INFLIGHT_REQUESTS: {e}"))?;
        let storage_node_rpc_admission_limit: usize =
            get("ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT")
                .unwrap_or_else(|| {
                    LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT.to_string()
                })
                .parse()
                .map_err(|e| format!("invalid ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT: {e}"))?;
        let storage_node_rpc_admission_wait_ms: u64 =
            get("ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS")
                .unwrap_or_else(|| {
                    LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT
                        .as_millis()
                        .to_string()
                })
                .parse()
                .map_err(|e| format!("invalid ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS: {e}"))?;
        let storage_node_rpc_admission_wait_timeout =
            Duration::from_millis(storage_node_rpc_admission_wait_ms);
        let storage_node_rpc_control_admission_wait_ms: u64 =
            get("ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS")
                .unwrap_or_else(|| {
                    LocalUnixStorageNodeClientConfig::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT
                        .as_millis()
                        .to_string()
                })
                .parse()
                .map_err(|e| {
                    format!("invalid ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS: {e}")
                })?;
        let storage_node_rpc_control_admission_wait_timeout =
            Duration::from_millis(storage_node_rpc_control_admission_wait_ms);
        let control_plane_state_path = get("ARGMIN_CONTROL_PLANE_STATE_PATH");
        let control_plane_socket_path = get("ARGMIN_CONTROL_PLANE_SOCKET_PATH");
        let control_plane_client_socket_paths = parse_control_plane_client_socket_paths(
            get("ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS"),
            control_plane_socket_path.as_deref(),
        )?;
        let control_plane_auth_cluster_id = get("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID");
        let control_plane_storage_auth_credentials = parse_control_plane_storage_auth_credentials(
            get("ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS"),
        )?;
        let control_plane_frontend_auth_instance_id =
            get("ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID");
        let control_plane_frontend_auth_credentials =
            parse_control_plane_frontend_auth_credentials(get(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
            ))?;
        let control_plane_admin_auth_instance_id =
            get("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID");
        let control_plane_admin_auth_credentials = parse_control_plane_admin_auth_credentials(
            get("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS"),
        )?;
        let control_plane_experimental_raft = match get("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT") {
            Some(value) => parse_bool_env("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", &value)?,
            None => false,
        };
        let control_plane_raft_cluster_name = get("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME");
        let control_plane_raft_node_id = match get("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID") {
            Some(value) => {
                let node_id = value
                    .parse::<u64>()
                    .map_err(|e| format!("invalid ARGMIN_CONTROL_PLANE_RAFT_NODE_ID: {e}"))?;
                Some(node_id)
            }
            None if control_plane_experimental_raft => Some(1),
            None => None,
        };
        let control_plane_raft_peer_socket_path = get("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH");
        let control_plane_raft_auth_credentials = parse_control_plane_raft_auth_credentials(get(
            "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
        ))?;
        let control_plane_lease_scan_ms: u64 = get("ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS")
            .unwrap_or_else(|| "250".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS: {e}"))?;
        let control_plane_lease_scan_interval = Duration::from_millis(control_plane_lease_scan_ms);
        let control_plane_frontend_refresh_ms: u64 =
            get("ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS")
                .unwrap_or_else(|| "250".to_string())
                .parse()
                .map_err(|e| format!("invalid ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS: {e}"))?;
        let control_plane_frontend_refresh_interval =
            Duration::from_millis(control_plane_frontend_refresh_ms);
        let control_plane_heartbeat_lease_ms: u64 = get("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS")
            .unwrap_or_else(|| "2000".to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS: {e}"))?;
        let control_plane_heartbeat_lease_duration =
            Duration::from_millis(control_plane_heartbeat_lease_ms);
        let stream_read_chunk_size: usize = get("ARGMIN_STREAM_READ_CHUNK_SIZE")
            .unwrap_or_else(|| server_core::coordinator::INTERNAL_SEGMENT_SIZE.to_string())
            .parse()
            .map_err(|e| format!("invalid ARGMIN_STREAM_READ_CHUNK_SIZE: {e}"))?;
        let panic_on_500 = match get("ARGMIN_PANIC_ON_500") {
            Some(value) => parse_bool_env("ARGMIN_PANIC_ON_500", &value)?,
            None => false,
        };
        let abort_on_500 = match get("ARGMIN_ABORT_ON_500") {
            Some(value) => parse_bool_env("ARGMIN_ABORT_ON_500", &value)?,
            None => false,
        };
        let local_debug_endpoint = match get("ARGMIN_LOCAL_DEBUG_ENDPOINT") {
            Some(value) => parse_bool_env("ARGMIN_LOCAL_DEBUG_ENDPOINT", &value)?,
            None => false,
        };
        if local_debug_endpoint && !LOCAL_DEBUG_ENDPOINT_COMPILED_IN {
            return Err(
                "ARGMIN_LOCAL_DEBUG_ENDPOINT requires a test build or the local-debug-endpoints feature"
                    .to_string(),
            );
        }

        if pg_count == 0 {
            return Err("ARGMIN_PG_COUNT must be > 0".to_string());
        }
        if storage_cluster_epoch == 0 {
            return Err("ARGMIN_STORAGE_CLUSTER_EPOCH must be > 0".to_string());
        }
        let storage_pg_ids = parse_storage_pg_ids(get("ARGMIN_STORAGE_PG_IDS"), pg_count)?;
        let storage_node_sockets = parse_storage_node_sockets(
            get("ARGMIN_STORAGE_NODE_SOCKETS"),
            local_node_count,
            process_role.uses_remote_frontend_routing() && control_plane_socket_path.is_none(),
        )?;
        let control_plane_raft_peer_sockets =
            parse_control_plane_raft_peer_sockets(get("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS"))?;
        if process_role.has_storage_node() {
            let storage_node_id = storage_node_id.ok_or_else(|| {
                "ARGMIN_STORAGE_NODE_ID is required for storage roles".to_string()
            })?;
            if !storage_node_ids.contains(&storage_node_id) {
                return Err(
                    "ARGMIN_STORAGE_NODE_ID must identify a configured local storage node"
                        .to_string(),
                );
            }
            if storage_node_socket_path.is_none() {
                return Err(
                    "ARGMIN_STORAGE_NODE_SOCKET_PATH is required for storage roles".to_string(),
                );
            }
            if process_role.uses_remote_frontend_routing() && control_plane_socket_path.is_none() {
                let storage_node_socket_path = storage_node_socket_path
                    .as_deref()
                    .expect("storage role socket path was validated");
                let configured_self_socket_path = storage_node_sockets
                    .iter()
                    .find(|entry| entry.node_id == storage_node_id)
                    .map(|entry| entry.socket_path.as_str())
                    .expect("remote frontend socket map must contain every node");
                let configured_self_socket_path = canonical_storage_node_socket_path(
                    storage_node_id,
                    configured_self_socket_path,
                )?;
                let storage_node_socket_path =
                    canonical_storage_node_socket_path(storage_node_id, storage_node_socket_path)?;
                if configured_self_socket_path != storage_node_socket_path {
                    return Err(format!(
                        "ARGMIN_STORAGE_NODE_SOCKETS entry for node {storage_node_id} must match ARGMIN_STORAGE_NODE_SOCKET_PATH"
                    ));
                }
            }
        }
        let local_node_count_usize = usize::try_from(local_node_count)
            .map_err(|_| "ARGMIN_LOCAL_NODE_COUNT is too large for this platform".to_string())?;
        if local_node_count_usize < ec_config.total_shards() {
            return Err(format!(
                "ARGMIN_LOCAL_NODE_COUNT must be at least ARGMIN_EC_K + ARGMIN_EC_M ({}) for the configured EC shape",
                ec_config.total_shards()
            ));
        }
        if workers == 0 {
            return Err("ARGMIN_WORKERS must be > 0".to_string());
        }
        if max_connections == 0 {
            return Err("ARGMIN_MAX_CONNECTIONS must be > 0".to_string());
        }
        if max_inflight_requests == 0 {
            return Err("ARGMIN_MAX_INFLIGHT_REQUESTS must be > 0".to_string());
        }
        if storage_node_rpc_admission_limit
            < LocalUnixStorageNodeClientConfig::MIN_RPC_ADMISSION_LIMIT
        {
            return Err(format!(
                "ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT must be >= {}",
                LocalUnixStorageNodeClientConfig::MIN_RPC_ADMISSION_LIMIT
            ));
        }
        if storage_node_rpc_admission_wait_timeout.is_zero() {
            return Err("ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS must be > 0".to_string());
        }
        if storage_node_rpc_control_admission_wait_timeout.is_zero() {
            return Err(
                "ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS must be > 0".to_string(),
            );
        }
        if process_role == ProcessRole::ControlPlane && control_plane_state_path.is_none() {
            return Err(
                "ARGMIN_CONTROL_PLANE_STATE_PATH is required for control-plane role".to_string(),
            );
        }
        if process_role == ProcessRole::ControlPlane && control_plane_socket_path.is_none() {
            return Err(
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH is required for control-plane role".to_string(),
            );
        }
        if !control_plane_experimental_raft
            && (control_plane_raft_cluster_name.is_some()
                || control_plane_raft_node_id.is_some()
                || control_plane_raft_peer_socket_path.is_some()
                || !control_plane_raft_peer_sockets.is_empty()
                || !control_plane_raft_auth_credentials.is_empty())
        {
            return Err(
                "ARGMIN_CONTROL_PLANE_RAFT_* requires ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT"
                    .to_string(),
            );
        }
        if let Some(cluster_name) = &control_plane_raft_cluster_name {
            if cluster_name.is_empty() || !cluster_name.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(
                    "ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME must be non-empty and contain only printable non-space ASCII"
                        .to_string(),
                );
            }
        }
        if let Some(cluster_id) = &control_plane_auth_cluster_id {
            if cluster_id.is_empty() || !cluster_id.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(
                    "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID must be non-empty and contain only printable non-space ASCII"
                        .to_string(),
                );
            }
        }
        if let Some(node_id) = control_plane_raft_node_id {
            if node_id == 0 {
                return Err("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID must be > 0".to_string());
            }
        }
        if let Some(peer_socket_path) = &control_plane_raft_peer_socket_path {
            if peer_socket_path.is_empty() {
                return Err(
                    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH must not be empty".to_string(),
                );
            }
            if !Path::new(peer_socket_path).is_absolute() {
                return Err(
                    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH must use an absolute socket path"
                        .to_string(),
                );
            }
        }
        if !control_plane_raft_peer_sockets.is_empty() {
            let local_node_id = control_plane_raft_node_id.expect(
                "experimental raft node id is set when experimental raft config is enabled",
            );
            let local_peer_socket_path = control_plane_raft_peer_socket_path.as_deref().ok_or_else(
                || {
                    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH is required when ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS is set"
                        .to_string()
                },
            )?;
            let configured_local_socket_path = control_plane_raft_peer_sockets
                .iter()
                .find(|entry| entry.node_id == local_node_id)
                .map(|entry| entry.socket_path.as_str())
                .ok_or_else(|| {
                    format!(
                        "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS must include local Raft node id {local_node_id}"
                    )
                })?;
            let configured_local_socket_path = canonical_control_plane_raft_peer_socket_path(
                local_node_id,
                configured_local_socket_path,
            )?;
            let local_peer_socket_path = canonical_control_plane_raft_peer_socket_path(
                local_node_id,
                local_peer_socket_path,
            )?;
            if configured_local_socket_path != local_peer_socket_path {
                return Err(format!(
                    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS entry for local Raft node {local_node_id} must match ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH"
                ));
            }
            if control_plane_raft_peer_sockets.len() > 1
                && control_plane_raft_auth_credentials.is_empty()
            {
                return Err(
                    "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS is required when ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS configures multiple Raft nodes"
                        .to_string(),
                );
            }
        }
        if !control_plane_raft_auth_credentials.is_empty() {
            if control_plane_raft_cluster_name.is_none() {
                return Err(
                    "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS requires ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME"
                        .to_string(),
                );
            }
            let local_node_id = control_plane_raft_node_id.expect(
                "experimental raft node id is set when experimental raft config is enabled",
            );
            validate_control_plane_raft_auth_credentials_match_peer_policy(
                local_node_id,
                &control_plane_raft_peer_sockets,
                &control_plane_raft_auth_credentials,
            )?;
        }
        if !control_plane_storage_auth_credentials.is_empty() {
            if control_plane_auth_cluster_id.is_none() {
                return Err(
                    "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS requires ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"
                        .to_string(),
                );
            }
            if process_role.has_storage_node() {
                let storage_node_id = storage_node_id.expect("storage node id validated");
                if !control_plane_storage_auth_credentials
                    .iter()
                    .any(|entry| entry.node_id == storage_node_id)
                {
                    return Err(format!(
                        "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS must include local storage node id {storage_node_id}"
                    ));
                }
            }
        }
        if !control_plane_frontend_auth_credentials.is_empty() {
            if control_plane_auth_cluster_id.is_none() {
                return Err(
                    "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS requires ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"
                        .to_string(),
                );
            }
            if process_role.has_frontend() && control_plane_socket_path.is_some() {
                let instance_id = control_plane_frontend_auth_instance_id.as_deref().ok_or_else(
                    || {
                        "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID is required for frontend roles when ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS is set"
                            .to_string()
                    },
                )?;
                if !control_plane_frontend_auth_credentials
                    .iter()
                    .any(|entry| entry.instance_id == instance_id)
                {
                    return Err(format!(
                        "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS must include local frontend instance id {instance_id}"
                    ));
                }
            }
        }
        if let Some(instance_id) = &control_plane_frontend_auth_instance_id {
            validate_auth_instance_id(
                instance_id,
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
            )?;
        }
        if !control_plane_admin_auth_credentials.is_empty()
            && control_plane_auth_cluster_id.is_none()
        {
            return Err(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS requires ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"
                    .to_string(),
            );
        }
        if let Some(instance_id) = &control_plane_admin_auth_instance_id {
            validate_auth_instance_id(instance_id, "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID")?;
            if !control_plane_admin_auth_credentials.is_empty()
                && !control_plane_admin_auth_credentials
                    .iter()
                    .any(|entry| entry.instance_id == *instance_id)
            {
                return Err(format!(
                    "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS must include local admin instance id {instance_id}"
                ));
            }
        }
        if process_role.has_control_plane()
            && (!control_plane_storage_auth_credentials.is_empty()
                || !control_plane_frontend_auth_credentials.is_empty()
                || !control_plane_admin_auth_credentials.is_empty())
            && control_plane_admin_auth_credentials.is_empty()
        {
            return Err(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS is required when any Unix control-plane auth credentials are configured for the control-plane role"
                    .to_string(),
            );
        }
        if control_plane_lease_scan_interval.is_zero() {
            return Err("ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS must be > 0".to_string());
        }
        if control_plane_frontend_refresh_interval.is_zero() {
            return Err("ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS must be > 0".to_string());
        }
        if control_plane_heartbeat_lease_ms < STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS must be >= {STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS}"
            ));
        }
        if control_plane_heartbeat_lease_ms > MAX_HEARTBEAT_LEASE_MS {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS must be <= {MAX_HEARTBEAT_LEASE_MS}"
            ));
        }
        if stream_read_chunk_size == 0 {
            return Err("ARGMIN_STREAM_READ_CHUNK_SIZE must be > 0".to_string());
        }
        if local_debug_endpoint {
            if !process_role.has_frontend() {
                return Err(
                    "ARGMIN_LOCAL_DEBUG_ENDPOINT requires a frontend process role".to_string(),
                );
            }
            let listen_socket_addr: SocketAddr = listen_addr.parse().map_err(|e| {
                format!(
                    "ARGMIN_LOCAL_DEBUG_ENDPOINT requires ARGMIN_LISTEN_ADDR to be a loopback socket address: {e}"
                )
            })?;
            if !listen_socket_addr.ip().is_loopback() {
                return Err(
                    "ARGMIN_LOCAL_DEBUG_ENDPOINT requires ARGMIN_LISTEN_ADDR to be loopback"
                        .to_string(),
                );
            }
        }
        if let Some(host_id) = &host_id {
            if host_id.is_empty() || !host_id.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(
                    "ARGMIN_HOST_ID must be non-empty and contain only printable non-space ASCII"
                        .to_string(),
                );
            }
        }
        match (&tls_cert_path, &tls_key_path) {
            (Some(_), None) => {
                return Err(
                    "ARGMIN_TLS_KEY_PATH is required when ARGMIN_TLS_CERT_PATH is set".to_string(),
                )
            }
            (None, Some(_)) => {
                return Err(
                    "ARGMIN_TLS_CERT_PATH is required when ARGMIN_TLS_KEY_PATH is set".to_string(),
                )
            }
            _ => {}
        }

        Ok(Self {
            process_role,
            listen_addr,
            tls_cert_path,
            tls_key_path,
            data_dir,
            pg_count,
            storage_node_ids,
            storage_node_id,
            storage_node_data_dir,
            static_cluster_identity: None,
            static_initial_cluster_map: None,
            storage_node_socket_path,
            storage_node_sockets,
            storage_node_rpc_admission_limit,
            storage_node_rpc_admission_wait_timeout,
            storage_node_rpc_control_admission_wait_timeout,
            control_plane_state_path,
            control_plane_socket_path,
            control_plane_clock_recovery_socket_path: None,
            control_plane_client_socket_paths,
            control_plane_rpc_listeners: Vec::new(),
            control_plane_clock_recovery_rpc_listeners: Vec::new(),
            control_plane_rpc_client_endpoints: Vec::new(),
            control_plane_clock_recovery_rpc_client_endpoints: Vec::new(),
            control_plane_rpc_frame_transport: None,
            control_plane_clock_recovery_rpc_frame_transport: None,
            control_plane_auth_cluster_id,
            control_plane_storage_auth_credentials,
            control_plane_storage_auth_signing_credential: None,
            control_plane_frontend_auth_instance_id,
            control_plane_frontend_auth_credentials,
            control_plane_frontend_auth_signing_credential: None,
            control_plane_admin_auth_instance_id,
            control_plane_admin_auth_credentials,
            control_plane_experimental_raft,
            control_plane_raft_cluster_name,
            control_plane_raft_node_id,
            control_plane_raft_peer_socket_path,
            control_plane_raft_peer_sockets,
            control_plane_raft_peer_listeners: Vec::new(),
            control_plane_raft_peer_frame_transport: None,
            control_plane_raft_peer_transport_limits: ControlPlaneRaftPeerTransportLimits::default(
            ),
            control_plane_raft_peer_max_connections: 64,
            control_plane_raft_peer_connect_timeout: Duration::from_secs(1),
            control_plane_raft_peer_io_timeout: Duration::from_secs(1),
            control_plane_raft_auth_credentials,
            control_plane_raft_auth_signing_credential: None,
            control_plane_lease_scan_interval,
            control_plane_frontend_refresh_interval,
            control_plane_heartbeat_lease_duration,
            storage_cluster_epoch,
            storage_pg_ids,
            ec_k,
            ec_m,
            account_id,
            access_key_id,
            secret_access_key,
            uat_credentials,
            host_id,
            sse_c_validator_key_b64,
            sse_s3_wrapping_key_b64,
            region,
            workers,
            max_connections,
            max_inflight_requests,
            stream_read_chunk_size,
            panic_on_500,
            abort_on_500,
            local_debug_endpoint,
        })
    }
}

fn parse_bool_env(name: &str, value: &str) -> Result<bool, String> {
    match value.trim() {
        "1" | "true" | "TRUE" | "True" | "yes" | "YES" | "Yes" | "on" | "ON" | "On" => Ok(true),
        "0" | "false" | "FALSE" | "False" | "no" | "NO" | "No" | "off" | "OFF" | "Off" => Ok(false),
        _ => Err(format!("{name} must be a boolean value")),
    }
}

fn parse_process_role(value: &str) -> Result<ProcessRole, String> {
    match value.trim() {
        "frontend" => Ok(ProcessRole::Frontend),
        "storage-node" => Ok(ProcessRole::StorageNode),
        "combined" => Ok(ProcessRole::Combined),
        "control-plane" => Ok(ProcessRole::ControlPlane),
        "legacy-local" => Ok(ProcessRole::LegacyLocal),
        _ => Err(
            "ARGMIN_PROCESS_ROLE must be one of frontend, storage-node, combined, control-plane, legacy-local"
                .to_string(),
        ),
    }
}

pub(crate) fn parse_control_plane_client_socket_paths(
    value: Option<String>,
    primary_socket_path: Option<&str>,
) -> Result<Vec<String>, String> {
    let Some(value) = value else {
        return Ok(primary_socket_path
            .map(ToOwned::to_owned)
            .into_iter()
            .collect());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS must not be empty".to_owned());
    }
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    for raw_path in value.split(',') {
        let path = raw_path.trim();
        if path.is_empty() {
            return Err(
                "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS contains an empty entry".to_owned(),
            );
        }
        if !Path::new(path).is_absolute() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS entry {path:?} must use an absolute path"
            ));
        }
        if !seen.insert(path.to_owned()) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS contains duplicate path {path:?}"
            ));
        }
        paths.push(path.to_owned());
    }
    if let Some(primary_socket_path) = primary_socket_path {
        if !seen.contains(primary_socket_path) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS must include ARGMIN_CONTROL_PLANE_SOCKET_PATH {primary_socket_path:?}"
            ));
        }
    }
    Ok(paths)
}

fn parse_storage_pg_ids(value: Option<String>, pg_count: u32) -> Result<Vec<u32>, String> {
    let Some(value) = value else {
        return Ok((0..pg_count).collect());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_STORAGE_PG_IDS must not be empty".to_string());
    }
    let mut pg_ids = Vec::new();
    let mut seen = HashSet::new();
    for raw in value.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("ARGMIN_STORAGE_PG_IDS contains an empty PG id".to_string());
        }
        let pg_id: u32 = trimmed
            .parse()
            .map_err(|e| format!("invalid ARGMIN_STORAGE_PG_IDS entry {trimmed:?}: {e}"))?;
        if pg_id >= pg_count {
            return Err(format!(
                "ARGMIN_STORAGE_PG_IDS entry {pg_id} must be less than ARGMIN_PG_COUNT ({pg_count})"
            ));
        }
        if !seen.insert(pg_id) {
            return Err(format!(
                "ARGMIN_STORAGE_PG_IDS contains duplicate PG id {pg_id}"
            ));
        }
        pg_ids.push(pg_id);
    }
    Ok(pg_ids)
}

fn parse_storage_node_sockets(
    value: Option<String>,
    local_node_count: u32,
    require_complete: bool,
) -> Result<Vec<ConfiguredStorageNodeSocket>, String> {
    let Some(value) = value else {
        if require_complete {
            return Err(
                "ARGMIN_STORAGE_NODE_SOCKETS is required for frontend and combined roles"
                    .to_string(),
            );
        }
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_STORAGE_NODE_SOCKETS must not be empty".to_string());
    }

    let mut by_node = HashMap::<u32, String>::new();
    let mut socket_paths = HashSet::<PathBuf>::new();
    for raw in value.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("ARGMIN_STORAGE_NODE_SOCKETS contains an empty entry".to_string());
        }
        let (raw_node_id, raw_socket_path) = trimmed.split_once('=').ok_or_else(|| {
            format!(
                "ARGMIN_STORAGE_NODE_SOCKETS entry {trimmed:?} must be node_id=/absolute/socket"
            )
        })?;
        let node_id: u32 = raw_node_id.trim().parse().map_err(|e| {
            format!("invalid ARGMIN_STORAGE_NODE_SOCKETS node id {raw_node_id:?}: {e}")
        })?;
        if node_id >= local_node_count {
            return Err(format!(
                "ARGMIN_STORAGE_NODE_SOCKETS node id {node_id} must be less than ARGMIN_LOCAL_NODE_COUNT ({local_node_count})"
            ));
        }
        let socket_path = raw_socket_path.trim();
        if socket_path.is_empty() {
            return Err(format!(
                "ARGMIN_STORAGE_NODE_SOCKETS entry for node {node_id} has an empty socket path"
            ));
        }
        if !Path::new(socket_path).is_absolute() {
            return Err(format!(
                "ARGMIN_STORAGE_NODE_SOCKETS entry for node {node_id} must use an absolute socket path"
            ));
        }
        let canonical_socket_path = canonical_storage_node_socket_path(node_id, socket_path)?;
        if by_node.insert(node_id, socket_path.to_string()).is_some() {
            return Err(format!(
                "ARGMIN_STORAGE_NODE_SOCKETS contains duplicate node id {node_id}"
            ));
        }
        if !socket_paths.insert(canonical_socket_path) {
            return Err(format!(
                "ARGMIN_STORAGE_NODE_SOCKETS contains duplicate socket path {socket_path:?}"
            ));
        }
    }

    if require_complete {
        for node_id in 0..local_node_count {
            if !by_node.contains_key(&node_id) {
                return Err(format!(
                    "ARGMIN_STORAGE_NODE_SOCKETS must include node id {node_id}"
                ));
            }
        }
    }

    let mut entries: Vec<ConfiguredStorageNodeSocket> = by_node
        .into_iter()
        .map(|(node_id, socket_path)| ConfiguredStorageNodeSocket {
            node_id,
            socket_path,
        })
        .collect();
    entries.sort_by_key(|entry| entry.node_id);
    Ok(entries)
}

fn parse_control_plane_raft_peer_sockets(
    value: Option<String>,
) -> Result<Vec<ConfiguredControlPlaneRaftPeerSocket>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS must not be empty".to_string());
    }

    let mut by_node = HashMap::<u64, String>::new();
    let mut socket_paths = HashSet::<PathBuf>::new();
    for raw in value.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS contains an empty entry".to_string(),
            );
        }
        let (raw_node_id, raw_socket_path) = trimmed.split_once('=').ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS entry {trimmed:?} must be node_id=/absolute/socket"
            )
        })?;
        let node_id: u64 = raw_node_id.trim().parse().map_err(|e| {
            format!("invalid ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS node id {raw_node_id:?}: {e}")
        })?;
        if node_id == 0 {
            return Err("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS node id must be > 0".to_string());
        }
        let socket_path = raw_socket_path.trim();
        if socket_path.is_empty() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS entry for node {node_id} has an empty socket path"
            ));
        }
        if !Path::new(socket_path).is_absolute() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS entry for node {node_id} must use an absolute socket path"
            ));
        }
        let canonical_socket_path =
            canonical_control_plane_raft_peer_socket_path(node_id, socket_path)?;
        if by_node.insert(node_id, socket_path.to_string()).is_some() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS contains duplicate node id {node_id}"
            ));
        }
        if !socket_paths.insert(canonical_socket_path) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS contains duplicate socket path {socket_path:?}"
            ));
        }
    }

    let mut entries: Vec<ConfiguredControlPlaneRaftPeerSocket> = by_node
        .into_iter()
        .map(
            |(node_id, socket_path)| ConfiguredControlPlaneRaftPeerSocket {
                node_id,
                socket_path,
            },
        )
        .collect();
    entries.sort_by_key(|entry| entry.node_id);
    Ok(entries)
}

fn parse_control_plane_raft_auth_credentials(
    value: Option<String>,
) -> Result<Vec<ConfiguredControlPlaneRaftAuthCredential>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS must not be empty".to_string());
    }

    let mut entries = Vec::new();
    let mut seen = HashSet::<(u64, String, u64)>::new();
    for (entry_index, raw) in value.split(',').enumerate() {
        let entry_number = entry_index + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS entry {entry_number} is empty"
            ));
        }
        let (raw_node_id, raw_credential) = trimmed.split_once('=').ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS entry {entry_number} must be node_id=credential_id:version:secret"
            )
        })?;
        let node_id: u64 = raw_node_id.trim().parse().map_err(|e| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS node id in entry {entry_number}: {e}"
            )
        })?;
        if node_id == 0 {
            return Err(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS node id must be > 0".to_string(),
            );
        }
        let mut parts = raw_credential.splitn(3, ':');
        let credential_id = parts.next().unwrap_or_default().trim();
        let raw_version = parts.next().unwrap_or_default().trim();
        let secret = parts.next().unwrap_or_default();
        if credential_id.is_empty()
            || !credential_id
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b',' && b != b':' && b != b'=')
        {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS credential id for node {node_id} must be non-empty printable ASCII without ',', ':' or '='"
            ));
        }
        let credential_version: u64 = raw_version.parse().map_err(|e| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS credential version for node {node_id}: {e}"
            )
        })?;
        if credential_version == 0 {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS credential version for node {node_id} must be > 0"
            ));
        }
        if secret.is_empty() || secret.contains(',') {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS secret for node {node_id} must be non-empty and must not contain ','"
            ));
        }
        let entry = ConfiguredControlPlaneRaftAuthCredential {
            node_id,
            credential_id: credential_id.to_string(),
            credential_version,
            secret: BinarySecretConfigValue::from_utf8(secret.to_string()),
        };
        if !seen.insert((node_id, entry.credential_id.clone(), credential_version)) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS contains duplicate credential identity for node {node_id}"
            ));
        }
        entries.push(entry);
    }

    entries.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.credential_id.cmp(&right.credential_id))
            .then_with(|| left.credential_version.cmp(&right.credential_version))
    });
    Ok(entries)
}

fn parse_control_plane_storage_auth_credentials(
    value: Option<String>,
) -> Result<Vec<ConfiguredControlPlaneStorageAuthCredential>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS must not be empty".to_string());
    }

    let mut entries = Vec::new();
    let mut seen = HashSet::<(u32, String, u64)>::new();
    for (entry_index, raw) in value.split(',').enumerate() {
        let entry_number = entry_index + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS entry {entry_number} is empty"
            ));
        }
        let (raw_node_id, raw_credential) = trimmed.split_once('=').ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS entry {entry_number} must be node_id=credential_id:version:secret"
            )
        })?;
        let node_id: u32 = raw_node_id.trim().parse().map_err(|e| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS node id in entry {entry_number}: {e}"
            )
        })?;
        let mut parts = raw_credential.splitn(3, ':');
        let credential_id = parts.next().unwrap_or_default().trim();
        let raw_version = parts.next().unwrap_or_default().trim();
        let secret = parts.next().unwrap_or_default();
        if credential_id.is_empty()
            || !credential_id
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b',' && b != b':' && b != b'=')
        {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS credential id for node {node_id} must be non-empty printable ASCII without ',', ':' or '='"
            ));
        }
        let credential_version: u64 = raw_version.parse().map_err(|e| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS credential version for node {node_id}: {e}"
            )
        })?;
        if credential_version == 0 {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS credential version for node {node_id} must be > 0"
            ));
        }
        if secret.is_empty() || secret.contains(',') {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS secret for node {node_id} must be non-empty and must not contain ','"
            ));
        }
        let entry = ConfiguredControlPlaneStorageAuthCredential {
            node_id,
            credential_id: credential_id.to_string(),
            credential_version,
            secret: BinarySecretConfigValue::from_utf8(secret.to_string()),
        };
        if !seen.insert((node_id, entry.credential_id.clone(), credential_version)) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS contains duplicate credential identity for node {node_id}"
            ));
        }
        entries.push(entry);
    }

    entries.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.credential_id.cmp(&right.credential_id))
            .then_with(|| left.credential_version.cmp(&right.credential_version))
    });
    Ok(entries)
}

fn parse_control_plane_frontend_auth_credentials(
    value: Option<String>,
) -> Result<Vec<ConfiguredControlPlaneFrontendAuthCredential>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS must not be empty".to_string());
    }

    let mut entries = Vec::new();
    let mut seen = HashSet::<(String, String, u64)>::new();
    for (entry_index, raw) in value.split(',').enumerate() {
        let entry_number = entry_index + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS entry {entry_number} is empty"
            ));
        }
        let (raw_instance_id, raw_credential) = trimmed.split_once('=').ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS entry {entry_number} must be instance_id=credential_id:version:secret"
            )
        })?;
        let instance_id = raw_instance_id.trim();
        validate_auth_instance_id(
            instance_id,
            "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
        )?;
        let mut parts = raw_credential.splitn(3, ':');
        let credential_id = parts.next().unwrap_or_default().trim();
        let raw_version = parts.next().unwrap_or_default().trim();
        let secret = parts.next().unwrap_or_default();
        if !auth_config_token_is_valid(credential_id) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS credential id for instance {instance_id} must be non-empty printable ASCII without ',', ':' or '='"
            ));
        }
        let credential_version: u64 = raw_version.parse().map_err(|e| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS credential version for instance {instance_id}: {e}"
            )
        })?;
        if credential_version == 0 {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS credential version for instance {instance_id} must be > 0"
            ));
        }
        if secret.is_empty() || secret.contains(',') {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS secret for instance {instance_id} must be non-empty and must not contain ','"
            ));
        }
        let entry = ConfiguredControlPlaneFrontendAuthCredential {
            instance_id: instance_id.to_string(),
            credential_id: credential_id.to_string(),
            credential_version,
            secret: BinarySecretConfigValue::from_utf8(secret.to_string()),
        };
        if !seen.insert((
            entry.instance_id.clone(),
            entry.credential_id.clone(),
            credential_version,
        )) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS contains duplicate credential identity for instance {instance_id}"
            ));
        }
        entries.push(entry);
    }

    entries.sort_by(|left, right| {
        left.instance_id
            .cmp(&right.instance_id)
            .then_with(|| left.credential_id.cmp(&right.credential_id))
            .then_with(|| left.credential_version.cmp(&right.credential_version))
    });
    Ok(entries)
}

fn parse_control_plane_admin_auth_credentials(
    value: Option<String>,
) -> Result<Vec<ConfiguredControlPlaneAdminAuthCredential>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.trim().is_empty() {
        return Err("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS must not be empty".to_string());
    }

    let mut entries = Vec::new();
    let mut seen = HashSet::<(String, String, u64)>::new();
    for (entry_index, raw) in value.split(',').enumerate() {
        let entry_number = entry_index + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS entry {entry_number} is empty"
            ));
        }
        let (raw_instance_id, raw_credential) = trimmed.split_once('=').ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS entry {entry_number} must be instance_id=credential_id:version:secret"
            )
        })?;
        let instance_id = raw_instance_id.trim();
        validate_auth_instance_id(instance_id, "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS")?;
        let mut parts = raw_credential.splitn(3, ':');
        let credential_id = parts.next().unwrap_or_default().trim();
        let raw_version = parts.next().unwrap_or_default().trim();
        let secret = parts.next().unwrap_or_default();
        if !auth_config_token_is_valid(credential_id) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS credential id for instance {instance_id} must be non-empty printable ASCII without ',', ':' or '='"
            ));
        }
        let credential_version: u64 = raw_version.parse().map_err(|e| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS credential version for instance {instance_id}: {e}"
            )
        })?;
        if credential_version == 0 {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS credential version for instance {instance_id} must be > 0"
            ));
        }
        if secret.is_empty() || secret.contains(',') {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS secret for instance {instance_id} must be non-empty and must not contain ','"
            ));
        }
        let entry = ConfiguredControlPlaneAdminAuthCredential {
            instance_id: instance_id.to_string(),
            credential_id: credential_id.to_string(),
            credential_version,
            secret: BinarySecretConfigValue::from_utf8(secret.to_string()),
        };
        if !seen.insert((
            entry.instance_id.clone(),
            entry.credential_id.clone(),
            credential_version,
        )) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS contains duplicate credential identity for instance {instance_id}"
            ));
        }
        entries.push(entry);
    }

    entries.sort_by(|left, right| {
        left.instance_id
            .cmp(&right.instance_id)
            .then_with(|| left.credential_id.cmp(&right.credential_id))
            .then_with(|| left.credential_version.cmp(&right.credential_version))
    });
    Ok(entries)
}

fn validate_auth_instance_id(value: &str, field: &'static str) -> Result<(), String> {
    if auth_config_token_is_valid(value) {
        Ok(())
    } else {
        Err(format!(
            "{field} must be non-empty printable ASCII without ',', ':' or '='"
        ))
    }
}

fn auth_config_token_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b',' && b != b':' && b != b'=')
}

fn validate_control_plane_raft_auth_credentials_match_peer_policy(
    local_node_id: u64,
    peer_sockets: &[ConfiguredControlPlaneRaftPeerSocket],
    credentials: &[ConfiguredControlPlaneRaftAuthCredential],
) -> Result<(), String> {
    let credential_nodes: HashSet<u64> = credentials.iter().map(|entry| entry.node_id).collect();
    if !credential_nodes.contains(&local_node_id) {
        return Err(format!(
            "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS must include local Raft node id {local_node_id}"
        ));
    }
    if peer_sockets.is_empty() {
        if credential_nodes.len() != 1 {
            return Err(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS without ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS may only include the local node"
                    .to_string(),
            );
        }
        return Ok(());
    }

    let peer_nodes: HashSet<u64> = peer_sockets.iter().map(|entry| entry.node_id).collect();
    for peer_node_id in &peer_nodes {
        if !credential_nodes.contains(peer_node_id) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS must include configured Raft peer node id {peer_node_id}"
            ));
        }
    }
    for credential_node_id in &credential_nodes {
        if !peer_nodes.contains(credential_node_id) {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS contains node id {credential_node_id} not present in ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS"
            ));
        }
    }
    Ok(())
}

fn canonical_storage_node_socket_path(node_id: u32, socket_path: &str) -> Result<PathBuf, String> {
    let path = Path::new(socket_path);
    let parent = path.parent().ok_or_else(|| {
        format!("ARGMIN_STORAGE_NODE_SOCKETS entry for node {node_id} is missing a parent")
    })?;
    let file_name = path.file_name().ok_or_else(|| {
        format!("ARGMIN_STORAGE_NODE_SOCKETS entry for node {node_id} is missing a file name")
    })?;
    let canonical_parent = parent.canonicalize().map_err(|e| {
        format!(
            "ARGMIN_STORAGE_NODE_SOCKETS parent for node {node_id} could not be canonicalized: {e}"
        )
    })?;
    Ok(canonical_parent.join(file_name))
}

fn canonical_control_plane_raft_peer_socket_path(
    node_id: u64,
    socket_path: &str,
) -> Result<PathBuf, String> {
    let path = Path::new(socket_path);
    let parent = path.parent().ok_or_else(|| {
        format!(
            "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS entry for node {node_id} is missing a parent"
        )
    })?;
    let file_name = path.file_name().ok_or_else(|| {
        format!("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS entry for node {node_id} is missing a file name")
    })?;
    let canonical_parent = parent.canonicalize().map_err(|e| {
        format!(
            "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS parent for node {node_id} could not be canonicalized: {e}"
        )
    })?;
    Ok(canonical_parent.join(file_name))
}

fn validate_account_id(name: &str, value: &str) -> Result<(), String> {
    if value.len() != 12 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{name} must be a 12-digit AWS account ID"));
    }
    Ok(())
}

fn optional_pair<F: Fn(&str) -> Option<String>>(
    get: &F,
    access_key_name: &str,
    secret_key_name: &str,
) -> Result<Option<(String, String)>, String> {
    match (get(access_key_name), get(secret_key_name)) {
        (Some(access_key_id), Some(secret_access_key)) => {
            Ok(Some((access_key_id, secret_access_key)))
        }
        (None, None) => Ok(None),
        (None, Some(_)) => Err(format!(
            "{access_key_name} is required with {secret_key_name}"
        )),
        (Some(_), None) => Err(format!(
            "{secret_key_name} is required with {access_key_name}"
        )),
    }
}

fn read_uat_credentials<F: Fn(&str) -> Option<String>>(
    get: &F,
    primary_account_id: &str,
) -> Result<Vec<ConfiguredCredential>, String> {
    let mut credentials = Vec::new();

    match (
        get("ARGMIN_UAT_ALT_ACCOUNT_ID"),
        get("ARGMIN_UAT_ALT_ACCESS_KEY_ID"),
        get("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY"),
    ) {
        (Some(account_id), Some(access_key_id), Some(secret_access_key)) => {
            validate_account_id("ARGMIN_UAT_ALT_ACCOUNT_ID", &account_id)?;
            if account_id == primary_account_id {
                return Err(
                    "ARGMIN_UAT_ALT_ACCOUNT_ID must differ from ARGMIN_ACCOUNT_ID".to_string(),
                );
            }
            credentials.push(ConfiguredCredential {
                access_key_id,
                secret_access_key: SecretKey::new(secret_access_key),
                account_id: account_id.clone(),
                principal: account_id.clone(),
                display_name: "argmin-uat-alt-account".to_string(),
                authorization_profile: ConfiguredCredentialProfile::OwnerAccountAdmin,
            });
        }
        (None, None, None) => {}
        _ => {
            return Err(
                "ARGMIN_UAT_ALT_ACCOUNT_ID, ARGMIN_UAT_ALT_ACCESS_KEY_ID, and ARGMIN_UAT_ALT_SECRET_ACCESS_KEY must be set together".to_string(),
            );
        }
    }

    if let Some((access_key_id, secret_access_key)) = optional_pair(
        get,
        "ARGMIN_UAT_SECOND_ACCESS_KEY_ID",
        "ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY",
    )? {
        credentials.push(ConfiguredCredential {
            access_key_id,
            secret_access_key: SecretKey::new(secret_access_key),
            account_id: primary_account_id.to_string(),
            principal: format!("arn:aws:iam::{primary_account_id}:user/limited"),
            display_name: "argmin-uat-second-user".to_string(),
            authorization_profile: ConfiguredCredentialProfile::Standard,
        });
    }

    if let Some((access_key_id, secret_access_key)) = optional_pair(
        get,
        "ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID",
        "ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY",
    )? {
        credentials.push(ConfiguredCredential {
            access_key_id,
            secret_access_key: SecretKey::new(secret_access_key),
            account_id: primary_account_id.to_string(),
            principal: format!("arn:aws:iam::{primary_account_id}:root"),
            display_name: "argmin-uat-owner-root".to_string(),
            authorization_profile: ConfiguredCredentialProfile::OwnerAccountAdmin,
        });
    }

    Ok(credentials)
}

fn reject_duplicate_access_keys(
    primary_access_key_id: &str,
    uat_credentials: &[ConfiguredCredential],
) -> Result<(), String> {
    let mut seen = HashSet::new();
    seen.insert(primary_access_key_id);
    for credential in uat_credentials {
        if !seen.insert(credential.access_key_id.as_str()) {
            return Err(format!(
                "duplicate access key ID in configured credentials: {}",
                credential.access_key_id
            ));
        }
    }
    Ok(())
}

fn reject_reserved_session_access_keys(
    primary_access_key_id: &str,
    uat_credentials: &[ConfiguredCredential],
) -> Result<(), String> {
    if auth::is_reserved_session_access_key_id(primary_access_key_id) {
        return Err("ARGMIN_ACCESS_KEY_ID uses the reserved ARGS session namespace".to_string());
    }
    if uat_credentials
        .iter()
        .any(|credential| auth::is_reserved_session_access_key_id(&credential.access_key_id))
    {
        return Err("a UAT access key ID uses the reserved ARGS session namespace".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_env<'a>(overrides: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| {
            overrides
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.to_string())
        }
    }

    fn make_required_env<'a>(
        overrides: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Option<String> + 'a {
        let mut values = required_only();
        for (key, value) in overrides {
            values.insert(key, value);
        }
        move |key| values.get(key).map(std::string::ToString::to_string)
    }

    fn required_only() -> HashMap<&'static str, &'static str> {
        let mut m = HashMap::new();
        m.insert("ARGMIN_ACCOUNT_ID", "111122223333");
        m.insert("ARGMIN_ACCESS_KEY_ID", "AKID");
        m.insert("ARGMIN_SECRET_ACCESS_KEY", "SECRET");
        m.insert(
            "ARGMIN_SSE_S3_WRAPPING_KEY",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        );
        m
    }

    fn assert_auth_credential_parse_error_redacts_secret<T, F>(
        parser: F,
        value: &str,
        expected_message: &str,
        secret: &str,
    ) where
        F: FnOnce(Option<String>) -> Result<Vec<T>, String>,
    {
        let err = match parser(Some(value.to_owned())) {
            Ok(_) => panic!("auth credential parser unexpectedly accepted {value:?}"),
            Err(err) => err,
        };
        assert!(err.contains(expected_message), "unexpected error: {err}");
        assert!(
            !err.contains(secret),
            "auth credential parse error leaked secret {secret:?}: {err}"
        );
        assert!(
            !err.contains(value),
            "auth credential parse error leaked raw entry {value:?}: {err}"
        );
    }

    fn lookup<'a>(m: &'a HashMap<&'a str, &'a str>) -> impl Fn(&str) -> Option<String> + 'a {
        move |key| m.get(key).map(std::string::ToString::to_string)
    }

    #[test]
    fn missing_access_key_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_ACCESS_KEY_ID"));
    }

    #[test]
    fn missing_secret_access_key() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_ACCESS_KEY_ID", "a"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_SECRET_ACCESS_KEY"));
    }

    #[test]
    fn missing_account_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_ACCOUNT_ID"));
    }

    #[test]
    fn invalid_account_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "not-an-account"),
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("12-digit AWS account ID"));
    }

    #[test]
    fn defaults_applied() {
        let m = required_only();
        let cfg = ServerConfig::from_lookup(lookup(&m)).unwrap();
        assert_eq!(cfg.process_role, ProcessRole::LegacyLocal);
        assert_eq!(cfg.listen_addr, "127.0.0.1:9000");
        assert_eq!(cfg.tls_cert_path, None);
        assert_eq!(cfg.tls_key_path, None);
        assert_eq!(cfg.data_dir, "./data");
        assert_eq!(cfg.pg_count, 16);
        assert_eq!(cfg.storage_node_ids, (0..6).collect::<Vec<_>>());
        assert_eq!(cfg.storage_node_id, None);
        assert_eq!(cfg.storage_node_data_dir, None);
        assert_eq!(cfg.storage_node_socket_path, None);
        assert!(cfg.storage_node_sockets.is_empty());
        assert_eq!(cfg.storage_cluster_epoch, 1);
        assert_eq!(cfg.storage_pg_ids, (0..16).collect::<Vec<_>>());
        assert_eq!(cfg.ec_k, 4);
        assert_eq!(cfg.ec_m, 2);
        assert_eq!(cfg.account_id, "111122223333");
        assert_eq!(cfg.region, "us-east-1");
        assert!(cfg.uat_credentials.is_empty());
        assert_eq!(cfg.workers, 4);
        assert_eq!(cfg.max_connections, 512);
        assert_eq!(cfg.max_inflight_requests, 32);
        assert_eq!(
            cfg.storage_node_rpc_admission_limit,
            LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT
        );
        assert_eq!(
            cfg.storage_node_rpc_admission_wait_timeout,
            LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT
        );
        assert_eq!(
            cfg.storage_node_rpc_control_admission_wait_timeout,
            LocalUnixStorageNodeClientConfig::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT
        );
        assert_eq!(cfg.control_plane_state_path, None);
        assert_eq!(cfg.control_plane_socket_path, None);
        assert_eq!(cfg.control_plane_auth_cluster_id, None);
        assert!(cfg.control_plane_storage_auth_credentials.is_empty());
        assert_eq!(cfg.control_plane_frontend_auth_instance_id, None);
        assert!(cfg.control_plane_frontend_auth_credentials.is_empty());
        assert_eq!(cfg.control_plane_admin_auth_instance_id, None);
        assert!(cfg.control_plane_admin_auth_credentials.is_empty());
        assert!(!cfg.control_plane_experimental_raft);
        assert_eq!(cfg.control_plane_raft_cluster_name, None);
        assert_eq!(cfg.control_plane_raft_node_id, None);
        assert_eq!(cfg.control_plane_raft_peer_socket_path, None);
        assert!(cfg.control_plane_raft_peer_sockets.is_empty());
        assert_eq!(
            cfg.control_plane_lease_scan_interval,
            Duration::from_millis(250)
        );
        assert_eq!(
            cfg.control_plane_frontend_refresh_interval,
            Duration::from_millis(250)
        );
        assert_eq!(
            cfg.control_plane_heartbeat_lease_duration,
            Duration::from_millis(2000)
        );
        assert_eq!(
            cfg.stream_read_chunk_size,
            server_core::coordinator::INTERNAL_SEGMENT_SIZE
        );
        assert!(!cfg.panic_on_500);
        assert!(!cfg.abort_on_500);
        assert_eq!(cfg.access_key_id, "AKID");
        assert_eq!(cfg.secret_access_key.as_str(), "SECRET");
        assert_eq!(cfg.host_id, None);
        assert!(cfg.sse_c_validator_key_b64.is_none());
        assert_eq!(
            cfg.sse_s3_wrapping_key_b64.as_str(),
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        );
    }

    #[test]
    fn custom_values() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_ACCESS_KEY_ID", "mykey"),
            ("ARGMIN_SECRET_ACCESS_KEY", "mysecret"),
            ("ARGMIN_HOST_ID", "custom-host-id"),
            ("ARGMIN_SSE_C_VALIDATOR_KEY", "Zm9v"),
            ("ARGMIN_SSE_S3_WRAPPING_KEY", "YmFy"),
            ("ARGMIN_LISTEN_ADDR", "0.0.0.0:8080"),
            ("ARGMIN_TLS_CERT_PATH", "/tmp/cert.pem"),
            ("ARGMIN_TLS_KEY_PATH", "/tmp/key.pem"),
            ("ARGMIN_DATA_DIR", "/tmp/storage"),
            ("ARGMIN_PG_COUNT", "32"),
            ("ARGMIN_STORAGE_CLUSTER_EPOCH", "7"),
            ("ARGMIN_STORAGE_PG_IDS", "2, 5, 31"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
                "2=storage-node:3:storage-2-secret,1=storage-node:3:storage-1-secret",
            ),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
                "frontend-a",
            ),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                "frontend-b=frontend:5:frontend-b-secret,frontend-a=frontend:5:frontend-a-secret",
            ),
            ("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID", "admin-a"),
            (
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                "admin-b=admin:6:admin-b-secret,admin-a=admin:6:admin-a-secret",
            ),
            ("ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS", "125"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-control"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "7"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/control-plane-raft-7.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "8=/tmp/control-plane-raft-8.sock,7=/tmp/control-plane-raft-7.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "7=raft-peer:1:peer-7-secret,8=raft-peer:1:peer-8-secret",
            ),
            ("ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS", "200"),
            ("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS", "2900"),
            ("ARGMIN_LOCAL_NODE_COUNT", "12"),
            ("ARGMIN_EC_K", "8"),
            ("ARGMIN_EC_M", "4"),
            ("ARGMIN_REGION", "eu-west-1"),
        ]))
        .unwrap();
        assert_eq!(cfg.listen_addr, "0.0.0.0:8080");
        assert_eq!(cfg.tls_cert_path.as_deref(), Some("/tmp/cert.pem"));
        assert_eq!(cfg.tls_key_path.as_deref(), Some("/tmp/key.pem"));
        assert_eq!(cfg.data_dir, "/tmp/storage");
        assert_eq!(cfg.pg_count, 32);
        assert_eq!(cfg.storage_cluster_epoch, 7);
        assert_eq!(cfg.storage_pg_ids, vec![2, 5, 31]);
        assert_eq!(
            cfg.control_plane_state_path.as_deref(),
            Some("/tmp/control-plane.state")
        );
        assert_eq!(
            cfg.control_plane_socket_path.as_deref(),
            Some("/tmp/control-plane.sock")
        );
        assert_eq!(
            cfg.control_plane_auth_cluster_id.as_deref(),
            Some("control-auth")
        );
        assert_eq!(
            cfg.control_plane_storage_auth_credentials,
            vec![
                ConfiguredControlPlaneStorageAuthCredential {
                    node_id: 1,
                    credential_id: "storage-node".to_string(),
                    credential_version: 3,
                    secret: BinarySecretConfigValue::from_utf8("storage-1-secret".to_string()),
                },
                ConfiguredControlPlaneStorageAuthCredential {
                    node_id: 2,
                    credential_id: "storage-node".to_string(),
                    credential_version: 3,
                    secret: BinarySecretConfigValue::from_utf8("storage-2-secret".to_string()),
                },
            ]
        );
        assert_eq!(
            cfg.control_plane_frontend_auth_instance_id.as_deref(),
            Some("frontend-a")
        );
        assert_eq!(
            cfg.control_plane_frontend_auth_credentials,
            vec![
                ConfiguredControlPlaneFrontendAuthCredential {
                    instance_id: "frontend-a".to_string(),
                    credential_id: "frontend".to_string(),
                    credential_version: 5,
                    secret: BinarySecretConfigValue::from_utf8("frontend-a-secret".to_string()),
                },
                ConfiguredControlPlaneFrontendAuthCredential {
                    instance_id: "frontend-b".to_string(),
                    credential_id: "frontend".to_string(),
                    credential_version: 5,
                    secret: BinarySecretConfigValue::from_utf8("frontend-b-secret".to_string()),
                },
            ]
        );
        assert_eq!(
            cfg.control_plane_admin_auth_instance_id.as_deref(),
            Some("admin-a")
        );
        assert_eq!(
            cfg.control_plane_admin_auth_credentials,
            vec![
                ConfiguredControlPlaneAdminAuthCredential {
                    instance_id: "admin-a".to_string(),
                    credential_id: "admin".to_string(),
                    credential_version: 6,
                    secret: BinarySecretConfigValue::from_utf8("admin-a-secret".to_string()),
                },
                ConfiguredControlPlaneAdminAuthCredential {
                    instance_id: "admin-b".to_string(),
                    credential_id: "admin".to_string(),
                    credential_version: 6,
                    secret: BinarySecretConfigValue::from_utf8("admin-b-secret".to_string()),
                },
            ]
        );
        assert!(cfg.control_plane_experimental_raft);
        assert_eq!(
            cfg.control_plane_raft_cluster_name.as_deref(),
            Some("raft-control")
        );
        assert_eq!(cfg.control_plane_raft_node_id, Some(7));
        assert_eq!(
            cfg.control_plane_raft_peer_socket_path.as_deref(),
            Some("/tmp/control-plane-raft-7.sock")
        );
        assert_eq!(
            cfg.control_plane_raft_peer_sockets,
            vec![
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 7,
                    socket_path: "/tmp/control-plane-raft-7.sock".to_string(),
                },
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 8,
                    socket_path: "/tmp/control-plane-raft-8.sock".to_string(),
                },
            ]
        );
        assert_eq!(
            cfg.control_plane_lease_scan_interval,
            Duration::from_millis(125)
        );
        assert_eq!(
            cfg.control_plane_frontend_refresh_interval,
            Duration::from_millis(200)
        );
        assert_eq!(
            cfg.control_plane_heartbeat_lease_duration,
            Duration::from_millis(2900)
        );
        assert_eq!(cfg.storage_node_ids, (0..12).collect::<Vec<_>>());
        assert_eq!(cfg.ec_k, 8);
        assert_eq!(cfg.ec_m, 4);
        assert_eq!(cfg.account_id, "444455556666");
        assert_eq!(cfg.region, "eu-west-1");
        assert_eq!(cfg.workers, 4); // not overridden, uses default
        assert_eq!(cfg.max_inflight_requests, 32); // not overridden, uses default
        assert_eq!(
            cfg.storage_node_rpc_admission_limit,
            LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT
        );
        assert_eq!(
            cfg.storage_node_rpc_admission_wait_timeout,
            LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT
        );
        assert_eq!(
            cfg.stream_read_chunk_size,
            server_core::coordinator::INTERNAL_SEGMENT_SIZE
        );
        assert_eq!(cfg.access_key_id, "mykey");
        assert_eq!(cfg.secret_access_key.as_str(), "mysecret");
        assert_eq!(cfg.host_id.as_deref(), Some("custom-host-id"));
        assert_eq!(
            cfg.sse_c_validator_key_b64
                .as_ref()
                .map(SecretConfigValue::as_str),
            Some("Zm9v")
        );
        assert_eq!(cfg.sse_s3_wrapping_key_b64.as_str(), "YmFy");
    }

    #[test]
    fn debug_redacts_secret_config_values() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_SECRET_ACCESS_KEY", "primary-secret"),
            ("ARGMIN_SSE_C_VALIDATOR_KEY", "validator-secret"),
            ("ARGMIN_SSE_S3_WRAPPING_KEY", "wrapping-secret"),
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "alt-key"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
        ]))
        .unwrap();

        let debug = format!("{cfg:?}");
        for secret in [
            "primary-secret",
            "validator-secret",
            "wrapping-secret",
            "alt-secret",
        ] {
            assert!(
                !debug.contains(secret),
                "ServerConfig Debug leaked {secret:?}: {debug}"
            );
        }
        assert!(
            debug.contains("secret_key") && debug.contains("config_secret"),
            "ServerConfig Debug should include redaction labels: {debug}"
        );
    }

    #[test]
    fn process_role_storage_node_requires_storage_identity_and_socket() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_PROCESS_ROLE",
            "storage-node",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_ID"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_SOCKET_PATH"));
    }

    #[test]
    fn process_role_storage_node_parses_storage_config() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_STORAGE_NODE_ID", "2"),
            ("ARGMIN_STORAGE_NODE_DATA_DIR", "/tmp/argmin-node-2"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin/node-2.sock"),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::StorageNode);
        assert_eq!(cfg.storage_node_id, Some(2));
        assert_eq!(
            cfg.storage_node_data_dir.as_deref(),
            Some("/tmp/argmin-node-2")
        );
        assert_eq!(
            cfg.storage_node_socket_path.as_deref(),
            Some("/tmp/argmin/node-2.sock")
        );
        assert_eq!(cfg.storage_cluster_epoch, 1);
        assert_eq!(cfg.storage_pg_ids, (0..16).collect::<Vec<_>>());
    }

    #[test]
    fn process_role_control_plane_requires_state_path() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_CONTROL_PLANE_STATE_PATH"));
    }

    #[test]
    fn process_role_control_plane_requires_socket_path() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_CONTROL_PLANE_SOCKET_PATH"));
    }

    #[test]
    fn process_role_control_plane_does_not_require_frontend_or_storage_secrets() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::ControlPlane);
        assert_eq!(
            cfg.control_plane_state_path.as_deref(),
            Some("/tmp/argmin-control-plane.state")
        );
        assert_eq!(
            cfg.control_plane_socket_path.as_deref(),
            Some("/tmp/argmin-control-plane.sock")
        );
        assert_eq!(
            cfg.control_plane_lease_scan_interval,
            Duration::from_millis(250)
        );
        assert_eq!(
            cfg.control_plane_frontend_refresh_interval,
            Duration::from_millis(250)
        );
        assert_eq!(
            cfg.control_plane_heartbeat_lease_duration,
            Duration::from_millis(2000)
        );
        assert_eq!(cfg.account_id, "");
        assert_eq!(cfg.storage_node_id, None);
    }

    #[test]
    fn control_plane_parses_storage_auth_credentials() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
                "2=storage-node:4:storage-2-secret,1=storage-node:5:storage-1-new-secret,1=storage-node:4:storage-1-secret",
            ),
            (
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                "admin-1=admin:4:admin-1-secret",
            ),
        ]))
        .unwrap();

        assert_eq!(
            cfg.control_plane_auth_cluster_id.as_deref(),
            Some("control-auth")
        );
        assert_eq!(
            cfg.control_plane_storage_auth_credentials,
            vec![
                ConfiguredControlPlaneStorageAuthCredential {
                    node_id: 1,
                    credential_id: "storage-node".to_string(),
                    credential_version: 4,
                    secret: BinarySecretConfigValue::from_utf8("storage-1-secret".to_string()),
                },
                ConfiguredControlPlaneStorageAuthCredential {
                    node_id: 1,
                    credential_id: "storage-node".to_string(),
                    credential_version: 5,
                    secret: BinarySecretConfigValue::from_utf8("storage-1-new-secret".to_string(),),
                },
                ConfiguredControlPlaneStorageAuthCredential {
                    node_id: 2,
                    credential_id: "storage-node".to_string(),
                    credential_version: 4,
                    secret: BinarySecretConfigValue::from_utf8("storage-2-secret".to_string()),
                },
            ]
        );
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("storage-1-secret"));
        assert!(!debug.contains("storage-1-new-secret"));
        assert!(!debug.contains("storage-2-secret"));
        assert!(!debug.contains("admin-1-secret"));
        assert!(!debug.contains("frontend-a-secret"));
        assert!(!debug.contains("frontend-b-secret"));
        assert!(debug.contains("config_binary_secret"));
    }

    #[test]
    fn control_plane_storage_auth_credentials_reject_duplicate_identity() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
                "1=storage-node:4:storage-1-secret,1=storage-node:4:storage-1-secret-replacement",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("duplicate credential identity for node 1"));
    }

    #[test]
    fn control_plane_auth_credential_parse_errors_redact_raw_entries_and_secrets() {
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_raft_auth_credentials,
            "11:raft-peer:1:raft-leaked-secret",
            "entry 1 must be node_id=credential_id:version:secret",
            "raft-leaked-secret",
        );
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_raft_auth_credentials,
            "11=raft-peer:raft-leaked-version-secret",
            "credential version for node 11",
            "raft-leaked-version-secret",
        );
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_storage_auth_credentials,
            "1:storage-node:1:storage-leaked-secret",
            "entry 1 must be node_id=credential_id:version:secret",
            "storage-leaked-secret",
        );
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_storage_auth_credentials,
            "1=storage-node:storage-leaked-version-secret",
            "credential version for node 1",
            "storage-leaked-version-secret",
        );
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_frontend_auth_credentials,
            "frontend-a:frontend:1:frontend-leaked-secret",
            "entry 1 must be instance_id=credential_id:version:secret",
            "frontend-leaked-secret",
        );
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_frontend_auth_credentials,
            "frontend-a=frontend:frontend-leaked-version-secret",
            "credential version for instance frontend-a",
            "frontend-leaked-version-secret",
        );
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_admin_auth_credentials,
            "admin-a:admin:1:admin-leaked-secret",
            "entry 1 must be instance_id=credential_id:version:secret",
            "admin-leaked-secret",
        );
        assert_auth_credential_parse_error_redacts_secret(
            parse_control_plane_admin_auth_credentials,
            "admin-a=admin:admin-leaked-version-secret",
            "credential version for instance admin-a",
            "admin-leaked-version-secret",
        );
    }

    #[test]
    fn control_plane_storage_auth_credentials_require_cluster_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
                "1=storage-node:4:storage-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"));
    }

    #[test]
    fn control_plane_frontend_auth_credentials_require_cluster_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                "frontend-1=frontend:4:frontend-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"));
    }

    #[test]
    fn control_plane_admin_auth_credentials_require_cluster_id() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                "admin-1=admin:4:admin-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID"));
    }

    #[test]
    fn admin_auth_credentials_must_include_local_instance_when_set() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            ("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID", "admin-2"),
            (
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                "admin-1=admin:4:admin-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("local admin instance id admin-2"));
    }

    #[test]
    fn control_plane_storage_auth_requires_admin_credentials() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
                "1=storage-node:4:storage-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(
            err.contains("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS is required"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn control_plane_frontend_auth_requires_admin_credentials() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            (
                "ARGMIN_CONTROL_PLANE_STATE_PATH",
                "/tmp/argmin-control-plane.state",
            ),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                "frontend-1=frontend:4:frontend-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(
            err.contains("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS is required"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn frontend_auth_credentials_must_include_local_instance() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
                "frontend-2",
            ),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                "frontend-1=frontend:4:frontend-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("local frontend instance id frontend-2"));
    }

    #[test]
    fn frontend_runtime_map_auth_only_config_parses_credentials() {
        let auth = ConfiguredControlPlaneFrontendRuntimeMapAuth::from_lookup(make_env(&[
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
                "frontend-1",
            ),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                "frontend-2=frontend:4:frontend-2-secret,frontend-1=frontend:5:frontend-1-new-secret,frontend-1=frontend:4:frontend-1-secret",
            ),
        ]))
        .expect("auth-only config should parse")
        .expect("auth-only config should be enabled");

        assert_eq!(auth.cluster_id, "control-auth");
        assert_eq!(auth.instance_id, "frontend-1");
        assert_eq!(
            auth.credentials,
            vec![
                ConfiguredControlPlaneFrontendAuthCredential {
                    instance_id: "frontend-1".to_string(),
                    credential_id: "frontend".to_string(),
                    credential_version: 4,
                    secret: BinarySecretConfigValue::from_utf8("frontend-1-secret".to_string()),
                },
                ConfiguredControlPlaneFrontendAuthCredential {
                    instance_id: "frontend-1".to_string(),
                    credential_id: "frontend".to_string(),
                    credential_version: 5,
                    secret: BinarySecretConfigValue::from_utf8("frontend-1-new-secret".to_string(),),
                },
                ConfiguredControlPlaneFrontendAuthCredential {
                    instance_id: "frontend-2".to_string(),
                    credential_id: "frontend".to_string(),
                    credential_version: 4,
                    secret: BinarySecretConfigValue::from_utf8("frontend-2-secret".to_string()),
                },
            ]
        );
        let debug = format!("{auth:?}");
        assert!(!debug.contains("frontend-1-secret"));
        assert!(!debug.contains("frontend-1-new-secret"));
        assert!(!debug.contains("frontend-2-secret"));
        assert!(debug.contains("config_binary_secret"));
    }

    #[test]
    fn frontend_runtime_map_auth_only_config_rejects_duplicate_identity() {
        let err = ConfiguredControlPlaneFrontendRuntimeMapAuth::from_lookup(make_env(&[
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
                "frontend-1",
            ),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                "frontend-1=frontend:4:frontend-1-secret,frontend-1=frontend:4:frontend-1-secret-replacement",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("duplicate credential identity for instance frontend-1"));
    }

    #[test]
    fn frontend_runtime_map_auth_only_config_rejects_missing_local_instance() {
        let err = ConfiguredControlPlaneFrontendRuntimeMapAuth::from_lookup(make_env(&[
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
                "frontend-2",
            ),
            (
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
                "frontend-1=frontend:4:frontend-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("local frontend instance id frontend-2"));
    }

    #[test]
    fn admin_command_auth_only_config_parses_credentials() {
        let auth = ConfiguredControlPlaneAdminCommandAuth::from_lookup(make_env(&[
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            ("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID", "admin-1"),
            (
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                "admin-2=admin:4:admin-2-secret,admin-1=admin:5:admin-1-new-secret,admin-1=admin:4:admin-1-secret",
            ),
        ]))
        .expect("auth-only config should parse")
        .expect("auth-only config should be enabled");

        assert_eq!(auth.cluster_id, "control-auth");
        assert_eq!(auth.instance_id, "admin-1");
        assert_eq!(
            auth.credentials,
            vec![
                ConfiguredControlPlaneAdminAuthCredential {
                    instance_id: "admin-1".to_string(),
                    credential_id: "admin".to_string(),
                    credential_version: 4,
                    secret: BinarySecretConfigValue::from_utf8("admin-1-secret".to_string()),
                },
                ConfiguredControlPlaneAdminAuthCredential {
                    instance_id: "admin-1".to_string(),
                    credential_id: "admin".to_string(),
                    credential_version: 5,
                    secret: BinarySecretConfigValue::from_utf8("admin-1-new-secret".to_string(),),
                },
                ConfiguredControlPlaneAdminAuthCredential {
                    instance_id: "admin-2".to_string(),
                    credential_id: "admin".to_string(),
                    credential_version: 4,
                    secret: BinarySecretConfigValue::from_utf8("admin-2-secret".to_string()),
                },
            ]
        );
        let debug = format!("{auth:?}");
        assert!(!debug.contains("admin-1-secret"));
        assert!(!debug.contains("admin-1-new-secret"));
        assert!(!debug.contains("admin-2-secret"));
        assert!(debug.contains("config_binary_secret"));
    }

    #[test]
    fn admin_command_auth_only_config_rejects_duplicate_identity() {
        let err = ConfiguredControlPlaneAdminCommandAuth::from_lookup(make_env(&[
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            ("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID", "admin-1"),
            (
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                "admin-1=admin:4:admin-1-secret,admin-1=admin:4:admin-1-secret-replacement",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("duplicate credential identity for instance admin-1"));
    }

    #[test]
    fn admin_command_auth_only_config_rejects_missing_local_instance() {
        let err = ConfiguredControlPlaneAdminCommandAuth::from_lookup(make_env(&[
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            ("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID", "admin-2"),
            (
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
                "admin-1=admin:4:admin-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("local admin instance id admin-2"));
    }

    #[test]
    fn storage_node_storage_auth_credentials_must_include_local_node() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_STORAGE_NODE_ID", "2"),
            ("ARGMIN_STORAGE_NODE_DATA_DIR", "/tmp/argmin-node-2"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin/node-2.sock"),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
            ("ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID", "control-auth"),
            (
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
                "1=storage-node:4:storage-1-secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("local storage node id 2"));
    }

    #[test]
    fn process_role_frontend_can_use_control_plane_socket_without_static_node_sockets() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::Frontend);
        assert_eq!(
            cfg.control_plane_socket_path.as_deref(),
            Some("/tmp/argmin-control-plane.sock")
        );
        assert!(cfg.storage_node_sockets.is_empty());
    }

    #[test]
    fn experimental_raft_control_plane_defaults_local_node_id() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
        ]))
        .unwrap();

        assert!(cfg.control_plane_experimental_raft);
        assert_eq!(cfg.control_plane_raft_node_id, Some(1));
        assert_eq!(cfg.control_plane_raft_cluster_name, None);
        assert_eq!(cfg.control_plane_raft_peer_socket_path, None);
        assert!(cfg.control_plane_raft_peer_sockets.is_empty());
    }

    #[test]
    fn experimental_raft_control_plane_parses_peer_socket_map() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "12=/tmp/argmin-cp-raft-12.sock,11=/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "12=raft-peer:1:peer-12-secret,11=raft-peer:1:peer-11-secret",
            ),
        ]))
        .unwrap();

        assert_eq!(
            cfg.control_plane_raft_cluster_name.as_deref(),
            Some("raft-cluster-a")
        );
        assert_eq!(cfg.control_plane_raft_node_id, Some(11));
        assert_eq!(
            cfg.control_plane_raft_peer_socket_path.as_deref(),
            Some("/tmp/argmin-cp-raft-11.sock")
        );
        assert_eq!(
            cfg.control_plane_raft_peer_sockets,
            vec![
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 11,
                    socket_path: "/tmp/argmin-cp-raft-11.sock".to_string(),
                },
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 12,
                    socket_path: "/tmp/argmin-cp-raft-12.sock".to_string(),
                },
            ]
        );
        assert_eq!(cfg.control_plane_raft_auth_credentials.len(), 2);
    }

    #[test]
    fn experimental_raft_control_plane_parses_peer_auth_credentials() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "12=/tmp/argmin-cp-raft-12.sock,11=/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "12=raft-peer:7:peer-12-secret,11=raft-peer:8:peer-11-new-secret,11=raft-peer:7:peer-11-secret",
            ),
        ]))
        .unwrap();

        assert_eq!(
            cfg.control_plane_raft_auth_credentials,
            vec![
                ConfiguredControlPlaneRaftAuthCredential {
                    node_id: 11,
                    credential_id: "raft-peer".to_string(),
                    credential_version: 7,
                    secret: BinarySecretConfigValue::from_utf8("peer-11-secret".to_string()),
                },
                ConfiguredControlPlaneRaftAuthCredential {
                    node_id: 11,
                    credential_id: "raft-peer".to_string(),
                    credential_version: 8,
                    secret: BinarySecretConfigValue::from_utf8("peer-11-new-secret".to_string(),),
                },
                ConfiguredControlPlaneRaftAuthCredential {
                    node_id: 12,
                    credential_id: "raft-peer".to_string(),
                    credential_version: 7,
                    secret: BinarySecretConfigValue::from_utf8("peer-12-secret".to_string()),
                },
            ]
        );
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("peer-11-secret"));
        assert!(!debug.contains("peer-11-new-secret"));
        assert!(!debug.contains("peer-12-secret"));
        assert!(debug.contains("config_binary_secret"));
    }

    #[test]
    fn experimental_raft_control_plane_rejects_config_without_flag() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_CONTROL_PLANE_RAFT_*"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "1=raft-peer:1:secret",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_CONTROL_PLANE_RAFT_*"));
    }

    #[test]
    fn experimental_raft_control_plane_rejects_invalid_peer_config() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "0"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID must be > 0"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "relative.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("absolute socket path"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "1=/tmp/argmin-cp-raft-1.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH is required"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "12=/tmp/argmin-cp-raft-12.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("must include local Raft node id 11"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "11=/tmp/argmin-cp-raft-other.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("must match ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH"));
    }

    #[test]
    fn experimental_raft_control_plane_rejects_invalid_peer_auth_config() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "11=/tmp/argmin-cp-raft-11.sock,12=/tmp/argmin-cp-raft-12.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("AUTH_CREDENTIALS is required"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "1=raft-peer:1:secret",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("requires ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "12=raft-peer:1:secret",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("must include local Raft node id 11"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "11=raft-peer:1:local,12=raft-peer:1:remote",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("may only include the local node"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "11=/tmp/argmin-cp-raft-11.sock,12=/tmp/argmin-cp-raft-12.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "11=raft-peer:1:local",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("must include configured Raft peer node id 12"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "1=bad:id:1:secret",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("credential version"));

        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "control-plane"),
            ("ARGMIN_CONTROL_PLANE_STATE_PATH", "/tmp/argmin-cp.state"),
            ("ARGMIN_CONTROL_PLANE_SOCKET_PATH", "/tmp/argmin-cp.sock"),
            ("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "true"),
            ("ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME", "raft-cluster-a"),
            ("ARGMIN_CONTROL_PLANE_RAFT_NODE_ID", "11"),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
                "/tmp/argmin-cp-raft-11.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
                "11=/tmp/argmin-cp-raft-11.sock,12=/tmp/argmin-cp-raft-12.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
                "11=raft-peer:7:peer-11-secret,11=raft-peer:7:peer-11-replacement-secret,12=raft-peer:7:peer-12-secret",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("duplicate credential identity for node 11"));
    }

    #[test]
    fn process_role_storage_node_does_not_require_frontend_secrets() {
        let cfg = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin-node-0.sock"),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::StorageNode);
        assert_eq!(cfg.account_id, "");
        assert_eq!(cfg.access_key_id, "");
        assert_eq!(cfg.secret_access_key.as_str(), "");
        assert!(cfg.uat_credentials.is_empty());
        assert_eq!(cfg.sse_s3_wrapping_key_b64.as_str(), "");
    }

    #[test]
    fn process_role_frontend_still_requires_frontend_config() {
        let err = ServerConfig::from_lookup(make_env(&[("ARGMIN_PROCESS_ROLE", "frontend")]))
            .unwrap_err();

        assert!(err.contains("ARGMIN_ACCOUNT_ID"));
    }

    #[test]
    fn process_role_frontend_requires_complete_storage_node_socket_map() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PROCESS_ROLE", "frontend")]))
                .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_SOCKETS"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            ("ARGMIN_LOCAL_NODE_COUNT", "2"),
            ("ARGMIN_STORAGE_NODE_SOCKETS", "0=/tmp/argmin-node-0.sock"),
        ]))
        .unwrap_err();
        assert!(err.contains("must include node id 1"));
    }

    #[test]
    fn process_role_frontend_parses_storage_node_socket_map() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            ("ARGMIN_LOCAL_NODE_COUNT", "2"),
            ("ARGMIN_EC_K", "1"),
            ("ARGMIN_EC_M", "1"),
            (
                "ARGMIN_STORAGE_NODE_SOCKETS",
                "1=/tmp/argmin-node-1.sock,0=/tmp/argmin-node-0.sock",
            ),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::Frontend);
        assert_eq!(
            cfg.storage_node_sockets,
            vec![
                ConfiguredStorageNodeSocket {
                    node_id: 0,
                    socket_path: "/tmp/argmin-node-0.sock".to_string(),
                },
                ConfiguredStorageNodeSocket {
                    node_id: 1,
                    socket_path: "/tmp/argmin-node-1.sock".to_string(),
                },
            ]
        );
    }

    #[test]
    fn process_role_frontend_rejects_relative_duplicate_or_out_of_range_socket_map() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            ("ARGMIN_LOCAL_NODE_COUNT", "1"),
            ("ARGMIN_STORAGE_NODE_SOCKETS", "0=relative.sock"),
        ]))
        .unwrap_err();
        assert!(err.contains("absolute socket path"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            ("ARGMIN_LOCAL_NODE_COUNT", "2"),
            ("ARGMIN_EC_K", "1"),
            ("ARGMIN_EC_M", "1"),
            (
                "ARGMIN_STORAGE_NODE_SOCKETS",
                "0=/tmp/argmin-node.sock,0=/tmp/argmin-other.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("duplicate node id 0"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            ("ARGMIN_LOCAL_NODE_COUNT", "2"),
            ("ARGMIN_EC_K", "1"),
            ("ARGMIN_EC_M", "1"),
            (
                "ARGMIN_STORAGE_NODE_SOCKETS",
                "0=/tmp/argmin-node.sock,1=/tmp/argmin-node.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("duplicate socket path"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            ("ARGMIN_LOCAL_NODE_COUNT", "2"),
            ("ARGMIN_EC_K", "1"),
            ("ARGMIN_EC_M", "1"),
            (
                "ARGMIN_STORAGE_NODE_SOCKETS",
                "0=/tmp/argmin-node.sock,1=/tmp/../tmp/argmin-node.sock",
            ),
        ]))
        .unwrap_err();
        assert!(err.contains("duplicate socket path"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            ("ARGMIN_LOCAL_NODE_COUNT", "1"),
            ("ARGMIN_STORAGE_NODE_SOCKETS", "1=/tmp/argmin-node-1.sock"),
        ]))
        .unwrap_err();
        assert!(err.contains("less than ARGMIN_LOCAL_NODE_COUNT"));
    }

    #[test]
    fn process_role_combined_requires_own_socket_to_match_socket_map() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "combined"),
            ("ARGMIN_LOCAL_NODE_COUNT", "1"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin-node-0.sock"),
            (
                "ARGMIN_STORAGE_NODE_SOCKETS",
                "0=/tmp/argmin-other-node-0.sock",
            ),
        ]))
        .unwrap_err();

        assert!(err.contains("must match ARGMIN_STORAGE_NODE_SOCKET_PATH"));
    }

    #[test]
    fn process_role_combined_allows_canonical_self_socket_match() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "combined"),
            ("ARGMIN_LOCAL_NODE_COUNT", "1"),
            ("ARGMIN_EC_K", "1"),
            ("ARGMIN_EC_M", "0"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
            (
                "ARGMIN_STORAGE_NODE_SOCKET_PATH",
                "/tmp/../tmp/argmin-node-0.sock",
            ),
            ("ARGMIN_STORAGE_NODE_SOCKETS", "0=/tmp/argmin-node-0.sock"),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::Combined);
    }

    #[test]
    fn process_role_combined_can_use_control_plane_socket_without_static_node_sockets() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "combined"),
            ("ARGMIN_STORAGE_NODE_ID", "1"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/node-1.sock"),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/argmin-control-plane.sock",
            ),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::Combined);
        assert_eq!(
            cfg.control_plane_socket_path.as_deref(),
            Some("/tmp/argmin-control-plane.sock")
        );
        assert!(cfg.storage_node_sockets.is_empty());
    }

    #[test]
    fn control_plane_client_socket_paths_include_primary_and_preserve_order() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/control-plane-1.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS",
                "/tmp/control-plane-1.sock,/tmp/control-plane-2.sock,/tmp/control-plane-3.sock",
            ),
        ]))
        .unwrap();

        assert_eq!(
            cfg.control_plane_client_socket_paths,
            [
                "/tmp/control-plane-1.sock",
                "/tmp/control-plane-2.sock",
                "/tmp/control-plane-3.sock",
            ]
        );
    }

    #[test]
    fn control_plane_client_socket_paths_reject_missing_primary() {
        let error = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "frontend"),
            (
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                "/tmp/control-plane-1.sock",
            ),
            (
                "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS",
                "/tmp/control-plane-2.sock,/tmp/control-plane-3.sock",
            ),
        ]))
        .unwrap_err();

        assert!(error.contains("must include ARGMIN_CONTROL_PLANE_SOCKET_PATH"));
    }

    #[test]
    fn process_role_combined_parses_frontend_and_storage_config() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "combined"),
            ("ARGMIN_LOCAL_NODE_COUNT", "1"),
            ("ARGMIN_EC_K", "1"),
            ("ARGMIN_EC_M", "0"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin-node-0.sock"),
            ("ARGMIN_STORAGE_NODE_SOCKETS", "0=/tmp/argmin-node-0.sock"),
        ]))
        .unwrap();

        assert_eq!(cfg.process_role, ProcessRole::Combined);
        assert_eq!(cfg.storage_node_id, Some(0));
        assert_eq!(
            cfg.storage_node_sockets,
            vec![ConfiguredStorageNodeSocket {
                node_id: 0,
                socket_path: "/tmp/argmin-node-0.sock".to_string(),
            }]
        );
    }

    #[test]
    fn process_role_rejects_unknown_value() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PROCESS_ROLE", "other")]))
            .unwrap_err();

        assert!(err.contains("ARGMIN_PROCESS_ROLE"));
    }

    #[test]
    fn storage_node_id_must_be_in_configured_local_node_set() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_LOCAL_NODE_COUNT", "6"),
            ("ARGMIN_STORAGE_NODE_ID", "6"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin/node-6.sock"),
        ]))
        .unwrap_err();

        assert!(err.contains("ARGMIN_STORAGE_NODE_ID"));
    }

    #[test]
    fn storage_cluster_epoch_must_be_nonzero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_STORAGE_CLUSTER_EPOCH", "0")]))
                .unwrap_err();

        assert!(err.contains("ARGMIN_STORAGE_CLUSTER_EPOCH must be > 0"));
    }

    #[test]
    fn storage_pg_ids_must_be_unique_and_within_pg_count() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PG_COUNT", "4"),
            ("ARGMIN_STORAGE_PG_IDS", "1,1"),
        ]))
        .unwrap_err();
        assert!(err.contains("duplicate PG id 1"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PG_COUNT", "4"),
            ("ARGMIN_STORAGE_PG_IDS", "4"),
        ]))
        .unwrap_err();
        assert!(err.contains("must be less than ARGMIN_PG_COUNT"));
    }

    #[test]
    fn uat_acceptance_credentials() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "alt"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
            ("ARGMIN_UAT_SECOND_ACCESS_KEY_ID", "second"),
            ("ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY", "second-secret"),
            ("ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID", "root"),
            ("ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY", "root-secret"),
        ]))
        .unwrap();

        assert_eq!(cfg.uat_credentials.len(), 3);
        assert_eq!(cfg.uat_credentials[0].access_key_id, "alt");
        assert_eq!(
            cfg.uat_credentials[0].secret_access_key.as_str(),
            "alt-secret"
        );
        assert_eq!(cfg.uat_credentials[0].account_id, "444455556666");
        assert_eq!(cfg.uat_credentials[0].principal, "444455556666");
        assert_eq!(
            cfg.uat_credentials[0].display_name,
            "argmin-uat-alt-account"
        );
        assert_eq!(
            cfg.uat_credentials[0].authorization_profile,
            ConfiguredCredentialProfile::OwnerAccountAdmin
        );

        assert_eq!(cfg.uat_credentials[1].access_key_id, "second");
        assert_eq!(cfg.uat_credentials[1].account_id, "111122223333");
        assert_eq!(
            cfg.uat_credentials[1].principal,
            "arn:aws:iam::111122223333:user/limited"
        );
        assert_eq!(
            cfg.uat_credentials[1].display_name,
            "argmin-uat-second-user"
        );
        assert_eq!(
            cfg.uat_credentials[1].authorization_profile,
            ConfiguredCredentialProfile::Standard
        );

        assert_eq!(cfg.uat_credentials[2].access_key_id, "root");
        assert_eq!(cfg.uat_credentials[2].account_id, "111122223333");
        assert_eq!(
            cfg.uat_credentials[2].principal,
            "arn:aws:iam::111122223333:root"
        );
        assert_eq!(cfg.uat_credentials[2].display_name, "argmin-uat-owner-root");
        assert_eq!(
            cfg.uat_credentials[2].authorization_profile,
            ConfiguredCredentialProfile::OwnerAccountAdmin
        );
    }

    #[test]
    fn uat_alt_credentials_must_be_complete() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_UAT_ALT_ACCESS_KEY_ID",
            "alt",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_UAT_ALT_ACCOUNT_ID"));
    }

    #[test]
    fn uat_alt_account_must_differ_from_primary() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "alt"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
        ]))
        .unwrap_err();
        assert!(err.contains("must differ"));
    }

    #[test]
    fn uat_second_credentials_must_be_complete() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY",
            "second-secret",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_UAT_SECOND_ACCESS_KEY_ID"));
    }

    #[test]
    fn uat_access_keys_must_be_unique() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "AKID"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
        ]))
        .unwrap_err();
        assert!(err.contains("duplicate access key ID"));
    }

    #[test]
    fn configured_long_lived_access_keys_reject_reserved_session_namespace() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_ACCESS_KEY_ID",
            "ARGS-configured-primary",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_ACCESS_KEY_ID"));
        assert!(err.contains("reserved ARGS session namespace"));

        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_UAT_ALT_ACCOUNT_ID", "444455556666"),
            ("ARGMIN_UAT_ALT_ACCESS_KEY_ID", "ARGS-configured-alt"),
            ("ARGMIN_UAT_ALT_SECRET_ACCESS_KEY", "alt-secret"),
        ]))
        .unwrap_err();
        assert!(err.contains("UAT access key ID"));
        assert!(err.contains("reserved ARGS session namespace"));
    }

    #[test]
    fn invalid_host_id() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_HOST_ID", "bad host")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_HOST_ID"));
    }

    #[test]
    fn missing_sse_s3_wrapping_key() {
        let err = ServerConfig::from_lookup(make_env(&[
            ("ARGMIN_ACCOUNT_ID", "111122223333"),
            ("ARGMIN_ACCESS_KEY_ID", "a"),
            ("ARGMIN_SECRET_ACCESS_KEY", "s"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_SSE_S3_WRAPPING_KEY"));
    }

    #[test]
    fn custom_workers() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_WORKERS", "8")])).unwrap();
        assert_eq!(cfg.workers, 8);
    }

    #[test]
    fn workers_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_WORKERS", "0")])).unwrap_err();
        assert!(err.contains("ARGMIN_WORKERS must be > 0"));
    }

    #[test]
    fn invalid_workers() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_WORKERS", "abc")])).unwrap_err();
        assert!(err.contains("ARGMIN_WORKERS"));
    }

    #[test]
    fn invalid_pg_count_non_integer() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PG_COUNT", "abc")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_PG_COUNT"));
    }

    #[test]
    fn pg_count_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PG_COUNT", "0")])).unwrap_err();
        assert!(err.contains("ARGMIN_PG_COUNT must be > 0"));
    }

    #[test]
    fn invalid_local_node_count_non_integer() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_LOCAL_NODE_COUNT", "abc")]))
                .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT"));
    }

    #[test]
    fn local_node_count_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_LOCAL_NODE_COUNT", "0")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT must be > 0"));
    }

    #[test]
    fn local_node_count_rejects_value_above_allocation_bound() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_LOCAL_NODE_COUNT",
            "4294967295",
        )]))
        .unwrap_err();
        assert_eq!(
            err,
            format!("ARGMIN_LOCAL_NODE_COUNT must be <= {MAX_LOCAL_NODE_COUNT}")
        );
    }

    #[test]
    fn local_node_count_defaults_to_ec_shape_total() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_EC_K", "8"),
            ("ARGMIN_EC_M", "4"),
        ]))
        .unwrap();
        assert_eq!(cfg.storage_node_ids, (0..12).collect::<Vec<_>>());
    }

    #[test]
    fn local_node_count_one_rejected_for_default_ec_shape() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_NODE_COUNT", "1"),
            ("ARGMIN_EC_K", "4"),
            ("ARGMIN_EC_M", "2"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT"));
        assert!(err.contains("at least ARGMIN_EC_K + ARGMIN_EC_M (6)"));
    }

    #[test]
    fn local_node_count_too_small_for_multihost_ec_shape() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_NODE_COUNT", "5"),
            ("ARGMIN_EC_K", "4"),
            ("ARGMIN_EC_M", "2"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_NODE_COUNT"));
        assert!(err.contains("at least ARGMIN_EC_K + ARGMIN_EC_M (6)"));
    }

    #[test]
    fn local_node_count_accepts_first_valid_multihost_ec_shape() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_NODE_COUNT", "6"),
            ("ARGMIN_EC_K", "4"),
            ("ARGMIN_EC_M", "2"),
        ]))
        .unwrap();
        assert_eq!(cfg.storage_node_ids, (0..6).collect::<Vec<_>>());
    }

    #[test]
    fn invalid_ec_k() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_EC_K", "not_a_number")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_EC_K"));
    }

    #[test]
    fn invalid_ec_m() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_EC_M", "xyz")])).unwrap_err();
        assert!(err.contains("ARGMIN_EC_M"));
    }

    #[test]
    fn custom_max_connections() {
        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_CONNECTIONS", "1024")]))
                .unwrap();
        assert_eq!(cfg.max_connections, 1024);
    }

    #[test]
    fn max_connections_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_CONNECTIONS", "0")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_CONNECTIONS must be > 0"));
    }

    #[test]
    fn invalid_max_connections() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_MAX_CONNECTIONS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_CONNECTIONS"));
    }

    #[test]
    fn custom_max_inflight_requests() {
        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_INFLIGHT_REQUESTS", "64")]))
                .unwrap();
        assert_eq!(cfg.max_inflight_requests, 64);
    }

    #[test]
    fn max_inflight_requests_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_MAX_INFLIGHT_REQUESTS", "0")]))
                .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_INFLIGHT_REQUESTS must be > 0"));
    }

    #[test]
    fn invalid_max_inflight_requests() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_MAX_INFLIGHT_REQUESTS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_MAX_INFLIGHT_REQUESTS"));
    }

    #[test]
    fn custom_storage_node_rpc_admission_limit() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT",
            "8",
        )]))
        .unwrap();
        assert_eq!(cfg.storage_node_rpc_admission_limit, 8);
    }

    #[test]
    fn custom_storage_node_rpc_admission_wait_timeout() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS",
            "75",
        )]))
        .unwrap();
        assert_eq!(
            cfg.storage_node_rpc_admission_wait_timeout,
            Duration::from_millis(75)
        );
    }

    #[test]
    fn custom_storage_node_rpc_control_admission_wait_timeout() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS",
            "1500",
        )]))
        .unwrap();
        assert_eq!(
            cfg.storage_node_rpc_control_admission_wait_timeout,
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn storage_node_rpc_admission_limit_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT",
            "0",
        )]))
        .unwrap_err();
        assert!(
            err.contains("ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT must be >= 8"),
            "{err}"
        );
    }

    #[test]
    fn storage_node_rpc_admission_limit_below_minimum() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT",
            "7",
        )]))
        .unwrap_err();
        assert!(
            err.contains("ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT must be >= 8"),
            "{err}"
        );
    }

    #[test]
    fn storage_node_rpc_admission_wait_timeout_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS",
            "0",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS must be > 0"));
    }

    #[test]
    fn storage_node_rpc_control_admission_wait_timeout_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS",
            "0",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS must be > 0"));
    }

    #[test]
    fn control_plane_lease_scan_interval_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS",
            "0",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS must be > 0"));
    }

    #[test]
    fn control_plane_frontend_refresh_interval_zero() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS",
            "0",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS must be > 0"));
    }

    #[test]
    fn control_plane_heartbeat_lease_duration_too_short_for_renewal_margin() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS",
            "1999",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS must be >= 2000"));
    }

    #[test]
    fn control_plane_frontend_refresh_interval_is_independent_of_heartbeat_lease() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS", "1000"),
            ("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS", "2500"),
        ]))
        .unwrap();
        assert_eq!(
            cfg.control_plane_frontend_refresh_interval,
            Duration::from_millis(1000)
        );
        assert_eq!(
            cfg.control_plane_heartbeat_lease_duration,
            Duration::from_millis(2500)
        );
    }

    #[test]
    fn control_plane_heartbeat_lease_duration_must_not_exceed_authority_cap() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS",
            "10001",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS must be <= 10000"));
    }

    #[test]
    fn invalid_storage_node_rpc_admission_limit() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_RPC_ADMISSION_LIMIT"));
    }

    #[test]
    fn invalid_storage_node_rpc_admission_wait_timeout() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_RPC_ADMISSION_WAIT_MS"));
    }

    #[test]
    fn invalid_storage_node_rpc_control_admission_wait_timeout() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_STORAGE_NODE_RPC_CONTROL_ADMISSION_WAIT_MS"));
    }

    #[test]
    fn invalid_control_plane_lease_scan_interval() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_LEASE_SCAN_MS"));
    }

    #[test]
    fn invalid_control_plane_frontend_refresh_interval() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_FRONTEND_REFRESH_MS"));
    }

    #[test]
    fn invalid_control_plane_heartbeat_lease_duration() {
        let err = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS",
            "not_a_number",
        )]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS"));
    }

    #[test]
    fn custom_stream_read_chunk_size() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_STREAM_READ_CHUNK_SIZE",
            "8388608",
        )]))
        .unwrap();
        assert_eq!(cfg.stream_read_chunk_size, 8 * 1024 * 1024);
    }

    #[test]
    fn stream_read_chunk_size_zero() {
        let err =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_STREAM_READ_CHUNK_SIZE", "0")]))
                .unwrap_err();
        assert!(err.contains("ARGMIN_STREAM_READ_CHUNK_SIZE must be > 0"));
    }

    #[test]
    fn panic_on_500_accepts_boolean_values() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PANIC_ON_500", "true")]))
            .unwrap();
        assert!(cfg.panic_on_500);

        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PANIC_ON_500", "0")])).unwrap();
        assert!(!cfg.panic_on_500);
    }

    #[test]
    fn panic_on_500_rejects_invalid_boolean() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_PANIC_ON_500", "maybe")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_PANIC_ON_500"));
    }

    #[test]
    fn abort_on_500_accepts_boolean_values() {
        let cfg =
            ServerConfig::from_lookup(make_required_env(&[("ARGMIN_ABORT_ON_500", "on")])).unwrap();
        assert!(cfg.abort_on_500);

        let cfg = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_ABORT_ON_500", "false")]))
            .unwrap();
        assert!(!cfg.abort_on_500);
    }

    #[test]
    fn abort_on_500_rejects_invalid_boolean() {
        let err = ServerConfig::from_lookup(make_required_env(&[("ARGMIN_ABORT_ON_500", "maybe")]))
            .unwrap_err();
        assert!(err.contains("ARGMIN_ABORT_ON_500"));
    }

    #[test]
    fn local_debug_endpoint_defaults_to_disabled() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[])).unwrap();
        assert!(!cfg.local_debug_endpoint);
    }

    #[test]
    fn local_debug_endpoint_accepts_loopback_frontend_listener() {
        let cfg = ServerConfig::from_lookup(make_required_env(&[(
            "ARGMIN_LOCAL_DEBUG_ENDPOINT",
            "true",
        )]))
        .unwrap();
        assert!(cfg.local_debug_endpoint);

        let cfg = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_DEBUG_ENDPOINT", "on"),
            ("ARGMIN_LISTEN_ADDR", "[::1]:19000"),
        ]))
        .unwrap();
        assert!(cfg.local_debug_endpoint);
    }

    #[test]
    fn local_debug_endpoint_rejects_non_loopback_listener() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_LOCAL_DEBUG_ENDPOINT", "true"),
            ("ARGMIN_LISTEN_ADDR", "0.0.0.0:19000"),
        ]))
        .unwrap_err();
        assert!(err.contains("ARGMIN_LOCAL_DEBUG_ENDPOINT"));
        assert!(err.contains("loopback"));
    }

    #[test]
    fn local_debug_endpoint_rejects_storage_node_only_role() {
        let err = ServerConfig::from_lookup(make_required_env(&[
            ("ARGMIN_PROCESS_ROLE", "storage-node"),
            ("ARGMIN_LOCAL_DEBUG_ENDPOINT", "true"),
            ("ARGMIN_STORAGE_NODE_ID", "0"),
            ("ARGMIN_STORAGE_NODE_SOCKET_PATH", "/tmp/argmin-node-0.sock"),
        ]))
        .unwrap_err();
        assert!(err.contains("frontend process role"));
    }
}
