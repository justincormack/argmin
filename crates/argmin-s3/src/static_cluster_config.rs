use crate::config::{
    BinarySecretConfigValue, ConfiguredControlPlaneAdminAuthCredential,
    ConfiguredControlPlaneFrontendAuthCredential, ConfiguredControlPlaneRaftAuthCredential,
    ConfiguredControlPlaneRaftPeerListener, ConfiguredControlPlaneRaftPeerSocket,
    ConfiguredControlPlaneRpcListener, ConfiguredControlPlaneStorageAuthCredential,
    ConfiguredStaticClusterIdentity, ConfiguredStaticInitialClusterMap,
    ConfiguredStorageNodeSocket, ServerConfig,
};
use ec::EcConfig;
use placement::{
    ClusterMap, Level, NodeId, NodeInfo, PlacementConfig, PlacementConstraint, Placer, TopologyKey,
};
use rustls::client::danger::ServerCertVerifier;
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::pem::{PemObject, SectionKind};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::OpenOptions;
use std::future::Future;
use std::io;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::control_plane::{
    connect_unix_stream_until, ControlPlaneError, ControlPlaneRpcFrameExchange,
    ControlPlaneRpcFrameExchangeError, ControlPlaneRpcFrameTransport,
    InitialClusterTopologyCertificate, CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
    CONTROL_PLANE_RPC_MAX_SERVER_OPERATION_TIMEOUT, CONTROL_PLANE_RPC_TLS_ALPN,
    CONTROL_PLANE_TOPOLOGY_DIGEST_LEN,
};
use storage::control_plane_auth::{
    ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
    ControlPlaneScopedCredentialStore,
};
use storage::control_plane_command::ControlPlaneCommand;
use storage::control_plane_raft::{
    validate_control_plane_command_replication_size, ControlPlaneRaftPeerFrameExchange,
    ControlPlaneRaftPeerFrameExchangeError, ControlPlaneRaftPeerFrameTransport,
    ControlPlaneRaftPeerTransportLimits, ControlPlaneRaftPeerTransportPolicy,
    CONTROL_PLANE_RAFT_TLS_ALPN,
};
use storage::{
    FrontendStorageRpcClientCapability, MaintenanceStorageRpcClientCapability, PgId,
    StorageNodeStorageRpcClientCapability, StorageRpcServerAuthConfig, StorageRpcTransportLimits,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use x509_cert::der::Decode;
use x509_cert::ext::pkix::{BasicConstraints, KeyUsage};
use x509_cert::Certificate;

const CLUSTER_MANIFEST_MAX_BYTES: u64 = 4 * 1024 * 1024;
const CLUSTER_MANIFEST_MAX_COLLECTION_ITEMS: usize = 4_096;
const CLUSTER_MANIFEST_MAX_ID_BYTES: usize = 128;
const CLUSTER_MANIFEST_MAX_CLUSTER_ID_BYTES: usize = 256;
const CLUSTER_MANIFEST_MAX_REGION_BYTES: usize = 128;
const CLUSTER_MANIFEST_MAX_URI_BYTES: usize = 4_096;
const CLUSTER_MANIFEST_MAX_PATH_BYTES: usize = 4_096;
const CLUSTER_MANIFEST_MAX_FRAME_BYTES: u64 = 64 * 1024 * 1024;
const CLUSTER_MANIFEST_MAX_CONNECTIONS: u32 = 65_536;
const CLUSTER_MANIFEST_MAX_TIMEOUT_MS: u64 = 60_000;
const CLUSTER_MANIFEST_MAX_RAFT_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const CLUSTER_MANIFEST_MAX_SNAPSHOT_BYTES: u64 = 16 * 1024 * 1024;
const CLUSTER_MANIFEST_RAFT_APPEND_FIXED_FRAME_OVERHEAD_BYTES: u64 = 64 * 1024;
const CLUSTER_MANIFEST_RAFT_SNAPSHOT_FIXED_FRAME_OVERHEAD_BYTES: u64 = 64 * 1024;
const CLUSTER_MANIFEST_MAX_AUTH_SECRET_BYTES: u64 = 4 * 1024;
const CLUSTER_MANIFEST_MAX_TLS_CERTIFICATE_BYTES: u64 = 1024 * 1024;
const CLUSTER_MANIFEST_MAX_TLS_PRIVATE_KEY_BYTES: u64 = 64 * 1024;
const CLUSTER_MANIFEST_MAX_TLS_TRUST_BUNDLE_BYTES: u64 = 1024 * 1024;
const CLUSTER_MANIFEST_MAX_SELECTED_MATERIAL_FILES: usize = 256;
const CLUSTER_MANIFEST_MAX_SELECTED_MATERIAL_BYTES: u64 = 16 * 1024 * 1024;
const INITIAL_PG_PLACEMENT_KEY_DOMAIN: &[u8] = b"argmin-initial-pg-placement-v1";
const TOPOLOGY_IDENTITY_DOMAIN: &str = "argmin-static-cluster-topology-v1";
const PROCESS_IDENTITY_DOMAIN: &str = "argmin-static-cluster-process-identity-v1";
const FULL_CONFIG_FINGERPRINT_DOMAIN: &str = "argmin-static-cluster-full-config-v1";
const LEGACY_CLUSTER_ENV_KEYS: &[&str] = &[
    "ARGMIN_PROCESS_ROLE",
    "ARGMIN_HOST_ID",
    "ARGMIN_DATA_DIR",
    "ARGMIN_PG_COUNT",
    "ARGMIN_STORAGE_CLUSTER_EPOCH",
    "ARGMIN_STORAGE_PG_IDS",
    "ARGMIN_EC_K",
    "ARGMIN_EC_M",
    "ARGMIN_LOCAL_NODE_COUNT",
    "ARGMIN_STORAGE_NODE_ID",
    "ARGMIN_STORAGE_NODE_DATA_DIR",
    "ARGMIN_STORAGE_NODE_SOCKET_PATH",
    "ARGMIN_STORAGE_NODE_SOCKETS",
    "ARGMIN_CONTROL_PLANE_STATE_PATH",
    "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
    "ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS",
    "ARGMIN_CONTROL_PLANE_AUTH_CLUSTER_ID",
    "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS",
    "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_INSTANCE_ID",
    "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS",
    "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_INSTANCE_ID",
    "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS",
    "ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT",
    "ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME",
    "ARGMIN_CONTROL_PLANE_RAFT_NODE_ID",
    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH",
    "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKETS",
    "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS",
    "ARGMIN_REGION",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum DeploymentMode {
    Standalone,
    Replicated,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum FailureDomain {
    None,
    Disk,
    Host,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum InternalAuth {
    Required,
    Disabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum ProcessKind {
    AllInOne,
    Frontend,
    StorageNode,
    Combined,
    ControlPlane,
}

impl ProcessKind {
    fn has_frontend(self) -> bool {
        matches!(self, Self::AllInOne | Self::Frontend | Self::Combined)
    }

    fn has_storage_node(self) -> bool {
        matches!(self, Self::AllInOne | Self::StorageNode | Self::Combined)
    }

    fn has_control_plane(self) -> bool {
        matches!(self, Self::AllInOne | Self::ControlPlane)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum AuthorityKind {
    Single,
    RaftVoter,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
enum EndpointProtocol {
    RaftPeer,
    ControlPlane,
    AuthorityClockRecovery,
    StorageRpc,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
enum AuthPrincipal {
    RaftPeer,
    StorageNode,
    Frontend,
    Admin,
    Maintenance,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StaticClusterManifestInput {
    schema_version: u32,
    cluster: ClusterInput,
    deployment: DeploymentInput,
    storage: StorageInput,
    raft: RaftInput,
    transport_profiles: Vec<TransportProfileInput>,
    hosts: Vec<HostInput>,
    disks: Vec<DiskInput>,
    processes: Vec<ProcessInput>,
    authorities: Vec<AuthorityInput>,
    storage_nodes: Vec<StorageNodeInput>,
    endpoints: Vec<EndpointInput>,
    tls_identities: Vec<TlsIdentityInput>,
    tls_trust_bundles: Vec<TlsTrustBundleInput>,
    auth_credentials: Vec<AuthCredentialInput>,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ClusterInput {
    id: String,
    topology_generation: u64,
    region: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DeploymentInput {
    mode: DeploymentMode,
    failure_domain: FailureDomain,
    failure_tolerance: u8,
    internal_auth: InternalAuth,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StorageInput {
    pg_count: u32,
    ec_data_shards: u8,
    ec_parity_shards: u8,
    initial_cluster_epoch: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RaftInput {
    max_append_entries: u64,
    max_append_bytes: u64,
    max_snapshot_bytes: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TransportProfileInput {
    id: String,
    max_frame_bytes: u64,
    max_connections: u32,
    connect_timeout_ms: u64,
    io_timeout_ms: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct HostInput {
    id: String,
    zone: String,
    rack: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DiskInput {
    id: String,
    host_id: String,
    mount_path: PathBuf,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ProcessInput {
    id: String,
    host_id: String,
    kind: ProcessKind,
    frontend_instance_id: Option<String>,
    admin_instance_id: Option<String>,
    maintenance_instance_id: Option<String>,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AuthorityInput {
    id: String,
    kind: AuthorityKind,
    raft_node_id: Option<u64>,
    process_id: String,
    disk_id: String,
    state_path: PathBuf,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct StorageNodeInput {
    node_id: u32,
    process_id: String,
    disk_id: String,
    data_dir: PathBuf,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct EndpointInput {
    id: String,
    owner_process_id: String,
    protocol: EndpointProtocol,
    priority: u32,
    listen: String,
    advertise: String,
    transport_profile_id: String,
    tls_identity_id: Option<String>,
    tls_trust_bundle_id: Option<String>,
    tls_server_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalRaftPeerEndpoint {
    endpoint_id: String,
    owner_process_id: String,
    advertise: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalStorageNodeEndpoint {
    endpoint_id: String,
    owner_process_id: String,
    advertise: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TlsIdentityInput {
    id: String,
    certificate_ref: String,
    private_key_ref: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct TlsTrustBundleInput {
    id: String,
    ca_bundle_ref: String,
}

#[derive(Clone, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct AuthCredentialInput {
    principal: AuthPrincipal,
    node_id: Option<u64>,
    instance_id: Option<String>,
    credential_id: String,
    credential_version: u64,
    use_for_signing: bool,
    accept_from_ms: u64,
    accept_until_ms: Option<u64>,
    secret_ref: String,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ValidatedStaticClusterManifest {
    manifest: StaticClusterManifestInput,
    selected_process_index: usize,
    initial_pg_acting_sets: Vec<Vec<u32>>,
    canonical_raft_peer_endpoints: BTreeMap<u64, CanonicalRaftPeerEndpoint>,
    canonical_storage_node_endpoints: BTreeMap<u32, CanonicalStorageNodeEndpoint>,
    topology_digest: String,
    process_identity_digest: String,
    full_config_fingerprint: String,
}

struct ResolvedStaticAuthCredential {
    principal: CredentialPrincipalKey,
    credential_id: String,
    credential_version: u64,
    use_for_signing: bool,
    accept_from_ms: u64,
    accept_until_ms: Option<u64>,
    secret: Vec<u8>,
}

impl fmt::Debug for ResolvedStaticAuthCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedStaticAuthCredential")
            .field("principal", &self.principal)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("use_for_signing", &self.use_for_signing)
            .field("accept_from_ms", &self.accept_from_ms)
            .field("accept_until_ms", &self.accept_until_ms)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl Drop for ResolvedStaticAuthCredential {
    fn drop(&mut self) {
        self.secret.fill(0);
    }
}

struct ResolvedStaticTlsIdentity {
    certified_key: Arc<CertifiedKey>,
}

impl fmt::Debug for ResolvedStaticTlsIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedStaticTlsIdentity")
            .field("certificate_count", &self.certified_key.cert.len())
            .field("private_key", &"<redacted>")
            .finish()
    }
}

struct ResolvedStaticTlsTrustBundle {
    roots: Arc<RootCertStore>,
}

impl fmt::Debug for ResolvedStaticTlsTrustBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedStaticTlsTrustBundle")
            .field("root_count", &self.roots.len())
            .finish()
    }
}

pub(crate) struct ResolvedStaticClusterMaterial {
    auth_credentials: Vec<ResolvedStaticAuthCredential>,
    tls_identities: BTreeMap<String, ResolvedStaticTlsIdentity>,
    tls_trust_bundles: BTreeMap<String, ResolvedStaticTlsTrustBundle>,
}

#[derive(Clone)]
struct StaticSingleCertificateResolver {
    certified_key: Arc<CertifiedKey>,
}

impl fmt::Debug for StaticSingleCertificateResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticSingleCertificateResolver")
            .field("certificate_count", &self.certified_key.cert.len())
            .field("private_key", &"<redacted>")
            .finish()
    }
}

impl ResolvesServerCert for StaticSingleCertificateResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.certified_key))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolvedStaticRaftPeerAddress {
    Unix(PathBuf),
    Tcp {
        host: String,
        port: u16,
        server_name: String,
    },
}

#[derive(Clone)]
struct ResolvedStaticRaftPeerEndpoint {
    node_id: u64,
    endpoint_id: String,
    advertise: String,
    address: ResolvedStaticRaftPeerAddress,
    transport_profile_id: String,
    tls_client_config: Option<Arc<RustlsClientConfig>>,
}

impl fmt::Debug for ResolvedStaticRaftPeerEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedStaticRaftPeerEndpoint")
            .field("node_id", &self.node_id)
            .field("endpoint_id", &self.endpoint_id)
            .field("advertise", &self.advertise)
            .field("address", &self.address)
            .field("transport_profile_id", &self.transport_profile_id)
            .field("tls", &self.tls_client_config.is_some())
            .finish()
    }
}

#[derive(Clone)]
struct ResolvedStaticRaftListenerEndpoint {
    endpoint_id: String,
    listen: EndpointAddress,
    advertise: EndpointAddress,
    transport_profile_id: String,
    tls_server_config: Option<Arc<rustls::ServerConfig>>,
}

impl fmt::Debug for ResolvedStaticRaftListenerEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedStaticRaftListenerEndpoint")
            .field("endpoint_id", &self.endpoint_id)
            .field("listen", &self.listen)
            .field("advertise", &self.advertise)
            .field("transport_profile_id", &self.transport_profile_id)
            .field("tls", &self.tls_server_config.is_some())
            .finish()
    }
}

#[derive(Clone, Debug)]
struct ResolvedStaticRaftTransportPlan {
    local_node_id: u64,
    listeners: Vec<ResolvedStaticRaftListenerEndpoint>,
    peers: BTreeMap<u64, ResolvedStaticRaftPeerEndpoint>,
}

#[derive(Clone)]
struct StaticRaftTcpPeer {
    endpoint: String,
    host: String,
    port: u16,
    server_name: String,
    tls_client_config: Arc<RustlsClientConfig>,
}

impl fmt::Debug for StaticRaftTcpPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticRaftTcpPeer")
            .field("endpoint", &self.endpoint)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("server_name", &self.server_name)
            .field("tls", &true)
            .finish()
    }
}

#[derive(Clone)]
struct StaticRaftTcpPeerFrameTransport {
    peers: Arc<BTreeMap<u64, StaticRaftTcpPeer>>,
}

impl fmt::Debug for StaticRaftTcpPeerFrameTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticRaftTcpPeerFrameTransport")
            .field("peer_count", &self.peers.len())
            .finish()
    }
}

fn static_raft_tcp_exchange_context(
    context_prefix: &'static str,
    phase: &'static str,
) -> &'static str {
    match (context_prefix, phase) {
        ("", "write") => "write transport",
        ("", "read") => "read transport",
        ("full_snapshot ", "write") => "full_snapshot write transport",
        ("full_snapshot ", "read") => "full_snapshot read transport",
        (_, "connect") => "connect",
        (_, "tls") => "TLS handshake",
        _ => "TCP peer transport",
    }
}

fn static_raft_tcp_io_error(
    context: &'static str,
    source: io::Error,
) -> ControlPlaneRaftPeerFrameExchangeError {
    ControlPlaneRaftPeerFrameExchangeError::new(
        context,
        ControlPlaneError::Io {
            context: "exchange control-plane OpenRaft TLS/TCP peer frame",
            source,
        },
    )
}

async fn static_raft_tcp_io_until<T>(
    deadline: Instant,
    context: &'static str,
    operation: impl Future<Output = io::Result<T>>,
) -> Result<T, ControlPlaneRaftPeerFrameExchangeError> {
    match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(source)) => Err(static_raft_tcp_io_error(context, source)),
        Err(_) => Err(static_raft_tcp_io_error(
            context,
            io::Error::new(
                io::ErrorKind::TimedOut,
                "control-plane OpenRaft TLS/TCP peer deadline expired",
            ),
        )),
    }
}

async fn exchange_static_raft_tcp_peer_frame(
    peer: StaticRaftTcpPeer,
    exchange: ControlPlaneRaftPeerFrameExchange,
) -> Result<Vec<u8>, ControlPlaneRaftPeerFrameExchangeError> {
    if exchange.request_frame.len() > exchange.max_frame_bytes {
        return Err(ControlPlaneRaftPeerFrameExchangeError::new(
            static_raft_tcp_exchange_context(exchange.context_prefix, "write"),
            ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane OpenRaft TLS/TCP request frame size {} bytes exceeds limit {}",
                    exchange.request_frame.len(),
                    exchange.max_frame_bytes
                ),
            },
        ));
    }
    if peer.endpoint != exchange.endpoint {
        return Err(ControlPlaneRaftPeerFrameExchangeError::new(
            static_raft_tcp_exchange_context(exchange.context_prefix, "connect"),
            ControlPlaneError::RpcProtocol {
                message: format!(
                    "static Raft TCP endpoint mismatch for node {}",
                    exchange.target
                ),
            },
        ));
    }
    let connect_deadline = Instant::now()
        .checked_add(exchange.connect_timeout)
        .map_or(exchange.deadline, |configured| {
            configured.min(exchange.deadline)
        });
    let stream = static_raft_tcp_io_until(
        connect_deadline,
        static_raft_tcp_exchange_context(exchange.context_prefix, "connect"),
        TcpStream::connect((peer.host.as_str(), peer.port)),
    )
    .await?;
    let server_name = ServerName::try_from(peer.server_name.clone()).map_err(|_| {
        ControlPlaneRaftPeerFrameExchangeError::new(
            static_raft_tcp_exchange_context(exchange.context_prefix, "tls"),
            ControlPlaneError::RpcProtocol {
                message: "static Raft TCP peer has an invalid TLS server name".to_string(),
            },
        )
    })?;
    let mut stream = static_raft_tcp_io_until(
        exchange.deadline,
        static_raft_tcp_exchange_context(exchange.context_prefix, "tls"),
        TlsConnector::from(peer.tls_client_config).connect(server_name, stream),
    )
    .await?;
    if stream.get_ref().1.alpn_protocol() != Some(CONTROL_PLANE_RAFT_TLS_ALPN) {
        return Err(ControlPlaneRaftPeerFrameExchangeError::new(
            static_raft_tcp_exchange_context(exchange.context_prefix, "tls"),
            ControlPlaneError::RpcProtocol {
                message:
                    "control-plane OpenRaft TLS peer did not negotiate required argmin-raft/1 ALPN"
                        .to_string(),
            },
        ));
    }
    let request_len = u32::try_from(exchange.request_frame.len()).map_err(|_| {
        ControlPlaneRaftPeerFrameExchangeError::new(
            static_raft_tcp_exchange_context(exchange.context_prefix, "write"),
            ControlPlaneError::RpcProtocol {
                message: "control-plane OpenRaft TLS/TCP request frame length exceeds u32"
                    .to_string(),
            },
        )
    })?;
    static_raft_tcp_io_until(
        exchange.deadline,
        static_raft_tcp_exchange_context(exchange.context_prefix, "write"),
        async {
            stream.write_all(&request_len.to_be_bytes()).await?;
            stream.write_all(&exchange.request_frame).await
        },
    )
    .await?;
    let mut header = [0_u8; std::mem::size_of::<u32>()];
    static_raft_tcp_io_until(
        exchange.deadline,
        static_raft_tcp_exchange_context(exchange.context_prefix, "read"),
        stream.read_exact(&mut header),
    )
    .await?;
    let response_len = usize::try_from(u32::from_be_bytes(header)).map_err(|_| {
        ControlPlaneRaftPeerFrameExchangeError::new(
            static_raft_tcp_exchange_context(exchange.context_prefix, "read"),
            ControlPlaneError::RpcProtocol {
                message: "control-plane OpenRaft TLS/TCP response length does not fit usize"
                    .to_string(),
            },
        )
    })?;
    if response_len > exchange.max_frame_bytes {
        return Err(ControlPlaneRaftPeerFrameExchangeError::new(
            static_raft_tcp_exchange_context(exchange.context_prefix, "read"),
            ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane OpenRaft TLS/TCP response frame size {response_len} bytes exceeds limit {}",
                    exchange.max_frame_bytes
                ),
            },
        ));
    }
    let mut response = vec![0_u8; response_len];
    static_raft_tcp_io_until(
        exchange.deadline,
        static_raft_tcp_exchange_context(exchange.context_prefix, "read"),
        stream.read_exact(&mut response),
    )
    .await?;
    Ok(response)
}

impl ControlPlaneRaftPeerFrameTransport for StaticRaftTcpPeerFrameTransport {
    fn name(&self) -> &'static str {
        "TLS/TCP"
    }

    fn exchange(
        &self,
        exchange: ControlPlaneRaftPeerFrameExchange,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<u8>, ControlPlaneRaftPeerFrameExchangeError>>
                + Send
                + '_,
        >,
    > {
        let peer = self.peers.get(&exchange.target).cloned();
        Box::pin(async move {
            let peer = peer.ok_or_else(|| {
                ControlPlaneRaftPeerFrameExchangeError::new(
                    static_raft_tcp_exchange_context(exchange.context_prefix, "connect"),
                    ControlPlaneError::RpcProtocol {
                        message: format!(
                            "static Raft TCP transport has no peer for node {}",
                            exchange.target
                        ),
                    },
                )
            })?;
            exchange_static_raft_tcp_peer_frame(peer, exchange).await
        })
    }
}

#[derive(Clone)]
struct StaticControlPlaneTcpPeer {
    endpoint: String,
    host: String,
    port: u16,
    server_name: String,
    connect_timeout: Duration,
    max_frame_bytes: usize,
    tls_client_config: Arc<RustlsClientConfig>,
}

#[derive(Clone)]
struct StaticControlPlaneUnixPeer {
    endpoint: String,
    path: PathBuf,
    max_frame_bytes: usize,
}

#[derive(Clone)]
enum StaticControlPlanePeer {
    Unix(StaticControlPlaneUnixPeer),
    Tcp(StaticControlPlaneTcpPeer),
}

impl fmt::Debug for StaticControlPlaneTcpPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticControlPlaneTcpPeer")
            .field("endpoint", &self.endpoint)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("server_name", &self.server_name)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("tls", &true)
            .finish()
    }
}

#[derive(Clone)]
struct StaticControlPlaneFrameTransport {
    peers: Arc<BTreeMap<String, StaticControlPlanePeer>>,
}

struct ConfiguredStaticControlPlaneRpcClients {
    endpoints: Vec<String>,
    frame_transport: Option<Arc<dyn ControlPlaneRpcFrameTransport>>,
}

impl fmt::Debug for StaticControlPlaneFrameTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticControlPlaneFrameTransport")
            .field("peer_count", &self.peers.len())
            .finish()
    }
}

fn static_control_plane_unix_error(
    context: &'static str,
    source: io::Error,
    request_started: bool,
) -> ControlPlaneRpcFrameExchangeError {
    let error = ControlPlaneError::Io { context, source };
    if request_started {
        ControlPlaneRpcFrameExchangeError::after_request_started(error)
    } else {
        ControlPlaneRpcFrameExchangeError::before_request(error)
    }
}

fn static_control_plane_unix_remaining(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "control-plane static Unix RPC deadline expired",
        ));
    }
    Ok(remaining)
}

fn exchange_static_control_plane_unix_frame(
    peer: StaticControlPlaneUnixPeer,
    exchange: ControlPlaneRpcFrameExchange,
) -> Result<Vec<u8>, ControlPlaneRpcFrameExchangeError> {
    let frame_limit = exchange.max_frame_bytes.min(peer.max_frame_bytes);
    if exchange.request_frame.len() > frame_limit {
        return Err(ControlPlaneRpcFrameExchangeError::before_request(
            ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane static Unix request frame size {} bytes exceeds limit {}",
                    exchange.request_frame.len(),
                    frame_limit
                ),
            },
        ));
    }
    if peer.endpoint != exchange.endpoint {
        return Err(ControlPlaneRpcFrameExchangeError::before_request(
            ControlPlaneError::RpcProtocol {
                message: "static control-plane Unix endpoint identity mismatch".to_string(),
            },
        ));
    }
    let mut stream =
        connect_unix_stream_until(&peer.path, exchange.deadline).map_err(|source| {
            static_control_plane_unix_error(
                "connect control-plane static Unix endpoint",
                source,
                false,
            )
        })?;
    let mut written = 0;
    while written < exchange.request_frame.len() {
        let timeout = static_control_plane_unix_remaining(exchange.deadline).map_err(|source| {
            static_control_plane_unix_error(
                "set control-plane static Unix write deadline",
                source,
                written != 0,
            )
        })?;
        stream.set_write_timeout(Some(timeout)).map_err(|source| {
            static_control_plane_unix_error(
                "set control-plane static Unix write deadline",
                source,
                written != 0,
            )
        })?;
        let count = stream
            .write(&exchange.request_frame[written..])
            .map_err(|source| {
                static_control_plane_unix_error(
                    "write control-plane static Unix request frame",
                    source,
                    true,
                )
            })?;
        if count == 0 {
            return Err(static_control_plane_unix_error(
                "write control-plane static Unix request frame",
                io::Error::new(
                    io::ErrorKind::WriteZero,
                    "control-plane static Unix request write stalled",
                ),
                true,
            ));
        }
        written += count;
    }
    let mut response = Vec::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let timeout = static_control_plane_unix_remaining(exchange.deadline).map_err(|source| {
            static_control_plane_unix_error(
                "set control-plane static Unix read deadline",
                source,
                true,
            )
        })?;
        stream.set_read_timeout(Some(timeout)).map_err(|source| {
            static_control_plane_unix_error(
                "set control-plane static Unix read deadline",
                source,
                true,
            )
        })?;
        let count = stream.read(&mut buffer).map_err(|source| {
            static_control_plane_unix_error(
                "read control-plane static Unix response frame",
                source,
                true,
            )
        })?;
        if count == 0 {
            break;
        }
        if response.len().saturating_add(count) > frame_limit {
            return Err(ControlPlaneRpcFrameExchangeError::after_request_started(
                ControlPlaneError::RpcProtocol {
                    message: format!(
                        "control-plane static Unix response frame exceeds limit {frame_limit}"
                    ),
                },
            ));
        }
        response.extend_from_slice(&buffer[..count]);
    }
    Ok(response)
}

fn static_control_plane_tcp_error(
    context: &'static str,
    source: io::Error,
    request_started: bool,
) -> ControlPlaneRpcFrameExchangeError {
    let error = ControlPlaneError::Io { context, source };
    if request_started {
        ControlPlaneRpcFrameExchangeError::after_request_started(error)
    } else {
        ControlPlaneRpcFrameExchangeError::before_request(error)
    }
}

async fn static_control_plane_tcp_io_until<T>(
    deadline: Instant,
    context: &'static str,
    request_started: bool,
    operation: impl Future<Output = io::Result<T>>,
) -> Result<T, ControlPlaneRpcFrameExchangeError> {
    match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), operation).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(source)) => Err(static_control_plane_tcp_error(
            context,
            source,
            request_started,
        )),
        Err(_) => Err(static_control_plane_tcp_error(
            context,
            io::Error::new(
                io::ErrorKind::TimedOut,
                "control-plane TLS/TCP RPC deadline expired",
            ),
            request_started,
        )),
    }
}

async fn exchange_static_control_plane_tcp_frame(
    peer: StaticControlPlaneTcpPeer,
    exchange: ControlPlaneRpcFrameExchange,
) -> Result<Vec<u8>, ControlPlaneRpcFrameExchangeError> {
    let frame_limit = exchange.max_frame_bytes.min(peer.max_frame_bytes);
    if exchange.request_frame.len() > frame_limit {
        return Err(ControlPlaneRpcFrameExchangeError::before_request(
            ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane TLS/TCP request frame size {} bytes exceeds limit {}",
                    exchange.request_frame.len(),
                    frame_limit
                ),
            },
        ));
    }
    if peer.endpoint != exchange.endpoint {
        return Err(ControlPlaneRpcFrameExchangeError::before_request(
            ControlPlaneError::RpcProtocol {
                message: "static control-plane TCP endpoint identity mismatch".to_string(),
            },
        ));
    }
    let connect_deadline = Instant::now()
        .checked_add(peer.connect_timeout)
        .unwrap_or(exchange.deadline)
        .min(exchange.deadline);
    let stream = static_control_plane_tcp_io_until(
        connect_deadline,
        "connect control-plane TLS/TCP endpoint",
        false,
        TcpStream::connect((peer.host.as_str(), peer.port)),
    )
    .await?;
    let server_name = ServerName::try_from(peer.server_name.clone()).map_err(|_| {
        ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::RpcProtocol {
            message: "static control-plane TCP endpoint has an invalid TLS server name".to_string(),
        })
    })?;
    let mut stream = static_control_plane_tcp_io_until(
        exchange.deadline,
        "complete control-plane TLS client handshake",
        false,
        TlsConnector::from(peer.tls_client_config).connect(server_name, stream),
    )
    .await?;
    if stream.get_ref().1.alpn_protocol() != Some(CONTROL_PLANE_RPC_TLS_ALPN) {
        return Err(ControlPlaneRpcFrameExchangeError::before_request(
            ControlPlaneError::RpcProtocol {
                message:
                    "control-plane TLS peer did not negotiate required argmin-control-plane/1 ALPN"
                        .to_string(),
            },
        ));
    }
    static_control_plane_tcp_io_until(
        exchange.deadline,
        "write control-plane TLS/TCP request frame",
        true,
        stream.write_all(&exchange.request_frame),
    )
    .await?;
    let read_limit = u64::try_from(frame_limit)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut response = Vec::new();
    static_control_plane_tcp_io_until(
        exchange.deadline,
        "read control-plane TLS/TCP response frame",
        true,
        stream.take(read_limit).read_to_end(&mut response),
    )
    .await?;
    if response.len() > frame_limit {
        return Err(ControlPlaneRpcFrameExchangeError::after_request_started(
            ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane TLS/TCP response frame size {} bytes exceeds limit {}",
                    response.len(),
                    frame_limit
                ),
            },
        ));
    }
    Ok(response)
}

impl ControlPlaneRpcFrameTransport for StaticControlPlaneFrameTransport {
    fn name(&self) -> &'static str {
        "static Unix/TLS/TCP"
    }

    fn exchange(
        &self,
        exchange: ControlPlaneRpcFrameExchange,
    ) -> Result<Vec<u8>, ControlPlaneRpcFrameExchangeError> {
        let peer = self.peers.get(&exchange.endpoint).cloned().ok_or_else(|| {
            ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::RpcProtocol {
                message: format!(
                    "static control-plane transport has no endpoint {}",
                    exchange.endpoint
                ),
            })
        })?;
        let peer = match peer {
            StaticControlPlanePeer::Unix(peer) => {
                return exchange_static_control_plane_unix_frame(peer, exchange);
            }
            StaticControlPlanePeer::Tcp(peer) => peer,
        };
        let future = exchange_static_control_plane_tcp_frame(peer, exchange);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            Ok(_) => std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|source| {
                        ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::Io {
                            context: "create control-plane TLS/TCP client runtime",
                            source,
                        })
                    })?
                    .block_on(future)
            })
            .join()
            .map_err(|_| {
                ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::RpcProtocol {
                    message: "control-plane TLS/TCP client runtime thread panicked".to_string(),
                })
            })?,
            Err(_) => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source| {
                    ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::Io {
                        context: "create control-plane TLS/TCP client runtime",
                        source,
                    })
                })?
                .block_on(future),
        }
    }
}

#[derive(Clone, Copy)]
struct StaticMaterialLimits {
    max_files: usize,
    max_bytes: u64,
}

impl StaticMaterialLimits {
    const PRODUCTION: Self = Self {
        max_files: CLUSTER_MANIFEST_MAX_SELECTED_MATERIAL_FILES,
        max_bytes: CLUSTER_MANIFEST_MAX_SELECTED_MATERIAL_BYTES,
    };
}

struct StaticMaterialBudget {
    limits: StaticMaterialLimits,
    files_read: usize,
    bytes_read: u64,
}

impl StaticMaterialBudget {
    fn new(limits: StaticMaterialLimits) -> Self {
        Self {
            limits,
            files_read: 0,
            bytes_read: 0,
        }
    }

    fn read(
        &mut self,
        reference: &str,
        per_file_max_bytes: u64,
        access: StaticMaterialFileAccess,
        label: &str,
    ) -> Result<Vec<u8>, String> {
        if self.files_read >= self.limits.max_files {
            return Err(format!(
                "selected-process material exceeds the {}-file aggregate limit",
                self.limits.max_files
            ));
        }
        let remaining_bytes = self
            .limits
            .max_bytes
            .checked_sub(self.bytes_read)
            .ok_or_else(|| {
                "selected-process material exceeded its aggregate byte limit".to_string()
            })?;
        let bytes = read_static_material_file_with_aggregate_limit(
            reference,
            per_file_max_bytes,
            remaining_bytes,
            access,
            label,
        )?;
        self.files_read += 1;
        self.bytes_read = self
            .bytes_read
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| "selected-process material byte count overflowed".to_string())?;
        Ok(bytes)
    }
}

impl fmt::Debug for ResolvedStaticClusterMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedStaticClusterMaterial")
            .field("auth_credentials", &self.auth_credentials)
            .field("tls_identities", &self.tls_identities)
            .field("tls_trust_bundles", &self.tls_trust_bundles)
            .finish()
    }
}

impl ResolvedStaticClusterMaterial {
    pub(crate) fn auth_credential_count(&self) -> usize {
        self.auth_credentials.len()
    }

    pub(crate) fn tls_identity_count(&self) -> usize {
        self.tls_identities.len()
    }

    pub(crate) fn tls_trust_bundle_count(&self) -> usize {
        self.tls_trust_bundles.len()
    }
}

impl ValidatedStaticClusterManifest {
    pub(crate) fn cluster_id(&self) -> &str {
        &self.manifest.cluster.id
    }

    pub(crate) fn topology_generation(&self) -> u64 {
        self.manifest.cluster.topology_generation
    }

    pub(crate) fn selected_process_id(&self) -> &str {
        &self.manifest.processes[self.selected_process_index].id
    }

    pub(crate) fn deployment_mode(&self) -> &'static str {
        match self.manifest.deployment.mode {
            DeploymentMode::Standalone => "standalone",
            DeploymentMode::Replicated => "replicated",
        }
    }

    pub(crate) fn topology_digest(&self) -> &str {
        &self.topology_digest
    }

    pub(crate) fn process_identity_digest(&self) -> &str {
        &self.process_identity_digest
    }

    pub(crate) fn full_config_fingerprint(&self) -> &str {
        &self.full_config_fingerprint
    }

    pub(crate) fn resolve_selected_process_material(
        &self,
    ) -> Result<ResolvedStaticClusterMaterial, String> {
        self.resolve_selected_process_material_at(storage::clock::current_time_millis())
    }

    fn resolve_selected_process_material_at(
        &self,
        authority_now_ms: u64,
    ) -> Result<ResolvedStaticClusterMaterial, String> {
        self.resolve_selected_process_material_at_with_limits(
            authority_now_ms,
            StaticMaterialLimits::PRODUCTION,
        )
    }

    fn resolve_selected_process_material_at_with_limits(
        &self,
        authority_now_ms: u64,
        limits: StaticMaterialLimits,
    ) -> Result<ResolvedStaticClusterMaterial, String> {
        let mut material_budget = StaticMaterialBudget::new(limits);
        let required_principals = self.selected_process_auth_principals()?;
        let active_credentials = self
            .manifest
            .auth_credentials
            .iter()
            .filter_map(|credential| {
                let principal = credential_principal_key(credential)
                    .expect("validated credential has a canonical principal");
                let active = credential.accept_from_ms <= authority_now_ms
                    && credential
                        .accept_until_ms
                        .is_none_or(|until_ms| authority_now_ms < until_ms);
                (required_principals.contains(&principal) && active)
                    .then_some((credential, principal))
            })
            .collect::<Vec<_>>();
        if self.manifest.deployment.internal_auth == InternalAuth::Required {
            for required in &required_principals {
                let active_signers = active_credentials
                    .iter()
                    .filter(|(credential, principal)| {
                        principal == required && credential.use_for_signing
                    })
                    .count();
                if active_signers != 1 {
                    return Err(format!(
                        "required principal {required:?} must have exactly one signing credential active at process startup"
                    ));
                }
            }
        }
        let auth_credentials = active_credentials
            .into_iter()
            .map(|(credential, principal)| {
                let secret = material_budget.read(
                    &credential.secret_ref,
                    CLUSTER_MANIFEST_MAX_AUTH_SECRET_BYTES,
                    StaticMaterialFileAccess::Private,
                    "credential secret",
                )?;
                Ok(ResolvedStaticAuthCredential {
                    principal,
                    credential_id: credential.credential_id.clone(),
                    credential_version: credential.credential_version,
                    use_for_signing: credential.use_for_signing,
                    accept_from_ms: credential.accept_from_ms,
                    accept_until_ms: credential.accept_until_ms,
                    secret,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let selected_process = &self.manifest.processes[self.selected_process_index];
        let local_tcp_endpoints = self
            .manifest
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.owner_process_id == selected_process.id)
            .filter(|endpoint| endpoint.advertise.starts_with("tcp://"))
            .collect::<Vec<_>>();
        let required_tls_identity_ids = local_tcp_endpoints
            .iter()
            .filter_map(|endpoint| endpoint.tls_identity_id.as_deref())
            .collect::<BTreeSet<_>>();
        let required_tls_trust_bundle_ids = self
            .selected_process_outbound_tls_protocols()
            .into_iter()
            .flat_map(|protocol| {
                self.manifest
                    .endpoints
                    .iter()
                    .filter(move |endpoint| endpoint.protocol == protocol)
            })
            .filter(|endpoint| endpoint.advertise.starts_with("tcp://"))
            .filter_map(|endpoint| endpoint.tls_trust_bundle_id.as_deref())
            .chain(
                local_tcp_endpoints
                    .iter()
                    .filter_map(|endpoint| endpoint.tls_trust_bundle_id.as_deref()),
            )
            .collect::<BTreeSet<_>>();

        let provider = rustls::crypto::ring::default_provider();
        let mut tls_identities = BTreeMap::new();
        for identity in self
            .manifest
            .tls_identities
            .iter()
            .filter(|identity| required_tls_identity_ids.contains(identity.id.as_str()))
        {
            let certificate_bytes = material_budget.read(
                &identity.certificate_ref,
                CLUSTER_MANIFEST_MAX_TLS_CERTIFICATE_BYTES,
                StaticMaterialFileAccess::Public,
                "TLS certificate chain",
            )?;
            let certificates = parse_exact_certificate_pem(
                &certificate_bytes,
                &format!("TLS identity {} certificate reference", identity.id),
            )?;
            let private_key_bytes = material_budget.read(
                &identity.private_key_ref,
                CLUSTER_MANIFEST_MAX_TLS_PRIVATE_KEY_BYTES,
                StaticMaterialFileAccess::Private,
                "TLS private key",
            )?;
            let private_key = parse_exact_private_key_pem(
                &private_key_bytes,
                &format!("TLS identity {} private-key reference", identity.id),
            )?;
            let certified_key = CertifiedKey::from_der(certificates, private_key, &provider)
                .map_err(|_| {
                    format!(
                        "TLS identity {} certificate and private key are incompatible",
                        identity.id
                    )
                })?;
            tls_identities.insert(
                identity.id.clone(),
                ResolvedStaticTlsIdentity {
                    certified_key: Arc::new(certified_key),
                },
            );
        }

        let mut tls_trust_bundles = BTreeMap::new();
        for bundle in self
            .manifest
            .tls_trust_bundles
            .iter()
            .filter(|bundle| required_tls_trust_bundle_ids.contains(bundle.id.as_str()))
        {
            let bundle_bytes = material_budget.read(
                &bundle.ca_bundle_ref,
                CLUSTER_MANIFEST_MAX_TLS_TRUST_BUNDLE_BYTES,
                StaticMaterialFileAccess::Public,
                "TLS trust bundle",
            )?;
            let certificates = parse_exact_certificate_pem(
                &bundle_bytes,
                &format!("TLS trust bundle {} reference", bundle.id),
            )?;
            let mut roots = RootCertStore::empty();
            for certificate in certificates {
                validate_ca_trust_anchor(&certificate).map_err(|_| {
                    format!(
                        "TLS trust bundle {} must contain only CA certificates with critical CA constraints and certificate-signing usage",
                        bundle.id
                    )
                })?;
                roots.add(certificate).map_err(|_| {
                    format!(
                        "TLS trust bundle {} must contain only valid CA certificates",
                        bundle.id
                    )
                })?;
            }
            tls_trust_bundles.insert(
                bundle.id.clone(),
                ResolvedStaticTlsTrustBundle {
                    roots: Arc::new(roots),
                },
            );
        }

        for endpoint in local_tcp_endpoints {
            let identity_id = endpoint
                .tls_identity_id
                .as_deref()
                .expect("validated TCP endpoint has a TLS identity");
            let trust_bundle_id = endpoint
                .tls_trust_bundle_id
                .as_deref()
                .expect("validated TCP endpoint has a TLS trust bundle");
            let server_name = endpoint
                .tls_server_name
                .as_deref()
                .expect("validated TCP endpoint has a TLS server name");
            let identity = tls_identities
                .get(identity_id)
                .expect("selected-process TLS identity was resolved");
            let trust_bundle = tls_trust_bundles
                .get(trust_bundle_id)
                .expect("selected-process TLS trust bundle was resolved");
            let verifier = WebPkiServerVerifier::builder_with_provider(
                Arc::clone(&trust_bundle.roots),
                Arc::new(provider.clone()),
            )
            .build()
            .map_err(|_| {
                format!(
                    "TCP endpoint {} has an invalid TLS trust bundle",
                    endpoint.id
                )
            })?;
            let server_name = ServerName::try_from(server_name.to_string()).map_err(|_| {
                format!(
                    "TCP endpoint {} has an invalid TLS server name",
                    endpoint.id
                )
            })?;
            let (end_entity, intermediates) =
                identity.certified_key.cert.split_first().ok_or_else(|| {
                    format!(
                        "TCP endpoint {} TLS identity has no certificate",
                        endpoint.id
                    )
                })?;
            verifier
                .verify_server_cert(
                    end_entity,
                    intermediates,
                    &server_name,
                    &[],
                    UnixTime::now(),
                )
                .map_err(|_| {
                    format!(
                        "TCP endpoint {} TLS certificate does not match its server name or trust bundle",
                        endpoint.id
                    )
                })?;
        }

        Ok(ResolvedStaticClusterMaterial {
            auth_credentials,
            tls_identities,
            tls_trust_bundles,
        })
    }

    fn selected_process_auth_principals(&self) -> Result<BTreeSet<CredentialPrincipalKey>, String> {
        let selected_process = &self.manifest.processes[self.selected_process_index];
        let mut required = BTreeSet::new();

        required.extend(
            self.manifest
                .authorities
                .iter()
                .filter(|authority| authority.process_id == selected_process.id)
                .filter_map(|authority| authority.raft_node_id)
                .map(|node_id| CredentialPrincipalKey {
                    principal: AuthPrincipal::RaftPeer,
                    id: CredentialPrincipalId::Node(node_id),
                }),
        );
        required.extend(
            self.manifest
                .storage_nodes
                .iter()
                .filter(|storage_node| storage_node.process_id == selected_process.id)
                .map(|storage_node| CredentialPrincipalKey {
                    principal: AuthPrincipal::StorageNode,
                    id: CredentialPrincipalId::Node(u64::from(storage_node.node_id)),
                }),
        );
        if let Some(instance_id) = &selected_process.frontend_instance_id {
            required.insert(CredentialPrincipalKey {
                principal: AuthPrincipal::Frontend,
                id: CredentialPrincipalId::Instance(instance_id.clone()),
            });
        }
        if let Some(instance_id) = &selected_process.admin_instance_id {
            required.insert(CredentialPrincipalKey {
                principal: AuthPrincipal::Admin,
                id: CredentialPrincipalId::Instance(instance_id.clone()),
            });
        }
        if let Some(instance_id) = &selected_process.maintenance_instance_id {
            required.insert(CredentialPrincipalKey {
                principal: AuthPrincipal::Maintenance,
                id: CredentialPrincipalId::Instance(instance_id.clone()),
            });
        }

        for endpoint in self
            .manifest
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.owner_process_id == selected_process.id)
        {
            let accepted_roles = match endpoint.protocol {
                EndpointProtocol::RaftPeer => &[AuthPrincipal::RaftPeer][..],
                EndpointProtocol::ControlPlane
                | EndpointProtocol::AuthorityClockRecovery
                | EndpointProtocol::StorageRpc => &[
                    AuthPrincipal::StorageNode,
                    AuthPrincipal::Frontend,
                    AuthPrincipal::Admin,
                    AuthPrincipal::Maintenance,
                ][..],
            };
            for credential in &self.manifest.auth_credentials {
                if accepted_roles.contains(&credential.principal) {
                    required.insert(credential_principal_key(credential)?);
                }
            }
        }
        Ok(required)
    }

    fn selected_process_outbound_tls_protocols(&self) -> BTreeSet<EndpointProtocol> {
        let selected_process = &self.manifest.processes[self.selected_process_index];
        let mut protocols = BTreeSet::new();
        if selected_process.kind.has_control_plane() {
            protocols.insert(EndpointProtocol::RaftPeer);
        }
        if selected_process.kind.has_storage_node() || selected_process.kind.has_frontend() {
            protocols.insert(EndpointProtocol::ControlPlane);
            protocols.insert(EndpointProtocol::StorageRpc);
        }
        if selected_process.admin_instance_id.is_some()
            || selected_process.maintenance_instance_id.is_some()
        {
            protocols.insert(EndpointProtocol::ControlPlane);
            protocols.insert(EndpointProtocol::AuthorityClockRecovery);
        }
        protocols
    }

    pub(crate) fn initialize_selected_storage(&self) -> Result<(), String> {
        let selected = &self.manifest.processes[self.selected_process_index];
        if !selected.kind.has_storage_node() {
            return Err("selected process does not own durable storage-node state".to_string());
        }
        let storage_node = self
            .manifest
            .storage_nodes
            .iter()
            .find(|storage_node| storage_node.process_id == selected.id)
            .ok_or_else(|| "selected storage process has no storage node".to_string())?;
        let pg_ids: Vec<u32> = (0..self.manifest.storage.pg_count).collect();
        crate::static_cluster_state::initialize_static_storage(
            &self.configured_static_identity(),
            storage_node.node_id,
            &storage_node.data_dir,
            &pg_ids,
            storage::EcShape {
                k: self.manifest.storage.ec_data_shards,
                m: self.manifest.storage.ec_parity_shards,
            },
            storage::ClusterEpoch::new(self.manifest.storage.initial_cluster_epoch)
                .expect("validated static initial cluster epoch is nonzero"),
        )
    }

    pub(crate) fn initialize_selected_process_state(&self) -> Result<(), String> {
        match self.manifest.deployment.mode {
            DeploymentMode::Standalone => self.initialize_selected_storage(),
            DeploymentMode::Replicated => {
                let selected = &self.manifest.processes[self.selected_process_index];
                if selected.kind.has_storage_node() {
                    return self.initialize_selected_storage();
                }
                if selected.kind != ProcessKind::ControlPlane {
                    return Err(
                        "selected replicated process does not own durable state".to_string()
                    );
                }
                let authority = self
                    .manifest
                    .authorities
                    .iter()
                    .find(|authority| authority.process_id == selected.id)
                    .ok_or_else(|| {
                        "selected replicated control-plane process has no authority".to_string()
                    })?;
                let raft_node_id = authority.raft_node_id.ok_or_else(|| {
                    "selected replicated authority has no Raft node id".to_string()
                })?;
                crate::static_cluster_state::initialize_static_control_plane_identity(
                    &self.configured_static_identity(),
                    raft_node_id,
                    &authority.state_path,
                )
            }
        }
    }

    fn resolved_static_raft_transport_plan(
        &self,
        material: &ResolvedStaticClusterMaterial,
    ) -> Result<ResolvedStaticRaftTransportPlan, String> {
        let selected = &self.manifest.processes[self.selected_process_index];
        if self.manifest.deployment.mode != DeploymentMode::Replicated
            || selected.kind != ProcessKind::ControlPlane
        {
            return Err(
                "static Raft transport planning requires a replicated control-plane process"
                    .to_string(),
            );
        }
        let local_node_id = self
            .manifest
            .authorities
            .iter()
            .find(|authority| authority.process_id == selected.id)
            .and_then(|authority| authority.raft_node_id)
            .ok_or_else(|| "selected replicated authority has no Raft node id".to_string())?;
        let provider = rustls::crypto::ring::default_provider();
        let mut peers = BTreeMap::new();
        for (&node_id, canonical_endpoint) in &self.canonical_raft_peer_endpoints {
            let endpoint = self
                .manifest
                .endpoints
                .iter()
                .find(|endpoint| {
                    endpoint.id == canonical_endpoint.endpoint_id
                        && endpoint.owner_process_id == canonical_endpoint.owner_process_id
                        && endpoint.protocol == EndpointProtocol::RaftPeer
                        && endpoint.advertise == canonical_endpoint.advertise
                })
                .ok_or_else(|| {
                    format!(
                        "canonical Raft endpoint {} for voter {node_id} no longer matches its validated owner and address",
                        canonical_endpoint.endpoint_id
                    )
                })?;
            let (address, tls_client_config) =
                match parse_endpoint_address(&endpoint.advertise, false)? {
                    EndpointAddress::Unix(path) => {
                        (ResolvedStaticRaftPeerAddress::Unix(path), None)
                    }
                    EndpointAddress::Tcp { host, port } => {
                        let trust_bundle_id = endpoint
                            .tls_trust_bundle_id
                            .as_deref()
                            .expect("validated TCP endpoint has a TLS trust bundle");
                        let roots =
                            material
                                .tls_trust_bundles
                                .get(trust_bundle_id)
                                .ok_or_else(|| {
                                    format!(
                                "selected process did not resolve Raft endpoint {} trust bundle",
                                endpoint.id
                            )
                                })?;
                        let server_name = endpoint
                            .tls_server_name
                            .clone()
                            .expect("validated TCP endpoint has a TLS server name");
                        let mut client_config =
                            RustlsClientConfig::builder_with_provider(Arc::new(provider.clone()))
                                .with_protocol_versions(&[&rustls::version::TLS13])
                                .map_err(|_| {
                                    "failed to select the static Raft TLS protocol".to_string()
                                })?
                                .with_root_certificates((*roots.roots).clone())
                                .with_no_client_auth();
                        client_config.alpn_protocols = vec![CONTROL_PLANE_RAFT_TLS_ALPN.to_vec()];
                        (
                            ResolvedStaticRaftPeerAddress::Tcp {
                                host,
                                port,
                                server_name,
                            },
                            Some(Arc::new(client_config)),
                        )
                    }
                };
            let replaced = peers.insert(
                node_id,
                ResolvedStaticRaftPeerEndpoint {
                    node_id,
                    endpoint_id: endpoint.id.clone(),
                    advertise: endpoint.advertise.clone(),
                    address,
                    transport_profile_id: endpoint.transport_profile_id.clone(),
                    tls_client_config,
                },
            );
            debug_assert!(replaced.is_none());
        }

        let local_peer = peers
            .get(&local_node_id)
            .expect("validated canonical Raft map contains the local voter");
        let mut local_endpoints = self
            .manifest
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.owner_process_id == selected.id
                    && endpoint.protocol == EndpointProtocol::RaftPeer
            })
            .collect::<Vec<_>>();
        local_endpoints.sort_by_key(|endpoint| (endpoint.priority, endpoint.id.as_str()));
        let listeners = local_endpoints
            .into_iter()
            .map(|endpoint| {
                let listen = parse_endpoint_address(&endpoint.listen, true)?;
                let advertise = parse_endpoint_address(&endpoint.advertise, false)?;
                let tls_server_config = match &advertise {
                    EndpointAddress::Unix(_) => None,
                    EndpointAddress::Tcp { .. } => {
                        let identity_id = endpoint
                            .tls_identity_id
                            .as_deref()
                            .expect("validated TCP endpoint has a TLS identity");
                        let identity =
                            material.tls_identities.get(identity_id).ok_or_else(|| {
                                format!(
                                "selected process did not resolve Raft endpoint {} TLS identity",
                                endpoint.id
                            )
                            })?;
                        let resolver = StaticSingleCertificateResolver {
                            certified_key: Arc::clone(&identity.certified_key),
                        };
                        let mut server_config =
                            rustls::ServerConfig::builder_with_provider(Arc::new(provider.clone()))
                                .with_protocol_versions(&[&rustls::version::TLS13])
                                .map_err(|_| {
                                    "failed to select the static Raft TLS protocol".to_string()
                                })?
                                .with_no_client_auth()
                                .with_cert_resolver(Arc::new(resolver));
                        server_config.alpn_protocols = vec![CONTROL_PLANE_RAFT_TLS_ALPN.to_vec()];
                        Some(Arc::new(server_config))
                    }
                };
                Ok(ResolvedStaticRaftListenerEndpoint {
                    endpoint_id: endpoint.id.clone(),
                    listen,
                    advertise,
                    transport_profile_id: endpoint.transport_profile_id.clone(),
                    tls_server_config,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if !listeners
            .iter()
            .any(|listener| listener.endpoint_id == local_peer.endpoint_id)
        {
            return Err(format!(
                "local voter {local_node_id} canonical Raft endpoint {} is not a configured local listener",
                local_peer.endpoint_id
            ));
        }
        Ok(ResolvedStaticRaftTransportPlan {
            local_node_id,
            listeners,
            peers,
        })
    }

    fn configured_static_control_plane_rpc_listeners(
        &self,
        material: &ResolvedStaticClusterMaterial,
        protocol: EndpointProtocol,
    ) -> Result<Vec<ConfiguredControlPlaneRpcListener>, String> {
        debug_assert!(matches!(
            protocol,
            EndpointProtocol::ControlPlane | EndpointProtocol::AuthorityClockRecovery
        ));
        let selected = &self.manifest.processes[self.selected_process_index];
        let provider = rustls::crypto::ring::default_provider();
        let mut endpoints = self
            .manifest
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.owner_process_id == selected.id && endpoint.protocol == protocol
            })
            .collect::<Vec<_>>();
        endpoints.sort_by_key(|endpoint| (endpoint.priority, endpoint.id.as_str()));
        endpoints
            .into_iter()
            .map(|endpoint| {
                let transport = self
                    .manifest
                    .transport_profiles
                    .iter()
                    .find(|profile| profile.id == endpoint.transport_profile_id)
                    .expect("validated control-plane endpoint transport profile exists");
                let max_connections = usize::try_from(transport.max_connections)
                    .map_err(|_| "control-plane connection limit does not fit usize".to_string())?;
                let max_frame_bytes = usize::try_from(transport.max_frame_bytes)
                    .map_err(|_| "control-plane frame limit does not fit usize".to_string())?;
                let io_timeout = Duration::from_millis(transport.io_timeout_ms);
                match parse_endpoint_address(&endpoint.listen, true)? {
                    EndpointAddress::Unix(path) => Ok(ConfiguredControlPlaneRpcListener::Unix {
                        endpoint_id: endpoint.id.clone(),
                        socket_path: path.to_string_lossy().into_owned(),
                        max_connections,
                        max_frame_bytes,
                        io_timeout,
                    }),
                    EndpointAddress::Tcp { host, port } => {
                        let identity_id = endpoint
                            .tls_identity_id
                            .as_deref()
                            .expect("validated TCP endpoint has a TLS identity");
                        let identity = material.tls_identities.get(identity_id).ok_or_else(|| {
                            format!(
                                "selected process did not resolve control-plane endpoint {} TLS identity",
                                endpoint.id
                            )
                        })?;
                        let resolver = StaticSingleCertificateResolver {
                            certified_key: Arc::clone(&identity.certified_key),
                        };
                        let mut server_config = rustls::ServerConfig::builder_with_provider(
                            Arc::new(provider.clone()),
                        )
                        .with_protocol_versions(&[&rustls::version::TLS13])
                        .map_err(|_| {
                            "failed to select the static control-plane TLS protocol".to_string()
                        })?
                        .with_no_client_auth()
                        .with_cert_resolver(Arc::new(resolver));
                        server_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
                        Ok(ConfiguredControlPlaneRpcListener::Tcp {
                            endpoint_id: endpoint.id.clone(),
                            bind_addr: tcp_socket_address(&host, port),
                            tls_server_config: Arc::new(server_config),
                            max_connections,
                            max_frame_bytes,
                            io_timeout,
                        })
                    }
                }
            })
            .collect()
    }

    fn configured_static_control_plane_rpc_clients(
        &self,
        material: &ResolvedStaticClusterMaterial,
        protocol: EndpointProtocol,
    ) -> Result<ConfiguredStaticControlPlaneRpcClients, String> {
        debug_assert!(matches!(
            protocol,
            EndpointProtocol::ControlPlane | EndpointProtocol::AuthorityClockRecovery
        ));
        let selected = &self.manifest.processes[self.selected_process_index];
        let mut targets = self
            .manifest
            .authorities
            .iter()
            .map(|authority| {
                let process = self
                    .manifest
                    .processes
                    .iter()
                    .find(|process| process.id == authority.process_id)
                    .expect("validated authority process exists");
                (authority, process)
            })
            .collect::<Vec<_>>();
        targets.sort_by_key(|(authority, process)| {
            (
                authority.raft_node_id.unwrap_or_default(),
                authority.id.as_str(),
                process.id.as_str(),
            )
        });
        let mut candidate_sets = Vec::with_capacity(targets.len());
        for (_, target) in targets {
            let target_is_local = target.host_id == selected.host_id;
            let mut candidates = self
                .manifest
                .endpoints
                .iter()
                .filter(|endpoint| {
                    endpoint.owner_process_id == target.id && endpoint.protocol == protocol
                })
                .filter(|endpoint| {
                    target_is_local
                        || matches!(
                            parse_endpoint_address(&endpoint.advertise, false),
                            Ok(EndpointAddress::Tcp { .. })
                        )
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|endpoint| (endpoint.priority, endpoint.id.as_str()));
            if candidates.is_empty() {
                return Err(format!(
                    "authority process {} has no {protocol:?} endpoint reachable from process {}",
                    target.id, selected.id
                ));
            }
            candidate_sets.push(candidates);
        }
        let max_candidates = candidate_sets.iter().map(Vec::len).max().ok_or_else(|| {
            format!("static {protocol:?} client has no configured authority targets")
        })?;
        let mut selected_endpoints = Vec::new();
        for candidate_index in 0..max_candidates {
            for candidates in &candidate_sets {
                if let Some(endpoint) = candidates.get(candidate_index) {
                    selected_endpoints.push(*endpoint);
                }
            }
        }
        if selected_endpoints.is_empty() {
            return Err(format!(
                "static {protocol:?} client has no endpoint reachable from process {}",
                selected.id
            ));
        }
        let uses_tcp = selected_endpoints.iter().any(|endpoint| {
            matches!(
                parse_endpoint_address(&endpoint.advertise, false),
                Ok(EndpointAddress::Tcp { .. })
            )
        });
        if !uses_tcp {
            let paths = selected_endpoints
                .into_iter()
                .map(
                    |endpoint| match parse_endpoint_address(&endpoint.advertise, false)? {
                        EndpointAddress::Unix(path) => Ok(path.to_string_lossy().into_owned()),
                        EndpointAddress::Tcp { .. } => unreachable!("TCP use was precomputed"),
                    },
                )
                .collect::<Result<Vec<_>, String>>()?;
            return Ok(ConfiguredStaticControlPlaneRpcClients {
                endpoints: paths,
                frame_transport: None,
            });
        }

        let provider = rustls::crypto::ring::default_provider();
        let mut peers = BTreeMap::new();
        let mut endpoints = Vec::with_capacity(selected_endpoints.len());
        for endpoint in selected_endpoints {
            let transport = self
                .manifest
                .transport_profiles
                .iter()
                .find(|profile| profile.id == endpoint.transport_profile_id)
                .expect("validated control-plane endpoint transport profile exists");
            let max_frame_bytes = usize::try_from(transport.max_frame_bytes)
                .map_err(|_| "control-plane frame limit does not fit usize".to_string())?;
            let advertise = endpoint.advertise.clone();
            endpoints.push(advertise.clone());
            let peer = match parse_endpoint_address(&endpoint.advertise, false)? {
                EndpointAddress::Unix(path) => {
                    StaticControlPlanePeer::Unix(StaticControlPlaneUnixPeer {
                        endpoint: advertise.clone(),
                        path,
                        max_frame_bytes,
                    })
                }
                EndpointAddress::Tcp { host, port } => {
                    let trust_bundle_id = endpoint
                        .tls_trust_bundle_id
                        .as_deref()
                        .expect("validated TCP endpoint has a TLS trust bundle");
                    let roots = material
                        .tls_trust_bundles
                        .get(trust_bundle_id)
                        .ok_or_else(|| {
                            format!(
                                "selected process did not resolve control-plane endpoint {} trust bundle",
                                endpoint.id
                            )
                        })?;
                    let server_name = endpoint
                        .tls_server_name
                        .clone()
                        .expect("validated TCP endpoint has a TLS server name");
                    let mut client_config =
                        RustlsClientConfig::builder_with_provider(Arc::new(provider.clone()))
                            .with_protocol_versions(&[&rustls::version::TLS13])
                            .map_err(|_| {
                                "failed to select the static control-plane TLS protocol".to_string()
                            })?
                            .with_root_certificates((*roots.roots).clone())
                            .with_no_client_auth();
                    client_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
                    StaticControlPlanePeer::Tcp(StaticControlPlaneTcpPeer {
                        endpoint: advertise.clone(),
                        host,
                        port,
                        server_name,
                        connect_timeout: Duration::from_millis(transport.connect_timeout_ms),
                        max_frame_bytes,
                        tls_client_config: Arc::new(client_config),
                    })
                }
            };
            if peers.insert(advertise.clone(), peer).is_some() {
                return Err(format!(
                    "multiple authority endpoint candidates use the same {protocol:?} address {advertise}"
                ));
            }
        }
        Ok(ConfiguredStaticControlPlaneRpcClients {
            endpoints,
            frame_transport: Some(Arc::new(StaticControlPlaneFrameTransport {
                peers: Arc::new(peers),
            })),
        })
    }

    fn replicated_unix_control_plane_server_config<F>(
        &self,
        material: &ResolvedStaticClusterMaterial,
        get: F,
    ) -> Result<ServerConfig, String>
    where
        F: Fn(&str) -> Option<String>,
    {
        if self.manifest.deployment.mode != DeploymentMode::Replicated {
            return Err(
                "replicated control-plane mapping requires deployment mode replicated".to_string(),
            );
        }
        if self.manifest.deployment.internal_auth != InternalAuth::Required {
            return Err(
                "replicated control-plane mapping requires internal authentication".to_string(),
            );
        }
        let selected = &self.manifest.processes[self.selected_process_index];
        if selected.kind != ProcessKind::ControlPlane {
            return Err(
                "this static runtime slice currently maps replicated control-plane processes only"
                    .to_string(),
            );
        }
        for endpoint in self
            .manifest
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.owner_process_id == selected.id)
        {
            let listen = parse_endpoint_address(&endpoint.listen, true)?;
            let advertise = parse_endpoint_address(&endpoint.advertise, false)?;
            if endpoint.protocol == EndpointProtocol::StorageRpc
                && (!matches!(listen, EndpointAddress::Unix(_))
                    || !matches!(advertise, EndpointAddress::Unix(_)))
            {
                return Err(format!(
                    "TCP static cluster runtime activation is not implemented for protocol {:?}; replicated control-plane mapping cannot ignore configured listener {}",
                    endpoint.protocol,
                    endpoint.id
                ));
            }
        }
        let authority = self
            .manifest
            .authorities
            .iter()
            .find(|authority| authority.process_id == selected.id)
            .ok_or_else(|| "selected control-plane process has no authority".to_string())?;
        let raft_node_id = authority
            .raft_node_id
            .ok_or_else(|| "selected replicated authority has no Raft node id".to_string())?;
        let raft_transport_plan = self.resolved_static_raft_transport_plan(material)?;
        let control_plane_rpc_listeners = self.configured_static_control_plane_rpc_listeners(
            material,
            EndpointProtocol::ControlPlane,
        )?;
        let control_plane_clock_recovery_rpc_listeners = self
            .configured_static_control_plane_rpc_listeners(
                material,
                EndpointProtocol::AuthorityClockRecovery,
            )?;
        let ConfiguredStaticControlPlaneRpcClients {
            endpoints: control_plane_client_endpoints,
            frame_transport: control_plane_rpc_frame_transport,
        } = self.configured_static_control_plane_rpc_clients(
            material,
            EndpointProtocol::ControlPlane,
        )?;
        let ConfiguredStaticControlPlaneRpcClients {
            endpoints: control_plane_clock_recovery_client_endpoints,
            frame_transport: control_plane_clock_recovery_rpc_frame_transport,
        } = self.configured_static_control_plane_rpc_clients(
            material,
            EndpointProtocol::AuthorityClockRecovery,
        )?;
        let local_control_endpoint =
            self.preferred_owned_endpoint(selected, EndpointProtocol::ControlPlane)?;
        let local_recovery_endpoint =
            self.preferred_owned_endpoint(selected, EndpointProtocol::AuthorityClockRecovery)?;
        let local_control_path =
            match parse_endpoint_address(&local_control_endpoint.advertise, false)? {
                EndpointAddress::Unix(path) => path.to_string_lossy().into_owned(),
                EndpointAddress::Tcp { .. } => local_control_endpoint.advertise.clone(),
            };
        let local_recovery_path =
            match parse_endpoint_address(&local_recovery_endpoint.advertise, false)? {
                EndpointAddress::Unix(path) => path.to_string_lossy().into_owned(),
                EndpointAddress::Tcp { .. } => local_recovery_endpoint.advertise.clone(),
            };
        debug_assert_eq!(raft_transport_plan.local_node_id, raft_node_id);
        let local_raft_listener = raft_transport_plan
            .listeners
            .iter()
            .find(|listener| {
                raft_transport_plan
                    .peers
                    .get(&raft_node_id)
                    .is_some_and(|peer| peer.endpoint_id == listener.endpoint_id)
            })
            .expect("resolved Raft transport includes the canonical local listener");
        let local_raft_endpoint = self
            .manifest
            .endpoints
            .iter()
            .find(|endpoint| endpoint.id == local_raft_listener.endpoint_id)
            .expect("resolved local Raft listener endpoint exists");
        let local_raft_transport = self
            .manifest
            .transport_profiles
            .iter()
            .find(|profile| profile.id == local_raft_endpoint.transport_profile_id)
            .expect("validated Raft endpoint transport profile exists");
        let shared_raft_max_frame_bytes = raft_transport_plan
            .peers
            .values()
            .map(|peer| {
                self.manifest
                    .transport_profiles
                    .iter()
                    .find(|profile| profile.id == peer.transport_profile_id)
                    .expect("validated canonical Raft peer transport profile exists")
                    .max_frame_bytes
            })
            .min()
            .expect("validated replicated topology has at least one Raft voter");
        let raft_transport_limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: usize::try_from(shared_raft_max_frame_bytes)
                .map_err(|_| "Raft frame limit does not fit usize".to_string())?,
            max_append_entries: usize::try_from(self.manifest.raft.max_append_entries)
                .map_err(|_| "Raft append-entry limit does not fit usize".to_string())?,
            max_append_entries_bytes: usize::try_from(self.manifest.raft.max_append_bytes)
                .map_err(|_| "Raft append-byte limit does not fit usize".to_string())?,
            max_snapshot_bytes: usize::try_from(self.manifest.raft.max_snapshot_bytes)
                .map_err(|_| "Raft snapshot limit does not fit usize".to_string())?,
        };

        let mut raft_peer_listeners = Vec::new();
        for listener in &raft_transport_plan.listeners {
            let transport = self
                .manifest
                .transport_profiles
                .iter()
                .find(|profile| profile.id == listener.transport_profile_id)
                .expect("validated Raft listener transport profile exists");
            let max_connections = usize::try_from(transport.max_connections)
                .map_err(|_| "Raft connection limit does not fit usize".to_string())?;
            let io_timeout = Duration::from_millis(transport.io_timeout_ms);
            let configured = match &listener.listen {
                EndpointAddress::Unix(path) => ConfiguredControlPlaneRaftPeerListener::Unix {
                    endpoint_id: listener.endpoint_id.clone(),
                    socket_path: path.to_string_lossy().into_owned(),
                    max_connections,
                    io_timeout,
                },
                EndpointAddress::Tcp { host, port } => {
                    let tls_server_config =
                        listener.tls_server_config.as_ref().ok_or_else(|| {
                            format!(
                                "resolved TCP Raft listener {} has no TLS server configuration",
                                listener.endpoint_id
                            )
                        })?;
                    ConfiguredControlPlaneRaftPeerListener::Tcp {
                        endpoint_id: listener.endpoint_id.clone(),
                        bind_addr: tcp_socket_address(host, *port),
                        tls_server_config: Arc::clone(tls_server_config),
                        max_connections,
                        io_timeout,
                    }
                }
            };
            raft_peer_listeners.push(configured);
        }

        let has_tcp_peer = raft_transport_plan
            .peers
            .values()
            .any(|peer| matches!(peer.address, ResolvedStaticRaftPeerAddress::Tcp { .. }));
        let raft_peer_frame_transport: Option<Arc<dyn ControlPlaneRaftPeerFrameTransport>> =
            if has_tcp_peer {
                if raft_transport_plan
                    .peers
                    .values()
                    .any(|peer| matches!(peer.address, ResolvedStaticRaftPeerAddress::Unix(_)))
                {
                    return Err(
                        "canonical Raft peers must use one transport protocol per static topology"
                            .to_string(),
                    );
                }
                let peers = raft_transport_plan
                    .peers
                    .iter()
                    .map(|(&node_id, peer)| {
                        let ResolvedStaticRaftPeerAddress::Tcp {
                            host,
                            port,
                            server_name,
                        } = &peer.address
                        else {
                            unreachable!("mixed canonical Raft transports were rejected")
                        };
                        let tls_client_config =
                            peer.tls_client_config.as_ref().ok_or_else(|| {
                                format!(
                                    "resolved TCP Raft peer {} has no TLS client configuration",
                                    peer.endpoint_id
                                )
                            })?;
                        Ok((
                            node_id,
                            StaticRaftTcpPeer {
                                endpoint: peer.advertise.clone(),
                                host: host.clone(),
                                port: *port,
                                server_name: server_name.clone(),
                                tls_client_config: Arc::clone(tls_client_config),
                            },
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, String>>()?;
                Some(Arc::new(StaticRaftTcpPeerFrameTransport {
                    peers: Arc::new(peers),
                }))
            } else {
                None
            };

        let mut raft_peer_sockets = Vec::new();
        for peer_authority in &self.manifest.authorities {
            let peer_node_id = peer_authority
                .raft_node_id
                .expect("validated replicated authority has a Raft node id");
            let peer = raft_transport_plan
                .peers
                .get(&peer_node_id)
                .expect("resolved Raft transport contains every voter");
            raft_peer_sockets.push(ConfiguredControlPlaneRaftPeerSocket {
                node_id: peer_node_id,
                socket_path: match &peer.address {
                    ResolvedStaticRaftPeerAddress::Unix(socket_path) => {
                        socket_path.to_string_lossy().into_owned()
                    }
                    ResolvedStaticRaftPeerAddress::Tcp { .. } => peer.advertise.clone(),
                },
            });
        }
        raft_peer_sockets.sort_by_key(|peer| peer.node_id);

        let mut storage_node_sockets = Vec::new();
        for storage_node in &self.manifest.storage_nodes {
            let endpoint = self
                .canonical_storage_node_endpoints
                .get(&storage_node.node_id)
                .expect("validated storage-node endpoint map contains every storage node");
            let address = match parse_endpoint_address(&endpoint.advertise, false)? {
                EndpointAddress::Unix(path) => path.to_string_lossy().into_owned(),
                EndpointAddress::Tcp { .. } => endpoint.advertise.clone(),
            };
            storage_node_sockets.push(ConfiguredStorageNodeSocket {
                node_id: storage_node.node_id,
                socket_path: address,
            });
        }
        storage_node_sockets.sort_by_key(|node| node.node_id);

        let mut raft_auth_credentials = Vec::new();
        let mut storage_auth_credentials = Vec::new();
        let mut frontend_auth_credentials = Vec::new();
        let mut admin_auth_credentials = Vec::new();
        let mut raft_signer = None;
        for credential in &material.auth_credentials {
            let secret = BinarySecretConfigValue::from_bytes(credential.secret.clone());
            let signer = (
                credential.credential_id.clone(),
                credential.credential_version,
            );
            match (&credential.principal.principal, &credential.principal.id) {
                (AuthPrincipal::RaftPeer, CredentialPrincipalId::Node(node_id)) => {
                    raft_auth_credentials.push(ConfiguredControlPlaneRaftAuthCredential {
                        node_id: *node_id,
                        credential_id: credential.credential_id.clone(),
                        credential_version: credential.credential_version,
                        secret,
                    });
                    if credential.use_for_signing && *node_id == raft_node_id {
                        raft_signer = Some(signer);
                    }
                }
                (AuthPrincipal::StorageNode, CredentialPrincipalId::Node(node_id)) => {
                    storage_auth_credentials.push(ConfiguredControlPlaneStorageAuthCredential {
                        node_id: u32::try_from(*node_id).map_err(|_| {
                            "resolved storage-node auth principal does not fit u32".to_string()
                        })?,
                        credential_id: credential.credential_id.clone(),
                        credential_version: credential.credential_version,
                        secret,
                    });
                }
                (AuthPrincipal::Frontend, CredentialPrincipalId::Instance(instance_id)) => {
                    frontend_auth_credentials.push(ConfiguredControlPlaneFrontendAuthCredential {
                        instance_id: instance_id.clone(),
                        credential_id: credential.credential_id.clone(),
                        credential_version: credential.credential_version,
                        secret,
                    });
                }
                (AuthPrincipal::Admin, CredentialPrincipalId::Instance(instance_id)) => {
                    admin_auth_credentials.push(ConfiguredControlPlaneAdminAuthCredential {
                        instance_id: instance_id.clone(),
                        credential_id: credential.credential_id.clone(),
                        credential_version: credential.credential_version,
                        secret,
                    });
                }
                (AuthPrincipal::Maintenance, _) => {
                    return Err(
                        "maintenance-principal runtime activation is not implemented".to_string(),
                    );
                }
                _ => {
                    return Err(
                        "resolved static auth credential has a mismatched principal identity"
                            .to_string(),
                    );
                }
            }
        }
        let has_tcp_control_plane_listener = control_plane_rpc_listeners
            .iter()
            .any(|listener| matches!(listener, ConfiguredControlPlaneRpcListener::Tcp { .. }));
        if has_tcp_control_plane_listener
            && frontend_auth_credentials.is_empty()
            && admin_auth_credentials.is_empty()
        {
            return Err(
                "TCP control-plane listeners require an active frontend or admin runtime-map credential"
                    .to_string(),
            );
        }
        let has_tcp_recovery_listener = control_plane_clock_recovery_rpc_listeners
            .iter()
            .any(|listener| matches!(listener, ConfiguredControlPlaneRpcListener::Tcp { .. }));
        if has_tcp_recovery_listener && admin_auth_credentials.is_empty() {
            return Err(
                "TCP authority-clock recovery listeners require an active admin credential"
                    .to_string(),
            );
        }
        let raft_signer = raft_signer.ok_or_else(|| {
            "selected replicated authority has no active Raft signing credential".to_string()
        })?;
        let admin_instance_id = selected.admin_instance_id.clone().ok_or_else(|| {
            "selected replicated control-plane process has no admin instance id".to_string()
        })?;

        let mut manifest_values = BTreeMap::<&'static str, String>::new();
        manifest_values.insert("ARGMIN_PROCESS_ROLE", "control-plane".to_string());
        manifest_values.insert(
            "ARGMIN_PG_COUNT",
            self.manifest.storage.pg_count.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_STORAGE_CLUSTER_EPOCH",
            self.manifest.storage.initial_cluster_epoch.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_K",
            self.manifest.storage.ec_data_shards.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_M",
            self.manifest.storage.ec_parity_shards.to_string(),
        );
        manifest_values.insert("ARGMIN_REGION", self.manifest.cluster.region.clone());
        manifest_values.insert("ARGMIN_HOST_ID", selected.host_id.clone());
        manifest_values.insert(
            "ARGMIN_CONTROL_PLANE_STATE_PATH",
            authority.state_path.to_string_lossy().into_owned(),
        );
        manifest_values.insert(
            "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
            local_control_path.clone(),
        );
        manifest_values.insert("ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT", "1".to_string());
        manifest_values.insert(
            "ARGMIN_CONTROL_PLANE_RAFT_CLUSTER_NAME",
            self.raft_cluster_identity(),
        );
        manifest_values.insert(
            "ARGMIN_CONTROL_PLANE_RAFT_NODE_ID",
            raft_node_id.to_string(),
        );
        let mut config = ServerConfig::from_lookup(|key| {
            manifest_values.get(key).cloned().or_else(|| get(key))
        })?;
        config.control_plane_state_path = Some(authority.state_path.to_string_lossy().into_owned());
        config.control_plane_socket_path = Some(local_control_path);
        config.control_plane_clock_recovery_socket_path = Some(local_recovery_path);
        if control_plane_rpc_frame_transport.is_none() {
            config.control_plane_client_socket_paths = control_plane_client_endpoints.clone();
        } else {
            config.control_plane_client_socket_paths.clear();
        }
        config.control_plane_rpc_listeners = control_plane_rpc_listeners;
        config.control_plane_clock_recovery_rpc_listeners =
            control_plane_clock_recovery_rpc_listeners;
        config.control_plane_rpc_client_endpoints = control_plane_client_endpoints;
        config.control_plane_clock_recovery_rpc_client_endpoints =
            control_plane_clock_recovery_client_endpoints;
        config.control_plane_rpc_frame_transport = control_plane_rpc_frame_transport;
        config.control_plane_clock_recovery_rpc_frame_transport =
            control_plane_clock_recovery_rpc_frame_transport;
        config.control_plane_auth_cluster_id = Some(self.raft_cluster_identity());
        config.control_plane_storage_auth_credentials = storage_auth_credentials;
        config.control_plane_frontend_auth_credentials = frontend_auth_credentials;
        config.control_plane_admin_auth_instance_id = Some(admin_instance_id);
        config.control_plane_admin_auth_credentials = admin_auth_credentials;
        config.control_plane_raft_cluster_name = Some(self.raft_cluster_identity());
        config.control_plane_raft_peer_socket_path = match &local_raft_listener.listen {
            EndpointAddress::Unix(path) => Some(path.to_string_lossy().into_owned()),
            EndpointAddress::Tcp { .. } => None,
        };
        config.control_plane_raft_peer_listeners = raft_peer_listeners;
        config.control_plane_raft_peer_frame_transport = raft_peer_frame_transport;
        config.control_plane_raft_peer_sockets = raft_peer_sockets;
        config.control_plane_raft_peer_transport_limits = raft_transport_limits;
        config.control_plane_raft_peer_max_connections =
            usize::try_from(local_raft_transport.max_connections)
                .map_err(|_| "Raft connection limit does not fit usize".to_string())?;
        config.control_plane_raft_peer_connect_timeout =
            Duration::from_millis(local_raft_transport.connect_timeout_ms);
        config.control_plane_raft_peer_io_timeout =
            Duration::from_millis(local_raft_transport.io_timeout_ms);
        config.control_plane_raft_auth_credentials = raft_auth_credentials;
        config.control_plane_raft_auth_signing_credential = Some(raft_signer);
        let static_initial_cluster_map =
            self.configured_static_initial_cluster_map(&storage_node_sockets)?;
        config.storage_node_sockets = storage_node_sockets;
        config.static_cluster_identity = Some(self.configured_static_identity());
        config.static_initial_cluster_map = Some(static_initial_cluster_map);
        Ok(config)
    }

    fn configured_static_identity(&self) -> ConfiguredStaticClusterIdentity {
        let selected = &self.manifest.processes[self.selected_process_index];
        ConfiguredStaticClusterIdentity {
            cluster_id: self.manifest.cluster.id.clone(),
            topology_generation: self.manifest.cluster.topology_generation,
            topology_digest: self.topology_digest.clone(),
            process_id: selected.id.clone(),
            process_identity_digest: self.process_identity_digest.clone(),
        }
    }

    fn configured_static_initial_cluster_map(
        &self,
        storage_node_sockets: &[ConfiguredStorageNodeSocket],
    ) -> Result<ConfiguredStaticInitialClusterMap, String> {
        let topology_digest = decode_topology_digest(&self.topology_digest)?;
        let raft_voters = self.canonical_raft_peer_endpoints.keys().copied().collect();
        let pg_acting_sets = self
            .initial_pg_acting_sets
            .iter()
            .enumerate()
            .map(|(pg_id, acting_set)| {
                let pg_id = u32::try_from(pg_id)
                    .map(PgId::new)
                    .map_err(|_| "static PG id does not fit u32".to_string())?;
                Ok((pg_id, acting_set.iter().copied().map(NodeId::new).collect()))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let nodes = storage_node_sockets
            .iter()
            .map(|node| (NodeId::new(node.node_id), node.socket_path.clone()))
            .collect::<Vec<_>>();
        let topology = InitialClusterTopologyCertificate::new_for_bootstrap_map(
            self.manifest.cluster.topology_generation,
            topology_digest,
            raft_voters,
            &nodes,
            &pg_acting_sets,
        )
        .map_err(|error| format!("invalid static initial topology certificate: {error}"))?;
        Ok(ConfiguredStaticInitialClusterMap {
            topology,
            pg_acting_sets,
        })
    }

    fn raft_cluster_identity(&self) -> String {
        format!(
            "{}:topology:{}:{}",
            self.manifest.cluster.id,
            self.manifest.cluster.topology_generation,
            self.topology_digest
        )
    }

    fn preferred_owned_endpoint(
        &self,
        owner: &ProcessInput,
        protocol: EndpointProtocol,
    ) -> Result<&EndpointInput, String> {
        self.manifest
            .endpoints
            .iter()
            .filter(|endpoint| endpoint.owner_process_id == owner.id)
            .filter(|endpoint| endpoint.protocol == protocol)
            .min_by_key(|endpoint| (endpoint.priority, endpoint.id.as_str()))
            .ok_or_else(|| {
                format!(
                    "process {} has no endpoint for protocol {protocol:?}",
                    owner.id
                )
            })
    }

    fn replicated_data_process_server_config<F>(
        &self,
        material: &ResolvedStaticClusterMaterial,
        get: F,
    ) -> Result<ServerConfig, String>
    where
        F: Fn(&str) -> Option<String>,
    {
        if self.manifest.deployment.mode != DeploymentMode::Replicated {
            return Err("replicated data-process mapping requires replicated mode".to_string());
        }
        let selected = &self.manifest.processes[self.selected_process_index];
        let process_role = match selected.kind {
            ProcessKind::Frontend => "frontend",
            ProcessKind::StorageNode => "storage-node",
            ProcessKind::Combined => {
                return Err(
                    "replicated combined-process storage RPC auth requires role-specific client credentials"
                        .to_string(),
                );
            }
            ProcessKind::ControlPlane | ProcessKind::AllInOne => {
                return Err("selected process is not a replicated data process".to_string());
            }
        };
        let selected_storage_node = self
            .manifest
            .storage_nodes
            .iter()
            .find(|node| node.process_id == selected.id);
        if selected.kind == ProcessKind::StorageNode && selected_storage_node.is_none() {
            return Err("selected storage process has no storage node".to_string());
        }

        let ConfiguredStaticControlPlaneRpcClients {
            endpoints: control_plane_client_endpoints,
            frame_transport: control_plane_rpc_frame_transport,
        } = self.configured_static_control_plane_rpc_clients(
            material,
            EndpointProtocol::ControlPlane,
        )?;
        let control_plane_endpoint = control_plane_client_endpoints
            .first()
            .cloned()
            .ok_or_else(|| "replicated data process has no control-plane route".to_string())?;

        let mut storage_node_sockets = Vec::with_capacity(self.manifest.storage_nodes.len());
        for storage_node in &self.manifest.storage_nodes {
            let endpoint = self
                .canonical_storage_node_endpoints
                .get(&storage_node.node_id)
                .expect("validated storage-node endpoint map contains every node");
            let EndpointAddress::Unix(path) = parse_endpoint_address(&endpoint.advertise, false)?
            else {
                return Err(
                    "replicated storage RPC TCP activation is not implemented; every canonical storage endpoint must be Unix"
                        .to_string(),
                );
            };
            storage_node_sockets.push(ConfiguredStorageNodeSocket {
                node_id: storage_node.node_id,
                socket_path: path.to_string_lossy().into_owned(),
            });
        }
        storage_node_sockets.sort_by_key(|node| node.node_id);

        let local_storage_socket = if let Some(storage_node) = selected_storage_node {
            if self
                .manifest
                .endpoints
                .iter()
                .filter(|endpoint| endpoint.owner_process_id == selected.id)
                .filter(|endpoint| endpoint.protocol == EndpointProtocol::StorageRpc)
                .any(|endpoint| {
                    !endpoint.listen.starts_with("unix://")
                        || !endpoint.advertise.starts_with("unix://")
                })
            {
                return Err(
                    "replicated Unix storage-node activation cannot ignore a configured TCP storage RPC listener"
                        .to_string(),
                );
            }
            let endpoint = self.preferred_owned_endpoint(selected, EndpointProtocol::StorageRpc)?;
            let EndpointAddress::Unix(path) = parse_endpoint_address(&endpoint.listen, true)?
            else {
                return Err(
                    "replicated Unix storage-node activation requires a Unix storage RPC listener"
                        .to_string(),
                );
            };
            Some((storage_node, path.to_string_lossy().into_owned()))
        } else {
            None
        };

        let storage_transport_limits =
            |endpoint_id: &str| -> Result<StorageRpcTransportLimits, String> {
                let endpoint = self
                    .manifest
                    .endpoints
                    .iter()
                    .find(|endpoint| endpoint.id == endpoint_id)
                    .ok_or_else(|| {
                        format!("storage endpoint {endpoint_id} disappeared after validation")
                    })?;
                let profile = self
                .manifest
                .transport_profiles
                .iter()
                .find(|profile| profile.id == endpoint.transport_profile_id)
                .ok_or_else(|| format!("storage endpoint {endpoint_id} transport profile disappeared after validation"))?;
                StorageRpcTransportLimits::new(
                    usize::try_from(profile.max_frame_bytes).map_err(|_| {
                        format!("storage endpoint {endpoint_id} frame limit does not fit usize")
                    })?,
                    usize::try_from(profile.max_connections).map_err(|_| {
                        format!(
                            "storage endpoint {endpoint_id} connection limit does not fit usize"
                        )
                    })?,
                    Duration::from_millis(profile.io_timeout_ms),
                )
                .map_err(|error| {
                    format!("invalid storage endpoint {endpoint_id} transport limits: {error}")
                })
            };
        let client_transport_limits = self
            .canonical_storage_node_endpoints
            .values()
            .map(|endpoint| storage_transport_limits(&endpoint.endpoint_id))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .reduce(|left, right| {
                StorageRpcTransportLimits::new(
                    left.max_frame_bytes().min(right.max_frame_bytes()),
                    left.max_connections().min(right.max_connections()),
                    left.io_timeout().min(right.io_timeout()),
                )
                .expect("minimum validated storage transport limits remain valid")
            })
            .ok_or_else(|| "replicated data process has no storage transport limits".to_string())?;
        let local_server_transport_limits = local_storage_socket
            .as_ref()
            .map(|(storage_node, _)| {
                let endpoint = self
                    .canonical_storage_node_endpoints
                    .get(&storage_node.node_id)
                    .expect("validated canonical storage endpoint exists");
                storage_transport_limits(&endpoint.endpoint_id)
            })
            .transpose()?;

        let scoped_credentials = material
            .auth_credentials
            .iter()
            .filter_map(|credential| {
                let principal = match (&credential.principal.principal, &credential.principal.id) {
                    (AuthPrincipal::StorageNode, CredentialPrincipalId::Node(node_id)) => {
                        let node_id = u32::try_from(*node_id).map_err(|_| {
                            "storage RPC credential node id does not fit u32".to_string()
                        });
                        Some(node_id.map(|node_id| {
                            ControlPlaneAuthPrincipal::StorageNodeProcess {
                                node_id: storage::NodeId::new(node_id),
                            }
                        }))
                    }
                    (AuthPrincipal::Frontend, CredentialPrincipalId::Instance(instance_id)) => {
                        Some(Ok(ControlPlaneAuthPrincipal::Frontend {
                            instance_id: instance_id.clone(),
                        }))
                    }
                    (AuthPrincipal::Admin, CredentialPrincipalId::Instance(instance_id)) => {
                        Some(Ok(ControlPlaneAuthPrincipal::Admin {
                            instance_id: instance_id.clone(),
                        }))
                    }
                    (AuthPrincipal::Maintenance, CredentialPrincipalId::Instance(process_id)) => {
                        Some(Ok(ControlPlaneAuthPrincipal::LocalMaintenance {
                            process_id: process_id.clone(),
                        }))
                    }
                    (AuthPrincipal::RaftPeer, CredentialPrincipalId::Node(_)) => None,
                    _ => Some(Err(
                        "resolved static storage RPC credential has a mismatched principal identity"
                            .to_string(),
                    )),
                }?;
                Some(principal.and_then(|principal| {
                    ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
                        cluster_id: self.manifest.cluster.id.clone(),
                        credential_id: credential.credential_id.clone(),
                        credential_version: credential.credential_version,
                        principal,
                        secret: credential.secret.clone(),
                    })
                    .map(|scoped| (credential, scoped))
                    .map_err(|error| format!("invalid storage RPC credential: {error}"))
                }))
            })
            .collect::<Result<Vec<_>, String>>()?;

        let primary_client_credential = scoped_credentials
            .iter()
            .find(|(resolved, scoped)| {
                resolved.use_for_signing
                    && match (selected.kind, scoped.principal()) {
                        (
                            ProcessKind::Frontend,
                            ControlPlaneAuthPrincipal::Frontend { instance_id },
                        ) => selected.frontend_instance_id.as_ref() == Some(instance_id),
                        (
                            ProcessKind::StorageNode,
                            ControlPlaneAuthPrincipal::StorageNodeProcess { node_id },
                        ) => selected_storage_node
                            .is_some_and(|node| node.node_id == node_id.as_u32()),
                        _ => false,
                    }
            })
            .map(|(_, scoped)| scoped.clone())
            .ok_or_else(|| {
                "selected replicated data process has no active storage RPC signing credential"
                    .to_string()
            })?;
        let (storage_rpc_frontend_client_auth, storage_rpc_storage_node_client_auth) =
            match selected.kind {
                ProcessKind::Frontend => (
                    Some(
                        FrontendStorageRpcClientCapability::new_with_transport_limits(
                            primary_client_credential,
                            self.manifest.cluster.topology_generation,
                            self.topology_digest.clone(),
                            client_transport_limits,
                        )
                        .map_err(|error| {
                            format!("invalid frontend storage RPC capability: {error}")
                        })?,
                    ),
                    None,
                ),
                ProcessKind::StorageNode => (
                    None,
                    Some(
                        StorageNodeStorageRpcClientCapability::new_with_transport_limits(
                            primary_client_credential,
                            self.manifest.cluster.topology_generation,
                            self.topology_digest.clone(),
                            client_transport_limits,
                        )
                        .map_err(|error| {
                            format!("invalid storage-node storage RPC capability: {error}")
                        })?,
                    ),
                ),
                _ => unreachable!("replicated data process kind was validated above"),
            };
        let storage_rpc_maintenance_client_auth = selected
            .maintenance_instance_id
            .as_ref()
            .map(|maintenance_instance_id| {
                let maintenance_credential = scoped_credentials
                .iter()
                .find(|(resolved, scoped)| {
                    resolved.use_for_signing
                        && matches!(
                            scoped.principal(),
                            ControlPlaneAuthPrincipal::LocalMaintenance { process_id }
                                if process_id == maintenance_instance_id
                        )
                })
                .map(|(_, scoped)| scoped.clone())
                .ok_or_else(|| {
                    "selected replicated data process has no active maintenance storage RPC signing credential"
                        .to_string()
                })?;
                MaintenanceStorageRpcClientCapability::new_with_transport_limits(
                    maintenance_credential,
                    self.manifest.cluster.topology_generation,
                    self.topology_digest.clone(),
                    client_transport_limits,
                )
                .map_err(|error| format!("invalid maintenance storage RPC capability: {error}"))
            })
            .transpose()?;
        let storage_rpc_server_auth = local_storage_socket
            .as_ref()
            .map(|_| {
                let verifier = ControlPlaneScopedCredentialStore::new(
                    scoped_credentials
                        .iter()
                        .map(|(_, credential)| credential.clone())
                        .collect(),
                )
                .map_err(|error| format!("invalid storage RPC verifier: {error}"))?;
                StorageRpcServerAuthConfig::new(
                    self.manifest.cluster.id.clone(),
                    verifier,
                    self.manifest.cluster.topology_generation,
                    self.topology_digest.clone(),
                )
                .map(|config| {
                    config.with_transport_limits(
                        local_server_transport_limits
                            .expect("local storage endpoint has validated transport limits"),
                    )
                })
                .map_err(|error| format!("invalid storage RPC server auth config: {error}"))
            })
            .transpose()?;

        let mut storage_control_plane_credentials = Vec::new();
        let mut frontend_control_plane_credentials = Vec::new();
        let mut storage_control_plane_signer = None;
        let mut frontend_control_plane_signer = None;
        for credential in &material.auth_credentials {
            let signer = (
                credential.credential_id.clone(),
                credential.credential_version,
            );
            match (&credential.principal.principal, &credential.principal.id) {
                (AuthPrincipal::StorageNode, CredentialPrincipalId::Node(node_id)) => {
                    let node_id = u32::try_from(*node_id).map_err(|_| {
                        "resolved storage-node auth principal does not fit u32".to_string()
                    })?;
                    storage_control_plane_credentials.push(
                        ConfiguredControlPlaneStorageAuthCredential {
                            node_id,
                            credential_id: credential.credential_id.clone(),
                            credential_version: credential.credential_version,
                            secret: BinarySecretConfigValue::from_bytes(credential.secret.clone()),
                        },
                    );
                    if credential.use_for_signing
                        && selected_storage_node.is_some_and(|node| node.node_id == node_id)
                    {
                        storage_control_plane_signer = Some(signer);
                    }
                }
                (AuthPrincipal::Frontend, CredentialPrincipalId::Instance(instance_id)) => {
                    frontend_control_plane_credentials.push(
                        ConfiguredControlPlaneFrontendAuthCredential {
                            instance_id: instance_id.clone(),
                            credential_id: credential.credential_id.clone(),
                            credential_version: credential.credential_version,
                            secret: BinarySecretConfigValue::from_bytes(credential.secret.clone()),
                        },
                    );
                    if credential.use_for_signing
                        && selected.frontend_instance_id.as_ref() == Some(instance_id)
                    {
                        frontend_control_plane_signer = Some(signer);
                    }
                }
                _ => {}
            }
        }

        let mut manifest_values = BTreeMap::<&'static str, String>::new();
        manifest_values.insert("ARGMIN_PROCESS_ROLE", process_role.to_string());
        manifest_values.insert(
            "ARGMIN_PG_COUNT",
            self.manifest.storage.pg_count.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_STORAGE_CLUSTER_EPOCH",
            self.manifest.storage.initial_cluster_epoch.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_K",
            self.manifest.storage.ec_data_shards.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_M",
            self.manifest.storage.ec_parity_shards.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_LOCAL_NODE_COUNT",
            self.manifest.storage_nodes.len().to_string(),
        );
        manifest_values.insert("ARGMIN_REGION", self.manifest.cluster.region.clone());
        manifest_values.insert("ARGMIN_HOST_ID", selected.host_id.clone());
        manifest_values.insert("ARGMIN_CONTROL_PLANE_SOCKET_PATH", control_plane_endpoint);
        if let Some((storage_node, socket_path)) = &local_storage_socket {
            manifest_values.insert("ARGMIN_STORAGE_NODE_ID", "0".to_string());
            manifest_values.insert(
                "ARGMIN_STORAGE_NODE_DATA_DIR",
                storage_node.data_dir.to_string_lossy().into_owned(),
            );
            manifest_values.insert("ARGMIN_STORAGE_NODE_SOCKET_PATH", socket_path.clone());
        }
        let mut config = ServerConfig::from_lookup(|key| {
            manifest_values.get(key).cloned().or_else(|| get(key))
        })?;
        config.storage_node_ids = self
            .manifest
            .storage_nodes
            .iter()
            .map(|node| node.node_id)
            .collect();
        config.storage_node_ids.sort_unstable();
        config.storage_node_sockets = storage_node_sockets;
        config.storage_node_rpc_admission_limit = config
            .storage_node_rpc_admission_limit
            .min(client_transport_limits.max_connections());
        config.storage_node_id = selected_storage_node.map(|node| node.node_id);
        config.storage_node_data_dir =
            selected_storage_node.map(|node| node.data_dir.to_string_lossy().into_owned());
        config.storage_node_socket_path = local_storage_socket.map(|(_, path)| path);
        config.control_plane_client_socket_paths = if control_plane_rpc_frame_transport.is_none() {
            control_plane_client_endpoints.clone()
        } else {
            Vec::new()
        };
        config.control_plane_rpc_client_endpoints = control_plane_client_endpoints;
        config.control_plane_rpc_frame_transport = control_plane_rpc_frame_transport;
        config.control_plane_auth_cluster_id = Some(self.raft_cluster_identity());
        config.control_plane_storage_auth_credentials = storage_control_plane_credentials;
        config.control_plane_storage_auth_signing_credential = storage_control_plane_signer;
        config.control_plane_frontend_auth_instance_id = selected.frontend_instance_id.clone();
        config.control_plane_frontend_auth_credentials = frontend_control_plane_credentials;
        config.control_plane_frontend_auth_signing_credential = frontend_control_plane_signer;
        config.storage_rpc_frontend_client_auth = storage_rpc_frontend_client_auth;
        config.storage_rpc_maintenance_client_auth = storage_rpc_maintenance_client_auth;
        config.storage_rpc_storage_node_client_auth = storage_rpc_storage_node_client_auth;
        config.storage_rpc_server_auth = storage_rpc_server_auth;
        config.static_cluster_identity = Some(self.configured_static_identity());
        Ok(config)
    }

    fn standalone_legacy_server_config<F>(&self, get: F) -> Result<ServerConfig, String>
    where
        F: Fn(&str) -> Option<String>,
    {
        if self.manifest.deployment.mode != DeploymentMode::Standalone {
            return Err(
                "replicated cluster manifests require the static secret and transport runtime slices"
                    .to_string(),
            );
        }
        if self.manifest.deployment.internal_auth != InternalAuth::Disabled
            || !self.manifest.auth_credentials.is_empty()
            || !self.manifest.tls_identities.is_empty()
            || !self.manifest.tls_trust_bundles.is_empty()
            || self
                .manifest
                .endpoints
                .iter()
                .any(|endpoint| !endpoint.advertise.starts_with("unix://"))
        {
            return Err(
                "standalone manifest runtime mapping currently requires Unix endpoints with internal auth disabled and no unresolved secret references"
                    .to_string(),
            );
        }

        let selected = &self.manifest.processes[self.selected_process_index];
        if selected.kind != ProcessKind::AllInOne {
            return Err(
                "standalone manifest runtime mapping requires an all-in-one process".to_string(),
            );
        }
        let storage_node = self
            .manifest
            .storage_nodes
            .iter()
            .find(|storage_node| storage_node.process_id == selected.id)
            .ok_or_else(|| "all-in-one process has no storage node".to_string())?;
        let disk = self
            .manifest
            .disks
            .iter()
            .find(|disk| disk.id == storage_node.disk_id)
            .ok_or_else(|| "storage node disk disappeared after validation".to_string())?;
        let mut manifest_values = BTreeMap::<&'static str, String>::new();
        manifest_values.insert("ARGMIN_PROCESS_ROLE", "legacy-local".to_string());
        manifest_values.insert(
            "ARGMIN_DATA_DIR",
            disk.mount_path.to_string_lossy().into_owned(),
        );
        manifest_values.insert(
            "ARGMIN_PG_COUNT",
            self.manifest.storage.pg_count.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_STORAGE_CLUSTER_EPOCH",
            self.manifest.storage.initial_cluster_epoch.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_K",
            self.manifest.storage.ec_data_shards.to_string(),
        );
        manifest_values.insert(
            "ARGMIN_EC_M",
            self.manifest.storage.ec_parity_shards.to_string(),
        );
        manifest_values.insert("ARGMIN_LOCAL_NODE_COUNT", "1".to_string());
        manifest_values.insert("ARGMIN_REGION", self.manifest.cluster.region.clone());
        manifest_values.insert("ARGMIN_HOST_ID", selected.host_id.clone());

        let mut config = ServerConfig::from_lookup(|key| {
            manifest_values.get(key).cloned().or_else(|| get(key))
        })?;
        config.storage_node_ids = vec![storage_node.node_id];
        config.storage_node_id = Some(storage_node.node_id);
        config.storage_node_data_dir = Some(storage_node.data_dir.to_string_lossy().into_owned());
        config.static_cluster_identity = Some(ConfiguredStaticClusterIdentity {
            cluster_id: self.manifest.cluster.id.clone(),
            topology_generation: self.manifest.cluster.topology_generation,
            topology_digest: self.topology_digest.clone(),
            process_id: selected.id.clone(),
            process_identity_digest: self.process_identity_digest.clone(),
        });
        Ok(config)
    }

    #[cfg(test)]
    fn initial_pg_acting_sets(&self) -> &[Vec<u32>] {
        &self.initial_pg_acting_sets
    }

    #[cfg(test)]
    fn canonical_raft_peer_endpoints(&self) -> &BTreeMap<u64, CanonicalRaftPeerEndpoint> {
        &self.canonical_raft_peer_endpoints
    }
}

fn decode_topology_digest(digest: &str) -> Result<[u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN], String> {
    if digest.len() != CONTROL_PLANE_TOPOLOGY_DIGEST_LEN * 2 {
        return Err("static topology digest must contain 32 bytes".to_string());
    }
    let mut decoded = [0_u8; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN];
    for (output, pair) in decoded.iter_mut().zip(digest.as_bytes().chunks_exact(2)) {
        let high = decode_hex_nibble(pair[0])
            .ok_or_else(|| "static topology digest contains invalid hex".to_string())?;
        let low = decode_hex_nibble(pair[1])
            .ok_or_else(|| "static topology digest contains invalid hex".to_string())?;
        *output = (high << 4) | low;
    }
    Ok(decoded)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub(crate) fn load_server_config_from_environment() -> Result<ServerConfig, String> {
    let config_path = std::env::var_os("ARGMIN_CLUSTER_CONFIG_PATH");
    let process_id = std::env::var_os("ARGMIN_PROCESS_ID");
    let config_path = config_path.as_deref().map(Path::new);
    let process_id = process_id
        .as_deref()
        .map(|value| {
            value
                .to_str()
                .ok_or_else(|| "ARGMIN_PROCESS_ID must contain valid UTF-8".to_string())
        })
        .transpose()?;
    load_server_config_from_inputs(config_path, process_id, |key| std::env::var(key).ok())
}

fn load_server_config_from_inputs<F>(
    config_path: Option<&Path>,
    process_id: Option<&str>,
    get: F,
) -> Result<ServerConfig, String>
where
    F: Fn(&str) -> Option<String>,
{
    load_server_config_from_inputs_with_filesystem_validator(
        config_path,
        process_id,
        get,
        validate_selected_host_filesystem,
    )
}

fn load_server_config_from_inputs_with_filesystem_validator<F, V>(
    config_path: Option<&Path>,
    process_id: Option<&str>,
    get: F,
    validate_filesystem: V,
) -> Result<ServerConfig, String>
where
    F: Fn(&str) -> Option<String>,
    V: FnOnce(&ValidatedStaticClusterManifest) -> Result<(), String>,
{
    match (config_path, process_id) {
        (None, None) => ServerConfig::from_lookup(get),
        (Some(_), None) => {
            Err("ARGMIN_PROCESS_ID is required with ARGMIN_CLUSTER_CONFIG_PATH".to_string())
        }
        (None, Some(_)) => {
            Err("ARGMIN_CLUSTER_CONFIG_PATH is required with ARGMIN_PROCESS_ID".to_string())
        }
        (Some(config_path), Some(process_id)) => {
            for key in LEGACY_CLUSTER_ENV_KEYS {
                if get(key).is_some() {
                    return Err(format!(
                        "{key} cannot be set when ARGMIN_CLUSTER_CONFIG_PATH is active"
                    ));
                }
            }
            let manifest = load_static_cluster_manifest_structural(config_path, process_id)?;
            validate_filesystem(&manifest)?;
            match manifest.manifest.deployment.mode {
                DeploymentMode::Standalone => manifest.standalone_legacy_server_config(get),
                DeploymentMode::Replicated => {
                    let material = manifest.resolve_selected_process_material()?;
                    match manifest.manifest.processes[manifest.selected_process_index].kind {
                        ProcessKind::ControlPlane => {
                            manifest.replicated_unix_control_plane_server_config(&material, get)
                        }
                        ProcessKind::Frontend
                        | ProcessKind::StorageNode
                        | ProcessKind::Combined => {
                            manifest.replicated_data_process_server_config(&material, get)
                        }
                        ProcessKind::AllInOne => {
                            Err("all-in-one process is invalid in replicated mode".to_string())
                        }
                    }
                }
            }
        }
    }
}

impl fmt::Debug for ValidatedStaticClusterManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidatedStaticClusterManifest")
            .field("schema_version", &self.manifest.schema_version)
            .field("cluster_id", &self.manifest.cluster.id)
            .field(
                "topology_generation",
                &self.manifest.cluster.topology_generation,
            )
            .field("deployment_mode", &self.manifest.deployment.mode)
            .field("selected_process_id", &self.selected_process_id())
            .field("hosts", &self.manifest.hosts.len())
            .field("disks", &self.manifest.disks.len())
            .field("processes", &self.manifest.processes.len())
            .field("authorities", &self.manifest.authorities.len())
            .field("storage_nodes", &self.manifest.storage_nodes.len())
            .field("endpoints", &self.manifest.endpoints.len())
            .field("tls_identities", &self.manifest.tls_identities.len())
            .field("tls_trust_bundles", &self.manifest.tls_trust_bundles.len())
            .field("auth_credentials", &self.manifest.auth_credentials.len())
            .field("initial_pg_acting_sets", &self.initial_pg_acting_sets.len())
            .field(
                "canonical_raft_peer_endpoints",
                &self.canonical_raft_peer_endpoints.len(),
            )
            .field(
                "canonical_storage_node_endpoints",
                &self.canonical_storage_node_endpoints.len(),
            )
            .field("topology_digest", &self.topology_digest)
            .field("process_identity_digest", &self.process_identity_digest)
            .field("full_config_fingerprint", &self.full_config_fingerprint)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CredentialPrincipalId {
    Node(u64),
    Instance(String),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CredentialPrincipalKey {
    principal: AuthPrincipal,
    id: CredentialPrincipalId,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct CredentialIdentity {
    principal: CredentialPrincipalKey,
    credential_id: String,
    credential_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EndpointAddress {
    Unix(PathBuf),
    Tcp { host: String, port: u16 },
}

#[derive(Default)]
struct CanonicalEncoder {
    bytes: Vec<u8>,
}

impl CanonicalEncoder {
    fn field(&mut self, tag: u16, value: &[u8]) {
        self.bytes.extend_from_slice(&tag.to_be_bytes());
        self.bytes.extend_from_slice(
            &u64::try_from(value.len())
                .expect("validated manifest field length fits u64")
                .to_be_bytes(),
        );
        self.bytes.extend_from_slice(value);
    }

    fn string(&mut self, tag: u16, value: &str) {
        self.field(tag, value.as_bytes());
    }

    fn optional_string(&mut self, tag: u16, value: Option<&str>) {
        let mut encoded = Vec::new();
        match value {
            Some(value) => {
                encoded.push(1);
                encoded.extend_from_slice(
                    &u64::try_from(value.len())
                        .expect("validated manifest field length fits u64")
                        .to_be_bytes(),
                );
                encoded.extend_from_slice(value.as_bytes());
            }
            None => encoded.push(0),
        }
        self.field(tag, &encoded);
    }

    fn path(&mut self, tag: u16, value: &Path) {
        self.field(tag, value.as_os_str().as_encoded_bytes());
    }

    fn u8(&mut self, tag: u16, value: u8) {
        self.field(tag, &[value]);
    }

    fn u32(&mut self, tag: u16, value: u32) {
        self.field(tag, &value.to_be_bytes());
    }

    fn u64(&mut self, tag: u16, value: u64) {
        self.field(tag, &value.to_be_bytes());
    }

    fn optional_u64(&mut self, tag: u16, value: Option<u64>) {
        let mut encoded = Vec::with_capacity(9);
        match value {
            Some(value) => {
                encoded.push(1);
                encoded.extend_from_slice(&value.to_be_bytes());
            }
            None => encoded.push(0),
        }
        self.field(tag, &encoded);
    }

    fn boolean(&mut self, tag: u16, value: bool) {
        self.u8(tag, u8::from(value));
    }

    fn collection<I>(&mut self, tag: u16, values: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        let values: Vec<Vec<u8>> = values.into_iter().collect();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(
            &u32::try_from(values.len())
                .expect("validated manifest collection length fits u32")
                .to_be_bytes(),
        );
        for value in values {
            encoded.extend_from_slice(&1_u16.to_be_bytes());
            encoded.extend_from_slice(
                &u64::try_from(value.len())
                    .expect("validated manifest item length fits u64")
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(&value);
        }
        self.field(tag, &encoded);
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

fn topology_digest(
    manifest: &StaticClusterManifestInput,
    initial_pg_acting_sets: &[Vec<u32>],
    canonical_raft_peer_endpoints: &BTreeMap<u64, CanonicalRaftPeerEndpoint>,
) -> String {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, TOPOLOGY_IDENTITY_DOMAIN);
    encoder.u32(2, manifest.schema_version);
    encoder.string(3, &manifest.cluster.id);
    encoder.u64(4, manifest.cluster.topology_generation);

    encoder.field(5, &encode_deployment_topology(&manifest.deployment));
    encoder.field(6, &encode_storage_topology(&manifest.storage));
    encoder.field(7, &encode_raft_topology(&manifest.raft));
    encoder.collection(
        8,
        manifest
            .transport_profiles
            .iter()
            .map(encode_transport_profile_topology),
    );
    encoder.collection(9, manifest.hosts.iter().map(encode_host_topology));
    encoder.collection(10, manifest.disks.iter().map(encode_disk_topology));
    encoder.collection(11, manifest.processes.iter().map(encode_process_topology));
    encoder.collection(
        12,
        manifest.authorities.iter().map(encode_authority_topology),
    );
    encoder.collection(
        13,
        manifest
            .storage_nodes
            .iter()
            .map(encode_storage_node_topology),
    );
    encoder.collection(14, manifest.endpoints.iter().map(encode_endpoint_topology));
    encoder.collection(
        15,
        required_principal_identities(manifest)
            .iter()
            .map(encode_principal_identity),
    );
    encoder.collection(
        16,
        initial_pg_acting_sets
            .iter()
            .enumerate()
            .map(|(pg_id, acting_set)| encode_initial_pg_acting_set(pg_id, acting_set)),
    );
    encoder.collection(
        17,
        canonical_raft_peer_endpoints
            .iter()
            .map(|(node_id, endpoint)| encode_raft_peer_endpoint(*node_id, endpoint)),
    );
    auth::canonical::sha256_hex(&encoder.finish())
}

fn process_identity_digest(
    manifest: &StaticClusterManifestInput,
    selected_process_index: usize,
    topology_digest: &str,
) -> String {
    let selected = &manifest.processes[selected_process_index];
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, PROCESS_IDENTITY_DOMAIN);
    encoder.string(2, topology_digest);
    encoder.string(3, &selected.id);
    encoder.string(4, &selected.host_id);
    encoder.u8(5, process_kind_tag(selected.kind));
    encoder.collection(
        6,
        manifest
            .authorities
            .iter()
            .filter(|authority| authority.process_id == selected.id)
            .map(encode_authority_process_identity),
    );
    encoder.collection(
        7,
        manifest
            .storage_nodes
            .iter()
            .filter(|storage_node| storage_node.process_id == selected.id)
            .map(encode_storage_node_process_identity),
    );
    auth::canonical::sha256_hex(&encoder.finish())
}

fn full_config_fingerprint(manifest: &StaticClusterManifestInput) -> String {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, FULL_CONFIG_FINGERPRINT_DOMAIN);
    encoder.u32(2, manifest.schema_version);
    encoder.field(3, &encode_cluster_full(&manifest.cluster));
    encoder.field(4, &encode_deployment_full(&manifest.deployment));
    encoder.field(5, &encode_storage_topology(&manifest.storage));
    encoder.field(6, &encode_raft_topology(&manifest.raft));
    encoder.collection(
        7,
        manifest
            .transport_profiles
            .iter()
            .map(encode_transport_profile_full),
    );
    encoder.collection(8, manifest.hosts.iter().map(encode_host_topology));
    encoder.collection(9, manifest.disks.iter().map(encode_disk_full));
    encoder.collection(10, manifest.processes.iter().map(encode_process_full));
    encoder.collection(11, manifest.authorities.iter().map(encode_authority_full));
    encoder.collection(
        12,
        manifest.storage_nodes.iter().map(encode_storage_node_full),
    );
    encoder.collection(13, manifest.endpoints.iter().map(encode_endpoint_full));
    encoder.collection(
        14,
        manifest.tls_identities.iter().map(encode_tls_identity_full),
    );
    encoder.collection(
        15,
        manifest
            .tls_trust_bundles
            .iter()
            .map(encode_tls_trust_bundle_full),
    );
    encoder.collection(
        16,
        manifest
            .auth_credentials
            .iter()
            .map(encode_auth_credential_full),
    );
    auth::canonical::sha256_hex(&encoder.finish())
}

fn encode_deployment_topology(deployment: &DeploymentInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u8(1, deployment_mode_tag(deployment.mode));
    encoder.u8(2, failure_domain_tag(deployment.failure_domain));
    encoder.u8(3, deployment.failure_tolerance);
    encoder.u8(4, internal_auth_tag(deployment.internal_auth));
    encoder.finish()
}

fn encode_deployment_full(deployment: &DeploymentInput) -> Vec<u8> {
    encode_deployment_topology(deployment)
}

fn encode_cluster_full(cluster: &ClusterInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &cluster.id);
    encoder.u64(2, cluster.topology_generation);
    encoder.string(3, &cluster.region);
    encoder.finish()
}

fn encode_storage_topology(storage: &StorageInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u32(1, storage.pg_count);
    encoder.u8(2, storage.ec_data_shards);
    encoder.u8(3, storage.ec_parity_shards);
    encoder.u64(4, storage.initial_cluster_epoch);
    encoder.finish()
}

fn encode_raft_topology(raft: &RaftInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u64(1, raft.max_append_entries);
    encoder.u64(2, raft.max_append_bytes);
    encoder.u64(3, raft.max_snapshot_bytes);
    encoder.finish()
}

fn encode_transport_profile_topology(profile: &TransportProfileInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &profile.id);
    encoder.u64(2, profile.max_frame_bytes);
    encoder.finish()
}

fn encode_transport_profile_full(profile: &TransportProfileInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &profile.id);
    encoder.u64(2, profile.max_frame_bytes);
    encoder.u32(3, profile.max_connections);
    encoder.u64(4, profile.connect_timeout_ms);
    encoder.u64(5, profile.io_timeout_ms);
    encoder.finish()
}

fn encode_host_topology(host: &HostInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &host.id);
    encoder.string(2, &host.zone);
    encoder.string(3, &host.rack);
    encoder.finish()
}

fn encode_disk_topology(disk: &DiskInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &disk.id);
    encoder.string(2, &disk.host_id);
    encoder.finish()
}

fn encode_disk_full(disk: &DiskInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &disk.id);
    encoder.string(2, &disk.host_id);
    encoder.path(3, &disk.mount_path);
    encoder.finish()
}

fn encode_process_topology(process: &ProcessInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &process.id);
    encoder.string(2, &process.host_id);
    encoder.u8(3, process_kind_tag(process.kind));
    encoder.optional_string(4, process.frontend_instance_id.as_deref());
    encoder.optional_string(5, process.admin_instance_id.as_deref());
    encoder.optional_string(6, process.maintenance_instance_id.as_deref());
    encoder.finish()
}

fn encode_process_full(process: &ProcessInput) -> Vec<u8> {
    encode_process_topology(process)
}

fn encode_authority_topology(authority: &AuthorityInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &authority.id);
    encoder.u8(2, authority_kind_tag(authority.kind));
    encoder.optional_u64(3, authority.raft_node_id);
    encoder.string(4, &authority.process_id);
    encoder.string(5, &authority.disk_id);
    encoder.finish()
}

fn encode_authority_process_identity(authority: &AuthorityInput) -> Vec<u8> {
    encode_authority_topology(authority)
}

fn encode_authority_full(authority: &AuthorityInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &authority.id);
    encoder.u8(2, authority_kind_tag(authority.kind));
    encoder.optional_u64(3, authority.raft_node_id);
    encoder.string(4, &authority.process_id);
    encoder.string(5, &authority.disk_id);
    encoder.path(6, &authority.state_path);
    encoder.finish()
}

fn encode_storage_node_topology(storage_node: &StorageNodeInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u32(1, storage_node.node_id);
    encoder.string(2, &storage_node.process_id);
    encoder.string(3, &storage_node.disk_id);
    encoder.finish()
}

fn encode_storage_node_process_identity(storage_node: &StorageNodeInput) -> Vec<u8> {
    encode_storage_node_topology(storage_node)
}

fn encode_storage_node_full(storage_node: &StorageNodeInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u32(1, storage_node.node_id);
    encoder.string(2, &storage_node.process_id);
    encoder.string(3, &storage_node.disk_id);
    encoder.path(4, &storage_node.data_dir);
    encoder.finish()
}

fn encode_endpoint_topology(endpoint: &EndpointInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &endpoint.id);
    encoder.string(2, &endpoint.owner_process_id);
    encoder.u8(3, endpoint_protocol_tag(endpoint.protocol));
    encoder.u32(4, endpoint.priority);
    encoder.string(5, &endpoint.advertise);
    encoder.string(6, &endpoint.transport_profile_id);
    encoder.optional_string(7, endpoint.tls_identity_id.as_deref());
    encoder.optional_string(8, endpoint.tls_trust_bundle_id.as_deref());
    encoder.optional_string(9, endpoint.tls_server_name.as_deref());
    encoder.finish()
}

fn encode_endpoint_full(endpoint: &EndpointInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &endpoint.id);
    encoder.string(2, &endpoint.owner_process_id);
    encoder.u8(3, endpoint_protocol_tag(endpoint.protocol));
    encoder.u32(4, endpoint.priority);
    encoder.string(5, &endpoint.listen);
    encoder.string(6, &endpoint.advertise);
    encoder.string(7, &endpoint.transport_profile_id);
    encoder.optional_string(8, endpoint.tls_identity_id.as_deref());
    encoder.optional_string(9, endpoint.tls_trust_bundle_id.as_deref());
    encoder.optional_string(10, endpoint.tls_server_name.as_deref());
    encoder.finish()
}

fn encode_tls_identity_full(identity: &TlsIdentityInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &identity.id);
    encoder.string(2, &identity.certificate_ref);
    encoder.string(3, &identity.private_key_ref);
    encoder.finish()
}

fn encode_tls_trust_bundle_full(bundle: &TlsTrustBundleInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.string(1, &bundle.id);
    encoder.string(2, &bundle.ca_bundle_ref);
    encoder.finish()
}

fn encode_auth_credential_full(credential: &AuthCredentialInput) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u8(1, auth_principal_tag(credential.principal));
    encoder.optional_u64(2, credential.node_id);
    encoder.optional_string(3, credential.instance_id.as_deref());
    encoder.string(4, &credential.credential_id);
    encoder.u64(5, credential.credential_version);
    encoder.boolean(6, credential.use_for_signing);
    encoder.u64(7, credential.accept_from_ms);
    encoder.optional_u64(8, credential.accept_until_ms);
    encoder.string(9, &credential.secret_ref);
    encoder.finish()
}

fn encode_principal_identity(principal: &CredentialPrincipalKey) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u8(1, auth_principal_tag(principal.principal));
    match &principal.id {
        CredentialPrincipalId::Node(node_id) => {
            encoder.u8(2, 1);
            encoder.u64(3, *node_id);
        }
        CredentialPrincipalId::Instance(instance_id) => {
            encoder.u8(2, 2);
            encoder.string(3, instance_id);
        }
    }
    encoder.finish()
}

fn encode_initial_pg_acting_set(pg_id: usize, acting_set: &[u32]) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u64(
        1,
        u64::try_from(pg_id).expect("validated PG index fits in u64"),
    );
    encoder.collection(
        2,
        acting_set
            .iter()
            .map(|node_id| node_id.to_be_bytes().to_vec()),
    );
    encoder.finish()
}

fn encode_raft_peer_endpoint(node_id: u64, endpoint: &CanonicalRaftPeerEndpoint) -> Vec<u8> {
    let mut encoder = CanonicalEncoder::default();
    encoder.u64(1, node_id);
    encoder.string(2, &endpoint.endpoint_id);
    encoder.string(3, &endpoint.owner_process_id);
    encoder.string(4, &endpoint.advertise);
    encoder.finish()
}

fn required_principal_identities(
    manifest: &StaticClusterManifestInput,
) -> BTreeSet<CredentialPrincipalKey> {
    let raft_nodes = manifest
        .authorities
        .iter()
        .filter_map(|authority| authority.raft_node_id)
        .collect();
    let storage_nodes = manifest
        .storage_nodes
        .iter()
        .map(|storage_node| u64::from(storage_node.node_id))
        .collect();
    let frontend_ids = manifest
        .processes
        .iter()
        .filter_map(|process| process.frontend_instance_id.as_deref())
        .collect();
    let admin_ids = manifest
        .processes
        .iter()
        .filter_map(|process| process.admin_instance_id.as_deref())
        .collect();
    let maintenance_ids = manifest
        .processes
        .iter()
        .filter_map(|process| process.maintenance_instance_id.as_deref())
        .collect();
    required_auth_principals(
        &raft_nodes,
        &storage_nodes,
        &frontend_ids,
        &admin_ids,
        &maintenance_ids,
    )
}

const fn deployment_mode_tag(value: DeploymentMode) -> u8 {
    match value {
        DeploymentMode::Standalone => 1,
        DeploymentMode::Replicated => 2,
    }
}

const fn failure_domain_tag(value: FailureDomain) -> u8 {
    match value {
        FailureDomain::None => 1,
        FailureDomain::Disk => 2,
        FailureDomain::Host => 3,
    }
}

const fn internal_auth_tag(value: InternalAuth) -> u8 {
    match value {
        InternalAuth::Required => 1,
        InternalAuth::Disabled => 2,
    }
}

const fn process_kind_tag(value: ProcessKind) -> u8 {
    match value {
        ProcessKind::AllInOne => 1,
        ProcessKind::Frontend => 2,
        ProcessKind::StorageNode => 3,
        ProcessKind::Combined => 4,
        ProcessKind::ControlPlane => 5,
    }
}

const fn authority_kind_tag(value: AuthorityKind) -> u8 {
    match value {
        AuthorityKind::Single => 1,
        AuthorityKind::RaftVoter => 2,
    }
}

const fn endpoint_protocol_tag(value: EndpointProtocol) -> u8 {
    match value {
        EndpointProtocol::RaftPeer => 1,
        EndpointProtocol::ControlPlane => 2,
        EndpointProtocol::AuthorityClockRecovery => 3,
        EndpointProtocol::StorageRpc => 4,
    }
}

const fn auth_principal_tag(value: AuthPrincipal) -> u8 {
    match value {
        AuthPrincipal::RaftPeer => 1,
        AuthPrincipal::StorageNode => 2,
        AuthPrincipal::Frontend => 3,
        AuthPrincipal::Admin => 4,
        AuthPrincipal::Maintenance => 5,
    }
}

pub(crate) fn load_static_cluster_manifest(
    path: &Path,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    let manifest = load_static_cluster_manifest_structural(path, process_id)?;
    validate_selected_host_filesystem(&manifest)?;
    Ok(manifest)
}

fn load_static_cluster_manifest_structural(
    path: &Path,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    if !path.is_absolute() {
        return Err("cluster manifest path must be absolute".to_string());
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|error| format!("open no-follow cluster manifest: {error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("read open cluster manifest metadata: {error}"))?;
    if !metadata.is_file() {
        return Err("cluster manifest must be a regular file".to_string());
    }
    if metadata.len() > CLUSTER_MANIFEST_MAX_BYTES {
        return Err(format!(
            "cluster manifest exceeds {} bytes",
            CLUSTER_MANIFEST_MAX_BYTES
        ));
    }

    let bounded_capacity = usize::try_from(metadata.len())
        .unwrap_or(usize::MAX)
        .min(CLUSTER_MANIFEST_MAX_BYTES as usize);
    let mut bytes = Vec::with_capacity(bounded_capacity);
    file.take(CLUSTER_MANIFEST_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read cluster manifest: {error}"))?;
    if bytes.len() > CLUSTER_MANIFEST_MAX_BYTES as usize {
        return Err(format!(
            "cluster manifest exceeds {} bytes",
            CLUSTER_MANIFEST_MAX_BYTES
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "cluster manifest must contain valid UTF-8".to_string())?;
    parse_static_cluster_manifest(text, process_id)
}

fn validate_selected_host_filesystem(
    validated: &ValidatedStaticClusterManifest,
) -> Result<(), String> {
    validate_selected_host_filesystem_with_mount_validator(
        validated,
        validate_selected_host_mount_boundary,
    )
}

fn validate_selected_host_filesystem_with_mount_validator<V>(
    validated: &ValidatedStaticClusterManifest,
    mut validate_mount: V,
) -> Result<(), String>
where
    V: FnMut(&Path, &std::fs::Metadata, &str) -> Result<(), String>,
{
    let selected_process = &validated.manifest.processes[validated.selected_process_index];
    let selected_host_id = selected_process.host_id.as_str();
    let effective_uid = {
        // SAFETY: geteuid has no preconditions and does not mutate memory.
        unsafe { libc::geteuid() }
    };
    let local_disks: BTreeMap<&str, (PathBuf, PathBuf, u64)> = validated
        .manifest
        .disks
        .iter()
        .filter(|disk| disk.host_id == selected_host_id)
        .map(|disk| {
            let normalized_mount = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
            let metadata = std::fs::symlink_metadata(&normalized_mount).map_err(|error| {
                format!(
                    "selected-host disk {} mount path cannot be inspected: {error}",
                    disk.id
                )
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(format!(
                    "selected-host disk {} mount path must be a non-symlink directory",
                    disk.id
                ));
            }
            validate_selected_host_path_permissions(
                &metadata,
                effective_uid,
                &format!("selected-host disk {} mount path", disk.id),
            )?;
            validate_mount(
                &normalized_mount,
                &metadata,
                &format!("selected-host disk {} mount path", disk.id),
            )?;
            let canonical_mount = normalized_mount.canonicalize().map_err(|error| {
                format!(
                    "selected-host disk {} mount path cannot be canonicalized: {error}",
                    disk.id
                )
            })?;
            Ok((
                disk.id.as_str(),
                (normalized_mount, canonical_mount, metadata.dev()),
            ))
        })
        .collect::<Result<_, String>>()?;

    for authority in &validated.manifest.authorities {
        let process = validated
            .manifest
            .processes
            .iter()
            .find(|process| process.id == authority.process_id)
            .expect("validated authority process must exist");
        if process.host_id != selected_host_id {
            continue;
        }
        let (mount_path, canonical_mount, mount_device) = local_disks
            .get(authority.disk_id.as_str())
            .expect("validated authority disk must exist");
        validate_selected_host_durable_path(
            &authority.state_path,
            mount_path.as_path(),
            canonical_mount,
            *mount_device,
            effective_uid,
            SelectedHostDurablePathKind::StateFile,
            &format!("authority {} state path", authority.id),
        )?;
    }

    for storage_node in &validated.manifest.storage_nodes {
        let process = validated
            .manifest
            .processes
            .iter()
            .find(|process| process.id == storage_node.process_id)
            .expect("validated storage-node process must exist");
        if process.host_id != selected_host_id {
            continue;
        }
        let (mount_path, canonical_mount, mount_device) = local_disks
            .get(storage_node.disk_id.as_str())
            .expect("validated storage-node disk must exist");
        validate_selected_host_durable_path(
            &storage_node.data_dir,
            mount_path.as_path(),
            canonical_mount,
            *mount_device,
            effective_uid,
            SelectedHostDurablePathKind::DataDirectory,
            &format!("storage node {} data path", storage_node.node_id),
        )?;
    }
    Ok(())
}

fn validate_selected_host_mount_boundary(
    mount_path: &Path,
    mount_metadata: &std::fs::Metadata,
    label: &str,
) -> Result<(), String> {
    let parent = mount_path
        .parent()
        .ok_or_else(|| format!("{label} has no parent filesystem boundary"))?;
    let parent_metadata = std::fs::symlink_metadata(parent)
        .map_err(|error| format!("{label} parent cannot be inspected: {error}"))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(format!("{label} parent must be a non-symlink directory"));
    }
    validate_distinct_mount_devices(mount_metadata.dev(), parent_metadata.dev(), label)
}

fn validate_distinct_mount_devices(
    mount_device: u64,
    parent_device: u64,
    label: &str,
) -> Result<(), String> {
    if mount_device == parent_device {
        return Err(format!(
            "{label} must be an exact distinct-device mount boundary"
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum SelectedHostDurablePathKind {
    StateFile,
    DataDirectory,
}

fn validate_selected_host_durable_path(
    path: &Path,
    mount_path: &Path,
    canonical_mount: &Path,
    mount_device: u64,
    effective_uid: u32,
    kind: SelectedHostDurablePathKind,
    label: &str,
) -> Result<(), String> {
    let normalized_path = normalize_absolute_path(path, label)?;
    let relative = normalized_path
        .strip_prefix(mount_path)
        .expect("validated durable path must be within its disk mount");
    let mut current = mount_path.to_path_buf();
    let mut deepest_existing = mount_path.to_path_buf();
    let mut final_metadata = None;

    for component in relative.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(format!(
                        "{label} must not traverse a symlink below its disk mount"
                    ));
                }
                if metadata.dev() != mount_device {
                    return Err(format!(
                        "{label} crosses away from its declared disk device"
                    ));
                }
                validate_selected_host_path_permissions(&metadata, effective_uid, label)?;
                deepest_existing = current.clone();
                final_metadata = Some(metadata);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(format!("{label} cannot be inspected: {error}")),
        }
    }

    let deepest_metadata = std::fs::symlink_metadata(&deepest_existing)
        .map_err(|error| format!("{label} existing ancestor cannot be inspected: {error}"))?;
    validate_selected_host_path_permissions(&deepest_metadata, effective_uid, label)?;
    let canonical_existing = deepest_existing
        .canonicalize()
        .map_err(|error| format!("{label} existing ancestor cannot be canonicalized: {error}"))?;
    if !canonical_existing.starts_with(canonical_mount) {
        return Err(format!("{label} resolves outside its declared disk mount"));
    }

    if deepest_existing == normalized_path {
        let metadata = final_metadata.expect("existing final path metadata must be retained");
        match kind {
            SelectedHostDurablePathKind::StateFile if !metadata.is_file() => {
                return Err(format!("{label} must be a regular file when it exists"));
            }
            SelectedHostDurablePathKind::DataDirectory if !metadata.is_dir() => {
                return Err(format!("{label} must be a directory when it exists"));
            }
            _ => {}
        }
    } else if !deepest_metadata.is_dir() {
        return Err(format!(
            "{label} existing parent component must be a directory"
        ));
    }
    Ok(())
}

fn validate_selected_host_path_permissions(
    metadata: &std::fs::Metadata,
    effective_uid: u32,
    label: &str,
) -> Result<(), String> {
    if metadata.uid() != effective_uid {
        return Err(format!(
            "{label} must be owned by the effective process user"
        ));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o022 != 0 {
        return Err(format!(
            "{label} must not be writable by group or other users"
        ));
    }
    Ok(())
}

pub(crate) fn parse_static_cluster_manifest(
    text: &str,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    if text.len() > CLUSTER_MANIFEST_MAX_BYTES as usize {
        return Err(format!(
            "cluster manifest exceeds {} bytes",
            CLUSTER_MANIFEST_MAX_BYTES
        ));
    }
    let input: StaticClusterManifestInput = toml::from_str(text).map_err(|error| {
        let location = error
            .span()
            .map(|span| format!(" at bytes {}..{}", span.start, span.end))
            .unwrap_or_default();
        let category = toml_error_category(error.message());
        format!("invalid cluster manifest {category}{location}")
    })?;
    validate_static_cluster_manifest(input, process_id)
}

fn validate_static_cluster_manifest(
    mut manifest: StaticClusterManifestInput,
    process_id: &str,
) -> Result<ValidatedStaticClusterManifest, String> {
    if manifest.schema_version != 1 {
        return Err(format!(
            "unsupported cluster manifest schema version {}",
            manifest.schema_version
        ));
    }
    validate_identifier(
        &manifest.cluster.id,
        CLUSTER_MANIFEST_MAX_CLUSTER_ID_BYTES,
        "cluster id",
    )?;
    validate_identifier(
        &manifest.cluster.region,
        CLUSTER_MANIFEST_MAX_REGION_BYTES,
        "cluster region",
    )?;
    require_nonzero(
        manifest.cluster.topology_generation,
        "cluster topology generation",
    )?;
    require_nonzero(manifest.storage.pg_count, "storage PG count")?;
    require_nonzero(
        manifest.storage.initial_cluster_epoch,
        "initial cluster epoch",
    )?;
    EcConfig::new(
        manifest.storage.ec_data_shards,
        manifest.storage.ec_parity_shards,
    )
    .map_err(|error| format!("invalid cluster manifest EC shape: {error}"))?;
    validate_raft(&manifest.raft)?;

    validate_collection_len(manifest.transport_profiles.len(), "transport profiles")?;
    validate_collection_len(manifest.hosts.len(), "hosts")?;
    validate_collection_len(manifest.disks.len(), "disks")?;
    validate_collection_len(manifest.processes.len(), "processes")?;
    validate_collection_len(manifest.authorities.len(), "authorities")?;
    validate_collection_len(manifest.storage_nodes.len(), "storage nodes")?;
    validate_collection_len(manifest.endpoints.len(), "endpoints")?;
    validate_collection_len(manifest.tls_identities.len(), "TLS identities")?;
    validate_collection_len(manifest.tls_trust_bundles.len(), "TLS trust bundles")?;
    validate_collection_len(manifest.auth_credentials.len(), "auth credentials")?;

    manifest
        .transport_profiles
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest.hosts.sort_by(|left, right| left.id.cmp(&right.id));
    manifest.disks.sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .processes
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .authorities
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .storage_nodes
        .sort_by_key(|storage_node| storage_node.node_id);
    manifest
        .endpoints
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .tls_identities
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .tls_trust_bundles
        .sort_by(|left, right| left.id.cmp(&right.id));
    manifest
        .auth_credentials
        .sort_by(|left, right| credential_sort_key(left).cmp(&credential_sort_key(right)));

    let transport_profiles = validate_transport_profiles(&manifest.transport_profiles)?;
    let hosts = validate_hosts(&manifest.hosts)?;
    let disks = validate_disks(&manifest.disks, &hosts)?;
    let processes = validate_processes(&manifest.processes, &hosts)?;
    let selected_process_index = manifest
        .processes
        .binary_search_by(|process| process.id.as_str().cmp(process_id))
        .map_err(|_| format!("selected process id {process_id:?} is absent from manifest"))?;
    let authorities = validate_authorities(
        &manifest.authorities,
        &processes,
        &disks,
        &manifest.deployment,
    )?;
    let storage_nodes = validate_storage_nodes(
        &manifest.storage_nodes,
        &processes,
        &disks,
        &manifest.deployment,
    )?;
    let tls_identities = validate_tls_identities(&manifest.tls_identities)?;
    let tls_trust_bundles = validate_tls_trust_bundles(&manifest.tls_trust_bundles)?;
    validate_endpoints(
        &manifest.endpoints,
        &processes,
        &authorities,
        &storage_nodes,
        &transport_profiles,
        &tls_identities,
        &tls_trust_bundles,
        &manifest.deployment,
    )?;
    validate_global_runtime_path_namespace(
        &manifest.authorities,
        &manifest.storage_nodes,
        &manifest.endpoints,
        &processes,
    )?;
    let initial_pg_acting_sets = validate_deployment(
        &manifest,
        &hosts,
        &disks,
        &processes,
        &authorities,
        &storage_nodes,
    )?;
    validate_auth_credentials(
        &manifest.auth_credentials,
        &processes,
        &authorities,
        &storage_nodes,
        manifest.deployment.internal_auth,
    )?;
    let canonical_raft_peer_endpoints =
        resolve_canonical_raft_peer_endpoints(&manifest, &authorities)?;
    let canonical_storage_node_endpoints =
        resolve_canonical_storage_node_endpoints(&manifest, &storage_nodes)?;
    validate_raft_transport_capacity(
        &manifest,
        &transport_profiles,
        &authorities,
        &canonical_raft_peer_endpoints,
    )?;
    let topology_digest = topology_digest(
        &manifest,
        &initial_pg_acting_sets,
        &canonical_raft_peer_endpoints,
    );
    validate_initial_bootstrap_replication_size(
        &manifest,
        &initial_pg_acting_sets,
        &canonical_raft_peer_endpoints,
        &canonical_storage_node_endpoints,
        &topology_digest,
    )?;
    let process_identity_digest =
        process_identity_digest(&manifest, selected_process_index, &topology_digest);
    let full_config_fingerprint = full_config_fingerprint(&manifest);

    Ok(ValidatedStaticClusterManifest {
        manifest,
        selected_process_index,
        initial_pg_acting_sets,
        canonical_raft_peer_endpoints,
        canonical_storage_node_endpoints,
        topology_digest,
        process_identity_digest,
        full_config_fingerprint,
    })
}

fn validate_initial_bootstrap_replication_size(
    manifest: &StaticClusterManifestInput,
    initial_pg_acting_sets: &[Vec<u32>],
    canonical_raft_peer_endpoints: &BTreeMap<u64, CanonicalRaftPeerEndpoint>,
    canonical_storage_node_endpoints: &BTreeMap<u32, CanonicalStorageNodeEndpoint>,
    topology_digest: &str,
) -> Result<(), String> {
    if manifest.deployment.mode != DeploymentMode::Replicated {
        return Ok(());
    }
    let nodes = manifest
        .storage_nodes
        .iter()
        .map(|storage_node| {
            let endpoint = canonical_storage_node_endpoints
                .get(&storage_node.node_id)
                .expect("validated storage endpoint map contains every storage node");
            let address = match parse_endpoint_address(&endpoint.advertise, false)
                .expect("validated endpoint has a canonical address")
            {
                EndpointAddress::Unix(path) => path.to_string_lossy().into_owned(),
                EndpointAddress::Tcp { .. } => endpoint.advertise.clone(),
            };
            (NodeId::new(storage_node.node_id), address)
        })
        .collect::<Vec<_>>();
    let pg_acting_sets = initial_pg_acting_sets
        .iter()
        .enumerate()
        .map(|(pg_id, acting_set)| {
            (
                PgId::new(
                    u32::try_from(pg_id).expect("validated manifest PG collection length fits u32"),
                ),
                acting_set.iter().copied().map(NodeId::new).collect(),
            )
        })
        .collect::<Vec<_>>();
    let topology = InitialClusterTopologyCertificate::new_for_bootstrap_map(
        manifest.cluster.topology_generation,
        decode_topology_digest(topology_digest)?,
        canonical_raft_peer_endpoints.keys().copied().collect(),
        &nodes,
        &pg_acting_sets,
    )
    .map_err(|error| format!("invalid static initial topology certificate: {error}"))?;
    validate_control_plane_command_replication_size(
        &ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes,
            pg_acting_sets,
            topology,
        },
    )
    .map_err(|error| format!("static initial bootstrap is not replication-safe: {error}"))
}

fn toml_error_category(message: &str) -> &'static str {
    if message.contains("unknown field") {
        "contains an unknown field"
    } else if message.contains("duplicate key") {
        "contains a duplicate key"
    } else if message.contains("unknown variant") {
        "contains an unknown variant"
    } else {
        "has invalid syntax or field types"
    }
}

fn validate_raft(raft: &RaftInput) -> Result<(), String> {
    let required_append_entries =
        u64::try_from(ControlPlaneRaftPeerTransportLimits::REPLICATION_REQUIRED_APPEND_ENTRIES)
            .expect("production Raft append-entry requirement fits u64");
    if raft.max_append_entries != required_append_entries {
        return Err(format!(
            "raft max_append_entries must equal the production replication batch size {required_append_entries}"
        ));
    }
    let required_append_bytes =
        u64::try_from(ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_APPEND_ENTRIES_BYTES)
            .expect("production Raft append-byte requirement fits u64");
    if raft.max_append_bytes != required_append_bytes {
        return Err(format!(
            "raft max_append_bytes must equal the production replication payload size {required_append_bytes}"
        ));
    }
    if raft.max_snapshot_bytes == 0 || raft.max_snapshot_bytes > CLUSTER_MANIFEST_MAX_SNAPSHOT_BYTES
    {
        return Err(format!(
            "raft max_snapshot_bytes must be in 1..={CLUSTER_MANIFEST_MAX_SNAPSHOT_BYTES}"
        ));
    }
    Ok(())
}

fn validate_transport_profiles(
    profiles: &[TransportProfileInput],
) -> Result<BTreeMap<&str, &TransportProfileInput>, String> {
    if profiles.is_empty() {
        return Err("cluster manifest requires at least one transport profile".to_string());
    }
    let mut result = BTreeMap::new();
    for profile in profiles {
        validate_identifier(
            &profile.id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "transport profile id",
        )?;
        if profile.max_frame_bytes == 0
            || profile.max_frame_bytes > CLUSTER_MANIFEST_MAX_FRAME_BYTES
        {
            return Err(format!(
                "transport profile {} max_frame_bytes must be in 1..={CLUSTER_MANIFEST_MAX_FRAME_BYTES}",
                profile.id
            ));
        }
        if profile.max_connections == 0
            || profile.max_connections > CLUSTER_MANIFEST_MAX_CONNECTIONS
        {
            return Err(format!(
                "transport profile {} max_connections must be in 1..={CLUSTER_MANIFEST_MAX_CONNECTIONS}",
                profile.id
            ));
        }
        validate_timeout(
            profile.connect_timeout_ms,
            &format!("transport profile {} connect_timeout_ms", profile.id),
        )?;
        validate_timeout(
            profile.io_timeout_ms,
            &format!("transport profile {} io_timeout_ms", profile.id),
        )?;
        if result.insert(profile.id.as_str(), profile).is_some() {
            return Err(format!("duplicate transport profile id {}", profile.id));
        }
    }
    Ok(result)
}

fn validate_hosts(hosts: &[HostInput]) -> Result<BTreeSet<&str>, String> {
    if hosts.is_empty() {
        return Err("cluster manifest requires at least one host".to_string());
    }
    let mut result = BTreeSet::new();
    for host in hosts {
        validate_identifier(&host.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "host id")?;
        validate_identifier(&host.zone, CLUSTER_MANIFEST_MAX_ID_BYTES, "host zone")?;
        validate_identifier(&host.rack, CLUSTER_MANIFEST_MAX_ID_BYTES, "host rack")?;
        if !result.insert(host.id.as_str()) {
            return Err(format!("duplicate host id {}", host.id));
        }
    }
    Ok(result)
}

fn validate_disks<'a>(
    disks: &'a [DiskInput],
    hosts: &BTreeSet<&str>,
) -> Result<BTreeMap<&'a str, &'a DiskInput>, String> {
    if disks.is_empty() {
        return Err("cluster manifest requires at least one disk".to_string());
    }
    let mut result = BTreeMap::new();
    let mut host_paths = BTreeSet::new();
    for disk in disks {
        validate_identifier(&disk.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "disk id")?;
        if !hosts.contains(disk.host_id.as_str()) {
            return Err(format!(
                "disk {} references unknown host {}",
                disk.id, disk.host_id
            ));
        }
        let normalized = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
        if !host_paths.insert((disk.host_id.as_str(), normalized)) {
            return Err(format!(
                "duplicate disk mount path on host {}",
                disk.host_id
            ));
        }
        if result.insert(disk.id.as_str(), disk).is_some() {
            return Err(format!("duplicate disk id {}", disk.id));
        }
    }
    Ok(result)
}

fn validate_processes<'a>(
    processes: &'a [ProcessInput],
    hosts: &BTreeSet<&str>,
) -> Result<BTreeMap<&'a str, &'a ProcessInput>, String> {
    if processes.is_empty() {
        return Err("cluster manifest requires at least one process".to_string());
    }
    let mut result = BTreeMap::new();
    let mut frontend_ids = BTreeSet::new();
    let mut admin_ids = BTreeSet::new();
    let mut maintenance_ids = BTreeSet::new();
    for process in processes {
        validate_identifier(&process.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "process id")?;
        if !hosts.contains(process.host_id.as_str()) {
            return Err(format!(
                "process {} references unknown host {}",
                process.id, process.host_id
            ));
        }
        validate_optional_role_id(
            process.frontend_instance_id.as_deref(),
            process.kind.has_frontend(),
            "frontend_instance_id",
            &process.id,
            &mut frontend_ids,
        )?;
        validate_optional_role_id(
            process.admin_instance_id.as_deref(),
            process.kind.has_frontend() || process.kind.has_control_plane(),
            "admin_instance_id",
            &process.id,
            &mut admin_ids,
        )?;
        validate_optional_role_id(
            process.maintenance_instance_id.as_deref(),
            true,
            "maintenance_instance_id",
            &process.id,
            &mut maintenance_ids,
        )?;
        if result.insert(process.id.as_str(), process).is_some() {
            return Err(format!("duplicate process id {}", process.id));
        }
    }
    Ok(result)
}

fn validate_optional_role_id<'a>(
    value: Option<&'a str>,
    role_allowed: bool,
    field: &str,
    process_id: &str,
    seen: &mut BTreeSet<&'a str>,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if !role_allowed {
        return Err(format!(
            "process {process_id} cannot configure {field} for its kind"
        ));
    }
    validate_identifier(value, CLUSTER_MANIFEST_MAX_ID_BYTES, field)?;
    if !seen.insert(value) {
        return Err(format!("duplicate {field} {value}"));
    }
    Ok(())
}

fn validate_authorities<'a>(
    authorities: &'a [AuthorityInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    disks: &BTreeMap<&str, &DiskInput>,
    deployment: &DeploymentInput,
) -> Result<BTreeMap<&'a str, &'a AuthorityInput>, String> {
    if authorities.is_empty() {
        return Err("cluster manifest requires at least one authority".to_string());
    }
    let mut result = BTreeMap::new();
    let mut process_ids = BTreeSet::new();
    let mut raft_node_ids = BTreeSet::new();
    let mut host_paths = BTreeSet::new();
    for authority in authorities {
        validate_identifier(&authority.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "authority id")?;
        let process = processes
            .get(authority.process_id.as_str())
            .ok_or_else(|| {
                format!(
                    "authority {} references unknown process {}",
                    authority.id, authority.process_id
                )
            })?;
        if !process.kind.has_control_plane() {
            return Err(format!(
                "authority {} process {} cannot host a control plane",
                authority.id, authority.process_id
            ));
        }
        if !process_ids.insert(authority.process_id.as_str()) {
            return Err(format!(
                "process {} hosts more than one authority",
                authority.process_id
            ));
        }
        let disk = disks.get(authority.disk_id.as_str()).ok_or_else(|| {
            format!(
                "authority {} references unknown disk {}",
                authority.id, authority.disk_id
            )
        })?;
        if disk.host_id != process.host_id {
            return Err(format!(
                "authority {} disk and process are on different hosts",
                authority.id
            ));
        }
        let state_path = normalize_absolute_path(&authority.state_path, "authority state path")?;
        let mount_path = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
        if !state_path.starts_with(&mount_path) || state_path == mount_path {
            return Err(format!(
                "authority {} state path is not contained by disk {}",
                authority.id, authority.disk_id
            ));
        }
        if !host_paths.insert((process.host_id.as_str(), state_path)) {
            return Err(format!(
                "duplicate authority state path on host {}",
                process.host_id
            ));
        }
        match (authority.kind, authority.raft_node_id, deployment.mode) {
            (AuthorityKind::Single, None, DeploymentMode::Standalone) => {}
            (AuthorityKind::RaftVoter, Some(node_id), DeploymentMode::Replicated)
                if node_id != 0 =>
            {
                if !raft_node_ids.insert(node_id) {
                    return Err(format!("duplicate Raft node id {node_id}"));
                }
            }
            _ => {
                return Err(format!(
                    "authority {} kind/node id is incompatible with deployment mode",
                    authority.id
                ));
            }
        }
        if result.insert(authority.id.as_str(), authority).is_some() {
            return Err(format!("duplicate authority id {}", authority.id));
        }
    }
    Ok(result)
}

fn validate_storage_nodes<'a>(
    storage_nodes: &'a [StorageNodeInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    disks: &BTreeMap<&str, &DiskInput>,
    deployment: &DeploymentInput,
) -> Result<BTreeMap<u32, &'a StorageNodeInput>, String> {
    if storage_nodes.is_empty() {
        return Err("cluster manifest requires at least one storage node".to_string());
    }
    let mut result = BTreeMap::new();
    let mut process_ids = BTreeSet::new();
    let mut host_paths = BTreeSet::new();
    for storage_node in storage_nodes {
        require_nonzero(storage_node.node_id, "storage node id")?;
        let process = processes
            .get(storage_node.process_id.as_str())
            .ok_or_else(|| {
                format!(
                    "storage node {} references unknown process {}",
                    storage_node.node_id, storage_node.process_id
                )
            })?;
        if !process.kind.has_storage_node() {
            return Err(format!(
                "storage node {} process {} cannot host storage",
                storage_node.node_id, storage_node.process_id
            ));
        }
        if !process_ids.insert(storage_node.process_id.as_str()) {
            return Err(format!(
                "process {} hosts more than one storage node",
                storage_node.process_id
            ));
        }
        let disk = disks.get(storage_node.disk_id.as_str()).ok_or_else(|| {
            format!(
                "storage node {} references unknown disk {}",
                storage_node.node_id, storage_node.disk_id
            )
        })?;
        if disk.host_id != process.host_id {
            return Err(format!(
                "storage node {} disk and process are on different hosts",
                storage_node.node_id
            ));
        }
        let data_dir = normalize_absolute_path(&storage_node.data_dir, "storage data path")?;
        let mount_path = normalize_absolute_path(&disk.mount_path, "disk mount path")?;
        if !data_dir.starts_with(&mount_path) || data_dir == mount_path {
            return Err(format!(
                "storage node {} data path is not contained by disk {}",
                storage_node.node_id, storage_node.disk_id
            ));
        }
        if !host_paths.insert((process.host_id.as_str(), data_dir)) {
            return Err(format!(
                "duplicate storage data path on host {}",
                process.host_id
            ));
        }
        if result.insert(storage_node.node_id, storage_node).is_some() {
            return Err(format!(
                "duplicate storage node id {}",
                storage_node.node_id
            ));
        }
    }
    if deployment.mode == DeploymentMode::Standalone && storage_nodes.len() != 1 {
        return Err("standalone deployment requires exactly one storage node".to_string());
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimePathReservationKind {
    File,
    Directory,
    FilenamePrefix,
}

#[derive(Debug)]
struct RuntimePathReservation {
    host_id: String,
    path: PathBuf,
    kind: RuntimePathReservationKind,
    label: String,
}

fn validate_global_runtime_path_namespace(
    authorities: &[AuthorityInput],
    storage_nodes: &[StorageNodeInput],
    endpoints: &[EndpointInput],
    processes: &BTreeMap<&str, &ProcessInput>,
) -> Result<(), String> {
    let mut reservations = Vec::new();
    for authority in authorities {
        let process = processes[authority.process_id.as_str()];
        let state_path = normalize_absolute_path(&authority.state_path, "authority state path")?;
        reserve_runtime_path(
            &mut reservations,
            process.host_id.as_str(),
            state_path.clone(),
            RuntimePathReservationKind::File,
            format!("authority {} state", authority.id),
        );
        for (suffix, label) in [
            (".sentinel", "Raft artifact sentinel"),
            (".wal", "Raft WAL"),
            (".lock", "process state lock"),
            (".static-identity", "static identity"),
            (".identity.next", "prepared static identity"),
            (".clock", "authority-clock checkpoint"),
            (".clock.tmp", "prepared authority-clock checkpoint"),
            (".identity", "standalone identity"),
            (".identity.tmp", "prepared standalone identity"),
            (".journal", "standalone journal"),
            (".initialized", "standalone initialization marker"),
            (
                ".initialized.tmp",
                "prepared standalone initialization marker",
            ),
        ] {
            reserve_runtime_path(
                &mut reservations,
                process.host_id.as_str(),
                path_with_suffix(&state_path, suffix),
                RuntimePathReservationKind::File,
                format!("authority {} {label}", authority.id),
            );
        }
        reserve_runtime_path(
            &mut reservations,
            process.host_id.as_str(),
            state_path.with_extension("tmp"),
            RuntimePathReservationKind::File,
            format!("authority {} prepared standalone snapshot", authority.id),
        );
        for (base_path, label) in [
            (state_path.clone(), "Raft artifact temporary file"),
            (
                path_with_suffix(&state_path, ".sentinel"),
                "Raft artifact sentinel temporary file",
            ),
            (
                path_with_suffix(&state_path, ".wal"),
                "Raft WAL temporary file",
            ),
            (
                path_with_suffix(&state_path, ".journal"),
                "standalone journal temporary file",
            ),
        ] {
            reserve_runtime_path(
                &mut reservations,
                process.host_id.as_str(),
                path_with_suffix(&base_path, ".tmp."),
                RuntimePathReservationKind::FilenamePrefix,
                format!("authority {} {label}", authority.id),
            );
        }
    }
    for storage_node in storage_nodes {
        let process = processes[storage_node.process_id.as_str()];
        let path = normalize_absolute_path(&storage_node.data_dir, "storage data path")?;
        reserve_runtime_path(
            &mut reservations,
            process.host_id.as_str(),
            path,
            RuntimePathReservationKind::Directory,
            format!("storage node {} data directory", storage_node.node_id),
        );
    }
    for endpoint in endpoints {
        let process = processes[endpoint.owner_process_id.as_str()];
        let EndpointAddress::Unix(path) = parse_endpoint_address(&endpoint.listen, true)? else {
            continue;
        };
        reserve_runtime_path(
            &mut reservations,
            process.host_id.as_str(),
            path,
            RuntimePathReservationKind::File,
            format!("endpoint {} Unix socket", endpoint.id),
        );
    }

    let mut exact_paths = BTreeMap::<(String, PathBuf), usize>::new();
    for (index, reservation) in reservations.iter().enumerate() {
        let key = (reservation.host_id.clone(), reservation.path.clone());
        if let Some(previous) = exact_paths.insert(key, index) {
            return runtime_path_collision(&reservations[previous], reservation);
        }
    }
    for reservation in &reservations {
        for ancestor in reservation.path.ancestors().skip(1) {
            let Some(previous) =
                exact_paths.get(&(reservation.host_id.clone(), ancestor.to_path_buf()))
            else {
                continue;
            };
            return runtime_path_collision(&reservations[*previous], reservation);
        }
    }

    let mut reservations_by_parent = BTreeMap::<(String, PathBuf), Vec<usize>>::new();
    for (index, reservation) in reservations.iter().enumerate() {
        let Some(parent) = reservation.path.parent() else {
            continue;
        };
        reservations_by_parent
            .entry((reservation.host_id.clone(), parent.to_path_buf()))
            .or_default()
            .push(index);
    }
    for indexes in reservations_by_parent.values_mut() {
        indexes.sort_by(|left, right| {
            reservations[*left]
                .path
                .file_name()
                .expect("runtime path reservation has a file name")
                .as_bytes()
                .cmp(
                    reservations[*right]
                        .path
                        .file_name()
                        .expect("runtime path reservation has a file name")
                        .as_bytes(),
                )
        });
        for (position, index) in indexes.iter().enumerate() {
            let prefix = &reservations[*index];
            if prefix.kind != RuntimePathReservationKind::FilenamePrefix {
                continue;
            }
            let prefix_bytes = prefix
                .path
                .file_name()
                .expect("runtime path prefix reservation has a file name")
                .as_bytes();
            if let Some(candidate_index) = indexes.get(position + 1) {
                let candidate = &reservations[*candidate_index];
                let candidate_bytes = candidate
                    .path
                    .file_name()
                    .expect("runtime path reservation has a file name")
                    .as_bytes();
                if !candidate_bytes.starts_with(prefix_bytes) {
                    continue;
                }
                return runtime_path_collision(prefix, candidate);
            }
        }
    }
    Ok(())
}

fn reserve_runtime_path(
    reservations: &mut Vec<RuntimePathReservation>,
    host_id: &str,
    path: PathBuf,
    kind: RuntimePathReservationKind,
    label: String,
) {
    reservations.push(RuntimePathReservation {
        host_id: host_id.to_string(),
        path,
        kind,
        label,
    });
}

fn runtime_path_collision(
    first: &RuntimePathReservation,
    second: &RuntimePathReservation,
) -> Result<(), String> {
    Err(format!(
        "runtime path collision on host {}: {} conflicts with {}",
        first.host_id, first.label, second.label
    ))
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut result = path.as_os_str().to_os_string();
    result.push(suffix);
    PathBuf::from(result)
}

fn validate_tls_identities(identities: &[TlsIdentityInput]) -> Result<BTreeSet<&str>, String> {
    let mut result = BTreeSet::new();
    for identity in identities {
        validate_identifier(
            &identity.id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "TLS identity id",
        )?;
        validate_file_reference(&identity.certificate_ref, "TLS certificate reference")?;
        validate_file_reference(&identity.private_key_ref, "TLS private key reference")?;
        if !result.insert(identity.id.as_str()) {
            return Err(format!("duplicate TLS identity id {}", identity.id));
        }
    }
    Ok(result)
}

fn validate_tls_trust_bundles(bundles: &[TlsTrustBundleInput]) -> Result<BTreeSet<&str>, String> {
    let mut result = BTreeSet::new();
    for bundle in bundles {
        validate_identifier(
            &bundle.id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "TLS trust bundle id",
        )?;
        validate_file_reference(&bundle.ca_bundle_ref, "TLS CA bundle reference")?;
        if !result.insert(bundle.id.as_str()) {
            return Err(format!("duplicate TLS trust bundle id {}", bundle.id));
        }
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn validate_endpoints(
    endpoints: &[EndpointInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
    transport_profiles: &BTreeMap<&str, &TransportProfileInput>,
    tls_identities: &BTreeSet<&str>,
    tls_trust_bundles: &BTreeSet<&str>,
    deployment: &DeploymentInput,
) -> Result<(), String> {
    let authority_by_process: BTreeMap<&str, &AuthorityInput> = authorities
        .values()
        .map(|authority| (authority.process_id.as_str(), *authority))
        .collect();
    let storage_by_process: BTreeMap<&str, &StorageNodeInput> = storage_nodes
        .values()
        .map(|storage_node| (storage_node.process_id.as_str(), *storage_node))
        .collect();
    let mut endpoint_ids = BTreeSet::new();
    let mut priorities = BTreeSet::new();
    let mut unix_paths = BTreeSet::new();
    let mut tcp_listeners: Vec<(&str, String, u16, &str)> = Vec::new();
    let mut protocols_by_process = BTreeSet::new();
    let mut tcp_protocols_by_process = BTreeSet::new();

    for endpoint in endpoints {
        validate_identifier(&endpoint.id, CLUSTER_MANIFEST_MAX_ID_BYTES, "endpoint id")?;
        if !endpoint_ids.insert(endpoint.id.as_str()) {
            return Err(format!("duplicate endpoint id {}", endpoint.id));
        }
        let process = processes
            .get(endpoint.owner_process_id.as_str())
            .ok_or_else(|| {
                format!(
                    "endpoint {} references unknown process {}",
                    endpoint.id, endpoint.owner_process_id
                )
            })?;
        require_nonzero(endpoint.priority, "endpoint priority")?;
        if !priorities.insert((
            endpoint.owner_process_id.as_str(),
            endpoint.protocol,
            endpoint.priority,
        )) {
            return Err(format!(
                "duplicate endpoint priority for process {} protocol {:?}",
                endpoint.owner_process_id, endpoint.protocol
            ));
        }
        let profile = transport_profiles
            .get(endpoint.transport_profile_id.as_str())
            .ok_or_else(|| {
                format!(
                    "endpoint {} references unknown transport profile {}",
                    endpoint.id, endpoint.transport_profile_id
                )
            })?;
        if endpoint.protocol == EndpointProtocol::RaftPeer
            && profile.max_frame_bytes > CLUSTER_MANIFEST_MAX_RAFT_FRAME_BYTES
        {
            return Err(format!(
                "Raft endpoint {} frame limit exceeds {}",
                endpoint.id, CLUSTER_MANIFEST_MAX_RAFT_FRAME_BYTES
            ));
        }
        if matches!(
            endpoint.protocol,
            EndpointProtocol::ControlPlane | EndpointProtocol::AuthorityClockRecovery
        ) && profile.max_frame_bytes
            != u64::try_from(CONTROL_PLANE_RPC_MAX_FRAME_BYTES)
                .expect("control-plane maximum frame size fits u64")
        {
            return Err(format!(
                "control-plane endpoint {} frame limit must equal the protocol maximum encoded frame size {}",
                endpoint.id,
                CONTROL_PLANE_RPC_MAX_FRAME_BYTES
            ));
        }

        let listen = parse_endpoint_address(&endpoint.listen, true)?;
        let advertise = parse_endpoint_address(&endpoint.advertise, false)?;
        if endpoint.listen != canonical_endpoint_uri(&listen) {
            return Err(format!(
                "endpoint {} listen URI is not canonical",
                endpoint.id
            ));
        }
        if endpoint.advertise != canonical_endpoint_uri(&advertise) {
            return Err(format!(
                "endpoint {} advertise URI is not canonical",
                endpoint.id
            ));
        }
        match (&listen, &advertise) {
            (EndpointAddress::Unix(listen), EndpointAddress::Unix(advertise)) => {
                if listen != advertise {
                    return Err(format!(
                        "Unix endpoint {} listen and advertise paths must match",
                        endpoint.id
                    ));
                }
                if endpoint.tls_identity_id.is_some()
                    || endpoint.tls_trust_bundle_id.is_some()
                    || endpoint.tls_server_name.is_some()
                {
                    return Err(format!(
                        "Unix endpoint {} must omit TLS fields",
                        endpoint.id
                    ));
                }
                if !unix_paths.insert((process.host_id.as_str(), listen.clone())) {
                    return Err(format!(
                        "duplicate Unix endpoint path on host {}",
                        process.host_id
                    ));
                }
            }
            (
                EndpointAddress::Tcp {
                    host: listen_host,
                    port: listen_port,
                },
                EndpointAddress::Tcp {
                    host: advertise_host,
                    port: advertise_port,
                },
            ) => {
                for (existing_host_id, existing_host, existing_port, existing_endpoint_id) in
                    &tcp_listeners
                {
                    if *existing_host_id == process.host_id
                        && *existing_port == *listen_port
                        && tcp_listener_hosts_collide(existing_host, listen_host)
                    {
                        return Err(format!(
                            "TCP listener {} at {}:{} collides on host {} with endpoint {} at {}:{}",
                            endpoint.id,
                            listen_host,
                            listen_port,
                            process.host_id,
                            existing_endpoint_id,
                            existing_host,
                            existing_port
                        ));
                    }
                }
                tcp_listeners.push((
                    process.host_id.as_str(),
                    listen_host.clone(),
                    *listen_port,
                    endpoint.id.as_str(),
                ));
                if listen_port != advertise_port {
                    return Err(format!(
                        "TCP endpoint {} listen and advertise ports must match",
                        endpoint.id
                    ));
                }
                let identity = endpoint.tls_identity_id.as_deref().ok_or_else(|| {
                    format!("TCP endpoint {} requires tls_identity_id", endpoint.id)
                })?;
                if !tls_identities.contains(identity) {
                    return Err(format!(
                        "TCP endpoint {} references unknown TLS identity {}",
                        endpoint.id, identity
                    ));
                }
                let trust_bundle = endpoint.tls_trust_bundle_id.as_deref().ok_or_else(|| {
                    format!("TCP endpoint {} requires tls_trust_bundle_id", endpoint.id)
                })?;
                if !tls_trust_bundles.contains(trust_bundle) {
                    return Err(format!(
                        "TCP endpoint {} references unknown TLS trust bundle {}",
                        endpoint.id, trust_bundle
                    ));
                }
                let server_name = endpoint.tls_server_name.as_deref().ok_or_else(|| {
                    format!("TCP endpoint {} requires tls_server_name", endpoint.id)
                })?;
                validate_server_name(server_name)?;
                if advertise_host != server_name {
                    return Err(format!(
                        "TCP endpoint {} tls_server_name must match its advertised host",
                        endpoint.id
                    ));
                }
                if deployment.internal_auth != InternalAuth::Required {
                    return Err(format!(
                        "TCP endpoint {} requires internal authentication",
                        endpoint.id
                    ));
                }
                if matches!(
                    endpoint.protocol,
                    EndpointProtocol::ControlPlane | EndpointProtocol::AuthorityClockRecovery
                ) && Duration::from_millis(profile.io_timeout_ms)
                    < CONTROL_PLANE_RPC_MAX_SERVER_OPERATION_TIMEOUT
                {
                    return Err(format!(
                        "control-plane TCP endpoint {} io timeout must be at least {} ms to cover the longest authenticated RPC",
                        endpoint.id,
                        CONTROL_PLANE_RPC_MAX_SERVER_OPERATION_TIMEOUT.as_millis()
                    ));
                }
                tcp_protocols_by_process
                    .insert((endpoint.owner_process_id.as_str(), endpoint.protocol));
            }
            _ => {
                return Err(format!(
                    "endpoint {} listen and advertise transports differ",
                    endpoint.id
                ));
            }
        }

        match endpoint.protocol {
            EndpointProtocol::RaftPeer => {
                let authority = authority_by_process
                    .get(endpoint.owner_process_id.as_str())
                    .ok_or_else(|| {
                        format!(
                            "Raft endpoint {} owner does not host an authority",
                            endpoint.id
                        )
                    })?;
                if authority.kind != AuthorityKind::RaftVoter {
                    return Err(format!(
                        "Raft endpoint {} owner is not a Raft voter",
                        endpoint.id
                    ));
                }
            }
            EndpointProtocol::ControlPlane | EndpointProtocol::AuthorityClockRecovery => {
                if !authority_by_process.contains_key(endpoint.owner_process_id.as_str()) {
                    return Err(format!(
                        "control-plane endpoint {} owner does not host an authority",
                        endpoint.id
                    ));
                }
            }
            EndpointProtocol::StorageRpc => {
                if !storage_by_process.contains_key(endpoint.owner_process_id.as_str()) {
                    return Err(format!(
                        "storage endpoint {} owner does not host a storage node",
                        endpoint.id
                    ));
                }
            }
        }
        protocols_by_process.insert((endpoint.owner_process_id.as_str(), endpoint.protocol));
    }

    for authority in authorities.values() {
        let process = processes[authority.process_id.as_str()];
        let embedded_single =
            authority.kind == AuthorityKind::Single && process.kind == ProcessKind::AllInOne;
        if authority.kind == AuthorityKind::RaftVoter
            && !protocols_by_process
                .contains(&(authority.process_id.as_str(), EndpointProtocol::RaftPeer))
        {
            return Err(format!(
                "Raft voter {} is missing a raft-peer endpoint",
                authority.id
            ));
        }
        if authority.kind == AuthorityKind::RaftVoter
            && authorities
                .values()
                .any(|peer| processes[peer.process_id.as_str()].host_id != process.host_id)
            && !tcp_protocols_by_process
                .contains(&(authority.process_id.as_str(), EndpointProtocol::RaftPeer))
        {
            return Err(format!(
                "Raft voter {} requires a TCP raft-peer endpoint for cross-host peers",
                authority.id
            ));
        }
        for protocol in [
            EndpointProtocol::ControlPlane,
            EndpointProtocol::AuthorityClockRecovery,
        ] {
            if !embedded_single
                && !protocols_by_process.contains(&(authority.process_id.as_str(), protocol))
            {
                return Err(format!(
                    "authority {} is missing required {:?} endpoint",
                    authority.id, protocol
                ));
            }
        }
        let remote_control_plane_client = processes.values().any(|client| {
            (client.kind.has_frontend() || client.kind.has_storage_node())
                && client.host_id != process.host_id
        });
        if remote_control_plane_client
            && !tcp_protocols_by_process.contains(&(
                authority.process_id.as_str(),
                EndpointProtocol::ControlPlane,
            ))
        {
            return Err(format!(
                "authority {} requires a TCP control-plane endpoint for cross-host clients",
                authority.id
            ));
        }
        let remote_clock_client = processes
            .values()
            .any(|client| client.admin_instance_id.is_some() && client.host_id != process.host_id);
        if remote_clock_client
            && !tcp_protocols_by_process.contains(&(
                authority.process_id.as_str(),
                EndpointProtocol::AuthorityClockRecovery,
            ))
        {
            return Err(format!(
                "authority {} requires a TCP authority-clock-recovery endpoint for cross-host clients",
                authority.id
            ));
        }
    }
    for storage_node in storage_nodes.values() {
        if !protocols_by_process.contains(&(
            storage_node.process_id.as_str(),
            EndpointProtocol::StorageRpc,
        )) {
            return Err(format!(
                "storage node {} is missing a storage-rpc endpoint",
                storage_node.node_id
            ));
        }
        let process = processes[storage_node.process_id.as_str()];
        let remote_storage_client = processes.values().any(|client| {
            (client.kind.has_frontend() || client.kind.has_storage_node())
                && client.host_id != process.host_id
        });
        if remote_storage_client
            && !tcp_protocols_by_process.contains(&(
                storage_node.process_id.as_str(),
                EndpointProtocol::StorageRpc,
            ))
        {
            return Err(format!(
                "storage node {} requires a TCP storage-rpc endpoint for cross-host clients",
                storage_node.node_id
            ));
        }
    }
    Ok(())
}

fn tcp_listener_hosts_collide(first: &str, second: &str) -> bool {
    first == second || matches!(first, "0.0.0.0" | "::") || matches!(second, "0.0.0.0" | "::")
}

fn validate_deployment(
    manifest: &StaticClusterManifestInput,
    hosts: &BTreeSet<&str>,
    disks: &BTreeMap<&str, &DiskInput>,
    processes: &BTreeMap<&str, &ProcessInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
) -> Result<Vec<Vec<u32>>, String> {
    match manifest.deployment.mode {
        DeploymentMode::Standalone => {
            if hosts.len() != 1 {
                return Err("standalone deployment requires exactly one host".to_string());
            }
            if manifest.deployment.failure_domain != FailureDomain::None
                || manifest.deployment.failure_tolerance != 0
            {
                return Err(
                    "standalone deployment requires failure_domain=none and tolerance=0"
                        .to_string(),
                );
            }
            if manifest.storage.ec_data_shards != 1 || manifest.storage.ec_parity_shards != 0 {
                return Err("standalone deployment requires EC 1+0".to_string());
            }
            if authorities.len() != 1
                || authorities
                    .values()
                    .any(|authority| authority.kind != AuthorityKind::Single)
            {
                return Err(
                    "standalone deployment requires exactly one single authority".to_string(),
                );
            }
            if storage_nodes.len() != 1 {
                return Err("standalone deployment requires exactly one storage node".to_string());
            }
        }
        DeploymentMode::Replicated => {
            if processes
                .values()
                .any(|process| process.kind == ProcessKind::AllInOne)
            {
                return Err(
                    "all-in-one processes are permitted only in standalone deployment".to_string(),
                );
            }
            if manifest.deployment.internal_auth != InternalAuth::Required {
                return Err("replicated deployment requires internal authentication".to_string());
            }
            if !matches!(
                manifest.deployment.failure_domain,
                FailureDomain::Disk | FailureDomain::Host
            ) || manifest.deployment.failure_tolerance == 0
            {
                return Err(
                    "replicated deployment requires disk/host failure domain and nonzero tolerance"
                        .to_string(),
                );
            }
            if manifest.storage.ec_parity_shards < manifest.deployment.failure_tolerance {
                return Err("replicated deployment parity must cover failure tolerance".to_string());
            }
            let required_storage_domains = usize::from(manifest.storage.ec_data_shards)
                + usize::from(manifest.storage.ec_parity_shards);
            let storage_domains: BTreeSet<&str> = storage_nodes
                .values()
                .map(|storage_node| {
                    let process = processes[storage_node.process_id.as_str()];
                    match manifest.deployment.failure_domain {
                        FailureDomain::Disk => storage_node.disk_id.as_str(),
                        FailureDomain::Host => process.host_id.as_str(),
                        FailureDomain::None => unreachable!(),
                    }
                })
                .collect();
            if storage_domains.len() < required_storage_domains {
                return Err(format!(
                    "replicated deployment requires at least {required_storage_domains} storage failure domains"
                ));
            }
            let required_voters = usize::from(manifest.deployment.failure_tolerance) * 2 + 1;
            if authorities.len() < required_voters
                || authorities
                    .values()
                    .any(|authority| authority.kind != AuthorityKind::RaftVoter)
            {
                return Err(format!(
                    "replicated deployment requires at least {required_voters} Raft voters"
                ));
            }
            let voter_domains: BTreeSet<&str> = authorities
                .values()
                .map(|authority| {
                    let process = processes[authority.process_id.as_str()];
                    match manifest.deployment.failure_domain {
                        FailureDomain::Disk => authority.disk_id.as_str(),
                        FailureDomain::Host => process.host_id.as_str(),
                        FailureDomain::None => unreachable!(),
                    }
                })
                .collect();
            if voter_domains.len() != authorities.len() {
                return Err("Raft voters must occupy distinct selected failure domains".to_string());
            }
        }
    }

    for process in processes.values() {
        let hosts_authority = authorities
            .values()
            .any(|authority| authority.process_id == process.id);
        let hosts_storage = storage_nodes
            .values()
            .any(|storage_node| storage_node.process_id == process.id);
        if process.kind.has_control_plane() != hosts_authority {
            return Err(format!(
                "process {} control-plane kind does not match hosted authority",
                process.id
            ));
        }
        if process.kind.has_storage_node() != hosts_storage {
            return Err(format!(
                "process {} storage kind does not match hosted storage node",
                process.id
            ));
        }
        if manifest.deployment.internal_auth == InternalAuth::Required
            && process.kind.has_frontend()
            && process.frontend_instance_id.is_none()
        {
            return Err(format!(
                "authenticated frontend process {} requires frontend_instance_id",
                process.id
            ));
        }
        if manifest.deployment.mode == DeploymentMode::Replicated
            && manifest.deployment.internal_auth == InternalAuth::Required
            && process.kind.has_frontend()
            && process.maintenance_instance_id.is_none()
        {
            return Err(format!(
                "authenticated replicated frontend process {} requires maintenance_instance_id for background workflows",
                process.id
            ));
        }
        if manifest.deployment.internal_auth == InternalAuth::Required
            && (process.kind.has_frontend() || process.kind.has_control_plane())
            && process.admin_instance_id.is_none()
        {
            return Err(format!(
                "authenticated admin-capable process {} requires admin_instance_id",
                process.id
            ));
        }
    }

    validate_initial_pg_placement(manifest, hosts, disks, processes, storage_nodes)
}

fn validate_initial_pg_placement(
    manifest: &StaticClusterManifestInput,
    hosts: &BTreeSet<&str>,
    disks: &BTreeMap<&str, &DiskInput>,
    processes: &BTreeMap<&str, &ProcessInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
) -> Result<Vec<Vec<u32>>, String> {
    let pg_count = usize::try_from(manifest.storage.pg_count)
        .map_err(|_| "storage PG count does not fit this platform".to_string())?;
    validate_collection_len(pg_count, "storage PG count")?;
    let host_domains: BTreeMap<&str, u32> = hosts
        .iter()
        .enumerate()
        .map(|(index, host_id)| {
            let domain = u32::try_from(index + 1)
                .expect("manifest collection bound guarantees a u32 host domain");
            (*host_id, domain)
        })
        .collect();
    let disk_domains: BTreeMap<&str, u32> = disks
        .keys()
        .enumerate()
        .map(|(index, disk_id)| {
            let domain = u32::try_from(index + 1)
                .expect("manifest collection bound guarantees a u32 disk domain");
            (*disk_id, domain)
        })
        .collect();
    let placement_nodes: Vec<NodeInfo> = storage_nodes
        .values()
        .map(|storage_node| {
            let process = processes[storage_node.process_id.as_str()];
            let location = TopologyKey::new(&[
                (Level::MACHINE, host_domains[process.host_id.as_str()]),
                (Level::DISK, disk_domains[storage_node.disk_id.as_str()]),
            ])
            .expect("distinct built-in topology levels");
            NodeInfo {
                id: NodeId::new(storage_node.node_id),
                location,
                weight: 1.0,
            }
        })
        .collect();
    let cluster_map = ClusterMap::new(&placement_nodes)
        .map_err(|error| format!("initial PG placement cluster map is invalid: {error}"))?;
    let total_shards = usize::from(manifest.storage.ec_data_shards)
        + usize::from(manifest.storage.ec_parity_shards);
    let placement_config = PlacementConfig::new(total_shards)
        .map_err(|error| format!("initial PG placement shape is invalid: {error}"))?;
    let constraint = match manifest.deployment.failure_domain {
        FailureDomain::None => PlacementConstraint::none(),
        FailureDomain::Disk => PlacementConstraint::level_cap(Level::DISK, 1),
        FailureDomain::Host => PlacementConstraint::level_cap(Level::MACHINE, 1),
    };
    let placer = Placer::new(placement_config, &cluster_map, constraint)
        .map_err(|error| format!("initial PG placement is impossible: {error}"))?;
    let node_domains: BTreeMap<u32, &str> = storage_nodes
        .values()
        .map(|storage_node| {
            let process = processes[storage_node.process_id.as_str()];
            let domain = match manifest.deployment.failure_domain {
                FailureDomain::None => process.host_id.as_str(),
                FailureDomain::Disk => storage_node.disk_id.as_str(),
                FailureDomain::Host => process.host_id.as_str(),
            };
            (storage_node.node_id, domain)
        })
        .collect();

    let mut acting_sets = Vec::with_capacity(pg_count);
    let mut placement_key =
        Vec::with_capacity(INITIAL_PG_PLACEMENT_KEY_DOMAIN.len() + std::mem::size_of::<u32>());
    for pg_id in 0..manifest.storage.pg_count {
        placement_key.clear();
        placement_key.extend_from_slice(INITIAL_PG_PLACEMENT_KEY_DOMAIN);
        placement_key.extend_from_slice(&pg_id.to_be_bytes());
        let mut acting_set = vec![NodeId::new(0); total_shards];
        placer
            .place(&placement_key, &mut acting_set)
            .map_err(|error| {
                format!("initial placement for PG {pg_id} violates deployment policy: {error}")
            })?;
        if manifest.deployment.failure_domain != FailureDomain::None {
            let distinct_domains: BTreeSet<&str> = acting_set
                .iter()
                .map(|node_id| node_domains[&node_id.as_u32()])
                .collect();
            if distinct_domains.len() != total_shards {
                return Err(format!(
                    "initial placement for PG {pg_id} does not occupy {total_shards} distinct failure domains"
                ));
            }
        }
        acting_sets.push(acting_set.into_iter().map(NodeId::as_u32).collect());
    }
    Ok(acting_sets)
}

fn validate_auth_credentials(
    credentials: &[AuthCredentialInput],
    processes: &BTreeMap<&str, &ProcessInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
    internal_auth: InternalAuth,
) -> Result<(), String> {
    let raft_nodes: BTreeSet<u64> = authorities
        .values()
        .filter_map(|authority| authority.raft_node_id)
        .collect();
    let storage_node_ids: BTreeSet<u64> = storage_nodes
        .keys()
        .map(|node_id| u64::from(*node_id))
        .collect();
    let frontend_ids: BTreeSet<&str> = processes
        .values()
        .filter_map(|process| process.frontend_instance_id.as_deref())
        .collect();
    let admin_ids: BTreeSet<&str> = processes
        .values()
        .filter_map(|process| process.admin_instance_id.as_deref())
        .collect();
    let maintenance_ids: BTreeSet<&str> = processes
        .values()
        .filter_map(|process| process.maintenance_instance_id.as_deref())
        .collect();

    let mut identities = BTreeSet::new();
    let mut signing_by_principal: BTreeMap<CredentialPrincipalKey, Vec<(u64, Option<u64>)>> =
        BTreeMap::new();
    for credential in credentials {
        validate_identifier(
            &credential.credential_id,
            CLUSTER_MANIFEST_MAX_ID_BYTES,
            "credential id",
        )?;
        require_nonzero(credential.credential_version, "credential version")?;
        validate_file_reference(&credential.secret_ref, "credential secret reference")?;
        if credential
            .accept_until_ms
            .is_some_and(|until| until <= credential.accept_from_ms)
        {
            return Err(format!(
                "credential {} acceptance window is empty",
                credential.credential_id
            ));
        }
        let principal = credential_principal_key(credential)?;
        match (&principal.principal, &principal.id) {
            (AuthPrincipal::RaftPeer, CredentialPrincipalId::Node(node_id))
                if raft_nodes.contains(node_id) => {}
            (AuthPrincipal::StorageNode, CredentialPrincipalId::Node(node_id))
                if storage_node_ids.contains(node_id) => {}
            (AuthPrincipal::Frontend, CredentialPrincipalId::Instance(instance_id))
                if frontend_ids.contains(instance_id.as_str()) => {}
            (AuthPrincipal::Admin, CredentialPrincipalId::Instance(instance_id))
                if admin_ids.contains(instance_id.as_str()) => {}
            (AuthPrincipal::Maintenance, CredentialPrincipalId::Instance(instance_id))
                if maintenance_ids.contains(instance_id.as_str()) => {}
            _ => {
                return Err(format!(
                    "credential {} references an unknown or wrong-kind principal",
                    credential.credential_id
                ));
            }
        }
        let identity = CredentialIdentity {
            principal: principal.clone(),
            credential_id: credential.credential_id.clone(),
            credential_version: credential.credential_version,
        };
        if !identities.insert(identity) {
            return Err(format!(
                "duplicate credential identity {} version {}",
                credential.credential_id, credential.credential_version
            ));
        }
        if credential.use_for_signing {
            signing_by_principal
                .entry(principal)
                .or_default()
                .push((credential.accept_from_ms, credential.accept_until_ms));
        }
    }

    for windows in signing_by_principal.values_mut() {
        windows.sort_unstable_by_key(|window| window.0);
        for pair in windows.windows(2) {
            if pair[0].1.is_none_or(|until| until > pair[1].0) {
                return Err("signing credential acceptance windows overlap".to_string());
            }
        }
    }

    if internal_auth == InternalAuth::Required {
        let required = required_auth_principals(
            &raft_nodes,
            &storage_node_ids,
            &frontend_ids,
            &admin_ids,
            &maintenance_ids,
        );
        for principal in required {
            if !signing_by_principal.contains_key(&principal) {
                return Err("required principal has no signing credential".to_string());
            }
        }
    }
    Ok(())
}

fn resolve_canonical_raft_peer_endpoints(
    manifest: &StaticClusterManifestInput,
    authorities: &BTreeMap<&str, &AuthorityInput>,
) -> Result<BTreeMap<u64, CanonicalRaftPeerEndpoint>, String> {
    let process_hosts: BTreeMap<&str, &str> = manifest
        .processes
        .iter()
        .map(|process| (process.id.as_str(), process.host_id.as_str()))
        .collect();
    let voter_hosts: BTreeSet<&str> = authorities
        .values()
        .filter(|authority| authority.kind == AuthorityKind::RaftVoter)
        .map(|authority| process_hosts[authority.process_id.as_str()])
        .collect();
    let mut resolved = BTreeMap::new();
    let mut advertised_voters = BTreeMap::<&str, (u64, &str)>::new();
    for authority in authorities
        .values()
        .filter(|authority| authority.kind == AuthorityKind::RaftVoter)
    {
        let target_host = process_hosts[authority.process_id.as_str()];
        let requires_tcp = voter_hosts
            .iter()
            .any(|source_host| *source_host != target_host);
        let endpoint = manifest
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.protocol == EndpointProtocol::RaftPeer
                    && endpoint.owner_process_id == authority.process_id
            })
            .filter(|endpoint| {
                !requires_tcp
                    || matches!(
                        parse_endpoint_address(&endpoint.advertise, false),
                        Ok(EndpointAddress::Tcp { .. })
                    )
            })
            .min_by_key(|endpoint| (endpoint.priority, endpoint.id.as_str()))
            .ok_or_else(|| {
                format!(
                    "Raft voter {} has no peer endpoint reachable from every configured voter",
                    authority.id
                )
            })?;
        let node_id = authority
            .raft_node_id
            .expect("replicated authority validation requires a Raft node id");
        if let Some((other_node_id, other_endpoint_id)) =
            advertised_voters.insert(endpoint.advertise.as_str(), (node_id, endpoint.id.as_str()))
        {
            return Err(format!(
                "Raft voters {other_node_id} and {node_id} select the same canonical advertised endpoint through {} and {}; each voter requires a distinct routable endpoint",
                other_endpoint_id, endpoint.id
            ));
        }
        resolved.insert(
            node_id,
            CanonicalRaftPeerEndpoint {
                endpoint_id: endpoint.id.clone(),
                owner_process_id: endpoint.owner_process_id.clone(),
                advertise: endpoint.advertise.clone(),
            },
        );
    }
    Ok(resolved)
}

fn resolve_canonical_storage_node_endpoints(
    manifest: &StaticClusterManifestInput,
    storage_nodes: &BTreeMap<u32, &StorageNodeInput>,
) -> Result<BTreeMap<u32, CanonicalStorageNodeEndpoint>, String> {
    let processes = manifest
        .processes
        .iter()
        .map(|process| (process.id.as_str(), process))
        .collect::<BTreeMap<_, _>>();
    let storage_client_hosts = manifest
        .processes
        .iter()
        .filter(|process| process.kind.has_frontend() || process.kind.has_storage_node())
        .map(|process| process.host_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut resolved = BTreeMap::new();
    let mut advertised_storage_nodes = BTreeMap::<&str, (u32, &str)>::new();
    for storage_node in storage_nodes.values() {
        let target = processes[storage_node.process_id.as_str()];
        let requires_tcp = storage_client_hosts
            .iter()
            .any(|source_host| *source_host != target.host_id);
        let endpoint = manifest
            .endpoints
            .iter()
            .filter(|endpoint| {
                endpoint.protocol == EndpointProtocol::StorageRpc
                    && endpoint.owner_process_id == storage_node.process_id
            })
            .filter(|endpoint| {
                !requires_tcp
                    || matches!(
                        parse_endpoint_address(&endpoint.advertise, false),
                        Ok(EndpointAddress::Tcp { .. })
                    )
            })
            .min_by_key(|endpoint| (endpoint.priority, endpoint.id.as_str()))
            .ok_or_else(|| {
                format!(
                    "storage node {} has no storage-rpc endpoint reachable from every configured storage client",
                    storage_node.node_id
                )
            })?;
        if let Some((other_node_id, other_endpoint_id)) = advertised_storage_nodes.insert(
            endpoint.advertise.as_str(),
            (storage_node.node_id, endpoint.id.as_str()),
        ) {
            return Err(format!(
                "storage nodes {other_node_id} and {} select the same canonical advertised endpoint through {} and {}; each storage node requires a distinct routable endpoint",
                storage_node.node_id, other_endpoint_id, endpoint.id
            ));
        }
        resolved.insert(
            storage_node.node_id,
            CanonicalStorageNodeEndpoint {
                endpoint_id: endpoint.id.clone(),
                owner_process_id: endpoint.owner_process_id.clone(),
                advertise: endpoint.advertise.clone(),
            },
        );
    }
    Ok(resolved)
}

fn validate_raft_transport_capacity(
    manifest: &StaticClusterManifestInput,
    transport_profiles: &BTreeMap<&str, &TransportProfileInput>,
    authorities: &BTreeMap<&str, &AuthorityInput>,
    canonical_raft_peer_endpoints: &BTreeMap<u64, CanonicalRaftPeerEndpoint>,
) -> Result<(), String> {
    let voter_processes: BTreeSet<&str> = authorities
        .values()
        .filter(|authority| authority.kind == AuthorityKind::RaftVoter)
        .map(|authority| authority.process_id.as_str())
        .collect();
    if voter_processes.is_empty() {
        return Ok(());
    }

    let mut max_advertised_uri_by_process = BTreeMap::new();
    for endpoint in manifest
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.protocol == EndpointProtocol::RaftPeer)
    {
        max_advertised_uri_by_process
            .entry(endpoint.owner_process_id.as_str())
            .and_modify(|current: &mut usize| *current = (*current).max(endpoint.advertise.len()))
            .or_insert(endpoint.advertise.len());
    }
    let membership_metadata_bytes =
        voter_processes
            .iter()
            .try_fold(0_u64, |total, process_id| -> Result<u64, String> {
                let uri_bytes =
                    *max_advertised_uri_by_process
                        .get(process_id)
                        .ok_or_else(|| {
                            format!("Raft voter process {process_id} has no peer endpoint")
                        })?;
                let per_voter = u64::try_from(uri_bytes)
                    .map_err(|_| "Raft peer URI length does not fit u64".to_string())?
                    .checked_add(32)
                    .ok_or_else(|| "Raft snapshot metadata size overflows".to_string())?;
                total
                    .checked_add(per_voter)
                    .ok_or_else(|| "Raft snapshot metadata size overflows".to_string())
            })?;
    let max_credential_id_bytes = manifest
        .auth_credentials
        .iter()
        .filter(|credential| credential.principal == AuthPrincipal::RaftPeer)
        .map(|credential| credential.credential_id.len())
        .max()
        .unwrap_or(0);
    let identity_bytes = manifest
        .cluster
        .id
        .len()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(max_credential_id_bytes))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| "Raft snapshot identity metadata size overflows".to_string())?;
    let required_append_frame_bytes = manifest
        .raft
        .max_append_bytes
        .checked_add(CLUSTER_MANIFEST_RAFT_APPEND_FIXED_FRAME_OVERHEAD_BYTES)
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .ok_or_else(|| "Raft append frame size overflows".to_string())?;
    let required_frame_bytes = manifest
        .raft
        .max_snapshot_bytes
        .checked_add(CLUSTER_MANIFEST_RAFT_SNAPSHOT_FIXED_FRAME_OVERHEAD_BYTES)
        .and_then(|bytes| bytes.checked_add(membership_metadata_bytes))
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .ok_or_else(|| "Raft snapshot frame size overflows".to_string())?;

    for endpoint in manifest
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.protocol == EndpointProtocol::RaftPeer)
    {
        let profile = transport_profiles[endpoint.transport_profile_id.as_str()];
        let limits = ControlPlaneRaftPeerTransportLimits {
            max_frame_bytes: usize::try_from(profile.max_frame_bytes)
                .map_err(|_| "Raft frame limit does not fit this platform".to_string())?,
            max_append_entries: usize::try_from(manifest.raft.max_append_entries)
                .map_err(|_| "Raft append-entry limit does not fit this platform".to_string())?,
            max_append_entries_bytes: usize::try_from(manifest.raft.max_append_bytes)
                .map_err(|_| "Raft append-byte limit does not fit this platform".to_string())?,
            max_snapshot_bytes: usize::try_from(manifest.raft.max_snapshot_bytes)
                .map_err(|_| "Raft snapshot limit does not fit this platform".to_string())?,
        };
        if profile.max_frame_bytes < required_append_frame_bytes {
            return Err(format!(
                "Raft endpoint {} frame limit cannot carry max append bytes plus identity and authentication overhead",
                endpoint.id
            ));
        }
        ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            manifest.cluster.id.clone(),
            canonical_raft_peer_endpoints
                .iter()
                .map(|(&node_id, endpoint)| (node_id, endpoint.advertise.clone())),
            limits,
        )
        .validate_replication_compatibility()
        .map_err(|_| {
            format!(
                "Raft endpoint {} transport limits are incompatible with production replication",
                endpoint.id
            )
        })?;
        if profile.max_frame_bytes < required_frame_bytes {
            return Err(format!(
                "Raft endpoint {} frame limit cannot carry max snapshot bytes plus metadata and authentication overhead",
                endpoint.id
            ));
        }
    }
    Ok(())
}

fn required_auth_principals(
    raft_nodes: &BTreeSet<u64>,
    storage_nodes: &BTreeSet<u64>,
    frontend_ids: &BTreeSet<&str>,
    admin_ids: &BTreeSet<&str>,
    maintenance_ids: &BTreeSet<&str>,
) -> BTreeSet<CredentialPrincipalKey> {
    let mut result = BTreeSet::new();
    result.extend(
        raft_nodes
            .iter()
            .copied()
            .map(|node_id| CredentialPrincipalKey {
                principal: AuthPrincipal::RaftPeer,
                id: CredentialPrincipalId::Node(node_id),
            }),
    );
    result.extend(
        storage_nodes
            .iter()
            .copied()
            .map(|node_id| CredentialPrincipalKey {
                principal: AuthPrincipal::StorageNode,
                id: CredentialPrincipalId::Node(node_id),
            }),
    );
    result.extend(
        frontend_ids
            .iter()
            .map(|instance_id| CredentialPrincipalKey {
                principal: AuthPrincipal::Frontend,
                id: CredentialPrincipalId::Instance((*instance_id).to_string()),
            }),
    );
    result.extend(admin_ids.iter().map(|instance_id| CredentialPrincipalKey {
        principal: AuthPrincipal::Admin,
        id: CredentialPrincipalId::Instance((*instance_id).to_string()),
    }));
    result.extend(
        maintenance_ids
            .iter()
            .map(|instance_id| CredentialPrincipalKey {
                principal: AuthPrincipal::Maintenance,
                id: CredentialPrincipalId::Instance((*instance_id).to_string()),
            }),
    );
    result
}

fn credential_principal_key(
    credential: &AuthCredentialInput,
) -> Result<CredentialPrincipalKey, String> {
    let id = match credential.principal {
        AuthPrincipal::RaftPeer | AuthPrincipal::StorageNode => {
            let node_id = credential.node_id.ok_or_else(|| {
                format!("credential {} requires node_id", credential.credential_id)
            })?;
            require_nonzero(node_id, "credential node id")?;
            if credential.instance_id.is_some() {
                return Err(format!(
                    "credential {} must omit instance_id",
                    credential.credential_id
                ));
            }
            CredentialPrincipalId::Node(node_id)
        }
        AuthPrincipal::Frontend | AuthPrincipal::Admin | AuthPrincipal::Maintenance => {
            let instance_id = credential.instance_id.as_deref().ok_or_else(|| {
                format!(
                    "credential {} requires instance_id",
                    credential.credential_id
                )
            })?;
            validate_identifier(
                instance_id,
                CLUSTER_MANIFEST_MAX_ID_BYTES,
                "credential instance id",
            )?;
            if credential.node_id.is_some() {
                return Err(format!(
                    "credential {} must omit node_id",
                    credential.credential_id
                ));
            }
            CredentialPrincipalId::Instance(instance_id.to_string())
        }
    };
    Ok(CredentialPrincipalKey {
        principal: credential.principal,
        id,
    })
}

fn credential_sort_key(
    credential: &AuthCredentialInput,
) -> (AuthPrincipal, Option<u64>, Option<&str>, &str, u64) {
    (
        credential.principal,
        credential.node_id,
        credential.instance_id.as_deref(),
        credential.credential_id.as_str(),
        credential.credential_version,
    )
}

fn canonical_endpoint_uri(address: &EndpointAddress) -> String {
    match address {
        EndpointAddress::Unix(path) => format!("unix://{}", path.display()),
        EndpointAddress::Tcp { host, port } if host.contains(':') => {
            format!("tcp://[{host}]:{port}")
        }
        EndpointAddress::Tcp { host, port } => format!("tcp://{host}:{port}"),
    }
}

fn tcp_socket_address(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn parse_endpoint_address(value: &str, listener: bool) -> Result<EndpointAddress, String> {
    if value.len() > CLUSTER_MANIFEST_MAX_URI_BYTES {
        return Err("endpoint URI exceeds size limit".to_string());
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err("endpoint URI contains a control character".to_string());
    }
    if let Some(path) = value.strip_prefix("unix://") {
        if path.contains(['?', '#']) {
            return Err("Unix endpoint URI must not contain query or fragment".to_string());
        }
        return Ok(EndpointAddress::Unix(normalize_absolute_path(
            Path::new(path),
            "Unix endpoint path",
        )?));
    }
    let Some(authority) = value.strip_prefix("tcp://") else {
        return Err("endpoint URI must use unix:// or tcp://".to_string());
    };
    if authority.contains(['/', '?', '#', '@']) {
        return Err("TCP endpoint URI contains unsupported components".to_string());
    }
    let (host, port_text) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest
            .split_once(']')
            .ok_or_else(|| "TCP IPv6 endpoint is missing closing bracket".to_string())?;
        let port = suffix
            .strip_prefix(':')
            .ok_or_else(|| "TCP endpoint is missing port".to_string())?;
        let address = host
            .parse::<Ipv6Addr>()
            .map_err(|_| "TCP endpoint has an invalid IPv6 address".to_string())?;
        let canonical = address.to_string();
        if host != canonical {
            return Err("TCP endpoint IPv6 address is not canonical".to_string());
        }
        (canonical, port)
    } else {
        let (host, port) = authority
            .rsplit_once(':')
            .ok_or_else(|| "TCP endpoint is missing port".to_string())?;
        if host.contains(':') {
            return Err("TCP IPv6 endpoint must use brackets".to_string());
        }
        validate_tcp_dns_or_ipv4_host(host)?;
        if listener && host.parse::<Ipv4Addr>().is_err() {
            return Err("TCP listener host must be a literal IP address".to_string());
        }
        (host.to_string(), port)
    };
    if !listener && matches!(host.as_str(), "0.0.0.0" | "::") {
        return Err("advertised TCP endpoint must not use an unspecified host".to_string());
    }
    let port = port_text
        .parse::<u16>()
        .map_err(|_| "TCP endpoint port is invalid".to_string())?;
    if port == 0 {
        return Err("TCP endpoint port must be nonzero".to_string());
    }
    Ok(EndpointAddress::Tcp { host, port })
}

fn validate_server_name(value: &str) -> Result<(), String> {
    if let Ok(address) = value.parse::<Ipv6Addr>() {
        if address.to_string() == value {
            return Ok(());
        }
        return Err("TLS server IPv6 address is not canonical".to_string());
    }
    validate_tcp_dns_or_ipv4_host(value)
}

fn validate_tcp_dns_or_ipv4_host(value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > CLUSTER_MANIFEST_MAX_ID_BYTES {
        return Err("TLS/TCP server name is invalid".to_string());
    }
    if let Ok(address) = value.parse::<Ipv4Addr>() {
        return if address.to_string() == value {
            Ok(())
        } else {
            Err("TCP IPv4 address is not canonical".to_string())
        };
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase())
        || value.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                || label.starts_with('-')
                || label.ends_with('-')
        })
    {
        return Err("TLS/TCP DNS name is not canonical".to_string());
    }
    Ok(())
}

fn normalize_absolute_path(path: &Path, field: &str) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(format!("{field} must be absolute"));
    }
    if path.as_os_str().as_encoded_bytes().len() > CLUSTER_MANIFEST_MAX_PATH_BYTES {
        return Err(format!("{field} exceeds path length limit"));
    }
    let mut result = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(component) => result.push(component),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() {
                    return Err(format!("{field} escapes filesystem root"));
                }
            }
            Component::Prefix(_) => {
                return Err(format!("{field} contains an unsupported path prefix"));
            }
        }
    }
    if result.as_os_str().as_encoded_bytes() != path.as_os_str().as_encoded_bytes() {
        return Err(format!("{field} is not canonical"));
    }
    if result == Path::new("/") {
        return Err(format!("{field} must not be filesystem root"));
    }
    Ok(result)
}

fn validate_file_reference(value: &str, field: &str) -> Result<(), String> {
    let path = value
        .strip_prefix("file:")
        .ok_or_else(|| format!("{field} must use file: with an absolute path"))?;
    normalize_absolute_path(Path::new(path), field)?;
    Ok(())
}

fn parse_exact_pem_sections(
    bytes: &[u8],
    label: &str,
) -> Result<Vec<(SectionKind, Vec<u8>)>, String> {
    if !bytes.is_ascii() {
        return Err(format!("{label} contains non-ASCII PEM content"));
    }

    let mut declared_kinds = Vec::new();
    let mut open_section: Option<(SectionKind, Vec<u8>)> = None;
    for raw_line in bytes.split(|byte| *byte == b'\n') {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        let Some((kind, section_label)) = open_section.as_ref() else {
            if line.is_empty() {
                continue;
            }
            let section_label = line
                .strip_prefix(b"-----BEGIN ")
                .and_then(|line| line.strip_suffix(b"-----"))
                .ok_or_else(|| format!("{label} contains content outside a PEM section"))?;
            let kind = SectionKind::try_from(section_label)
                .map_err(|_| format!("{label} contains an unsupported PEM section"))?;
            open_section = Some((kind, section_label.to_vec()));
            continue;
        };

        if let Some(end_label) = line
            .strip_prefix(b"-----END ")
            .and_then(|line| line.strip_suffix(b"-----"))
        {
            if end_label != section_label {
                return Err(format!("{label} contains a mismatched PEM end marker"));
            }
            declared_kinds.push(*kind);
            open_section = None;
        } else if line.starts_with(b"-----BEGIN ") || line.starts_with(b"-----END ") {
            return Err(format!(
                "{label} contains a nested or malformed PEM section"
            ));
        }
    }
    if open_section.is_some() {
        return Err(format!("{label} contains an unterminated PEM section"));
    }
    if declared_kinds.is_empty() {
        return Err(format!("{label} contains no PEM sections"));
    }

    let parsed = <(SectionKind, Vec<u8>)>::pem_slice_iter(bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| format!("{label} contains malformed PEM"))?;
    if parsed.len() != declared_kinds.len()
        || parsed
            .iter()
            .zip(&declared_kinds)
            .any(|((actual, _), declared)| actual != declared)
    {
        return Err(format!("{label} contains unparsed PEM content"));
    }
    Ok(parsed)
}

fn parse_exact_certificate_pem(
    bytes: &[u8],
    label: &str,
) -> Result<Vec<CertificateDer<'static>>, String> {
    parse_exact_pem_sections(bytes, label)?
        .into_iter()
        .map(|(kind, der)| {
            if kind != SectionKind::Certificate {
                return Err(format!(
                    "{label} must contain certificate PEM sections only"
                ));
            }
            Ok(CertificateDer::from(der))
        })
        .collect()
}

fn parse_exact_private_key_pem(
    bytes: &[u8],
    label: &str,
) -> Result<PrivateKeyDer<'static>, String> {
    let sections = parse_exact_pem_sections(bytes, label)?;
    if sections.len() != 1
        || !matches!(
            sections[0].0,
            SectionKind::RsaPrivateKey | SectionKind::PrivateKey | SectionKind::EcPrivateKey
        )
    {
        return Err(format!(
            "{label} must contain exactly one private-key PEM section"
        ));
    }
    PrivateKeyDer::from_pem_slice(bytes).map_err(|_| format!("{label} contains malformed PEM"))
}

fn validate_ca_trust_anchor(certificate: &CertificateDer<'_>) -> Result<(), ()> {
    let certificate = Certificate::from_der(certificate.as_ref()).map_err(|_| ())?;
    let Some((basic_constraints_critical, basic_constraints)) = certificate
        .tbs_certificate()
        .get_extension::<BasicConstraints>()
        .map_err(|_| ())?
    else {
        return Err(());
    };
    if !basic_constraints_critical || !basic_constraints.ca {
        return Err(());
    }
    let Some((key_usage_critical, key_usage)) = certificate
        .tbs_certificate()
        .get_extension::<KeyUsage>()
        .map_err(|_| ())?
    else {
        return Err(());
    };
    if !key_usage_critical || !key_usage.key_cert_sign() {
        return Err(());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum StaticMaterialFileAccess {
    Private,
    Public,
}

#[cfg(test)]
fn read_static_material_file(
    reference: &str,
    max_bytes: u64,
    access: StaticMaterialFileAccess,
    label: &str,
) -> Result<Vec<u8>, String> {
    read_static_material_file_with_aggregate_limit(reference, max_bytes, u64::MAX, access, label)
}

fn read_static_material_file_with_aggregate_limit(
    reference: &str,
    max_bytes: u64,
    aggregate_remaining_bytes: u64,
    access: StaticMaterialFileAccess,
    label: &str,
) -> Result<Vec<u8>, String> {
    let path = reference
        .strip_prefix("file:")
        .ok_or_else(|| format!("{label} reference must use file:"))?;
    let path = Path::new(path);
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .map_err(|error| format!("open {label} reference {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("inspect {label} reference {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "{label} reference {} is not a regular file",
            path.display()
        ));
    }
    let effective_uid = {
        // SAFETY: geteuid has no preconditions and does not mutate memory.
        unsafe { libc::geteuid() }
    };
    if metadata.uid() != effective_uid {
        return Err(format!(
            "{label} reference {} is not owned by the effective process user",
            path.display()
        ));
    }
    let mode = metadata.mode() & 0o777;
    match access {
        StaticMaterialFileAccess::Private if mode & 0o077 != 0 => {
            return Err(format!(
                "{label} reference {} grants group or other permissions",
                path.display()
            ));
        }
        StaticMaterialFileAccess::Public if mode & 0o022 != 0 => {
            return Err(format!(
                "{label} reference {} is group or other writable",
                path.display()
            ));
        }
        StaticMaterialFileAccess::Private | StaticMaterialFileAccess::Public => {}
    }
    if metadata.len() == 0 {
        return Err(format!("{label} reference {} is empty", path.display()));
    }
    if metadata.len() > max_bytes {
        return Err(format!(
            "{label} reference {} exceeds {max_bytes} bytes",
            path.display()
        ));
    }
    if metadata.len() > aggregate_remaining_bytes {
        return Err(format!(
            "selected-process material exceeds its aggregate byte limit while loading {label} reference {}",
            path.display()
        ));
    }
    let bounded_capacity = usize::try_from(metadata.len())
        .unwrap_or(usize::MAX)
        .min(max_bytes as usize);
    let mut bytes = Vec::with_capacity(bounded_capacity);
    file.take(max_bytes.min(aggregate_remaining_bytes) + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read {label} reference {}: {error}", path.display()))?;
    if bytes.len() > max_bytes as usize {
        return Err(format!(
            "{label} reference {} exceeds {max_bytes} bytes",
            path.display()
        ));
    }
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > aggregate_remaining_bytes {
        return Err(format!(
            "selected-process material exceeds its aggregate byte limit while loading {label} reference {}",
            path.display()
        ));
    }
    Ok(bytes)
}

fn validate_identifier(value: &str, max_len: usize, field: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > max_len
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(format!(
            "{field} must be nonempty printable non-space ASCII no longer than {max_len} bytes"
        ));
    }
    Ok(())
}

fn validate_timeout(value: u64, field: &str) -> Result<(), String> {
    if value == 0 || value > CLUSTER_MANIFEST_MAX_TIMEOUT_MS {
        return Err(format!(
            "{field} must be in 1..={CLUSTER_MANIFEST_MAX_TIMEOUT_MS}"
        ));
    }
    Ok(())
}

fn validate_collection_len(len: usize, field: &str) -> Result<(), String> {
    if len > CLUSTER_MANIFEST_MAX_COLLECTION_ITEMS {
        return Err(format!(
            "{field} exceeds {CLUSTER_MANIFEST_MAX_COLLECTION_ITEMS} entries"
        ));
    }
    Ok(())
}

fn require_nonzero<T>(value: T, field: &str) -> Result<(), String>
where
    T: PartialEq + From<u8>,
{
    if value == T::from(0) {
        return Err(format!("{field} must be nonzero"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProcessRole;
    use std::fmt::Write as _;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::os::unix::fs::symlink;

    fn complete_test_tls_handshake(
        client_config: Arc<RustlsClientConfig>,
        server_config: Arc<rustls::ServerConfig>,
        server_name: &str,
    ) {
        let mut client = rustls::ClientConnection::new(
            client_config,
            ServerName::try_from(server_name.to_string()).unwrap(),
        )
        .unwrap();
        let mut server = rustls::ServerConnection::new(server_config).unwrap();
        for _ in 0..16 {
            let mut client_tls = Vec::new();
            client.write_tls(&mut client_tls).unwrap();
            if !client_tls.is_empty() {
                server.read_tls(&mut Cursor::new(client_tls)).unwrap();
                server.process_new_packets().unwrap();
            }
            let mut server_tls = Vec::new();
            server.write_tls(&mut server_tls).unwrap();
            if !server_tls.is_empty() {
                client.read_tls(&mut Cursor::new(server_tls)).unwrap();
                client.process_new_packets().unwrap();
            }
            if !client.is_handshaking() && !server.is_handshaking() {
                break;
            }
        }
        assert!(!client.is_handshaking());
        assert!(!server.is_handshaking());
        assert_eq!(client.alpn_protocol(), Some(CONTROL_PLANE_RAFT_TLS_ALPN));
        assert_eq!(server.alpn_protocol(), Some(CONTROL_PLANE_RAFT_TLS_ALPN));

        client.writer().write_all(b"raft-frame").unwrap();
        let mut client_tls = Vec::new();
        client.write_tls(&mut client_tls).unwrap();
        server.read_tls(&mut Cursor::new(client_tls)).unwrap();
        server.process_new_packets().unwrap();
        let mut plaintext = [0_u8; 10];
        server.reader().read_exact(&mut plaintext).unwrap();
        assert_eq!(&plaintext, b"raft-frame");
    }

    fn standalone_runtime_environment() -> BTreeMap<&'static str, String> {
        BTreeMap::from([
            ("ARGMIN_ACCOUNT_ID", "111122223333".to_string()),
            ("ARGMIN_ACCESS_KEY_ID", "test-access-key".to_string()),
            (
                "ARGMIN_SECRET_ACCESS_KEY",
                "test-secret-access-key".to_string(),
            ),
            (
                "ARGMIN_SSE_S3_WRAPPING_KEY",
                "dGVzdC13cmFwcGluZy1rZXk=".to_string(),
            ),
        ])
    }

    fn write_manifest(contents: &str) -> (test_util::TempDir, PathBuf) {
        let dir = test_util::tempdir();
        let path = dir.path().join("cluster.toml");
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    fn private_dir(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn write_material_file(path: &Path, bytes: &[u8], mode: u32) {
        std::fs::write(path, bytes).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    fn materialized_replicated_manifest(
        selected_process_id: &str,
    ) -> (test_util::TempDir, ValidatedStaticClusterManifest) {
        materialized_replicated_manifest_from(selected_process_id, replicated_manifest())
    }

    fn materialized_replicated_manifest_from(
        selected_process_id: &str,
        manifest: String,
    ) -> (test_util::TempDir, ValidatedStaticClusterManifest) {
        let dir = test_util::tempdir();
        let material_dir = dir.path().join("material");
        private_dir(&material_dir);
        write_material_file(
            &material_dir.join("cluster-ca.pem"),
            include_bytes!("../../s3-tests/testdata/ca-cert.pem"),
            0o644,
        );
        write_material_file(
            &material_dir.join("host-1.crt"),
            include_bytes!("../../s3-tests/testdata/localhost-cert.pem"),
            0o644,
        );
        write_material_file(
            &material_dir.join("host-1.key"),
            include_bytes!("../../s3-tests/testdata/localhost-key.pem"),
            0o600,
        );
        for principal in ["raft", "storage", "admin"] {
            for number in 1..=3 {
                let secret = if principal == "raft" && number == 1 {
                    vec![0, 1, 2, 0xff]
                } else {
                    format!("{principal}-{number}-secret").into_bytes()
                };
                write_material_file(
                    &material_dir.join(format!("{principal}-{number}.key")),
                    &secret,
                    0o600,
                );
            }
        }
        write_material_file(
            &material_dir.join("frontend-1.key"),
            b"frontend-1-secret",
            0o600,
        );
        write_material_file(
            &material_dir.join("frontend-1-admin.key"),
            b"frontend-1-admin-secret",
            0o600,
        );
        write_material_file(
            &material_dir.join("frontend-1-maintenance.key"),
            b"frontend-1-maintenance-secret",
            0o600,
        );
        let manifest = manifest
            .replace("/run/argmin-secrets", material_dir.to_str().unwrap())
            .replace("tcp://control-1.internal:", "tcp://localhost:")
            .replace("tcp://storage-1.internal:", "tcp://localhost:")
            .replace(
                "tls_server_name = \"control-1.internal\"",
                "tls_server_name = \"localhost\"",
            )
            .replace(
                "tls_server_name = \"storage-1.internal\"",
                "tls_server_name = \"localhost\"",
            );
        let validated = parse_static_cluster_manifest(&manifest, selected_process_id).unwrap();
        (dir, validated)
    }

    fn validate_test_selected_host_filesystem(
        validated: &ValidatedStaticClusterManifest,
    ) -> Result<(), String> {
        validate_selected_host_filesystem_with_mount_validator(
            validated,
            |_path, _metadata, _label| Ok(()),
        )
    }

    fn standalone_manifest_on_mount(mount_path: &Path) -> String {
        standalone_manifest()
            .replace(
                "mount_path = \"/srv/argmin\"",
                &format!("mount_path = \"{}\"", mount_path.display()),
            )
            .replace(
                "state_path = \"/srv/argmin/control.state\"",
                &format!(
                    "state_path = \"{}\"",
                    mount_path.join("control.state").display()
                ),
            )
            .replace(
                "data_dir = \"/srv/argmin/data\"",
                &format!("data_dir = \"{}\"", mount_path.join("data").display()),
            )
    }

    fn replicated_manifest_with_host_one_mounts(control_mount: &Path, data_mount: &Path) -> String {
        replicated_manifest()
            .replace("/srv/argmin/control-1", control_mount.to_str().unwrap())
            .replace("/srv/argmin/data-1", data_mount.to_str().unwrap())
    }

    fn replicated_unix_manifest() -> String {
        replicated_unix_manifest_with_shape(3, 4, 2, 1)
    }

    fn replicated_unix_manifest_with_shape(
        node_count: u32,
        pg_count: u32,
        ec_data_shards: u8,
        ec_parity_shards: u8,
    ) -> String {
        let mut manifest = format!(
            r#"
schema_version = 1
tls_identities = []
tls_trust_bundles = []

[cluster]
id = "replicated-unix"
topology_generation = 9
region = "us-east-1"

[deployment]
mode = "replicated"
failure_domain = "disk"
failure_tolerance = 1
internal_auth = "required"

[storage]
pg_count = {pg_count}
ec_data_shards = {ec_data_shards}
ec_parity_shards = {ec_parity_shards}
initial_cluster_epoch = 3

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 15728640

[[transport_profiles]]
id = "internal"
max_frame_bytes = 16777216
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[transport_profiles]]
id = "control"
max_frame_bytes = 8388648
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[hosts]]
id = "host-1"
zone = "zone-a"
rack = "rack-1"
"#
        );
        for number in 1..=node_count {
            writeln!(
                manifest,
                r#"
[[disks]]
id = "control-disk-{number}"
host_id = "host-1"
mount_path = "/srv/argmin/control-{number}"

[[disks]]
id = "data-disk-{number}"
host_id = "host-1"
mount_path = "/srv/argmin/data-{number}"

[[processes]]
id = "control-{number}"
host_id = "host-1"
kind = "control-plane"
admin_instance_id = "admin-{number}"

[[processes]]
id = "storage-{number}"
host_id = "host-1"
kind = "storage-node"

[[authorities]]
id = "authority-{number}"
kind = "raft-voter"
raft_node_id = {raft_node_id}
process_id = "control-{number}"
disk_id = "control-disk-{number}"
state_path = "/srv/argmin/control-{number}/control.state"

[[storage_nodes]]
node_id = {number}
process_id = "storage-{number}"
disk_id = "data-disk-{number}"
data_dir = "/srv/argmin/data-{number}/node"
"#,
                raft_node_id = 100 + number
            )
            .unwrap();
            for (protocol, name, transport_profile) in [
                ("raft-peer", "raft", "internal"),
                ("control-plane", "control", "control"),
                ("authority-clock-recovery", "clock", "control"),
            ] {
                writeln!(
                    manifest,
                    r#"
[[endpoints]]
id = "{name}-{number}"
owner_process_id = "control-{number}"
protocol = "{protocol}"
priority = 10
listen = "unix:///run/argmin/{name}-{number}.sock"
advertise = "unix:///run/argmin/{name}-{number}.sock"
transport_profile_id = "{transport_profile}"
"#
                )
                .unwrap();
            }
            writeln!(
                manifest,
                r#"
[[endpoints]]
id = "storage-{number}"
owner_process_id = "storage-{number}"
protocol = "storage-rpc"
priority = 10
listen = "unix:///run/argmin/storage-{number}.sock"
advertise = "unix:///run/argmin/storage-{number}.sock"
transport_profile_id = "internal"
"#
            )
            .unwrap();
            for (principal, id_field, id_value, credential_id) in [
                (
                    "raft-peer",
                    "node_id",
                    (100 + number).to_string(),
                    format!("raft-{number}"),
                ),
                (
                    "storage-node",
                    "node_id",
                    number.to_string(),
                    format!("storage-{number}"),
                ),
                (
                    "admin",
                    "instance_id",
                    format!("\"admin-{number}\""),
                    format!("admin-{number}"),
                ),
            ] {
                writeln!(
                    manifest,
                    r#"
[[auth_credentials]]
principal = "{principal}"
{id_field} = {id_value}
credential_id = "{credential_id}"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/{credential_id}.key"
"#
                )
                .unwrap();
            }
        }
        manifest
    }

    fn resolved_test_material(
        manifest: &ValidatedStaticClusterManifest,
    ) -> ResolvedStaticClusterMaterial {
        ResolvedStaticClusterMaterial {
            auth_credentials: manifest
                .manifest
                .auth_credentials
                .iter()
                .map(|credential| ResolvedStaticAuthCredential {
                    principal: credential_principal_key(credential).unwrap(),
                    credential_id: credential.credential_id.clone(),
                    credential_version: credential.credential_version,
                    use_for_signing: credential.use_for_signing,
                    accept_from_ms: credential.accept_from_ms,
                    accept_until_ms: credential.accept_until_ms,
                    secret: if credential.credential_id == "raft-1" {
                        vec![0, 0xff, 7]
                    } else {
                        credential.credential_id.as_bytes().to_vec()
                    },
                })
                .collect(),
            tls_identities: BTreeMap::new(),
            tls_trust_bundles: BTreeMap::new(),
        }
    }

    fn standalone_manifest() -> String {
        r#"
schema_version = 1
tls_identities = []
tls_trust_bundles = []
auth_credentials = []

[cluster]
id = "test-cluster"
topology_generation = 1
region = "us-east-1"

[deployment]
mode = "standalone"
failure_domain = "none"
failure_tolerance = 0
internal_auth = "disabled"

[storage]
pg_count = 16
ec_data_shards = 1
ec_parity_shards = 0
initial_cluster_epoch = 1

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 16777216

[[transport_profiles]]
id = "control"
max_frame_bytes = 8388648
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 5000

[[hosts]]
id = "host-1"
zone = "zone-a"
rack = "rack-1"

[[disks]]
id = "disk-1"
host_id = "host-1"
mount_path = "/srv/argmin"

[[processes]]
id = "all-1"
host_id = "host-1"
kind = "all-in-one"

[[authorities]]
id = "authority-1"
kind = "single"
process_id = "all-1"
disk_id = "disk-1"
state_path = "/srv/argmin/control.state"

[[storage_nodes]]
node_id = 1
process_id = "all-1"
disk_id = "disk-1"
data_dir = "/srv/argmin/data"

[[endpoints]]
id = "control-1"
owner_process_id = "all-1"
protocol = "control-plane"
priority = 10
listen = "unix:///run/argmin/control.sock"
advertise = "unix:///run/argmin/control.sock"
transport_profile_id = "control"

[[endpoints]]
id = "clock-1"
owner_process_id = "all-1"
protocol = "authority-clock-recovery"
priority = 10
listen = "unix:///run/argmin/clock.sock"
advertise = "unix:///run/argmin/clock.sock"
transport_profile_id = "control"

[[endpoints]]
id = "storage-1"
owner_process_id = "all-1"
protocol = "storage-rpc"
priority = 10
listen = "unix:///run/argmin/storage.sock"
advertise = "unix:///run/argmin/storage.sock"
transport_profile_id = "control"
"#
        .to_string()
    }

    fn replicated_manifest() -> String {
        let mut manifest = r#"
schema_version = 1

[cluster]
id = "replicated-cluster"
topology_generation = 7
region = "us-east-1"

[deployment]
mode = "replicated"
failure_domain = "host"
failure_tolerance = 1
internal_auth = "required"

[storage]
pg_count = 16
ec_data_shards = 2
ec_parity_shards = 1
initial_cluster_epoch = 1

[raft]
max_append_entries = 64
max_append_bytes = 8388608
max_snapshot_bytes = 15728640

[[transport_profiles]]
id = "internal"
max_frame_bytes = 16777216
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 15000

[[transport_profiles]]
id = "control"
max_frame_bytes = 8388648
max_connections = 64
connect_timeout_ms = 1000
io_timeout_ms = 15000

[[tls_trust_bundles]]
id = "cluster-ca"
ca_bundle_ref = "file:/run/argmin-secrets/cluster-ca.pem"
"#
        .to_string();

        for host_number in 1..=3 {
            writeln!(
                manifest,
                r#"
[[hosts]]
id = "host-{host_number}"
zone = "zone-a"
rack = "rack-{host_number}"

[[disks]]
id = "host-{host_number}-control"
host_id = "host-{host_number}"
mount_path = "/srv/argmin/control-{host_number}"

[[disks]]
id = "host-{host_number}-data"
host_id = "host-{host_number}"
mount_path = "/srv/argmin/data-{host_number}"

[[processes]]
id = "control-{host_number}"
host_id = "host-{host_number}"
kind = "control-plane"
admin_instance_id = "control-{host_number}-admin"

[[processes]]
id = "storage-{host_number}"
host_id = "host-{host_number}"
kind = "storage-node"

[[authorities]]
id = "authority-{host_number}"
kind = "raft-voter"
raft_node_id = {raft_node_id}
process_id = "control-{host_number}"
disk_id = "host-{host_number}-control"
state_path = "/srv/argmin/control-{host_number}/control.state"

[[storage_nodes]]
node_id = {host_number}
process_id = "storage-{host_number}"
disk_id = "host-{host_number}-data"
data_dir = "/srv/argmin/data-{host_number}/node"

[[tls_identities]]
id = "host-{host_number}-identity"
certificate_ref = "file:/run/argmin-secrets/host-{host_number}.crt"
private_key_ref = "file:/run/argmin-secrets/host-{host_number}.key"
"#,
                raft_node_id = 100 + host_number
            )
            .unwrap();

            for (protocol, endpoint_name, port, transport_profile) in [
                ("raft-peer", "raft", 7400 + host_number, "internal"),
                ("control-plane", "control", 7500 + host_number, "control"),
                (
                    "authority-clock-recovery",
                    "clock",
                    7600 + host_number,
                    "control",
                ),
            ] {
                writeln!(
                    manifest,
                    r#"
[[endpoints]]
id = "{endpoint_name}-{host_number}"
owner_process_id = "control-{host_number}"
protocol = "{protocol}"
priority = 10
listen = "tcp://0.0.0.0:{port}"
advertise = "tcp://control-{host_number}.internal:{port}"
transport_profile_id = "{transport_profile}"
tls_identity_id = "host-{host_number}-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "control-{host_number}.internal"
"#
                )
                .unwrap();
            }
            writeln!(
                manifest,
                r#"
[[endpoints]]
id = "storage-{host_number}"
owner_process_id = "storage-{host_number}"
protocol = "storage-rpc"
priority = 10
listen = "tcp://0.0.0.0:{storage_port}"
advertise = "tcp://storage-{host_number}.internal:{storage_port}"
transport_profile_id = "internal"
tls_identity_id = "host-{host_number}-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "storage-{host_number}.internal"
"#,
                storage_port = 7700 + host_number
            )
            .unwrap();

            for (principal, id_field, id_value, credential_name) in [
                (
                    "raft-peer",
                    "node_id",
                    (100 + host_number).to_string(),
                    format!("raft-{host_number}"),
                ),
                (
                    "storage-node",
                    "node_id",
                    host_number.to_string(),
                    format!("storage-{host_number}"),
                ),
                (
                    "admin",
                    "instance_id",
                    format!("\"control-{host_number}-admin\""),
                    format!("admin-{host_number}"),
                ),
            ] {
                writeln!(
                    manifest,
                    r#"
[[auth_credentials]]
principal = "{principal}"
{id_field} = {id_value}
credential_id = "{credential_name}"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/{credential_name}.key"
"#
                )
                .unwrap();
            }
        }
        manifest
    }

    fn replicated_unix_data_manifest() -> String {
        let mut manifest =
            replicated_manifest().replace("failure_domain = \"host\"", "failure_domain = \"disk\"");
        for host_number in 2..=3 {
            manifest = manifest.replace(
                &format!("id = \"host-{host_number}-data\"\nhost_id = \"host-{host_number}\""),
                &format!("id = \"host-{host_number}-data\"\nhost_id = \"host-1\""),
            );
            manifest = manifest.replace(
                &format!(
                    "id = \"storage-{host_number}\"\nhost_id = \"host-{host_number}\"\nkind = \"storage-node\""
                ),
                &format!(
                    "id = \"storage-{host_number}\"\nhost_id = \"host-1\"\nkind = \"storage-node\""
                ),
            );
        }
        for host_number in 1..=3 {
            let tcp_block = format!(
                r#"[[endpoints]]
id = "storage-{host_number}"
owner_process_id = "storage-{host_number}"
protocol = "storage-rpc"
priority = 10
listen = "tcp://0.0.0.0:{port}"
advertise = "tcp://storage-{host_number}.internal:{port}"
transport_profile_id = "internal"
tls_identity_id = "host-{host_number}-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "storage-{host_number}.internal"
"#,
                port = 7700 + host_number
            );
            let unix_block = format!(
                r#"[[endpoints]]
id = "storage-{host_number}"
owner_process_id = "storage-{host_number}"
protocol = "storage-rpc"
priority = 10
listen = "unix:///run/argmin/storage-{host_number}.sock"
advertise = "unix:///run/argmin/storage-{host_number}.sock"
transport_profile_id = "internal"
"#
            );
            manifest = replace_once(&manifest, &tcp_block, &unix_block);
        }
        manifest.push_str(
            r#"
[[processes]]
id = "frontend-1"
host_id = "host-1"
kind = "frontend"
frontend_instance_id = "frontend-1"
admin_instance_id = "frontend-1-admin"
maintenance_instance_id = "frontend-1-maintenance"

[[auth_credentials]]
principal = "frontend"
instance_id = "frontend-1"
credential_id = "frontend-1"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/frontend-1.key"

[[auth_credentials]]
principal = "admin"
instance_id = "frontend-1-admin"
credential_id = "frontend-1-admin"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/frontend-1-admin.key"

[[auth_credentials]]
principal = "maintenance"
instance_id = "frontend-1-maintenance"
credential_id = "frontend-1-maintenance"
credential_version = 1
use_for_signing = true
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/frontend-1-maintenance.key"
"#,
        );
        manifest
    }

    fn replace_once(input: &str, from: &str, to: &str) -> String {
        assert_eq!(input.matches(from).count(), 1, "fixture replacement count");
        input.replacen(from, to, 1)
    }

    fn assert_only_full_fingerprint_changes(
        baseline: &ValidatedStaticClusterManifest,
        changed: &ValidatedStaticClusterManifest,
    ) {
        assert_eq!(changed.topology_digest(), baseline.topology_digest());
        assert_eq!(
            changed.process_identity_digest(),
            baseline.process_identity_digest()
        );
        assert_ne!(
            changed.full_config_fingerprint(),
            baseline.full_config_fingerprint()
        );
    }

    fn assert_topology_identity_changes(
        baseline: &ValidatedStaticClusterManifest,
        changed: &ValidatedStaticClusterManifest,
    ) {
        assert_ne!(changed.topology_digest(), baseline.topology_digest());
        assert_ne!(
            changed.process_identity_digest(),
            baseline.process_identity_digest()
        );
        assert_ne!(
            changed.full_config_fingerprint(),
            baseline.full_config_fingerprint()
        );
    }

    #[test]
    fn static_cluster_manifest_parses_valid_standalone_shape() {
        let manifest = parse_static_cluster_manifest(&standalone_manifest(), "all-1").unwrap();
        assert_eq!(manifest.cluster_id(), "test-cluster");
        assert_eq!(manifest.topology_generation(), 1);
        assert_eq!(manifest.selected_process_id(), "all-1");
        assert_eq!(manifest.deployment_mode(), "standalone");
        assert_eq!(manifest.initial_pg_acting_sets().len(), 16);
        assert!(manifest
            .initial_pg_acting_sets()
            .iter()
            .all(|acting_set| acting_set == &[1]));
        let debug = format!("{manifest:?}");
        assert!(debug.contains("test-cluster"));
        assert!(!debug.contains("/srv/argmin"));
    }

    #[test]
    fn static_cluster_manifest_maps_standalone_process_to_legacy_runtime_config() {
        let manifest = parse_static_cluster_manifest(&standalone_manifest(), "all-1").unwrap();
        let mut environment = standalone_runtime_environment();
        environment.insert("ARGMIN_LISTEN_ADDR", "127.0.0.1:19000".to_string());
        environment.insert("ARGMIN_WORKERS", "7".to_string());

        let config = manifest
            .standalone_legacy_server_config(|key| environment.get(key).cloned())
            .unwrap();

        assert_eq!(config.process_role, ProcessRole::LegacyLocal);
        assert_eq!(config.listen_addr, "127.0.0.1:19000");
        assert_eq!(config.workers, 7);
        assert_eq!(config.host_id.as_deref(), Some("host-1"));
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.data_dir, "/srv/argmin");
        assert_eq!(config.pg_count, 16);
        assert_eq!(config.storage_cluster_epoch, 1);
        assert_eq!((config.ec_k, config.ec_m), (1, 0));
        assert_eq!(config.storage_node_ids, vec![1]);
        assert_eq!(config.storage_node_id, Some(1));
        assert_eq!(
            config.storage_node_data_dir.as_deref(),
            Some("/srv/argmin/data")
        );
        assert_eq!(
            config
                .static_cluster_identity
                .as_ref()
                .map(|identity| identity.process_id.as_str()),
            Some("all-1")
        );
        assert_eq!(
            config
                .static_cluster_identity
                .as_ref()
                .map(|identity| identity.process_identity_digest.as_str()),
            Some(manifest.process_identity_digest())
        );
        assert_eq!(config.storage_node_socket_path, None);
        assert!(config.storage_node_sockets.is_empty());
        assert_eq!(config.control_plane_state_path, None);
        assert_eq!(config.control_plane_socket_path, None);
        assert!(config.control_plane_client_socket_paths.is_empty());
    }

    #[test]
    fn static_cluster_manifest_maps_replicated_unix_control_plane_with_binary_auth() {
        let manifest_text =
            replicated_unix_manifest().replace("max_connections = 64", "max_connections = 17");
        let manifest = parse_static_cluster_manifest(&manifest_text, "control-1").unwrap();
        let mut material = resolved_test_material(&manifest);
        material
            .auth_credentials
            .push(ResolvedStaticAuthCredential {
                principal: CredentialPrincipalKey {
                    principal: AuthPrincipal::RaftPeer,
                    id: CredentialPrincipalId::Node(101),
                },
                credential_id: "raft-1-next".to_string(),
                credential_version: 2,
                use_for_signing: false,
                accept_from_ms: 0,
                accept_until_ms: None,
                secret: vec![9, 8, 7],
            });
        let environment = standalone_runtime_environment();

        let config = manifest
            .replicated_unix_control_plane_server_config(&material, |key| {
                environment.get(key).cloned()
            })
            .unwrap();

        assert_eq!(config.process_role, ProcessRole::ControlPlane);
        assert!(config.control_plane_experimental_raft);
        assert_eq!(config.control_plane_raft_node_id, Some(101));
        assert_eq!(config.control_plane_raft_peer_max_connections, 17);
        assert_eq!(config.control_plane_raft_peer_listeners.len(), 1);
        assert!(config.control_plane_raft_peer_frame_transport.is_none());
        assert_eq!(
            config.control_plane_state_path.as_deref(),
            Some("/srv/argmin/control-1/control.state")
        );
        assert_eq!(
            config.control_plane_socket_path.as_deref(),
            Some("/run/argmin/control-1.sock")
        );
        assert_eq!(
            config.control_plane_clock_recovery_socket_path.as_deref(),
            Some("/run/argmin/clock-1.sock")
        );
        assert_eq!(
            config.control_plane_raft_peer_socket_path.as_deref(),
            Some("/run/argmin/raft-1.sock")
        );
        assert_eq!(
            config.control_plane_raft_peer_transport_limits,
            ControlPlaneRaftPeerTransportLimits {
                max_frame_bytes: 16 * 1024 * 1024,
                max_append_entries: 64,
                max_append_entries_bytes: 8 * 1024 * 1024,
                max_snapshot_bytes: 15 * 1024 * 1024,
            }
        );
        assert_eq!(
            config.control_plane_raft_peer_connect_timeout,
            Duration::from_secs(1)
        );
        assert_eq!(
            config.control_plane_raft_peer_io_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            config.control_plane_raft_peer_sockets,
            vec![
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 101,
                    socket_path: "/run/argmin/raft-1.sock".to_string(),
                },
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 102,
                    socket_path: "/run/argmin/raft-2.sock".to_string(),
                },
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 103,
                    socket_path: "/run/argmin/raft-3.sock".to_string(),
                },
            ]
        );
        assert_eq!(
            config.control_plane_client_socket_paths,
            vec![
                "/run/argmin/control-1.sock",
                "/run/argmin/control-2.sock",
                "/run/argmin/control-3.sock",
            ]
        );
        assert_eq!(
            config.control_plane_auth_cluster_id.as_deref(),
            Some(manifest.raft_cluster_identity().as_str())
        );
        assert_eq!(
            config.control_plane_raft_auth_signing_credential,
            Some(("raft-1".to_string(), 1))
        );
        let local_raft_credential = config
            .control_plane_raft_auth_credentials
            .iter()
            .find(|credential| credential.node_id == 101)
            .unwrap();
        assert_eq!(local_raft_credential.secret.as_bytes(), &[0, 0xff, 7]);
        let initial = config.static_initial_cluster_map.as_ref().unwrap();
        assert_eq!(initial.topology.topology_generation(), 9);
        assert_eq!(initial.topology.raft_voters(), &[101, 102, 103]);
        let bootstrap_nodes = config
            .storage_node_sockets
            .iter()
            .map(|node| (NodeId::new(node.node_id), node.socket_path.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            initial.topology.bootstrap_map_digest(),
            &storage::control_plane::initial_cluster_bootstrap_map_digest(
                &bootstrap_nodes,
                &initial.pg_acting_sets,
            )
        );
        assert_eq!(
            initial.pg_acting_sets,
            manifest
                .initial_pg_acting_sets()
                .iter()
                .enumerate()
                .map(|(pg_id, acting_set)| (
                    PgId::new(u32::try_from(pg_id).unwrap()),
                    acting_set.iter().copied().map(NodeId::new).collect()
                ))
                .collect::<Vec<_>>()
        );
        let raft_cluster_name = config.control_plane_raft_cluster_name.unwrap();
        assert!(raft_cluster_name.starts_with("replicated-unix:topology:9:"));
        assert!(raft_cluster_name.ends_with(manifest.topology_digest()));
        let identity = config.static_cluster_identity.unwrap();
        assert_eq!(identity.process_id, "control-1");
        assert_eq!(
            identity.process_identity_digest,
            manifest.process_identity_digest()
        );

        let reordered_authorities = replicated_unix_manifest()
            .replace("id = \"authority-1\"", "id = \"z-authority\"")
            .replace("id = \"authority-2\"", "id = \"y-authority\"")
            .replace("id = \"authority-3\"", "id = \"x-authority\"");
        let reordered = parse_static_cluster_manifest(&reordered_authorities, "control-1").unwrap();
        let reordered_material = resolved_test_material(&reordered);
        let reordered_config = reordered
            .replicated_unix_control_plane_server_config(&reordered_material, |key| {
                environment.get(key).cloned()
            })
            .unwrap();
        assert_eq!(
            reordered_config
                .static_initial_cluster_map
                .unwrap()
                .topology
                .raft_voters(),
            &[101, 102, 103]
        );
    }

    #[test]
    fn static_cluster_mapping_activates_tcp_control_plane_service_listeners() {
        let base_manifest = replicated_unix_manifest()
            .replace(
                "id = \"control\"\nmax_frame_bytes = 8388648\nmax_connections = 64\nconnect_timeout_ms = 1000\nio_timeout_ms = 5000",
                "id = \"control\"\nmax_frame_bytes = 8388648\nmax_connections = 64\nconnect_timeout_ms = 1000\nio_timeout_ms = 15000",
            )
            .replace(
                "tls_identities = []\ntls_trust_bundles = []",
                r#"[[tls_trust_bundles]]
id = "cluster-ca"
ca_bundle_ref = "file:/run/argmin-secrets/cluster-ca.pem"

[[tls_identities]]
id = "host-1-identity"
certificate_ref = "file:/run/argmin-secrets/host-1.crt"
private_key_ref = "file:/run/argmin-secrets/host-1.key""#,
            );
        for (protocol, endpoint_id, port) in [
            ("control-plane", "control-1-tcp", 8501),
            ("authority-clock-recovery", "clock-recovery-1-tcp", 8601),
        ] {
            let manifest = format!(
                r#"{base_manifest}

[[endpoints]]
id = "{endpoint_id}"
owner_process_id = "control-1"
protocol = "{protocol}"
priority = 20
listen = "tcp://0.0.0.0:{port}"
advertise = "tcp://control-1.internal:{port}"
transport_profile_id = "control"
tls_identity_id = "host-1-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "control-1.internal"
"#
            );
            let (_dir, manifest) = materialized_replicated_manifest_from("control-1", manifest);
            let material = manifest.resolve_selected_process_material_at(1).unwrap();

            let config = manifest
                .replicated_unix_control_plane_server_config(&material, |_| None)
                .unwrap();
            let listeners = if protocol == "control-plane" {
                &config.control_plane_rpc_listeners
            } else {
                &config.control_plane_clock_recovery_rpc_listeners
            };
            assert!(listeners.iter().any(|listener| matches!(
                listener,
                ConfiguredControlPlaneRpcListener::Tcp {
                    endpoint_id: configured_id,
                    bind_addr,
                    ..
                } if configured_id == endpoint_id && bind_addr == &format!("0.0.0.0:{port}")
            )));
        }
    }

    #[test]
    fn static_cluster_mapping_activates_canonical_tls_raft_transport() {
        let mut manifest = replicated_unix_manifest().replace(
            "tls_identities = []\ntls_trust_bundles = []",
            r#"[[tls_trust_bundles]]
id = "cluster-ca"
ca_bundle_ref = "file:/run/argmin-secrets/cluster-ca.pem"

[[tls_identities]]
id = "host-1-identity"
certificate_ref = "file:/run/argmin-secrets/host-1.crt"
private_key_ref = "file:/run/argmin-secrets/host-1.key""#,
        );
        for number in 1..=3 {
            let unix = format!(
                r#"[[endpoints]]
id = "raft-{number}"
owner_process_id = "control-{number}"
protocol = "raft-peer"
priority = 10
listen = "unix:///run/argmin/raft-{number}.sock"
advertise = "unix:///run/argmin/raft-{number}.sock"
transport_profile_id = "internal""#
            );
            let tcp = format!(
                r#"[[endpoints]]
id = "raft-{number}"
owner_process_id = "control-{number}"
protocol = "raft-peer"
priority = 10
listen = "tcp://127.0.0.1:{}"
advertise = "tcp://localhost:{}"
transport_profile_id = "internal"
tls_identity_id = "host-1-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "localhost""#,
                8400 + number,
                8400 + number,
            );
            manifest = replace_once(&manifest, &unix, &tcp);
        }
        let (_dir, manifest) = materialized_replicated_manifest_from("control-1", manifest);
        let material = manifest.resolve_selected_process_material_at(1).unwrap();

        let config = manifest
            .replicated_unix_control_plane_server_config(&material, |_| None)
            .unwrap();

        assert_eq!(config.control_plane_raft_peer_socket_path, None);
        assert_eq!(config.control_plane_raft_peer_listeners.len(), 1);
        assert!(matches!(
            &config.control_plane_raft_peer_listeners[0],
            ConfiguredControlPlaneRaftPeerListener::Tcp {
                endpoint_id,
                bind_addr,
                max_connections: 64,
                io_timeout,
                ..
            } if endpoint_id == "raft-1"
                && bind_addr == "127.0.0.1:8401"
                && *io_timeout == Duration::from_secs(5)
        ));
        assert_eq!(
            config.control_plane_raft_peer_sockets,
            vec![
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 101,
                    socket_path: "tcp://localhost:8401".to_string(),
                },
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 102,
                    socket_path: "tcp://localhost:8402".to_string(),
                },
                ConfiguredControlPlaneRaftPeerSocket {
                    node_id: 103,
                    socket_path: "tcp://localhost:8403".to_string(),
                },
            ]
        );
        let transport = config
            .control_plane_raft_peer_frame_transport
            .as_ref()
            .expect("canonical TCP peers require a TCP frame transport");
        assert_eq!(transport.name(), "TLS/TCP");
        let debug = format!("{transport:?}");
        assert!(debug.contains("peer_count"));
        assert!(!debug.contains("PRIVATE KEY"));
    }

    #[test]
    fn static_cluster_resolves_tls_control_plane_and_recovery_clients() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let material = manifest.resolve_selected_process_material_at(1).unwrap();

        for protocol in [
            EndpointProtocol::ControlPlane,
            EndpointProtocol::AuthorityClockRecovery,
        ] {
            let ConfiguredStaticControlPlaneRpcClients {
                endpoints,
                frame_transport,
            } = manifest
                .configured_static_control_plane_rpc_clients(&material, protocol)
                .unwrap();

            assert_eq!(endpoints.len(), 3);
            assert!(endpoints
                .iter()
                .all(|endpoint| endpoint.starts_with("tcp://")));
            assert_eq!(frame_transport.unwrap().name(), "static Unix/TLS/TCP");
        }

        let listeners = manifest
            .configured_static_control_plane_rpc_listeners(
                &material,
                EndpointProtocol::ControlPlane,
            )
            .unwrap();
        assert_eq!(listeners.len(), 1);
        assert!(matches!(
            &listeners[0],
            ConfiguredControlPlaneRpcListener::Tcp {
                endpoint_id,
                bind_addr,
                ..
            } if endpoint_id == "control-1" && bind_addr == "0.0.0.0:7501"
        ));
    }

    #[test]
    fn static_tcp_control_plane_requires_active_runtime_map_auth_credential() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let mut material = manifest.resolve_selected_process_material_at(1).unwrap();
        material.auth_credentials.retain(|credential| {
            credential.principal.principal != AuthPrincipal::Admin
                && credential.principal.principal != AuthPrincipal::Frontend
        });

        let error = manifest
            .replicated_unix_control_plane_server_config(&material, |_| None)
            .unwrap_err();

        assert!(
            error.contains("TCP control-plane listeners require an active frontend or admin"),
            "{error}"
        );
    }

    #[test]
    fn static_tcp_clock_recovery_requires_active_admin_credential() {
        let (_dir, manifest) =
            materialized_replicated_manifest_from("control-1", replicated_unix_data_manifest());
        let mut material = manifest.resolve_selected_process_material_at(1).unwrap();
        material.auth_credentials.retain(|credential| {
            !matches!(
                credential.principal.principal,
                AuthPrincipal::Admin | AuthPrincipal::Maintenance
            )
        });

        let error = manifest
            .replicated_unix_control_plane_server_config(&material, |_| None)
            .unwrap_err();

        assert!(
            error.contains("TCP authority-clock recovery listeners require an active admin"),
            "{error}"
        );
    }

    #[test]
    fn static_cluster_control_plane_clients_retain_prioritized_fallback_endpoints() {
        let mut manifest = replicated_manifest();
        for host_number in 1..=3 {
            for (protocol, endpoint_name, port) in [
                ("control-plane", "control-fallback", 8500 + host_number),
                (
                    "authority-clock-recovery",
                    "clock-fallback",
                    8600 + host_number,
                ),
            ] {
                writeln!(
                    manifest,
                    r#"
[[endpoints]]
id = "{endpoint_name}-{host_number}"
owner_process_id = "control-{host_number}"
protocol = "{protocol}"
priority = 20
listen = "tcp://127.0.0.1:{port}"
advertise = "tcp://control-{host_number}.internal:{port}"
transport_profile_id = "control"
tls_identity_id = "host-{host_number}-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "control-{host_number}.internal"
"#
                )
                .unwrap();
            }
        }
        let (_dir, manifest) = materialized_replicated_manifest_from("control-1", manifest);
        let material = manifest.resolve_selected_process_material_at(1).unwrap();

        for (protocol, primary_port, fallback_port) in [
            (EndpointProtocol::ControlPlane, 7500, 8500),
            (EndpointProtocol::AuthorityClockRecovery, 7600, 8600),
        ] {
            let clients = manifest
                .configured_static_control_plane_rpc_clients(&material, protocol)
                .unwrap();
            assert_eq!(clients.endpoints.len(), 6);
            for (index, host_number) in (1..=3).enumerate() {
                assert!(
                    clients.endpoints[index].ends_with(&format!(":{}", primary_port + host_number))
                );
                assert!(clients.endpoints[index + 3]
                    .ends_with(&format!(":{}", fallback_port + host_number)));
            }
            assert!(clients.frame_transport.is_some());
        }
    }

    #[test]
    fn static_cluster_control_plane_clients_resolve_transport_per_target_host() {
        let manifest = format!(
            "{}{}",
            replicated_manifest(),
            r#"
[[endpoints]]
id = "control-1-unix"
owner_process_id = "control-1"
protocol = "control-plane"
priority = 5
listen = "unix:///run/argmin/control-1.sock"
advertise = "unix:///run/argmin/control-1.sock"
transport_profile_id = "control"
"#
        );
        let (_dir, manifest) = materialized_replicated_manifest_from("control-1", manifest);
        let material = manifest.resolve_selected_process_material_at(1).unwrap();

        let clients = manifest
            .configured_static_control_plane_rpc_clients(&material, EndpointProtocol::ControlPlane)
            .unwrap();

        assert_eq!(clients.endpoints.len(), 4);
        assert_eq!(clients.endpoints[0], "unix:///run/argmin/control-1.sock");
        assert!(clients.endpoints[1].ends_with(":7502"));
        assert!(clients.endpoints[2].ends_with(":7503"));
        assert!(clients.endpoints[3].ends_with(":7501"));
        assert!(clients.frame_transport.is_some());
    }

    #[test]
    fn static_cluster_same_host_control_plane_clients_retain_unix_and_tcp_candidates() {
        let mut manifest = replicated_unix_manifest()
            .replace(
                "id = \"control\"\nmax_frame_bytes = 8388648\nmax_connections = 64\nconnect_timeout_ms = 1000\nio_timeout_ms = 5000",
                "id = \"control\"\nmax_frame_bytes = 8388648\nmax_connections = 64\nconnect_timeout_ms = 1000\nio_timeout_ms = 15000",
            )
            .replace(
                "tls_identities = []\ntls_trust_bundles = []",
                r#"[[tls_trust_bundles]]
id = "cluster-ca"
ca_bundle_ref = "file:/run/argmin-secrets/cluster-ca.pem"

[[tls_identities]]
id = "host-1-identity"
certificate_ref = "file:/run/argmin-secrets/host-1.crt"
private_key_ref = "file:/run/argmin-secrets/host-1.key""#,
            );
        for number in 1..=3 {
            writeln!(
                manifest,
                r#"
[[endpoints]]
id = "control-{number}-tcp"
owner_process_id = "control-{number}"
protocol = "control-plane"
priority = 20
listen = "tcp://127.0.0.1:{}"
advertise = "tcp://localhost:{}"
transport_profile_id = "control"
tls_identity_id = "host-1-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "localhost"
"#,
                8500 + number,
                8500 + number,
            )
            .unwrap();
        }
        let (_dir, manifest) = materialized_replicated_manifest_from("control-1", manifest);
        let material = manifest.resolve_selected_process_material_at(1).unwrap();

        let clients = manifest
            .configured_static_control_plane_rpc_clients(&material, EndpointProtocol::ControlPlane)
            .unwrap();

        assert_eq!(clients.endpoints.len(), 6);
        assert!(clients.endpoints[..3]
            .iter()
            .all(|endpoint| endpoint.starts_with("unix://")));
        assert!(clients.endpoints[3..]
            .iter()
            .all(|endpoint| endpoint.starts_with("tcp://")));
        assert_eq!(
            clients.frame_transport.unwrap().name(),
            "static Unix/TLS/TCP"
        );
    }

    #[test]
    fn static_control_plane_frame_transport_exchanges_unix_fallback_frame() {
        let dir = test_util::tempdir();
        let path = dir.path().join("control.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 7];
            stream.read_exact(&mut request).unwrap();
            assert_eq!(&request, b"request");
            stream.write_all(b"response").unwrap();
        });
        let endpoint = format!("unix://{}", path.display());
        let transport = StaticControlPlaneFrameTransport {
            peers: Arc::new(BTreeMap::from([(
                endpoint.clone(),
                StaticControlPlanePeer::Unix(StaticControlPlaneUnixPeer {
                    endpoint: endpoint.clone(),
                    path,
                    max_frame_bytes: 1024,
                }),
            )])),
        };

        let response = transport
            .exchange(ControlPlaneRpcFrameExchange {
                endpoint,
                request_frame: b"request".to_vec(),
                deadline: Instant::now() + Duration::from_secs(1),
                max_frame_bytes: 1024,
            })
            .unwrap();

        assert_eq!(response, b"response");
        server.join().unwrap();
    }

    #[test]
    fn static_cluster_manifest_rejects_tcp_listener_address_collisions() {
        let exact_collision = replicated_manifest().replacen(
            "listen = \"tcp://0.0.0.0:7601\"\nadvertise = \"tcp://control-1.internal:7601\"",
            "listen = \"tcp://0.0.0.0:7501\"\nadvertise = \"tcp://control-1.internal:7501\"",
            1,
        );
        assert!(parse_static_cluster_manifest(&exact_collision, "control-1")
            .unwrap_err()
            .contains("TCP listener"));

        let wildcard_collision = format!(
            "{}{}",
            replicated_manifest(),
            r#"
[[endpoints]]
id = "control-1-loopback"
owner_process_id = "control-1"
protocol = "control-plane"
priority = 20
listen = "tcp://127.0.0.1:7501"
advertise = "tcp://control-1-alt.internal:7501"
transport_profile_id = "control"
tls_identity_id = "host-1-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "control-1-alt.internal"
"#
        );
        let error = parse_static_cluster_manifest(&wildcard_collision, "control-1").unwrap_err();
        assert!(error.contains("collides on host host-1"));
    }

    #[test]
    fn static_cluster_manifest_rejects_dns_tcp_listener_hosts() {
        let dns_listener = replace_once(
            &replicated_manifest(),
            "listen = \"tcp://0.0.0.0:7501\"",
            "listen = \"tcp://control-1.internal:7501\"",
        );
        let error = parse_static_cluster_manifest(&dns_listener, "control-1").unwrap_err();
        assert!(error.contains("TCP listener host must be a literal IP address"));
    }

    #[test]
    fn static_cluster_manifest_rejects_incompatible_control_plane_frame_profiles() {
        let profile = "id = \"control\"\nmax_frame_bytes = 8388648";
        for value in [
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES - 1,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES + 1,
        ] {
            let changed = replace_once(
                &replicated_manifest(),
                profile,
                &format!("id = \"control\"\nmax_frame_bytes = {value}"),
            );
            let error = parse_static_cluster_manifest(&changed, "control-1").unwrap_err();
            assert!(error.contains("control-plane endpoint"));
            assert!(error.contains("frame limit must equal"));
        }
    }

    #[test]
    fn static_control_plane_tcp_transport_rejects_profile_oversize_before_connect() {
        let client_config = RustlsClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
        let endpoint = "tcp://127.0.0.1:1".to_string();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let error = runtime
            .block_on(exchange_static_control_plane_tcp_frame(
                StaticControlPlaneTcpPeer {
                    endpoint: endpoint.clone(),
                    host: "127.0.0.1".to_string(),
                    port: 1,
                    server_name: "localhost".to_string(),
                    connect_timeout: Duration::from_secs(1),
                    max_frame_bytes: 8,
                    tls_client_config: Arc::new(client_config),
                },
                ControlPlaneRpcFrameExchange {
                    endpoint,
                    request_frame: vec![0; 9],
                    deadline: Instant::now() + Duration::from_secs(1),
                    max_frame_bytes: 16,
                },
            ))
            .unwrap_err();

        assert!(!error.request_may_have_been_sent());
        assert!(format!("{error:?}").contains("exceeds limit 8"));
    }

    #[test]
    fn static_tls_control_plane_transport_authenticates_and_dispatches_recovery_rpc() {
        let certificates = CertificateDer::pem_slice_iter(include_bytes!(
            "../../s3-tests/testdata/localhost-cert.pem"
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
            "../../s3-tests/testdata/localhost-key.pem"
        ))
        .unwrap();
        let provider = rustls::crypto::ring::default_provider();
        let mut server_config =
            rustls::ServerConfig::builder_with_provider(Arc::new(provider.clone()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(certificates, private_key)
                .unwrap();
        server_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
        let server_config = Arc::new(server_config);

        let mut roots = RootCertStore::empty();
        roots
            .add(
                CertificateDer::pem_slice_iter(include_bytes!(
                    "../../s3-tests/testdata/ca-cert.pem"
                ))
                .next()
                .unwrap()
                .unwrap(),
            )
            .unwrap();
        let mut client_config = RustlsClientConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let endpoint = format!("tcp://localhost:{port}");
        let transport: Arc<dyn ControlPlaneRpcFrameTransport> =
            Arc::new(StaticControlPlaneFrameTransport {
                peers: Arc::new(BTreeMap::from([(
                    endpoint.clone(),
                    StaticControlPlanePeer::Tcp(StaticControlPlaneTcpPeer {
                        endpoint: endpoint.clone(),
                        host: "127.0.0.1".to_string(),
                        port,
                        server_name: "localhost".to_string(),
                        connect_timeout: Duration::from_secs(1),
                        max_frame_bytes: CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
                        tls_client_config: Arc::new(client_config),
                    }),
                )])),
            });
        let admin_credential = storage::control_plane::ControlPlaneAdminAuthCredential::new(
            storage::control_plane::ControlPlaneAdminAuthCredentialInput {
                instance_id: "tcp-admin".to_string(),
                credential_id: "tcp-admin".to_string(),
                credential_version: 1,
                secret: b"tcp-admin-secret".to_vec(),
            },
        )
        .unwrap();
        let verifier = Arc::new(
            storage::control_plane::ControlPlaneUnixAuthVerifier::new_empty("tcp-cluster")
                .unwrap()
                .with_admin_credentials(vec![admin_credential.clone()])
                .unwrap(),
        );
        let now_ms = storage::clock::current_time_millis();
        let authority_clock = Arc::new(std::sync::Mutex::new(
            storage::control_plane::ControlPlaneAuthorityClock::new(
                None,
                now_ms,
                storage::clock::clock_health_time_millis(),
            )
            .unwrap(),
        ));
        let state_dir = test_util::tempdir();
        let authority = Arc::new(std::sync::Mutex::new(
            storage::control_plane::SingleAuthorityControlPlane::open(
                storage::control_plane::FileControlPlaneStore::new(
                    state_dir.path().join("control.state"),
                ),
            )
            .unwrap(),
        ));
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            crate::spawn_control_plane_tcp_rpc_worker(
                stream,
                server_config,
                crate::ControlPlaneRpcWorkerAuthority::Shared(authority),
                Some(authority_clock),
                None,
                crate::ControlPlaneRpcWorkerPolicy {
                    gate_request_time_with_authority_clock: false,
                    require_authentication: true,
                    active_rpc_workers: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    worker_limit: 1,
                    max_frame_bytes: CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
                    io_timeout: Duration::from_secs(2),
                    pre_auth_byte_budget: Arc::new(crate::ControlPlaneRpcPreAuthByteBudget::new(
                        CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
                    )),
                    endpoint: crate::ControlPlaneRpcEndpoint::ClockRecovery,
                    auth_verifier: Some(verifier),
                    raft_authority_admission: None,
                    durable_response_publication: None,
                },
            );
        });
        let client = storage::control_plane::UnixControlPlaneClient::with_frame_transport(
            [endpoint],
            transport,
        )
        .unwrap();
        let client = storage::control_plane::AuthenticatedUnixControlPlaneClient::new(
            client,
            admin_credential.scoped_for_cluster("tcp-cluster").unwrap(),
        );

        let status = client.authority_clock_status(now_ms).unwrap();

        assert!(status.established());
        server.join().unwrap();
    }

    #[test]
    fn static_cluster_resolves_canonical_tls_raft_transport_plan() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let material = manifest.resolve_selected_process_material_at(1).unwrap();

        let plan = manifest
            .resolved_static_raft_transport_plan(&material)
            .unwrap();

        assert_eq!(plan.local_node_id, 101);
        assert_eq!(plan.listeners.len(), 1);
        let listener = &plan.listeners[0];
        assert_eq!(listener.endpoint_id, "raft-1");
        assert_eq!(
            listener.listen,
            EndpointAddress::Tcp {
                host: "0.0.0.0".to_string(),
                port: 7401,
            }
        );
        assert_eq!(
            listener.advertise,
            EndpointAddress::Tcp {
                host: "localhost".to_string(),
                port: 7401,
            }
        );
        assert_eq!(listener.transport_profile_id, "internal");
        let server_config = listener.tls_server_config.as_ref().unwrap();
        assert_eq!(server_config.alpn_protocols, &[CONTROL_PLANE_RAFT_TLS_ALPN]);
        assert_eq!(
            plan.peers.keys().copied().collect::<Vec<_>>(),
            [101, 102, 103]
        );
        for (&node_id, peer) in &plan.peers {
            assert_eq!(peer.node_id, node_id);
            assert_eq!(peer.endpoint_id, format!("raft-{}", node_id - 100));
            assert_eq!(peer.transport_profile_id, "internal");
            assert!(matches!(
                peer.address,
                ResolvedStaticRaftPeerAddress::Tcp { .. }
            ));
            let client_config = peer.tls_client_config.as_ref().unwrap();
            assert_eq!(client_config.alpn_protocols, &[CONTROL_PLANE_RAFT_TLS_ALPN]);
        }
        let local_peer = plan.peers.get(&plan.local_node_id).unwrap();
        let ResolvedStaticRaftPeerAddress::Tcp { server_name, .. } = &local_peer.address else {
            panic!("local resolved Raft peer must use TCP");
        };
        complete_test_tls_handshake(
            Arc::clone(local_peer.tls_client_config.as_ref().unwrap()),
            Arc::clone(server_config),
            server_name,
        );
        let debug = format!("{plan:?}");
        assert!(debug.contains("tls: true"));
        assert!(!debug.contains("PRIVATE KEY"));
    }

    #[test]
    fn static_raft_tcp_frame_transport_exchanges_complete_tls_frame() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let material = manifest.resolve_selected_process_material_at(1).unwrap();
        let plan = manifest
            .resolved_static_raft_transport_plan(&material)
            .unwrap();
        let peer = plan.peers.get(&101).unwrap();
        let listener_config = plan.listeners[0].tls_server_config.as_ref().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_config = Arc::clone(listener_config);
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let connection = rustls::ServerConnection::new(server_config).unwrap();
            let mut stream = rustls::StreamOwned::new(connection, stream);
            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).unwrap();
            let mut request = vec![0_u8; u32::from_be_bytes(length) as usize];
            stream.read_exact(&mut request).unwrap();
            assert_eq!(request, b"request-frame");
            stream
                .write_all(&(14_u32).to_be_bytes())
                .and_then(|()| stream.write_all(b"response-frame"))
                .unwrap();
        });
        let transport = StaticRaftTcpPeerFrameTransport {
            peers: Arc::new(BTreeMap::from([(
                101,
                StaticRaftTcpPeer {
                    endpoint: format!("tcp://localhost:{port}"),
                    host: "127.0.0.1".to_string(),
                    port,
                    server_name: "localhost".to_string(),
                    tls_client_config: Arc::clone(peer.tls_client_config.as_ref().unwrap()),
                },
            )])),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let response = runtime
            .block_on(transport.exchange(ControlPlaneRaftPeerFrameExchange {
                target: 101,
                endpoint: format!("tcp://localhost:{port}"),
                request_frame: b"request-frame".to_vec(),
                max_frame_bytes: 1024,
                connect_timeout: Duration::from_secs(1),
                deadline: Instant::now() + Duration::from_secs(1),
                context_prefix: "",
            }))
            .unwrap();

        assert_eq!(response, b"response-frame");
        server.join().unwrap();
    }

    #[test]
    fn static_raft_tcp_frame_transport_rejects_missing_alpn() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let material = manifest.resolve_selected_process_material_at(1).unwrap();
        let plan = manifest
            .resolved_static_raft_transport_plan(&material)
            .unwrap();
        let peer = plan.peers.get(&101).unwrap();
        let mut server_config = (**plan.listeners[0].tls_server_config.as_ref().unwrap()).clone();
        server_config.alpn_protocols.clear();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut connection = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();
            while connection.is_handshaking() {
                connection.complete_io(&mut stream).unwrap();
            }
            assert_eq!(connection.alpn_protocol(), None);
        });
        let transport = StaticRaftTcpPeerFrameTransport {
            peers: Arc::new(BTreeMap::from([(
                101,
                StaticRaftTcpPeer {
                    endpoint: format!("tcp://localhost:{port}"),
                    host: "127.0.0.1".to_string(),
                    port,
                    server_name: "localhost".to_string(),
                    tls_client_config: Arc::clone(peer.tls_client_config.as_ref().unwrap()),
                },
            )])),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let error = runtime
            .block_on(transport.exchange(ControlPlaneRaftPeerFrameExchange {
                target: 101,
                endpoint: format!("tcp://localhost:{port}"),
                request_frame: b"request-frame".to_vec(),
                max_frame_bytes: 1024,
                connect_timeout: Duration::from_secs(1),
                deadline: Instant::now() + Duration::from_secs(1),
                context_prefix: "",
            }))
            .unwrap_err();

        assert!(format!("{error:?}").contains("did not negotiate required"));
        server.join().unwrap();
    }

    #[test]
    fn static_raft_tcp_frame_transport_bounds_tls_handshake_by_absolute_deadline() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let material = manifest.resolve_selected_process_material_at(1).unwrap();
        let plan = manifest
            .resolved_static_raft_transport_plan(&material)
            .unwrap();
        let peer = plan.peers.get(&101).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(100));
        });
        let transport = StaticRaftTcpPeerFrameTransport {
            peers: Arc::new(BTreeMap::from([(
                101,
                StaticRaftTcpPeer {
                    endpoint: format!("tcp://localhost:{port}"),
                    host: "127.0.0.1".to_string(),
                    port,
                    server_name: "localhost".to_string(),
                    tls_client_config: Arc::clone(peer.tls_client_config.as_ref().unwrap()),
                },
            )])),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(transport.exchange(ControlPlaneRaftPeerFrameExchange {
                target: 101,
                endpoint: format!("tcp://localhost:{port}"),
                request_frame: b"request-frame".to_vec(),
                max_frame_bytes: 1024,
                connect_timeout: Duration::from_secs(1),
                deadline: Instant::now() + Duration::from_millis(20),
                context_prefix: "",
            }))
            .unwrap_err();

        assert!(format!("{error:?}").contains("TimedOut"));
        server.join().unwrap();
    }

    #[test]
    fn static_cluster_raft_transport_plan_retains_local_unix_listener_with_tcp_peer() {
        let manifest = format!(
            "{}\n{}",
            replicated_manifest(),
            r#"
[[endpoints]]
id = "raft-1-local"
owner_process_id = "control-1"
protocol = "raft-peer"
priority = 1
listen = "unix:///run/argmin/raft-1-local.sock"
advertise = "unix:///run/argmin/raft-1-local.sock"
transport_profile_id = "internal"
"#
        );
        let (_dir, manifest) = materialized_replicated_manifest_from("control-1", manifest);
        let material = manifest.resolve_selected_process_material_at(1).unwrap();

        let plan = manifest
            .resolved_static_raft_transport_plan(&material)
            .unwrap();

        assert_eq!(plan.listeners.len(), 2);
        assert_eq!(plan.listeners[0].endpoint_id, "raft-1-local");
        assert_eq!(
            plan.listeners[0].advertise,
            EndpointAddress::Unix(PathBuf::from("/run/argmin/raft-1-local.sock"))
        );
        assert!(plan.listeners[0].tls_server_config.is_none());
        assert_eq!(plan.listeners[1].endpoint_id, "raft-1");
        assert!(plan.listeners[1].tls_server_config.is_some());
        let local_peer = plan.peers.get(&plan.local_node_id).unwrap();
        assert_eq!(local_peer.endpoint_id, "raft-1");
        assert!(matches!(
            local_peer.address,
            ResolvedStaticRaftPeerAddress::Tcp { .. }
        ));
    }

    #[test]
    fn static_cluster_tls_raft_transport_plan_requires_resolved_material() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let material = ResolvedStaticClusterMaterial {
            auth_credentials: Vec::new(),
            tls_identities: BTreeMap::new(),
            tls_trust_bundles: BTreeMap::new(),
        };

        let error = manifest
            .resolved_static_raft_transport_plan(&material)
            .unwrap_err();

        assert!(error.contains("did not resolve Raft endpoint"), "{error}");
        assert!(!error.contains("/material/"), "{error}");
    }

    #[test]
    fn static_cluster_replicated_control_plane_requires_explicit_state_initialization() {
        let temp = test_util::tempdir();
        let control_dir = temp.path().join("control-1");
        std::fs::create_dir(&control_dir).unwrap();
        let manifest_text = replicated_unix_manifest()
            .replace("/srv/argmin/control-1", control_dir.to_str().unwrap());
        let manifest = parse_static_cluster_manifest(&manifest_text, "control-1").unwrap();
        let identity = manifest.configured_static_identity();
        let state_path = control_dir.join("control.state");

        let error = crate::static_cluster_state::bind_static_control_plane_identity(
            &identity,
            101,
            &state_path,
        )
        .unwrap_err();
        assert!(error.contains("run initialize-cluster-state"));

        manifest.initialize_selected_process_state().unwrap();
        manifest.initialize_selected_process_state().unwrap();
        crate::static_cluster_state::bind_static_control_plane_identity(
            &identity,
            101,
            &state_path,
        )
        .unwrap();
    }

    #[test]
    fn static_cluster_replicated_storage_initializes_identity_bound_pg_state() {
        let temp = test_util::tempdir();
        let data_mount = temp.path().join("data-1");
        let data_dir = data_mount.join("node");
        let manifest_text =
            replicated_unix_manifest().replace("/srv/argmin/data-1", data_mount.to_str().unwrap());
        let manifest = parse_static_cluster_manifest(&manifest_text, "storage-1").unwrap();
        let identity = manifest.configured_static_identity();
        let pg_ids = (0..4).collect::<Vec<_>>();

        manifest.initialize_selected_process_state().unwrap();
        manifest.initialize_selected_process_state().unwrap();

        drop(
            crate::static_cluster_state::lock_and_verify_standalone_storage_startup(
                &identity, 1, &data_dir, &pg_ids,
            )
            .unwrap(),
        );
        assert_eq!(identity.process_id, "storage-1");
        assert!(pg_ids.iter().all(|pg_id| data_dir
            .join(format!("pg-{pg_id:04}/metadata.db"))
            .is_file()));
    }

    #[test]
    fn static_cluster_loader_resolves_replicated_unix_control_plane_secrets() {
        let temp = test_util::tempdir();
        let material_dir = temp.path().join("secrets");
        private_dir(&material_dir);
        for credential_id in [
            "raft-1",
            "raft-2",
            "raft-3",
            "storage-1",
            "storage-2",
            "storage-3",
            "admin-1",
            "admin-2",
            "admin-3",
        ] {
            write_material_file(
                &material_dir.join(format!("{credential_id}.key")),
                credential_id.as_bytes(),
                0o600,
            );
        }
        let manifest = replicated_unix_manifest()
            .replace("/run/argmin-secrets", material_dir.to_str().unwrap());
        let (_manifest_dir, manifest_path) = write_manifest(&manifest);
        let environment = standalone_runtime_environment();

        let config = load_server_config_from_inputs_with_filesystem_validator(
            Some(&manifest_path),
            Some("control-1"),
            |key| environment.get(key).cloned(),
            |_manifest| Ok(()),
        )
        .unwrap();

        assert_eq!(config.process_role, ProcessRole::ControlPlane);
        assert_eq!(
            config.control_plane_raft_auth_signing_credential,
            Some(("raft-1".to_string(), 1))
        );
        assert_eq!(
            config
                .control_plane_raft_auth_credentials
                .iter()
                .find(|credential| credential.node_id == 101)
                .unwrap()
                .secret
                .as_bytes(),
            b"raft-1"
        );
    }

    #[test]
    fn static_cluster_standalone_runtime_does_not_start_unserved_refresh_path() {
        let dir = test_util::tempdir();
        let disk_path = dir.path().join("disk");
        let socket_path = dir.path().join("run");
        std::fs::create_dir_all(&disk_path).unwrap();
        let source = standalone_manifest()
            .replace("/srv/argmin", disk_path.to_str().unwrap())
            .replace("/run/argmin", socket_path.to_str().unwrap())
            .replace("initial_cluster_epoch = 1", "initial_cluster_epoch = 7");
        let manifest = parse_static_cluster_manifest(&source, "all-1").unwrap();
        let environment = standalone_runtime_environment();
        let config = manifest
            .standalone_legacy_server_config(|key| environment.get(key).cloned())
            .unwrap();
        let ec_config = EcConfig::new(config.ec_k, config.ec_m).unwrap();
        manifest.initialize_selected_storage().unwrap();

        let cluster = crate::build_legacy_local_storage_cluster(&config, &ec_config).unwrap();
        let handle = storage::StorageClusterRuntimeMapHandle::new(cluster.cluster());

        assert_eq!(
            handle.current().cluster_epoch(),
            storage::ClusterEpoch::new(7).unwrap()
        );
        assert!(crate::maybe_spawn_frontend_control_plane_refresh_loop(handle, &config).is_none());
        assert!(!socket_path.exists());
    }

    #[test]
    fn static_cluster_standalone_runtime_exclusively_owns_storage_directory() {
        let dir = test_util::tempdir();
        let disk_path = dir.path().join("disk");
        let socket_path = dir.path().join("run");
        std::fs::create_dir_all(&disk_path).unwrap();
        let source = standalone_manifest()
            .replace("/srv/argmin", disk_path.to_str().unwrap())
            .replace("/run/argmin", socket_path.to_str().unwrap());
        let manifest = parse_static_cluster_manifest(&source, "all-1").unwrap();
        let environment = standalone_runtime_environment();
        let config = manifest
            .standalone_legacy_server_config(|key| environment.get(key).cloned())
            .unwrap();
        let ec_config = EcConfig::new(config.ec_k, config.ec_m).unwrap();
        manifest.initialize_selected_storage().unwrap();
        let first = crate::build_legacy_local_storage_cluster(&config, &ec_config).unwrap();
        let data_dir = Path::new(config.storage_node_data_dir.as_deref().unwrap());
        let identity_path = data_dir.join(".argmin-static-storage.identity");
        let held_identity_path = data_dir.join(".argmin-static-storage.identity.held");
        std::fs::rename(&identity_path, &held_identity_path).unwrap();

        let error = match crate::build_legacy_local_storage_cluster(&config, &ec_config) {
            Ok(_) => panic!("second static runtime must not open the same storage directory"),
            Err(error) => error,
        };

        assert!(error.contains("initialization or runtime is already active"));
        assert!(
            !error.contains("identity is missing"),
            "runtime lock must be acquired before identity and PG verification"
        );
        std::fs::rename(&held_identity_path, &identity_path).unwrap();
        assert_eq!(first.local_node_count(), 1);
        drop(first);
        crate::build_legacy_local_storage_cluster(&config, &ec_config)
            .expect("storage directory lock must be released with runtime");
    }

    #[test]
    fn static_cluster_standalone_runtime_requires_initialized_storage_state() {
        let dir = test_util::tempdir();
        let disk_path = dir.path().join("disk");
        let socket_path = dir.path().join("run");
        std::fs::create_dir_all(&disk_path).unwrap();
        let source = standalone_manifest()
            .replace("/srv/argmin", disk_path.to_str().unwrap())
            .replace("/run/argmin", socket_path.to_str().unwrap());
        let manifest = parse_static_cluster_manifest(&source, "all-1").unwrap();
        let environment = standalone_runtime_environment();
        let config = manifest
            .standalone_legacy_server_config(|key| environment.get(key).cloned())
            .unwrap();
        let ec_config = EcConfig::new(config.ec_k, config.ec_m).unwrap();

        let error = match crate::build_legacy_local_storage_cluster(&config, &ec_config) {
            Ok(_) => panic!("uninitialized static storage must fail closed"),
            Err(error) => error,
        };

        assert!(error.contains("initialize-cluster-state"));
        assert!(!disk_path.join("data").exists());
    }

    #[test]
    fn static_cluster_runtime_loader_requires_complete_file_mode_selection() {
        let environment = standalone_runtime_environment();

        assert!(
            load_server_config_from_inputs(Some(Path::new("/cluster.toml")), None, |key| {
                environment.get(key).cloned()
            })
            .unwrap_err()
            .contains("ARGMIN_PROCESS_ID is required")
        );
        assert!(load_server_config_from_inputs(None, Some("all-1"), |key| {
            environment.get(key).cloned()
        })
        .unwrap_err()
        .contains("ARGMIN_CLUSTER_CONFIG_PATH is required"));
    }

    #[test]
    fn static_cluster_runtime_loader_rejects_mixed_cluster_environment() {
        let (_dir, path) = write_manifest(&standalone_manifest());
        let mut environment = standalone_runtime_environment();
        environment.insert("ARGMIN_PG_COUNT", "999".to_string());

        let error = load_server_config_from_inputs(Some(&path), Some("all-1"), |key| {
            environment.get(key).cloned()
        })
        .unwrap_err();

        assert_eq!(
            error,
            "ARGMIN_PG_COUNT cannot be set when ARGMIN_CLUSTER_CONFIG_PATH is active"
        );
        assert!(!error.contains("999"));
    }

    #[test]
    fn static_cluster_runtime_loader_keeps_env_only_compatibility_mode() {
        let mut environment = standalone_runtime_environment();
        environment.insert("ARGMIN_PG_COUNT", "3".to_string());
        environment.insert("ARGMIN_EC_K", "1".to_string());
        environment.insert("ARGMIN_EC_M", "0".to_string());
        environment.insert("ARGMIN_LOCAL_NODE_COUNT", "1".to_string());

        let config =
            load_server_config_from_inputs(None, None, |key| environment.get(key).cloned())
                .unwrap();

        assert_eq!(config.process_role, ProcessRole::LegacyLocal);
        assert_eq!(config.pg_count, 3);
        assert_eq!(config.storage_node_ids, vec![0]);
        assert_eq!(config.static_cluster_identity, None);
    }

    #[test]
    fn static_cluster_runtime_loader_validates_selected_host_filesystem() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        private_dir(&mount_path);
        let manifest = standalone_manifest_on_mount(&mount_path);
        let manifest_path = dir.path().join("cluster.toml");
        std::fs::write(&manifest_path, manifest).unwrap();
        let environment = standalone_runtime_environment();

        let config = load_server_config_from_inputs_with_filesystem_validator(
            Some(&manifest_path),
            Some("all-1"),
            |key| environment.get(key).cloned(),
            |_manifest| Ok(()),
        )
        .unwrap();

        assert_eq!(
            config.storage_node_data_dir.as_deref(),
            Some(mount_path.join("data").to_str().unwrap())
        );
    }

    #[test]
    fn static_cluster_runtime_loader_rejects_unmounted_selected_host_disk() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("unmounted-disk");
        private_dir(&mount_path);
        let manifest_path = dir.path().join("cluster.toml");
        std::fs::write(&manifest_path, standalone_manifest_on_mount(&mount_path)).unwrap();
        let environment = standalone_runtime_environment();

        let error = load_server_config_from_inputs(Some(&manifest_path), Some("all-1"), |key| {
            environment.get(key).cloned()
        })
        .unwrap_err();

        assert!(
            error.contains("must be an exact distinct-device mount boundary"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_mount_boundary_requires_distinct_device() {
        validate_distinct_mount_devices(11, 10, "test mount").unwrap();
        assert!(validate_distinct_mount_devices(10, 10, "test mount")
            .unwrap_err()
            .contains("exact distinct-device mount boundary"));
    }

    #[test]
    fn static_cluster_runtime_maps_remote_storage_addresses_for_authority_bootstrap() {
        let environment = standalone_runtime_environment();
        let (_dir, replicated) = materialized_replicated_manifest("control-1");
        let material = replicated.resolve_selected_process_material_at(1).unwrap();
        let config = replicated
            .replicated_unix_control_plane_server_config(&material, |key| {
                environment.get(key).cloned()
            })
            .unwrap();
        assert_eq!(
            config
                .storage_node_sockets
                .iter()
                .map(|node| (node.node_id, node.socket_path.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (1, "tcp://localhost:7701"),
                (2, "tcp://storage-2.internal:7702"),
                (3, "tcp://storage-3.internal:7703"),
            ]
        );
    }

    #[test]
    fn static_cluster_storage_bootstrap_uses_globally_reachable_tcp_fallback() {
        let environment = standalone_runtime_environment();
        let manifest = format!(
            "{}{}",
            replicated_manifest(),
            r#"
[[endpoints]]
id = "storage-1-local"
owner_process_id = "storage-1"
protocol = "storage-rpc"
priority = 1
listen = "unix:///run/argmin/storage-1.sock"
advertise = "unix:///run/argmin/storage-1.sock"
transport_profile_id = "internal"
"#
        );
        let validated = parse_static_cluster_manifest(&manifest, "control-1").unwrap();
        assert_eq!(
            validated.canonical_storage_node_endpoints[&1].advertise,
            "tcp://storage-1.internal:7701"
        );
        let (_dir, replicated) = materialized_replicated_manifest_from("control-1", manifest);
        let material = replicated.resolve_selected_process_material_at(1).unwrap();
        let config = replicated
            .replicated_unix_control_plane_server_config(&material, |key| {
                environment.get(key).cloned()
            })
            .unwrap();

        assert_eq!(
            config.storage_node_sockets[0].socket_path,
            "tcp://localhost:7701"
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_shared_canonical_storage_address() {
        let manifest = replace_once(
            &replicated_manifest(),
            "listen = \"tcp://0.0.0.0:7702\"",
            "listen = \"tcp://0.0.0.0:7701\"",
        );
        let manifest = replace_once(
            &manifest,
            "advertise = \"tcp://storage-2.internal:7702\"",
            "advertise = \"tcp://storage-1.internal:7701\"",
        );
        let manifest = replace_once(
            &manifest,
            "tls_server_name = \"storage-2.internal\"",
            "tls_server_name = \"storage-1.internal\"",
        );

        let error = parse_static_cluster_manifest(&manifest, "control-1").unwrap_err();

        assert!(
            error.contains("same canonical advertised endpoint"),
            "{error}"
        );
        assert!(error.contains("storage nodes 1 and 2"), "{error}");
        assert!(error.contains("storage-1"), "{error}");
        assert!(error.contains("storage-2"), "{error}");
    }

    #[test]
    fn static_cluster_runtime_loader_rejects_standalone_unresolved_credentials() {
        let environment = standalone_runtime_environment();

        let credentialed = format!(
            "{}{}",
            replace_once(&standalone_manifest(), "auth_credentials = []", ""),
            r#"
[[auth_credentials]]
principal = "storage-node"
node_id = 1
credential_id = "storage-1"
credential_version = 1
use_for_signing = false
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/storage-1.key"
"#
        );
        let credentialed = parse_static_cluster_manifest(&credentialed, "all-1").unwrap();
        let error = credentialed
            .standalone_legacy_server_config(|key| environment.get(key).cloned())
            .unwrap_err();
        assert!(error.contains("no unresolved secret references"));
    }

    #[test]
    fn static_cluster_manifest_parses_valid_three_host_replicated_shape() {
        let manifest = parse_static_cluster_manifest(&replicated_manifest(), "control-2").unwrap();
        assert_eq!(manifest.cluster_id(), "replicated-cluster");
        assert_eq!(manifest.topology_generation(), 7);
        assert_eq!(manifest.selected_process_id(), "control-2");
        assert_eq!(manifest.deployment_mode(), "replicated");
        assert_eq!(manifest.initial_pg_acting_sets().len(), 16);
        assert!(manifest.initial_pg_acting_sets().iter().all(|acting_set| {
            acting_set.len() == 3
                && acting_set.iter().copied().collect::<BTreeSet<_>>() == BTreeSet::from([1, 2, 3])
        }));
    }

    #[test]
    fn static_cluster_manifest_identity_has_stable_vectors() {
        let standalone = parse_static_cluster_manifest(&standalone_manifest(), "all-1").unwrap();
        assert_eq!(
            (
                standalone.topology_digest(),
                standalone.process_identity_digest(),
                standalone.full_config_fingerprint(),
            ),
            (
                "c21bc863ef6c10f2efbfde7d53287dd3d0d62fafcbc6324480c0b37d50fc8c19",
                "5306e4cf52af50954950cb21687ce502c61ea47d2f1e6376237de80874e18d03",
                "fda782a642f90307c35cae85dd8945f0db7d8278fa280adb14c9a3f273fd467b",
            )
        );

        let replicated =
            parse_static_cluster_manifest(&replicated_manifest(), "control-2").unwrap();
        assert_eq!(
            (
                replicated.topology_digest(),
                replicated.process_identity_digest(),
                replicated.full_config_fingerprint(),
            ),
            (
                "446edd7681e684beddd088d1876a3c67dacc0f7edc2d218c4955fec99ba0a9a7",
                "18a78e17086c8c48baa89cf09524ee7f837b9fe667ea90f5fcdf6248a6b50970",
                "eedd94865bcbfc491ee07d4e55b771154abbc929f062deb68ad5ae22a2ced59b",
            )
        );
    }

    #[test]
    fn static_cluster_manifest_identity_is_collection_order_independent() {
        let input: StaticClusterManifestInput = toml::from_str(&replicated_manifest()).unwrap();
        let canonical = validate_static_cluster_manifest(input.clone(), "control-2").unwrap();
        let mut reversed = input;
        reversed.transport_profiles.reverse();
        reversed.hosts.reverse();
        reversed.disks.reverse();
        reversed.processes.reverse();
        reversed.authorities.reverse();
        reversed.storage_nodes.reverse();
        reversed.endpoints.reverse();
        reversed.tls_identities.reverse();
        reversed.tls_trust_bundles.reverse();
        reversed.auth_credentials.reverse();
        let reversed = validate_static_cluster_manifest(reversed, "control-2").unwrap();

        assert_eq!(reversed.topology_digest(), canonical.topology_digest());
        assert_eq!(
            reversed.process_identity_digest(),
            canonical.process_identity_digest()
        );
        assert_eq!(
            reversed.full_config_fingerprint(),
            canonical.full_config_fingerprint()
        );
    }

    #[test]
    fn static_cluster_manifest_topology_identity_covers_compatibility_fields() {
        let source = replicated_manifest();
        let baseline = parse_static_cluster_manifest(&source, "control-1").unwrap();
        for changed in [
            replace_once(
                &source,
                "id = \"replicated-cluster\"",
                "id = \"replicated-cluster-next\"",
            ),
            replace_once(
                &source,
                "topology_generation = 7",
                "topology_generation = 8",
            ),
            replace_once(
                &source,
                "failure_domain = \"host\"",
                "failure_domain = \"disk\"",
            ),
            replace_once(&source, "pg_count = 16", "pg_count = 17"),
            replace_once(&source, "rack = \"rack-1\"", "rack = \"rack-next\""),
            source.replace("node_id = 101", "node_id = 111"),
            source.replace("control-1-admin", "control-1-admin-next"),
            source.replace("7401", "7491"),
            replace_once(
                &source,
                "max_snapshot_bytes = 15728640",
                "max_snapshot_bytes = 15728639",
            ),
        ] {
            let changed = parse_static_cluster_manifest(&changed, "control-1").unwrap();
            assert_topology_identity_changes(&baseline, &changed);
        }
    }

    #[test]
    fn static_cluster_manifest_durable_identity_excludes_rotation_and_local_tuning() {
        let source = replicated_manifest();
        let baseline = parse_static_cluster_manifest(&source, "control-1").unwrap();
        for changed in [
            replace_once(
                &source,
                "credential_id = \"raft-1\"",
                "credential_id = \"raft-1-rotated\"",
            ),
            replace_once(
                &source,
                "credential_id = \"raft-1\"\ncredential_version = 1",
                "credential_id = \"raft-1\"\ncredential_version = 2",
            ),
            replace_once(
                &source,
                "credential_id = \"raft-1\"\ncredential_version = 1\nuse_for_signing = true\naccept_from_ms = 0",
                "credential_id = \"raft-1\"\ncredential_version = 1\nuse_for_signing = true\naccept_from_ms = 1",
            ),
            replace_once(
                &source,
                "secret_ref = \"file:/run/argmin-secrets/raft-1.key\"",
                "secret_ref = \"file:/run/argmin-secrets/raft-1-next.key\"",
            ),
            replace_once(
                &source,
                "certificate_ref = \"file:/run/argmin-secrets/host-1.crt\"",
                "certificate_ref = \"file:/run/argmin-secrets/host-1-next.crt\"",
            ),
            replace_once(
                &source,
                "state_path = \"/srv/argmin/control-1/control.state\"",
                "state_path = \"/srv/argmin/control-1/relocated.state\"",
            ),
            replace_once(
                &source,
                "listen = \"tcp://0.0.0.0:7401\"",
                "listen = \"tcp://127.0.0.1:7401\"",
            ),
            replace_once(
                &source,
                "id = \"internal\"\nmax_frame_bytes = 16777216\nmax_connections = 64\nconnect_timeout_ms = 1000",
                "id = \"internal\"\nmax_frame_bytes = 16777216\nmax_connections = 64\nconnect_timeout_ms = 2000",
            ),
            replace_once(&source, "region = \"us-east-1\"", "region = \"us-west-2\""),
        ] {
            let changed = parse_static_cluster_manifest(&changed, "control-1").unwrap();
            assert_only_full_fingerprint_changes(&baseline, &changed);
        }

        let verify_only = format!(
            "{}{}",
            replace_once(&standalone_manifest(), "auth_credentials = []", ""),
            r#"
[[auth_credentials]]
principal = "storage-node"
node_id = 1
credential_id = "storage-1"
credential_version = 1
use_for_signing = false
accept_from_ms = 10
accept_until_ms = 20
secret_ref = "file:/run/argmin-secrets/storage-1.key"
"#
        );
        let signing = replace_once(
            &verify_only,
            "use_for_signing = false",
            "use_for_signing = true",
        );
        let verify_only = parse_static_cluster_manifest(&verify_only, "all-1").unwrap();
        let signing = parse_static_cluster_manifest(&signing, "all-1").unwrap();
        assert_only_full_fingerprint_changes(&verify_only, &signing);
    }

    #[test]
    fn static_cluster_manifest_process_identity_is_selection_specific() {
        let first = parse_static_cluster_manifest(&replicated_manifest(), "control-1").unwrap();
        let second = parse_static_cluster_manifest(&replicated_manifest(), "control-2").unwrap();
        assert_eq!(first.topology_digest(), second.topology_digest());
        assert_eq!(
            first.full_config_fingerprint(),
            second.full_config_fingerprint()
        );
        assert_ne!(
            first.process_identity_digest(),
            second.process_identity_digest()
        );
    }

    #[test]
    fn static_cluster_manifest_raft_policy_selects_globally_reachable_peer_endpoint() {
        let manifest = format!(
            "{}{}",
            replicated_manifest(),
            r#"
[[endpoints]]
id = "raft-local-1"
owner_process_id = "control-1"
protocol = "raft-peer"
priority = 5
listen = "unix:///run/argmin/raft-1.sock"
advertise = "unix:///run/argmin/raft-1.sock"
transport_profile_id = "internal"
"#
        );
        let validated = parse_static_cluster_manifest(&manifest, "control-1").unwrap();
        assert_eq!(
            validated
                .canonical_raft_peer_endpoints()
                .get(&101)
                .map(|endpoint| endpoint.advertise.as_str()),
            Some("tcp://control-1.internal:7401")
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_shared_canonical_raft_address() {
        let manifest = replace_once(
            &replicated_manifest(),
            r#"id = "raft-2"
owner_process_id = "control-2"
protocol = "raft-peer"
priority = 10
listen = "tcp://0.0.0.0:7402"
advertise = "tcp://control-2.internal:7402"
transport_profile_id = "internal"
tls_identity_id = "host-2-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "control-2.internal""#,
            r#"id = "raft-2"
owner_process_id = "control-2"
protocol = "raft-peer"
priority = 10
listen = "tcp://0.0.0.0:7401"
advertise = "tcp://control-1.internal:7401"
transport_profile_id = "internal"
tls_identity_id = "host-2-identity"
tls_trust_bundle_id = "cluster-ca"
tls_server_name = "control-1.internal""#,
        );

        let error = parse_static_cluster_manifest(&manifest, "control-1").unwrap_err();

        assert!(
            error.contains("same canonical advertised endpoint"),
            "{error}"
        );
        assert!(error.contains("raft-1"), "{error}");
        assert!(error.contains("raft-2"), "{error}");
    }

    #[test]
    fn static_cluster_manifest_rejects_unknown_and_duplicate_fields() {
        let unknown = replace_once(
            &standalone_manifest(),
            "topology_generation = 1",
            "topology_generation = 1\nunknown = true",
        );
        assert!(parse_static_cluster_manifest(&unknown, "all-1")
            .unwrap_err()
            .contains("unknown field"));

        let duplicate = replace_once(
            &standalone_manifest(),
            "topology_generation = 1",
            "topology_generation = 1\ntopology_generation = 2",
        );
        assert!(parse_static_cluster_manifest(&duplicate, "all-1")
            .unwrap_err()
            .contains("duplicate key"));
    }

    #[test]
    fn static_cluster_manifest_redacts_toml_field_and_enum_values() {
        let sentinel = "DO-NOT-LOG-THIS-MANIFEST-VALUE";
        let unknown_field = replace_once(
            &standalone_manifest(),
            "topology_generation = 1",
            &format!("topology_generation = 1\n{sentinel} = true"),
        );
        let field_error = parse_static_cluster_manifest(&unknown_field, "all-1").unwrap_err();
        assert!(field_error.contains("unknown field"));
        assert!(!field_error.contains(sentinel));

        let unknown_mode = replace_once(
            &standalone_manifest(),
            "mode = \"standalone\"",
            &format!("mode = \"{sentinel}\""),
        );
        let enum_error = parse_static_cluster_manifest(&unknown_mode, "all-1").unwrap_err();
        assert!(enum_error.contains("unknown variant"));
        assert!(!enum_error.contains(sentinel));
    }

    #[test]
    fn static_cluster_manifest_rejects_unknown_enum_and_duplicate_ids() {
        let unknown_mode = replace_once(
            &standalone_manifest(),
            "mode = \"standalone\"",
            "mode = \"future-mode\"",
        );
        assert!(parse_static_cluster_manifest(&unknown_mode, "all-1")
            .unwrap_err()
            .contains("unknown variant"));

        let duplicate_endpoint = replace_once(
            &standalone_manifest(),
            "id = \"clock-1\"",
            "id = \"control-1\"",
        );
        assert!(parse_static_cluster_manifest(&duplicate_endpoint, "all-1")
            .unwrap_err()
            .contains("duplicate endpoint id"));

        let duplicate_disk_path = format!(
            "{}\n[[disks]]\nid = \"disk-2\"\nhost_id = \"host-1\"\nmount_path = \"/srv/argmin\"\n",
            standalone_manifest()
        );
        assert!(parse_static_cluster_manifest(&duplicate_disk_path, "all-1")
            .unwrap_err()
            .contains("duplicate disk mount path"));
    }

    #[test]
    fn static_cluster_manifest_rejects_pg_ownership_field() {
        let manifest = replace_once(
            &standalone_manifest(),
            "data_dir = \"/srv/argmin/data\"",
            "data_dir = \"/srv/argmin/data\"\npg_ids = \"all\"",
        );
        assert!(parse_static_cluster_manifest(&manifest, "all-1")
            .unwrap_err()
            .contains("unknown field"));
    }

    #[test]
    fn static_cluster_manifest_rejects_missing_selected_process_and_dangling_reference() {
        assert!(
            parse_static_cluster_manifest(&standalone_manifest(), "missing")
                .unwrap_err()
                .contains("absent")
        );
        let dangling = replace_once(
            &standalone_manifest(),
            "disk_id = \"disk-1\"\nstate_path",
            "disk_id = \"missing\"\nstate_path",
        );
        assert!(parse_static_cluster_manifest(&dangling, "all-1")
            .unwrap_err()
            .contains("unknown disk"));
    }

    #[test]
    fn static_cluster_manifest_rejects_tcp_without_tls_or_auth() {
        let tcp = replace_once(
            &standalone_manifest(),
            "listen = \"unix:///run/argmin/control.sock\"\nadvertise = \"unix:///run/argmin/control.sock\"",
            "listen = \"tcp://0.0.0.0:7501\"\nadvertise = \"tcp://control.internal:7501\"",
        );
        assert!(parse_static_cluster_manifest(&tcp, "all-1")
            .unwrap_err()
            .contains("tls_identity_id"));
    }

    #[test]
    fn static_cluster_manifest_binds_tcp_server_name_to_advertised_host() {
        let mismatch = replicated_manifest().replacen(
            "tls_server_name = \"control-1.internal\"",
            "tls_server_name = \"wrong.internal\"",
            1,
        );
        assert!(parse_static_cluster_manifest(&mismatch, "control-1")
            .unwrap_err()
            .contains("must match its advertised host"));

        let invalid_ipv6 = replicated_manifest().replacen(
            "advertise = \"tcp://control-1.internal:7401\"",
            "advertise = \"tcp://[abcd::not-an-address]:7401\"",
            1,
        );
        assert!(parse_static_cluster_manifest(&invalid_ipv6, "control-1")
            .unwrap_err()
            .contains("invalid IPv6"));
    }

    #[test]
    fn static_cluster_manifest_scopes_paths_by_host() {
        let parsed = parse_endpoint_address("unix:///run/argmin/control.sock", true).unwrap();
        assert_eq!(
            parsed,
            EndpointAddress::Unix(PathBuf::from("/run/argmin/control.sock"))
        );

        let repeated_remote_paths = replicated_manifest()
            .replace("/srv/argmin/control-2", "/srv/argmin/control-1")
            .replace("/srv/argmin/data-2", "/srv/argmin/data-1");
        parse_static_cluster_manifest(&repeated_remote_paths, "control-1").unwrap();
    }

    #[test]
    fn static_cluster_filesystem_validation_probes_only_selected_host() {
        let dir = test_util::tempdir();
        let control_mount = dir.path().join("control");
        let data_mount = dir.path().join("data");
        private_dir(&control_mount);
        private_dir(&data_mount);
        let manifest = replicated_manifest_with_host_one_mounts(&control_mount, &data_mount);
        let validated = parse_static_cluster_manifest(&manifest, "control-1").unwrap();

        validate_test_selected_host_filesystem(&validated).unwrap();
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_local_symlink_traversal() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        let real_data = mount_path.join("real-data");
        private_dir(&mount_path);
        private_dir(&real_data);
        symlink(&real_data, mount_path.join("linked-data")).unwrap();
        let manifest = standalone_manifest_on_mount(&mount_path).replace(
            &format!("data_dir = \"{}\"", mount_path.join("data").display()),
            &format!(
                "data_dir = \"{}\"",
                mount_path.join("linked-data/node").display()
            ),
        );
        let validated = parse_static_cluster_manifest(&manifest, "all-1").unwrap();

        let error = validate_test_selected_host_filesystem(&validated).unwrap_err();

        assert!(
            error.contains("must not traverse a symlink"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_symlink_parent_component_escape() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        let outside_child = dir.path().join("outside/child");
        private_dir(&mount_path);
        private_dir(&outside_child);
        symlink(&outside_child, mount_path.join("jump")).unwrap();
        let noncanonical_data_dir = mount_path.join("jump/../node");
        let manifest = standalone_manifest_on_mount(&mount_path).replace(
            &format!("data_dir = \"{}\"", mount_path.join("data").display()),
            &format!("data_dir = \"{}\"", noncanonical_data_dir.display()),
        );

        let error = parse_static_cluster_manifest(&manifest, "all-1").unwrap_err();

        assert!(
            error.contains("storage data path is not canonical"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_missing_or_symlinked_mount() {
        let dir = test_util::tempdir();
        let missing_mount = dir.path().join("missing");
        let missing =
            parse_static_cluster_manifest(&standalone_manifest_on_mount(&missing_mount), "all-1")
                .unwrap();
        assert!(validate_test_selected_host_filesystem(&missing)
            .unwrap_err()
            .contains("mount path cannot be inspected"));

        let real_mount = dir.path().join("real");
        let linked_mount = dir.path().join("linked");
        private_dir(&real_mount);
        symlink(&real_mount, &linked_mount).unwrap();
        let linked =
            parse_static_cluster_manifest(&standalone_manifest_on_mount(&linked_mount), "all-1")
                .unwrap();
        assert!(validate_test_selected_host_filesystem(&linked)
            .unwrap_err()
            .contains("mount path must be a non-symlink directory"));
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_insecure_local_path() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        let data_path = mount_path.join("data");
        private_dir(&mount_path);
        private_dir(&data_path);
        std::fs::set_permissions(&data_path, std::fs::Permissions::from_mode(0o770)).unwrap();
        let manifest = standalone_manifest_on_mount(&mount_path);
        let validated = parse_static_cluster_manifest(&manifest, "all-1").unwrap();

        let error = validate_test_selected_host_filesystem(&validated).unwrap_err();

        assert!(error.contains("must not be writable by group or other users"));
    }

    #[test]
    fn static_cluster_filesystem_validation_rejects_wrong_existing_path_type() {
        let dir = test_util::tempdir();
        let mount_path = dir.path().join("disk");
        private_dir(&mount_path);
        private_dir(&mount_path.join("control.state"));
        let manifest = standalone_manifest_on_mount(&mount_path);
        let validated = parse_static_cluster_manifest(&manifest, "all-1").unwrap();

        let error = validate_test_selected_host_filesystem(&validated).unwrap_err();

        assert!(
            error.contains("state path must be a regular file"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_noncanonical_endpoint_uris() {
        let unix = standalone_manifest().replacen(
            "unix:///run/argmin/control.sock",
            "unix:///run/./argmin/control.sock",
            1,
        );
        assert!(parse_static_cluster_manifest(&unix, "all-1")
            .unwrap_err()
            .contains("not canonical"));

        let tcp = replace_once(
            &replicated_manifest(),
            "tcp://0.0.0.0:7401",
            "tcp://0.0.0.0:07401",
        );
        assert!(parse_static_cluster_manifest(&tcp, "control-1")
            .unwrap_err()
            .contains("URI is not canonical"));
    }

    #[test]
    fn static_cluster_manifest_rejects_relative_and_escaping_state_paths() {
        let relative = replace_once(
            &standalone_manifest(),
            "state_path = \"/srv/argmin/control.state\"",
            "state_path = \"control.state\"",
        );
        assert!(parse_static_cluster_manifest(&relative, "all-1")
            .unwrap_err()
            .contains("must be absolute"));

        let escaping = replace_once(
            &standalone_manifest(),
            "state_path = \"/srv/argmin/control.state\"",
            "state_path = \"/srv/argmin/../control.state\"",
        );
        assert!(parse_static_cluster_manifest(&escaping, "all-1")
            .unwrap_err()
            .contains("not canonical"));
    }

    #[test]
    fn static_cluster_manifest_rejects_replicated_policy_contradictions() {
        let undersized_append_batch = replace_once(
            &replicated_manifest(),
            "max_append_entries = 64",
            "max_append_entries = 32",
        );
        assert!(
            parse_static_cluster_manifest(&undersized_append_batch, "control-1")
                .unwrap_err()
                .contains("production replication batch size")
        );

        let undersized_append_payload = replace_once(
            &replicated_manifest(),
            "max_append_bytes = 8388608",
            "max_append_bytes = 4194304",
        );
        assert!(
            parse_static_cluster_manifest(&undersized_append_payload, "control-1")
                .unwrap_err()
                .contains("production replication payload size")
        );

        let unauthenticated = replace_once(
            &replicated_manifest(),
            "internal_auth = \"required\"",
            "internal_auth = \"disabled\"",
        );
        assert!(parse_static_cluster_manifest(&unauthenticated, "control-1")
            .unwrap_err()
            .contains("requires internal authentication"));

        let insufficient_parity = replace_once(
            &replicated_manifest(),
            "ec_parity_shards = 1",
            "ec_parity_shards = 0",
        );
        assert!(
            parse_static_cluster_manifest(&insufficient_parity, "control-1")
                .unwrap_err()
                .contains("parity must cover")
        );

        let incompatible_frame = replace_once(
            &replicated_manifest(),
            "max_frame_bytes = 16777216",
            "max_frame_bytes = 4194304",
        );
        assert!(
            parse_static_cluster_manifest(&incompatible_frame, "control-1")
                .unwrap_err()
                .contains("cannot carry max append bytes")
        );

        let below_production_frame = replace_once(
            &replicated_manifest(),
            "max_frame_bytes = 16777216",
            "max_frame_bytes = 9437184",
        );
        assert!(
            parse_static_cluster_manifest(&below_production_frame, "control-1")
                .unwrap_err()
                .contains("incompatible with production replication")
        );

        let undersized_control_plane_deadline = replace_once(
            &replicated_manifest(),
            "id = \"control\"\nmax_frame_bytes = 8388648\nmax_connections = 64\nconnect_timeout_ms = 1000\nio_timeout_ms = 15000",
            "id = \"control\"\nmax_frame_bytes = 8388648\nmax_connections = 64\nconnect_timeout_ms = 1000\nio_timeout_ms = 14999",
        );
        assert!(
            parse_static_cluster_manifest(&undersized_control_plane_deadline, "control-1")
                .unwrap_err()
                .contains("longest authenticated RPC")
        );

        let missing_admin_identity = replace_once(
            &replicated_manifest(),
            "admin_instance_id = \"control-1-admin\"",
            "maintenance_instance_id = \"control-1-maintenance\"",
        );
        assert!(
            parse_static_cluster_manifest(&missing_admin_identity, "control-1")
                .unwrap_err()
                .contains("requires admin_instance_id")
        );

        let all_in_one = replace_once(
            &replicated_manifest(),
            "id = \"control-1\"\nhost_id = \"host-1\"\nkind = \"control-plane\"",
            "id = \"control-1\"\nhost_id = \"host-1\"\nkind = \"all-in-one\"",
        );
        assert!(parse_static_cluster_manifest(&all_in_one, "control-1")
            .unwrap_err()
            .contains("only in standalone"));
    }

    #[test]
    fn static_cluster_manifest_rejects_impossible_snapshot_transport() {
        let raw_append_only = replace_once(
            &replicated_manifest(),
            "max_frame_bytes = 16777216",
            "max_frame_bytes = 8388608",
        );
        assert!(parse_static_cluster_manifest(&raw_append_only, "control-1")
            .unwrap_err()
            .contains("identity and authentication overhead"));

        let no_overhead_space = replace_once(
            &replicated_manifest(),
            "max_snapshot_bytes = 15728640",
            "max_snapshot_bytes = 16777216",
        );
        assert!(
            parse_static_cluster_manifest(&no_overhead_space, "control-1")
                .unwrap_err()
                .contains("plus metadata and authentication overhead")
        );

        let above_snapshot_limit = replace_once(
            &replicated_manifest(),
            "max_snapshot_bytes = 15728640",
            "max_snapshot_bytes = 67108864",
        );
        assert!(
            parse_static_cluster_manifest(&above_snapshot_limit, "control-1")
                .unwrap_err()
                .contains("max_snapshot_bytes must be")
        );
    }

    #[test]
    fn static_cluster_manifest_rejects_unreplicable_initial_bootstrap() {
        let oversized = replicated_unix_manifest_with_shape(6, 4_096, 5, 1);

        let error = parse_static_cluster_manifest(&oversized, "control-1").unwrap_err();

        assert!(error.contains("static initial bootstrap is not replication-safe"));
        assert!(error.contains("replication-safe per-entry limit"));
    }

    #[test]
    fn static_cluster_manifest_rejects_cross_role_durable_path_collision() {
        let collision = replace_once(
            &standalone_manifest(),
            "data_dir = \"/srv/argmin/data\"",
            "data_dir = \"/srv/argmin/control.state\"",
        );
        assert!(parse_static_cluster_manifest(&collision, "all-1")
            .unwrap_err()
            .contains("runtime path collision"));
    }

    #[test]
    fn static_cluster_manifest_rejects_unix_endpoint_on_authority_state_path() {
        let collision = standalone_manifest().replace(
            "unix:///run/argmin/control.sock",
            "unix:///srv/argmin/control.state",
        );
        let error = parse_static_cluster_manifest(&collision, "all-1").unwrap_err();
        assert!(error.contains("runtime path collision"));
        assert!(error.contains("authority authority-1 state"));
        assert!(error.contains("endpoint control-1 Unix socket"));
    }

    #[test]
    fn static_cluster_manifest_rejects_storage_path_on_derived_raft_file() {
        let collision = replace_once(
            &standalone_manifest(),
            "data_dir = \"/srv/argmin/data\"",
            "data_dir = \"/srv/argmin/control.state.wal\"",
        );
        let error = parse_static_cluster_manifest(&collision, "all-1").unwrap_err();
        assert!(error.contains("runtime path collision"));
        assert!(error.contains("Raft WAL"));
        assert!(error.contains("storage node 1 data directory"));
    }

    #[test]
    fn static_cluster_manifest_rejects_unix_endpoint_inside_storage_directory() {
        let collision = standalone_manifest().replace(
            "unix:///run/argmin/storage.sock",
            "unix:///srv/argmin/data/storage.sock",
        );
        let error = parse_static_cluster_manifest(&collision, "all-1").unwrap_err();
        assert!(error.contains("runtime path collision"));
        assert!(error.contains("storage node 1 data directory"));
        assert!(error.contains("endpoint storage-1 Unix socket"));
    }

    #[test]
    fn static_cluster_manifest_rejects_unix_endpoint_in_temporary_file_namespace() {
        let collision = standalone_manifest().replace(
            "unix:///run/argmin/control.sock",
            "unix:///srv/argmin/control.state.tmp.1234",
        );
        let error = parse_static_cluster_manifest(&collision, "all-1").unwrap_err();
        assert!(error.contains("runtime path collision"));
        assert!(error.contains("Raft artifact temporary file"));
        assert!(error.contains("endpoint control-1 Unix socket"));
    }

    #[test]
    fn static_cluster_manifest_rejects_unix_endpoint_in_sentinel_temporary_namespace() {
        let collision = standalone_manifest().replace(
            "unix:///run/argmin/control.sock",
            "unix:///srv/argmin/control.state.sentinel.tmp.1234",
        );
        let error = parse_static_cluster_manifest(&collision, "all-1").unwrap_err();
        assert!(error.contains("runtime path collision"));
        assert!(error.contains("Raft artifact sentinel temporary file"));
        assert!(error.contains("endpoint control-1 Unix socket"));
    }

    #[test]
    fn static_cluster_manifest_rejects_duplicate_credential_identity() {
        let duplicate = format!(
            "{}\n{}",
            replicated_manifest(),
            r#"
[[auth_credentials]]
principal = "raft-peer"
node_id = 101
credential_id = "raft-1"
credential_version = 1
use_for_signing = false
accept_from_ms = 0
secret_ref = "file:/run/argmin-secrets/duplicate.key"
"#
        );
        assert!(parse_static_cluster_manifest(&duplicate, "control-1")
            .unwrap_err()
            .contains("duplicate credential identity"));
    }

    #[test]
    fn static_cluster_manifest_parse_error_does_not_echo_values() {
        let secret = "DO-NOT-LOG-THIS-SECRET";
        let malformed = format!("{}\ninline_secret = \"{secret}\"\n", standalone_manifest());
        let error = parse_static_cluster_manifest(&malformed, "all-1").unwrap_err();
        assert!(!error.contains(secret));
    }

    #[test]
    fn static_cluster_material_resolution_is_selected_process_scoped_and_redacted() {
        let (_dir, control_manifest) = materialized_replicated_manifest("control-1");
        let control_material = control_manifest
            .resolve_selected_process_material()
            .unwrap();
        assert_eq!(control_material.auth_credential_count(), 9);
        assert_eq!(control_material.tls_identity_count(), 1);
        assert_eq!(control_material.tls_trust_bundle_count(), 1);
        assert_eq!(
            control_material
                .auth_credentials
                .iter()
                .find(|credential| credential.credential_id == "raft-1")
                .unwrap()
                .secret
                .as_slice(),
            &[0, 1, 2, 0xff]
        );
        let debug = format!("{control_material:?}");
        assert!(!debug.contains("raft-1-secret"));
        assert!(!debug.contains("BEGIN PRIVATE KEY"));
        assert!(!debug.contains(char::from(0xff)));

        let (_dir, storage_manifest) = materialized_replicated_manifest("storage-1");
        let storage_material = storage_manifest
            .resolve_selected_process_material()
            .unwrap();
        assert_eq!(storage_material.auth_credential_count(), 6);
        assert!(storage_material
            .auth_credentials
            .iter()
            .all(|credential| credential.principal.principal != AuthPrincipal::RaftPeer));
        assert_eq!(storage_material.tls_identity_count(), 1);
        assert_eq!(storage_material.tls_trust_bundle_count(), 1);
    }

    #[test]
    fn replicated_unix_data_processes_map_storage_rpc_auth_from_manifest() {
        let manifest_text = replicated_unix_data_manifest();
        let (_storage_dir, storage_manifest) =
            materialized_replicated_manifest_from("storage-1", manifest_text.clone());
        let storage_material = storage_manifest
            .resolve_selected_process_material_at(1)
            .unwrap();
        let storage_config = storage_manifest
            .replicated_data_process_server_config(&storage_material, |_| None)
            .unwrap();

        assert_eq!(storage_config.process_role, ProcessRole::StorageNode);
        assert_eq!(storage_config.storage_node_id, Some(1));
        assert_eq!(storage_config.storage_node_ids, vec![1, 2, 3]);
        assert_eq!(
            storage_config.storage_node_socket_path.as_deref(),
            Some("/run/argmin/storage-1.sock")
        );
        assert!(storage_config
            .storage_rpc_storage_node_client_auth
            .is_some());
        assert!(storage_config.storage_rpc_frontend_client_auth.is_none());
        assert!(storage_config.storage_rpc_server_auth.is_some());
        assert_eq!(storage_config.storage_node_rpc_admission_limit, 64);
        assert_eq!(
            storage_config
                .storage_rpc_server_auth
                .as_ref()
                .unwrap()
                .transport_limits()
                .max_connections(),
            64
        );

        let (_frontend_dir, frontend_manifest) =
            materialized_replicated_manifest_from("frontend-1", manifest_text);
        let frontend_material = frontend_manifest
            .resolve_selected_process_material_at(1)
            .unwrap();
        let environment = standalone_runtime_environment();
        let frontend_config = frontend_manifest
            .replicated_data_process_server_config(&frontend_material, |key| {
                environment.get(key).cloned()
            })
            .unwrap();

        assert_eq!(frontend_config.process_role, ProcessRole::Frontend);
        assert_eq!(frontend_config.storage_node_id, None);
        assert_eq!(frontend_config.storage_node_sockets.len(), 3);
        assert!(frontend_config.storage_rpc_frontend_client_auth.is_some());
        assert!(frontend_config
            .storage_rpc_maintenance_client_auth
            .is_some());
        assert_eq!(
            frontend_config
                .storage_rpc_frontend_client_auth
                .as_ref()
                .unwrap()
                .transport_limits()
                .io_timeout(),
            Duration::from_secs(15)
        );
        assert!(frontend_config.storage_rpc_server_auth.is_none());
        assert!(frontend_config.control_plane_rpc_frame_transport.is_some());
    }

    #[test]
    fn replicated_frontend_requires_maintenance_identity_for_background_workflows() {
        let manifest = replicated_unix_data_manifest()
            .replace("maintenance_instance_id = \"frontend-1-maintenance\"\n", "");

        let error = parse_static_cluster_manifest(&manifest, "frontend-1").unwrap_err();

        assert!(
            error.contains("requires maintenance_instance_id"),
            "{error}"
        );
        assert!(error.contains("background workflows"), "{error}");
    }

    #[test]
    fn static_cluster_material_resolution_applies_startup_rotation_windows() {
        let (_dir, mut manifest) = materialized_replicated_manifest("control-1");
        let old_index = manifest
            .manifest
            .auth_credentials
            .iter()
            .position(|credential| credential.credential_id == "raft-1")
            .unwrap();
        manifest.manifest.auth_credentials[old_index].accept_until_ms = Some(100);
        let mut next = manifest.manifest.auth_credentials[old_index].clone();
        next.credential_id = "raft-1-next".to_string();
        next.credential_version = 2;
        next.accept_from_ms = 100;
        next.accept_until_ms = None;
        next.secret_ref = "file:/missing/future-raft-credential".to_string();
        manifest.manifest.auth_credentials.push(next);

        let before_rotation = manifest.resolve_selected_process_material_at(99).unwrap();
        assert!(before_rotation
            .auth_credentials
            .iter()
            .any(|credential| credential.credential_id == "raft-1"));
        assert!(before_rotation
            .auth_credentials
            .iter()
            .all(|credential| credential.credential_id != "raft-1-next"));

        assert!(manifest
            .resolve_selected_process_material_at(100)
            .unwrap_err()
            .contains("future-raft-credential"));

        manifest.manifest.auth_credentials[old_index].accept_until_ms = Some(90);
        assert!(manifest
            .resolve_selected_process_material_at(95)
            .unwrap_err()
            .contains("exactly one signing credential active"));
    }

    #[test]
    fn static_cluster_material_resolution_loads_role_required_remote_trust_bundles() {
        let (dir, mut manifest) = materialized_replicated_manifest("control-1");
        let remote_ca_path = dir.path().join("material/remote-control-ca.pem");
        write_material_file(
            &remote_ca_path,
            include_bytes!("../../s3-tests/testdata/ca-cert.pem"),
            0o644,
        );
        manifest
            .manifest
            .tls_trust_bundles
            .push(TlsTrustBundleInput {
                id: "remote-control-ca".to_string(),
                ca_bundle_ref: format!("file:{}", remote_ca_path.display()),
            });
        for endpoint in manifest.manifest.endpoints.iter_mut().filter(|endpoint| {
            endpoint.owner_process_id != "control-1"
                && matches!(
                    endpoint.protocol,
                    EndpointProtocol::ControlPlane | EndpointProtocol::AuthorityClockRecovery
                )
        }) {
            endpoint.tls_trust_bundle_id = Some("remote-control-ca".to_string());
        }

        let material = manifest.resolve_selected_process_material().unwrap();

        assert_eq!(material.tls_trust_bundle_count(), 2);
        assert!(material.tls_trust_bundles.contains_key("remote-control-ca"));
    }

    #[test]
    fn static_cluster_material_resolution_enforces_aggregate_file_and_byte_limits() {
        let (_dir, manifest) = materialized_replicated_manifest("control-1");
        let file_error = manifest
            .resolve_selected_process_material_at_with_limits(
                0,
                StaticMaterialLimits {
                    max_files: 1,
                    max_bytes: CLUSTER_MANIFEST_MAX_SELECTED_MATERIAL_BYTES,
                },
            )
            .unwrap_err();
        assert!(
            file_error.contains("file aggregate limit"),
            "unexpected error: {file_error}"
        );

        let byte_error = manifest
            .resolve_selected_process_material_at_with_limits(
                0,
                StaticMaterialLimits {
                    max_files: CLUSTER_MANIFEST_MAX_SELECTED_MATERIAL_FILES,
                    max_bytes: 3,
                },
            )
            .unwrap_err();
        assert!(
            byte_error.contains("aggregate byte limit"),
            "unexpected error: {byte_error}"
        );
    }

    #[test]
    fn static_cluster_material_reader_rejects_unsafe_files_before_allocation() {
        let dir = test_util::tempdir();
        let target = dir.path().join("target");
        let link = dir.path().join("link");
        write_material_file(&target, b"secret", 0o600);
        symlink(&target, &link).unwrap();
        let link_reference = format!("file:{}", link.display());
        assert!(read_static_material_file(
            &link_reference,
            CLUSTER_MANIFEST_MAX_AUTH_SECRET_BYTES,
            StaticMaterialFileAccess::Private,
            "credential secret",
        )
        .unwrap_err()
        .contains("open credential secret"));

        let permissive = dir.path().join("permissive");
        write_material_file(&permissive, b"secret", 0o640);
        let permissive_reference = format!("file:{}", permissive.display());
        assert!(read_static_material_file(
            &permissive_reference,
            CLUSTER_MANIFEST_MAX_AUTH_SECRET_BYTES,
            StaticMaterialFileAccess::Private,
            "credential secret",
        )
        .unwrap_err()
        .contains("group or other permissions"));

        let empty = dir.path().join("empty");
        write_material_file(&empty, b"", 0o600);
        let empty_reference = format!("file:{}", empty.display());
        assert!(read_static_material_file(
            &empty_reference,
            CLUSTER_MANIFEST_MAX_AUTH_SECRET_BYTES,
            StaticMaterialFileAccess::Private,
            "credential secret",
        )
        .unwrap_err()
        .contains("is empty"));

        let oversized = dir.path().join("oversized");
        write_material_file(
            &oversized,
            &vec![b'x'; CLUSTER_MANIFEST_MAX_AUTH_SECRET_BYTES as usize + 1],
            0o600,
        );
        let oversized_reference = format!("file:{}", oversized.display());
        assert!(read_static_material_file(
            &oversized_reference,
            CLUSTER_MANIFEST_MAX_AUTH_SECRET_BYTES,
            StaticMaterialFileAccess::Private,
            "credential secret",
        )
        .unwrap_err()
        .contains("exceeds"));
    }

    #[test]
    fn static_cluster_material_resolution_rejects_malformed_or_untrusted_tls_without_leaks() {
        let (dir, manifest) = materialized_replicated_manifest("control-1");
        let certificate_path = dir.path().join("material/host-1.crt");
        let sentinel = b"DO-NOT-LOG-TLS-CONTENT";
        write_material_file(&certificate_path, sentinel, 0o644);
        let error = manifest.resolve_selected_process_material().unwrap_err();
        assert!(error.contains("content outside a PEM section"));
        assert!(!error.contains(std::str::from_utf8(sentinel).unwrap()));

        let (_dir, mut manifest) = materialized_replicated_manifest("control-1");
        for endpoint in manifest
            .manifest
            .endpoints
            .iter_mut()
            .filter(|endpoint| endpoint.owner_process_id == "control-1")
        {
            endpoint.tls_server_name = Some("wrong.internal".to_string());
        }
        assert!(manifest
            .resolve_selected_process_material()
            .unwrap_err()
            .contains("does not match its server name or trust bundle"));
    }

    #[test]
    fn static_cluster_material_resolution_rejects_non_ca_trust_anchors() {
        let (dir, manifest) = materialized_replicated_manifest("control-1");
        let trust_bundle_path = dir.path().join("material/cluster-ca.pem");
        let mut mixed_bundle = include_bytes!("../../s3-tests/testdata/ca-cert.pem").to_vec();
        mixed_bundle
            .extend_from_slice(include_bytes!("../../s3-tests/testdata/localhost-cert.pem"));
        write_material_file(&trust_bundle_path, &mixed_bundle, 0o644);

        let error = manifest.resolve_selected_process_material().unwrap_err();

        assert!(
            error.contains("critical CA constraints and certificate-signing usage"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_material_resolution_rejects_unexpected_pem_sections() {
        let (dir, manifest) = materialized_replicated_manifest("control-1");
        let certificate_path = dir.path().join("material/host-1.crt");
        let mut certificate_with_key =
            include_bytes!("../../s3-tests/testdata/localhost-cert.pem").to_vec();
        certificate_with_key
            .extend_from_slice(include_bytes!("../../s3-tests/testdata/localhost-key.pem"));
        write_material_file(&certificate_path, &certificate_with_key, 0o644);
        let error = manifest.resolve_selected_process_material().unwrap_err();
        assert!(
            error.contains("certificate PEM sections only"),
            "unexpected error: {error}"
        );

        let (dir, manifest) = materialized_replicated_manifest("control-1");
        let private_key_path = dir.path().join("material/host-1.key");
        let mut duplicate_keys =
            include_bytes!("../../s3-tests/testdata/localhost-key.pem").to_vec();
        duplicate_keys
            .extend_from_slice(include_bytes!("../../s3-tests/testdata/localhost-key.pem"));
        write_material_file(&private_key_path, &duplicate_keys, 0o600);
        let error = manifest.resolve_selected_process_material().unwrap_err();
        assert!(
            error.contains("exactly one private-key PEM section"),
            "unexpected error: {error}"
        );

        let (dir, manifest) = materialized_replicated_manifest("control-1");
        let trust_bundle_path = dir.path().join("material/cluster-ca.pem");
        let mut bundle_with_key = include_bytes!("../../s3-tests/testdata/ca-cert.pem").to_vec();
        bundle_with_key
            .extend_from_slice(include_bytes!("../../s3-tests/testdata/localhost-key.pem"));
        write_material_file(&trust_bundle_path, &bundle_with_key, 0o644);
        let error = manifest.resolve_selected_process_material().unwrap_err();
        assert!(
            error.contains("certificate PEM sections only"),
            "unexpected error: {error}"
        );

        let (dir, manifest) = materialized_replicated_manifest("control-1");
        let trust_bundle_path = dir.path().join("material/cluster-ca.pem");
        let mut bundle_with_text = b"unexpected material\n".to_vec();
        bundle_with_text.extend_from_slice(include_bytes!("../../s3-tests/testdata/ca-cert.pem"));
        write_material_file(&trust_bundle_path, &bundle_with_text, 0o644);
        let error = manifest.resolve_selected_process_material().unwrap_err();
        assert!(
            error.contains("content outside a PEM section"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn static_cluster_manifest_loader_rejects_relative_and_oversized_files() {
        assert!(
            load_static_cluster_manifest(Path::new("relative.toml"), "all-1")
                .unwrap_err()
                .contains("absolute")
        );

        let dir = test_util::tempdir();
        let path = dir.path().join("cluster.toml");
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![b'x'; CLUSTER_MANIFEST_MAX_BYTES as usize + 1])
            .unwrap();
        file.sync_all().unwrap();
        assert!(load_static_cluster_manifest(&path, "all-1")
            .unwrap_err()
            .contains("exceeds"));
    }

    #[test]
    fn static_cluster_manifest_loader_rejects_non_utf8_and_symlink() {
        let dir = test_util::tempdir();
        let invalid_utf8_path = dir.path().join("invalid-utf8.toml");
        let mut file = File::create(&invalid_utf8_path).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        assert!(load_static_cluster_manifest(&invalid_utf8_path, "all-1")
            .unwrap_err()
            .contains("UTF-8"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let manifest_path = dir.path().join("cluster.toml");
            let link_path = dir.path().join("cluster-link.toml");
            std::fs::write(&manifest_path, standalone_manifest()).unwrap();
            symlink(&manifest_path, &link_path).unwrap();
            assert!(load_static_cluster_manifest(&link_path, "all-1")
                .unwrap_err()
                .contains("no-follow"));
        }
    }
}
