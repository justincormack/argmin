use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::mem::MaybeUninit;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rustls::sign::CertifiedKey;
use thiserror::Error;
use tokio::runtime::Handle;

use crate::control_plane::ControlPlaneError;
use crate::control_plane_auth::{
    ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
    ControlPlaneScopedCredentialStore,
};
#[cfg(test)]
use crate::control_plane_raft::ControlPlaneRaftPeerServerCheckpoint;
#[cfg(any(test, feature = "test-hooks"))]
use crate::control_plane_raft::ControlPlaneRaftPeerTestClient;
use crate::control_plane_raft::{
    ControlPlaneRaftAuthority, ControlPlaneRaftNodeId, ControlPlaneRaftPeerAuthPolicy,
    ControlPlaneRaftPeerClientEndpoint, ControlPlaneRaftPeerNetworkConfig,
    ControlPlaneRaftPeerServerDurability, ControlPlaneRaftPeerServerListener,
    ControlPlaneRaftPeerServerPolicy, ControlPlaneRaftPeerTransportLimits,
    ControlPlaneRaftPeerTransportPolicy,
};
use crate::StaticInitialControlPlaneTopology;

/// Logical credential material for one Raft peer principal.
#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftPeerAuthCredentialInput {
    node_id: ControlPlaneRaftNodeId,
    credential_id: String,
    credential_version: u64,
    secret: Vec<u8>,
}

impl ControlPlaneRaftPeerAuthCredentialInput {
    #[must_use]
    pub fn new(
        node_id: ControlPlaneRaftNodeId,
        credential_id: impl Into<String>,
        credential_version: u64,
        secret: Vec<u8>,
    ) -> Self {
        Self {
            node_id,
            credential_id: credential_id.into(),
            credential_version,
            secret,
        }
    }
}

impl fmt::Debug for ControlPlaneRaftPeerAuthCredentialInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftPeerAuthCredentialInput")
            .field("node_id", &self.node_id)
            .field("credential_id", &"<redacted>")
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Storage-owned topology binding applied consistently to peer admission,
/// durable authority restore, and pending static initialization.
#[derive(Clone)]
pub enum ControlPlaneRaftPeerTopologyBinding {
    Unbound,
    Established { generation: u64, digest: String },
    StaticInitial(StaticInitialControlPlaneTopology),
}

impl fmt::Debug for ControlPlaneRaftPeerTopologyBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unbound => formatter.write_str("Unbound"),
            Self::Established { generation, .. } => formatter
                .debug_struct("Established")
                .field("generation", generation)
                .field("digest", &"<opaque>")
                .finish(),
            Self::StaticInitial(topology) => formatter
                .debug_tuple("StaticInitial")
                .field(&format_args!("{} PGs", topology.pg_count()))
                .finish(),
        }
    }
}

/// A process-owned socket binding handed to the storage-owned Raft peer
/// server bootstrap.
pub enum ControlPlaneRaftPeerServerListenerInput {
    Unix {
        endpoint_id: String,
        listener: UnixListener,
        max_connections: usize,
        io_timeout: Duration,
    },
    TlsTcp {
        endpoint_id: String,
        listener: TcpListener,
        certified_key: Arc<CertifiedKey>,
        max_connections: usize,
        io_timeout: Duration,
    },
}

impl fmt::Debug for ControlPlaneRaftPeerServerListenerInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (endpoint_id, transport, max_connections, io_timeout) = match self {
            Self::Unix {
                endpoint_id,
                max_connections,
                io_timeout,
                ..
            } => (endpoint_id, "unix", max_connections, io_timeout),
            Self::TlsTcp {
                endpoint_id,
                max_connections,
                io_timeout,
                ..
            } => (endpoint_id, "tls-tcp", max_connections, io_timeout),
        };
        formatter
            .debug_struct("ControlPlaneRaftPeerServerListenerInput")
            .field("endpoint_id", endpoint_id)
            .field("transport", &transport)
            .field("max_connections", max_connections)
            .field("io_timeout", io_timeout)
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneRaftPeerBootstrapError {
    #[error("replicated control-plane Raft bootstrap requires at least one peer")]
    MissingPeer,
    #[error("control-plane Raft peer bootstrap configuration is invalid")]
    InvalidPeerConfiguration,
    #[error("control-plane Raft peer authentication configuration is invalid")]
    InvalidAuthentication,
    #[error("control-plane Raft peer client endpoint configuration is invalid")]
    InvalidClientEndpoints,
    #[error("single-node control-plane Raft bootstrap cannot serve peer listeners")]
    SingleNodePeerListener,
    #[error("replicated control-plane Raft bootstrap requires at least one peer listener")]
    MissingPeerListener,
    #[error("control-plane Raft peer listener {index} is invalid")]
    InvalidPeerListener { index: usize },
    #[error(
        "control-plane Raft peer listeners {first_index} and {second_index} use the same endpoint id"
    )]
    DuplicatePeerListenerId {
        first_index: usize,
        second_index: usize,
    },
    #[error(
        "control-plane Raft peer listeners {first_index} and {second_index} refer to the same socket"
    )]
    AliasedPeerListener {
        first_index: usize,
        second_index: usize,
    },
    #[error("control-plane Raft peer server resource policy is invalid")]
    InvalidServerPolicy,
    #[error("control-plane Raft peer server durability belongs to another authority")]
    MismatchedDurabilityAuthority,
}

#[derive(Clone)]
struct ReplicatedPeerBootstrap {
    policy: ControlPlaneRaftPeerTransportPolicy,
    network: ControlPlaneRaftPeerNetworkConfig,
    static_initial_topology: Option<StaticInitialControlPlaneTopology>,
}

/// Opaque client/server/authority configuration for one Raft peer domain.
#[derive(Clone)]
pub struct ControlPlaneRaftPeerBootstrap {
    cluster_name: String,
    local_node_id: ControlPlaneRaftNodeId,
    replicated: Option<ReplicatedPeerBootstrap>,
}

impl fmt::Debug for ControlPlaneRaftPeerBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftPeerBootstrap")
            .field("local_node_id", &self.local_node_id)
            .field("replicated", &self.replicated.is_some())
            .field("peer_count", &self.peer_count())
            .field(
                "authenticated",
                &self
                    .replicated
                    .as_ref()
                    .is_some_and(|peer| peer.policy.auth_policy().is_some()),
            )
            .finish()
    }
}

impl ControlPlaneRaftPeerBootstrap {
    #[must_use]
    pub fn single_node(
        cluster_name: impl Into<String>,
        local_node_id: ControlPlaneRaftNodeId,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            local_node_id,
            replicated: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn replicated(
        cluster_name: impl Into<String>,
        local_node_id: ControlPlaneRaftNodeId,
        peer_endpoints: impl IntoIterator<Item = (ControlPlaneRaftNodeId, String)>,
        client_endpoints: Vec<(ControlPlaneRaftNodeId, ControlPlaneRaftPeerClientEndpoint)>,
        limits: ControlPlaneRaftPeerTransportLimits,
        connect_timeout: Duration,
        io_timeout: Duration,
        topology: ControlPlaneRaftPeerTopologyBinding,
        credentials: Vec<ControlPlaneRaftPeerAuthCredentialInput>,
        signing_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneRaftPeerBootstrapError> {
        let cluster_name = cluster_name.into();
        let peer_endpoints = peer_endpoints.into_iter().collect::<Vec<_>>();
        if peer_endpoints.is_empty() {
            return Err(ControlPlaneRaftPeerBootstrapError::MissingPeer);
        }
        let mut peer_node_ids = HashSet::with_capacity(peer_endpoints.len());
        let mut advertised_endpoints = HashSet::with_capacity(peer_endpoints.len());
        if peer_endpoints.iter().any(|(node_id, endpoint)| {
            !peer_node_ids.insert(*node_id) || !advertised_endpoints.insert(endpoint.as_str())
        }) {
            return Err(ControlPlaneRaftPeerBootstrapError::InvalidPeerConfiguration);
        }
        let mut policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            peer_endpoints,
            limits,
        )
        .with_timeouts(connect_timeout, io_timeout);
        let static_initial_topology = match topology {
            ControlPlaneRaftPeerTopologyBinding::Unbound => None,
            ControlPlaneRaftPeerTopologyBinding::Established { generation, digest } => {
                policy = policy.with_topology_identity(generation, digest);
                None
            }
            ControlPlaneRaftPeerTopologyBinding::StaticInitial(topology) => {
                policy = policy.with_static_initial_topology(&topology);
                Some(topology)
            }
        };
        policy
            .validate_cluster_name(&cluster_name)
            .and_then(|()| policy.validate_local_node(local_node_id))
            .and_then(|()| policy.validate_replication_compatibility())
            .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidPeerConfiguration)?;
        let auth_policy = build_auth_policy(
            &cluster_name,
            local_node_id,
            &peer_node_ids,
            credentials,
            signing_credential,
        )?;
        policy = policy.with_auth_policy(auth_policy);
        let network = if client_endpoints.is_empty() {
            ControlPlaneRaftPeerNetworkConfig::unix(io_timeout)
        } else {
            ControlPlaneRaftPeerNetworkConfig::with_peer_endpoints(io_timeout, client_endpoints)
                .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidClientEndpoints)?
        };
        network
            .validate_policy(&policy)
            .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidClientEndpoints)?;
        Ok(Self {
            cluster_name,
            local_node_id,
            replicated: Some(ReplicatedPeerBootstrap {
                policy,
                network,
                static_initial_topology,
            }),
        })
    }

    pub fn validate_replication_limits(
        limits: ControlPlaneRaftPeerTransportLimits,
    ) -> Result<(), ControlPlaneRaftPeerBootstrapError> {
        ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            "validation-only",
            [(1, "validation-only".to_owned())],
            limits,
        )
        .validate_replication_compatibility()
        .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidPeerConfiguration)
    }

    #[must_use]
    pub fn is_multi_node(&self) -> bool {
        self.peer_count() > 1
    }

    #[must_use]
    pub fn startup_requires_local_leader(&self) -> bool {
        !self.is_multi_node()
    }

    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.replicated
            .as_ref()
            .map_or(1, |peer| peer.policy.peers().len())
    }

    pub async fn open_durable_authority(
        &self,
        artifact_path: &Path,
        static_identity_established: bool,
    ) -> Result<ControlPlaneRaftAuthority, ControlPlaneError> {
        let Some(peer) = &self.replicated else {
            return ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                self.cluster_name.clone(),
                self.local_node_id,
                artifact_path,
            )
            .await;
        };
        if !static_identity_established {
            if let Some(topology) = &peer.static_initial_topology {
                return ControlPlaneRaftAuthority::new_experimental_peer_durable_pending_static_initialization_network(
                    self.cluster_name.clone(),
                    self.local_node_id,
                    artifact_path,
                    peer.policy.clone(),
                    topology,
                    peer.network.clone(),
                )
                .await;
            }
        }
        ControlPlaneRaftAuthority::new_experimental_peer_durable_network(
            self.cluster_name.clone(),
            self.local_node_id,
            artifact_path,
            peer.policy.clone(),
            peer.network.clone(),
        )
        .await
    }

    #[must_use]
    pub fn auth_diagnostics(&self) -> Option<String> {
        let peer = self.replicated.as_ref()?;
        Some(format_auth_diagnostics(&peer.policy))
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct ListenerIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

fn listener_endpoint_id(listener: &ControlPlaneRaftPeerServerListenerInput) -> &str {
    match listener {
        ControlPlaneRaftPeerServerListenerInput::Unix { endpoint_id, .. }
        | ControlPlaneRaftPeerServerListenerInput::TlsTcp { endpoint_id, .. } => endpoint_id,
    }
}

fn listener_identity(
    listener: &ControlPlaneRaftPeerServerListenerInput,
) -> io::Result<ListenerIdentity> {
    let file_descriptor = match listener {
        ControlPlaneRaftPeerServerListenerInput::Unix { listener, .. } => listener.as_raw_fd(),
        ControlPlaneRaftPeerServerListenerInput::TlsTcp { listener, .. } => listener.as_raw_fd(),
    };
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `metadata` points to writable storage for one `libc::stat`, and
    // the borrowed listener owns `file_descriptor` throughout this call.
    if unsafe { libc::fstat(file_descriptor, metadata.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful `fstat` initialized the complete `libc::stat`.
    let metadata = unsafe { metadata.assume_init() };
    Ok(ListenerIdentity {
        device: metadata.st_dev,
        inode: metadata.st_ino,
    })
}

fn validate_listener_identities(
    listeners: &[ControlPlaneRaftPeerServerListenerInput],
) -> Result<(), ControlPlaneRaftPeerBootstrapError> {
    let mut endpoint_ids = HashMap::with_capacity(listeners.len());
    let mut identities = HashMap::with_capacity(listeners.len());
    for (index, listener) in listeners.iter().enumerate() {
        if let Some(first_index) = endpoint_ids.insert(listener_endpoint_id(listener), index) {
            return Err(
                ControlPlaneRaftPeerBootstrapError::DuplicatePeerListenerId {
                    first_index,
                    second_index: index,
                },
            );
        }
        let identity = listener_identity(listener)
            .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidPeerListener { index })?;
        if let Some(first_index) = identities.insert(identity, index) {
            return Err(ControlPlaneRaftPeerBootstrapError::AliasedPeerListener {
                first_index,
                second_index: index,
            });
        }
    }
    debug_assert_eq!(endpoint_ids.len(), listeners.len());
    debug_assert_eq!(identities.len(), listeners.len());
    Ok(())
}

fn build_auth_policy(
    cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
    peer_node_ids: &HashSet<ControlPlaneRaftNodeId>,
    credentials: Vec<ControlPlaneRaftPeerAuthCredentialInput>,
    signing_credential: Option<(String, u64)>,
) -> Result<ControlPlaneRaftPeerAuthPolicy, ControlPlaneRaftPeerBootstrapError> {
    if credentials.is_empty() {
        return Err(ControlPlaneRaftPeerBootstrapError::InvalidAuthentication);
    }
    let credential_principals = credentials
        .iter()
        .map(|credential| credential.node_id)
        .collect::<HashSet<_>>();
    if credential_principals != *peer_node_ids {
        return Err(ControlPlaneRaftPeerBootstrapError::InvalidAuthentication);
    }
    let local = credentials
        .iter()
        .filter(|credential| credential.node_id == local_node_id)
        .filter(|credential| {
            signing_credential.as_ref().is_none_or(|(id, version)| {
                credential.credential_id == *id && credential.credential_version == *version
            })
        })
        .max_by(|left, right| {
            left.credential_version
                .cmp(&right.credential_version)
                .then_with(|| left.credential_id.cmp(&right.credential_id))
        })
        .ok_or(ControlPlaneRaftPeerBootstrapError::InvalidAuthentication)?;
    let local_identity = (local.credential_id.clone(), local.credential_version);
    let credentials = credentials
        .into_iter()
        .map(|credential| {
            ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
                cluster_id: cluster_name.to_owned(),
                credential_id: credential.credential_id,
                credential_version: credential.credential_version,
                principal: ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: credential.node_id,
                },
                secret: credential.secret,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidAuthentication)?;
    let local_credential = credentials
        .iter()
        .find(|credential| {
            credential.principal()
                == &ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: local_node_id,
                }
                && credential.credential_id() == local_identity.0
                && credential.credential_version() == local_identity.1
        })
        .cloned()
        .ok_or(ControlPlaneRaftPeerBootstrapError::InvalidAuthentication)?;
    let verifier = ControlPlaneScopedCredentialStore::new(credentials)
        .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidAuthentication)?;
    ControlPlaneRaftPeerAuthPolicy::new(local_credential, verifier)
        .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidAuthentication)
}

fn format_auth_diagnostics(policy: &ControlPlaneRaftPeerTransportPolicy) -> String {
    let status = policy.auth_status_snapshot();
    let metrics = status.metrics();
    let mut diagnostics = format!(
        "raft_peer_auth required={} local_node_id={} credential_version={} accepted_total={} rejected_total={} rejected_without_operation_total={}",
        status.required(),
        status.local_principal().and_then(|principal| match principal {
            ControlPlaneAuthPrincipal::RaftPeer { node_id } => Some(*node_id),
            _ => None,
        }).map_or_else(|| "-".to_owned(), |node_id| node_id.to_string()),
        status.credential_version().map_or_else(|| "-".to_owned(), |version| version.to_string()),
        metrics.accepted_total(),
        metrics.rejected_total(),
        metrics.rejected_without_operation_total(),
    );
    for (operation, count) in metrics.accepted_by_operation() {
        diagnostics.push_str(&format!(
            "\nraft_peer_auth accepted_by_operation{{operation=\"{operation:?}\"}} {count}"
        ));
    }
    for (operation, count) in metrics.rejected_by_operation() {
        diagnostics.push_str(&format!(
            "\nraft_peer_auth rejected_by_operation{{operation=\"{operation:?}\"}} {count}"
        ));
    }
    for (reason, count) in metrics.rejected_by_reason() {
        diagnostics.push_str(&format!(
            "\nraft_peer_auth rejected_by_reason{{reason=\"{reason:?}\"}} {count}"
        ));
    }
    diagnostics
}

pub struct ControlPlaneRaftPeerServerBootstrap {
    authority: Arc<ControlPlaneRaftAuthority>,
    listeners: Vec<ControlPlaneRaftPeerServerListener>,
    policy: ControlPlaneRaftPeerServerPolicy,
}

#[must_use = "dropping the handles detaches the Raft peer server loops"]
pub struct ControlPlaneRaftPeerServerLoops {
    handles: Vec<thread::JoinHandle<()>>,
}

/// Bounded owner facade for cross-crate peer-server tests.
#[cfg(feature = "test-hooks")]
pub struct ControlPlaneRaftPeerTestServer {
    authority: Arc<ControlPlaneRaftAuthority>,
    listener: Arc<ControlPlaneRaftPeerServerListener>,
    policy: ControlPlaneRaftPeerServerPolicy,
}

/// Opaque failure from a test peer-server accept operation.
#[cfg(feature = "test-hooks")]
#[derive(Debug, Error)]
#[error("control-plane Raft peer test server stopped accepting connections")]
pub struct ControlPlaneRaftPeerTestServerError;

impl fmt::Debug for ControlPlaneRaftPeerServerBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftPeerServerBootstrap")
            .field("listener_count", &self.listeners.len())
            .finish()
    }
}

impl fmt::Debug for ControlPlaneRaftPeerServerLoops {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftPeerServerLoops")
            .field("listener_count", &self.handles.len())
            .finish()
    }
}

impl ControlPlaneRaftPeerServerBootstrap {
    pub fn for_authority(
        authority: Arc<ControlPlaneRaftAuthority>,
        listeners: Vec<ControlPlaneRaftPeerServerListenerInput>,
        pre_auth_byte_budget: usize,
    ) -> Result<Option<Self>, ControlPlaneRaftPeerBootstrapError> {
        let Some((local_node_id, peer_policy)) = authority.peer_server_binding() else {
            return if listeners.is_empty() {
                Ok(None)
            } else {
                Err(ControlPlaneRaftPeerBootstrapError::SingleNodePeerListener)
            };
        };
        if listeners.is_empty() {
            return Err(ControlPlaneRaftPeerBootstrapError::MissingPeerListener);
        }
        validate_listener_identities(&listeners)?;
        let listeners = listeners
            .into_iter()
            .enumerate()
            .map(|(index, listener)| {
                match listener {
                    ControlPlaneRaftPeerServerListenerInput::Unix {
                        endpoint_id,
                        listener,
                        max_connections,
                        io_timeout,
                    } => ControlPlaneRaftPeerServerListener::unix(
                        endpoint_id,
                        listener,
                        max_connections,
                        io_timeout,
                    ),
                    ControlPlaneRaftPeerServerListenerInput::TlsTcp {
                        endpoint_id,
                        listener,
                        certified_key,
                        max_connections,
                        io_timeout,
                    } => ControlPlaneRaftPeerServerListener::tls_tcp(
                        endpoint_id,
                        listener,
                        certified_key,
                        max_connections,
                        io_timeout,
                    ),
                }
                .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidPeerListener { index })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let policy =
            ControlPlaneRaftPeerServerPolicy::new(local_node_id, peer_policy, pre_auth_byte_budget)
                .map_err(|_| ControlPlaneRaftPeerBootstrapError::InvalidServerPolicy)?;
        Ok(Some(Self {
            authority,
            listeners,
            policy,
        }))
    }

    pub fn serve(
        self,
        runtime: Handle,
        durability: ControlPlaneRaftPeerServerDurability,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<ControlPlaneRaftPeerServerLoops, ControlPlaneRaftPeerBootstrapError> {
        durability
            .validate_authority(&self.authority)
            .map_err(|_| ControlPlaneRaftPeerBootstrapError::MismatchedDurabilityAuthority)?;
        let policy = self
            .policy
            .with_durability(durability)
            .with_fatal_error_handler(Arc::clone(&terminal_failure_handler));
        let authority = self.authority;
        let handles = self
            .listeners
            .into_iter()
            .map(|listener| {
                let runtime = runtime.clone();
                let authority = Arc::clone(&authority);
                let policy = policy.clone();
                let terminal_failure_handler = Arc::clone(&terminal_failure_handler);
                thread::spawn(move || loop {
                    if listener
                        .accept_one(&runtime, Arc::clone(&authority), &policy)
                        .is_err()
                    {
                        eprintln!("control-plane OpenRaft peer listener stopped");
                        terminal_failure_handler();
                        return;
                    }
                })
            })
            .collect::<Vec<_>>();
        Ok(ControlPlaneRaftPeerServerLoops { handles })
    }
}

#[cfg(feature = "test-hooks")]
impl ControlPlaneRaftPeerTestServer {
    pub fn unix(
        authority: Arc<ControlPlaneRaftAuthority>,
        endpoint_id: impl Into<String>,
        listener: UnixListener,
        max_connections: usize,
        io_timeout: Duration,
        pre_auth_byte_budget: usize,
        durability: ControlPlaneRaftPeerServerDurability,
    ) -> Result<Self, ControlPlaneRaftPeerBootstrapError> {
        let server = ControlPlaneRaftPeerServerBootstrap::for_authority(
            authority,
            vec![ControlPlaneRaftPeerServerListenerInput::Unix {
                endpoint_id: endpoint_id.into(),
                listener,
                max_connections,
                io_timeout,
            }],
            pre_auth_byte_budget,
        )?
        .ok_or(ControlPlaneRaftPeerBootstrapError::SingleNodePeerListener)?;
        let authority = Arc::clone(&server.authority);
        durability
            .validate_authority(&authority)
            .map_err(|_| ControlPlaneRaftPeerBootstrapError::MismatchedDurabilityAuthority)?;
        let mut listeners = server.listeners;
        let listener = Arc::new(
            listeners
                .pop()
                .expect("test server construction provided exactly one listener"),
        );
        Ok(Self {
            authority,
            listener,
            policy: server.policy.with_durability(durability),
        })
    }

    pub fn accept_one(&self, runtime: &Handle) -> Result<(), ControlPlaneRaftPeerTestServerError> {
        self.listener
            .accept_one(runtime, Arc::clone(&self.authority), &self.policy)
            .map_err(|_| ControlPlaneRaftPeerTestServerError)
    }

    #[must_use]
    pub fn active_workers(&self) -> usize {
        self.listener.active_workers()
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftPeerTestClient {
    pub fn with_auth_credentials(
        self,
        cluster_name: &str,
        local_node_id: ControlPlaneRaftNodeId,
        credentials: Vec<ControlPlaneRaftPeerAuthCredentialInput>,
        signing_credential: Option<(String, u64)>,
    ) -> Result<Self, ControlPlaneRaftPeerBootstrapError> {
        let peer_node_ids = credentials
            .iter()
            .map(|credential| credential.node_id)
            .collect::<HashSet<_>>();
        let policy = build_auth_policy(
            cluster_name,
            local_node_id,
            &peer_node_ids,
            credentials,
            signing_credential,
        )?;
        Ok(self.with_auth_policy(policy))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_auth::{ControlPlaneAuthOperation, ControlPlaneAuthRejectionReason};
    use crate::control_plane_raft::ControlPlaneRaftTopologyIdentity;

    struct NoopPeerServerCheckpoint;

    impl ControlPlaneRaftPeerServerCheckpoint for NoopPeerServerCheckpoint {
        fn checkpoint_before_snapshot_response(
            &self,
            _authority: &ControlPlaneRaftAuthority,
        ) -> Result<(), ControlPlaneError> {
            Ok(())
        }
    }

    fn replicated_bootstrap(
        local_node_id: ControlPlaneRaftNodeId,
        topology: ControlPlaneRaftPeerTopologyBinding,
        mut credentials: Vec<ControlPlaneRaftPeerAuthCredentialInput>,
        signing_credential: Option<(String, u64)>,
    ) -> Result<ControlPlaneRaftPeerBootstrap, ControlPlaneRaftPeerBootstrapError> {
        if credentials.is_empty() {
            credentials = vec![
                credential(1, "node-1", 1, "node-1-secret"),
                credential(2, "node-2", 1, "node-2-secret"),
            ];
        }
        ControlPlaneRaftPeerBootstrap::replicated(
            "test-cluster",
            local_node_id,
            [(1, "node-1".to_owned()), (2, "node-2".to_owned())],
            Vec::new(),
            ControlPlaneRaftPeerTransportLimits::default(),
            Duration::from_secs(1),
            Duration::from_secs(2),
            topology,
            credentials,
            signing_credential,
        )
    }

    fn credential(
        node_id: ControlPlaneRaftNodeId,
        credential_id: &str,
        credential_version: u64,
        secret: &str,
    ) -> ControlPlaneRaftPeerAuthCredentialInput {
        ControlPlaneRaftPeerAuthCredentialInput::new(
            node_id,
            credential_id,
            credential_version,
            secret.as_bytes().to_vec(),
        )
    }

    #[test]
    fn bootstrap_binds_topology_to_the_shared_transport_policy() {
        let bootstrap = replicated_bootstrap(
            1,
            ControlPlaneRaftPeerTopologyBinding::Established {
                generation: 7,
                digest: "secret-topology-digest".to_owned(),
            },
            Vec::new(),
            None,
        )
        .expect("topology-bound bootstrap should build");
        let peer = bootstrap
            .replicated
            .as_ref()
            .expect("test bootstrap should be replicated");
        assert_eq!(
            peer.policy.topology_identity(),
            Some(&ControlPlaneRaftTopologyIdentity {
                generation: 7,
                digest: "secret-topology-digest".to_owned(),
            })
        );
        assert!(!format!("{bootstrap:?}").contains("secret-topology-digest"));
    }

    #[test]
    fn bootstrap_selects_latest_local_credential_and_redacts_diagnostics() {
        let bootstrap = replicated_bootstrap(
            2,
            ControlPlaneRaftPeerTopologyBinding::Unbound,
            vec![
                credential(1, "node-1", 1, "node-1-secret"),
                credential(2, "node-2-old", 1, "node-2-old-secret"),
                credential(2, "node-2-new", 2, "node-2-new-secret"),
            ],
            None,
        )
        .expect("authenticated bootstrap should build");
        let peer = bootstrap
            .replicated
            .as_ref()
            .expect("test bootstrap should be replicated");
        let auth = peer
            .policy
            .auth_policy()
            .expect("test bootstrap should require authentication");
        auth.record_peer_frame_rejection(
            ControlPlaneAuthOperation::RaftVote,
            ControlPlaneAuthRejectionReason::WrongCluster,
        );
        auth.record_peer_frame_rejection_without_operation(
            ControlPlaneAuthRejectionReason::Malformed,
        );
        let diagnostics = bootstrap
            .auth_diagnostics()
            .expect("replicated bootstrap should expose diagnostics");
        assert!(diagnostics.contains("required=true"), "{diagnostics}");
        assert!(diagnostics.contains("local_node_id=2"), "{diagnostics}");
        assert!(
            diagnostics.contains("credential_version=2"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("rejected_total=2"), "{diagnostics}");
        assert!(
            diagnostics.contains("rejected_by_operation{operation=\"RaftVote\"} 1"),
            "{diagnostics}"
        );
        for secret in [
            "node-1",
            "node-2-old",
            "node-2-new",
            "node-1-secret",
            "node-2-old-secret",
            "node-2-new-secret",
        ] {
            assert!(!diagnostics.contains(secret), "{diagnostics}");
            assert!(!format!("{bootstrap:?}").contains(secret));
        }
    }

    #[test]
    fn bootstrap_honours_explicit_signing_credential_and_rejects_crossed_selection() {
        let credentials = vec![
            credential(1, "node-1", 1, "node-1-secret"),
            credential(2, "node-2", 1, "node-2-secret"),
        ];
        let bootstrap = replicated_bootstrap(
            1,
            ControlPlaneRaftPeerTopologyBinding::Unbound,
            credentials.clone(),
            Some(("node-1".to_owned(), 1)),
        )
        .expect("explicit local signing credential should build");
        assert!(bootstrap
            .auth_diagnostics()
            .expect("replicated diagnostics should exist")
            .contains("credential_version=1"));
        assert_eq!(
            replicated_bootstrap(
                1,
                ControlPlaneRaftPeerTopologyBinding::Unbound,
                credentials,
                Some(("node-2".to_owned(), 1)),
            )
            .expect_err("another peer's credential must not sign local frames"),
            ControlPlaneRaftPeerBootstrapError::InvalidAuthentication
        );
    }

    #[test]
    fn bootstrap_requires_exact_peer_principal_credential_coverage() {
        for credentials in [
            Vec::new(),
            vec![credential(1, "node-1", 1, "node-1-secret")],
            vec![
                credential(1, "node-1", 1, "node-1-secret"),
                credential(2, "node-2", 1, "node-2-secret"),
                credential(3, "node-3", 1, "node-3-secret"),
            ],
        ] {
            let error = ControlPlaneRaftPeerBootstrap::replicated(
                "test-cluster",
                1,
                [(1, "node-1".to_owned()), (2, "node-2".to_owned())],
                Vec::new(),
                ControlPlaneRaftPeerTransportLimits::default(),
                Duration::from_secs(1),
                Duration::from_secs(1),
                ControlPlaneRaftPeerTopologyBinding::Unbound,
                credentials,
                None,
            )
            .expect_err("incomplete or extraneous peer credentials must fail closed");
            assert_eq!(
                error,
                ControlPlaneRaftPeerBootstrapError::InvalidAuthentication
            );
        }

        replicated_bootstrap(
            1,
            ControlPlaneRaftPeerTopologyBinding::Unbound,
            vec![
                credential(1, "node-1", 1, "node-1-secret"),
                credential(2, "node-2-old", 1, "node-2-old-secret"),
                credential(2, "node-2-new", 2, "node-2-new-secret"),
            ],
            None,
        )
        .expect("multiple credential versions still cover the exact peer principal set");
    }

    #[test]
    fn bootstrap_rejects_client_routes_that_do_not_match_peer_policy() {
        let error = ControlPlaneRaftPeerBootstrap::replicated(
            "test-cluster",
            1,
            [(1, "node-1".to_owned()), (2, "node-2".to_owned())],
            vec![(
                1,
                ControlPlaneRaftPeerClientEndpoint::unix("different-node-1"),
            )],
            ControlPlaneRaftPeerTransportLimits::default(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            ControlPlaneRaftPeerTopologyBinding::Unbound,
            vec![
                credential(1, "node-1", 1, "node-1-secret"),
                credential(2, "node-2", 1, "node-2-secret"),
            ],
            None,
        )
        .expect_err("incomplete client routes must fail closed");
        assert_eq!(
            error,
            ControlPlaneRaftPeerBootstrapError::InvalidClientEndpoints
        );
    }

    #[test]
    fn bootstrap_rejects_duplicate_peer_nodes_and_advertised_endpoints() {
        for peer_endpoints in [
            vec![(1, "node-1".to_owned()), (1, "node-1-other".to_owned())],
            vec![(1, "shared".to_owned()), (2, "shared".to_owned())],
        ] {
            assert_eq!(
                ControlPlaneRaftPeerBootstrap::replicated(
                    "test-cluster",
                    1,
                    peer_endpoints,
                    Vec::new(),
                    ControlPlaneRaftPeerTransportLimits::default(),
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    ControlPlaneRaftPeerTopologyBinding::Unbound,
                    vec![
                        credential(1, "node-1", 1, "node-1-secret"),
                        credential(2, "node-2", 1, "node-2-secret"),
                    ],
                    None,
                )
                .unwrap_err(),
                ControlPlaneRaftPeerBootstrapError::InvalidPeerConfiguration
            );
        }
    }

    #[test]
    fn membership_and_listener_admission_follow_authority_configuration() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let standalone = ControlPlaneRaftPeerBootstrap::single_node("test-cluster", 1);
            assert!(standalone.startup_requires_local_leader());
            let standalone_authority = Arc::new(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                    "test-cluster",
                    1,
                )
                .await
                .unwrap(),
            );
            assert!(standalone_authority
                .initialize_configured_membership_if_needed()
                .await
                .unwrap());
            assert!(ControlPlaneRaftPeerServerBootstrap::for_authority(
                standalone_authority,
                Vec::new(),
                1024,
            )
            .unwrap()
            .is_none());

            let first = replicated_bootstrap(
                1,
                ControlPlaneRaftPeerTopologyBinding::Unbound,
                Vec::new(),
                None,
            )
            .expect("first-peer bootstrap should build");
            let follower = replicated_bootstrap(
                2,
                ControlPlaneRaftPeerTopologyBinding::Unbound,
                Vec::new(),
                None,
            )
            .expect("follower bootstrap should build");
            assert!(!first.startup_requires_local_leader());
            let tmp = test_util::tempdir();
            let follower_authority = Arc::new(
                follower
                    .open_durable_authority(&tmp.path().join("follower.state"), true)
                    .await
                    .unwrap(),
            );
            assert!(!follower_authority
                .initialize_configured_membership_if_needed()
                .await
                .unwrap());
            assert_eq!(
                ControlPlaneRaftPeerServerBootstrap::for_authority(
                    follower_authority,
                    Vec::new(),
                    1024,
                )
                .unwrap_err(),
                ControlPlaneRaftPeerBootstrapError::MissingPeerListener
            );
        });
    }

    #[test]
    fn peer_server_rejects_durability_from_another_authority() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let tmp = test_util::tempdir();
            let bootstrap = replicated_bootstrap(
                1,
                ControlPlaneRaftPeerTopologyBinding::Unbound,
                Vec::new(),
                None,
            )
            .unwrap();
            let authority_a = Arc::new(
                bootstrap
                    .open_durable_authority(&tmp.path().join("authority-a.state"), true)
                    .await
                    .unwrap(),
            );
            let authority_b = Arc::new(
                bootstrap
                    .open_durable_authority(&tmp.path().join("authority-b.state"), true)
                    .await
                    .unwrap(),
            );
            let durability_b = authority_b
                .bind_peer_server_durability(Arc::new(NoopPeerServerCheckpoint))
                .unwrap();
            let listener = UnixListener::bind(tmp.path().join("peer.sock")).unwrap();
            let server = ControlPlaneRaftPeerServerBootstrap::for_authority(
                Arc::clone(&authority_a),
                vec![ControlPlaneRaftPeerServerListenerInput::Unix {
                    endpoint_id: "peer".to_owned(),
                    listener,
                    max_connections: 1,
                    io_timeout: Duration::from_secs(1),
                }],
                1024,
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                server
                    .serve(runtime.handle().clone(), durability_b, Arc::new(|| {}))
                    .unwrap_err(),
                ControlPlaneRaftPeerBootstrapError::MismatchedDurabilityAuthority
            );
            authority_a.shutdown().await.unwrap();
            authority_b.shutdown().await.unwrap();
        });
    }

    #[test]
    fn bootstrap_rejects_cloned_unix_peer_listeners() {
        let tmp = test_util::tempdir();
        let first = UnixListener::bind(tmp.path().join("peer.sock")).unwrap();
        let second = first.try_clone().unwrap();
        let listeners = [first, second]
            .into_iter()
            .enumerate()
            .map(
                |(index, listener)| ControlPlaneRaftPeerServerListenerInput::Unix {
                    endpoint_id: format!("peer-{index}"),
                    listener,
                    max_connections: 1,
                    io_timeout: Duration::from_secs(1),
                },
            )
            .collect::<Vec<_>>();
        assert_eq!(
            validate_listener_identities(&listeners).unwrap_err(),
            ControlPlaneRaftPeerBootstrapError::AliasedPeerListener {
                first_index: 0,
                second_index: 1,
            }
        );
    }

    #[test]
    fn bootstrap_rejects_duplicate_peer_listener_ids() {
        let tmp = test_util::tempdir();
        let first = UnixListener::bind(tmp.path().join("peer-1.sock")).unwrap();
        let second = UnixListener::bind(tmp.path().join("peer-2.sock")).unwrap();
        let listeners = [first, second]
            .into_iter()
            .map(|listener| ControlPlaneRaftPeerServerListenerInput::Unix {
                endpoint_id: "duplicate-peer".to_owned(),
                listener,
                max_connections: 1,
                io_timeout: Duration::from_secs(1),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            validate_listener_identities(&listeners).unwrap_err(),
            ControlPlaneRaftPeerBootstrapError::DuplicatePeerListenerId {
                first_index: 0,
                second_index: 1,
            }
        );
    }

    #[test]
    fn bootstrap_rejects_cloned_tcp_peer_listeners() {
        use rustls::pki_types::pem::PemObject as _;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};

        let certificates = CertificateDer::pem_slice_iter(include_bytes!(
            "../../s3-tests/testdata/localhost-cert.pem"
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
            "../../s3-tests/testdata/localhost-key.pem"
        ))
        .unwrap();
        let certified_key = Arc::new(
            CertifiedKey::from_der(
                certificates,
                private_key,
                &rustls::crypto::ring::default_provider(),
            )
            .unwrap(),
        );
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = first.try_clone().unwrap();
        let listeners = [first, second]
            .into_iter()
            .enumerate()
            .map(
                |(index, listener)| ControlPlaneRaftPeerServerListenerInput::TlsTcp {
                    endpoint_id: format!("peer-{index}"),
                    listener,
                    certified_key: Arc::clone(&certified_key),
                    max_connections: 1,
                    io_timeout: Duration::from_secs(1),
                },
            )
            .collect::<Vec<_>>();
        assert_eq!(
            validate_listener_identities(&listeners).unwrap_err(),
            ControlPlaneRaftPeerBootstrapError::AliasedPeerListener {
                first_index: 0,
                second_index: 1,
            }
        );
    }
}
