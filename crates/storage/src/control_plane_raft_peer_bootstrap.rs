// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

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
    ControlPlaneRaftAuthority, ControlPlaneRaftLogId, ControlPlaneRaftNodeId,
    ControlPlaneRaftPeerAuthPolicy, ControlPlaneRaftPeerClientEndpoint,
    ControlPlaneRaftPeerNetworkConfig, ControlPlaneRaftPeerServerDurability,
    ControlPlaneRaftPeerServerListener, ControlPlaneRaftPeerServerPolicy,
    ControlPlaneRaftPeerTransportLimits, ControlPlaneRaftPeerTransportPolicy,
};
use crate::control_plane_raft_durability::{
    ControlPlaneRaftCheckpointMonitor, ControlPlaneRaftOuterIdentityPublisher,
};
use crate::control_plane_raft_host::{
    ControlPlaneRaftAuthorityHost, PreparedControlPlaneRaftAuthorityClock,
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

/// Deployment-owned state of the outer static-cluster identity.
///
/// Storage uses this logical state to select durable-authority recovery and
/// to decide whether initial topology and outer identity publication are
/// required. The process never supplies the corresponding checkpoint or
/// membership decisions.
pub enum ControlPlaneRaftOuterIdentityStartup<'a> {
    NotConfigured,
    Established,
    Publish(&'a dyn ControlPlaneRaftOuterIdentityPublisher),
}

impl fmt::Debug for ControlPlaneRaftOuterIdentityStartup<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotConfigured => "NotConfigured",
            Self::Established => "Established",
            Self::Publish(_) => "Publish(<opaque>)",
        })
    }
}

impl ControlPlaneRaftOuterIdentityStartup<'_> {
    fn is_configured(&self) -> bool {
        !matches!(self, Self::NotConfigured)
    }

    fn is_established(&self) -> bool {
        matches!(self, Self::Established)
    }
}

/// A durable authority whose replay and validation have completed, but whose
/// inbound peer listeners have not yet been published.
///
/// This typestate lets the deployment bind sockets after durable replay while
/// preventing membership, checkpoint, topology, or host composition before
/// storage starts the prepared authority.
pub struct PreparedControlPlaneRaftAuthority<'a> {
    bootstrap: ControlPlaneRaftPeerBootstrap,
    authority: Arc<ControlPlaneRaftAuthority>,
    durability: crate::control_plane_raft_durability::ControlPlaneRaftAuthorityDurability,
    authority_clock: PreparedControlPlaneRaftAuthorityClock,
    outer_identity: ControlPlaneRaftOuterIdentityStartup<'a>,
}

impl fmt::Debug for PreparedControlPlaneRaftAuthority<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedControlPlaneRaftAuthority")
            .field("bootstrap", &self.bootstrap)
            .field("authority", &"<opaque>")
            .field("durability", &self.durability)
            .field("authority_clock", &"<classified>")
            .field("outer_identity", &self.outer_identity)
            .finish()
    }
}

/// One fully opened durable Raft authority service.
///
/// The host, checkpoint monitor, and peer listener loops are issued from one
/// authority and retained together. Callers receive only the logical host and
/// cannot replace any member of its durability/publication lifecycle.
#[must_use = "dropping the service detaches its authority worker threads"]
pub struct ControlPlaneRaftAuthorityService {
    host: ControlPlaneRaftAuthorityHost,
    _checkpoint_monitor: ControlPlaneRaftCheckpointMonitor,
    _peer_server_loops: Option<ControlPlaneRaftPeerServerLoops>,
    multi_node: bool,
}

struct AuthorityStartupFailureGuard {
    authority: Arc<ControlPlaneRaftAuthority>,
    armed: bool,
}

impl AuthorityStartupFailureGuard {
    fn new(authority: Arc<ControlPlaneRaftAuthority>) -> Self {
        Self {
            authority,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AuthorityStartupFailureGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(publication) = self.authority.durability_publication() {
                publication.poison("durable Raft authority startup did not complete");
            }
        }
    }
}

impl fmt::Debug for ControlPlaneRaftAuthorityService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftAuthorityService")
            .field("host", &self.host)
            .field("multi_node", &self.multi_node)
            .finish_non_exhaustive()
    }
}

impl ControlPlaneRaftAuthorityService {
    #[must_use]
    pub fn host(&self) -> &ControlPlaneRaftAuthorityHost {
        &self.host
    }

    pub fn host_mut(&mut self) -> &mut ControlPlaneRaftAuthorityHost {
        &mut self.host
    }

    #[must_use]
    pub fn is_multi_node(&self) -> bool {
        self.multi_node
    }
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

    pub(crate) async fn open_durable_authority(
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

    #[cfg(any(test, feature = "test-hooks"))]
    pub async fn open_durable_authority_for_test(
        &self,
        artifact_path: &Path,
        static_identity_established: bool,
    ) -> Result<ControlPlaneRaftAuthority, ControlPlaneError> {
        self.open_durable_authority(artifact_path, static_identity_established)
            .await
    }

    /// Classify the authority-clock checkpoint, then open and validate durable
    /// state before the deployment publishes any inbound peer listener.
    ///
    /// The returned typestate owns the exact authority and durability
    /// lifecycle needed to complete startup after process-owned socket
    /// binding.
    pub async fn prepare_durable_authority<'a>(
        &self,
        runtime: Handle,
        artifact_path: &Path,
        outer_identity: ControlPlaneRaftOuterIdentityStartup<'a>,
    ) -> Result<PreparedControlPlaneRaftAuthority<'a>, ControlPlaneError> {
        if outer_identity.is_configured()
            && self
                .replicated
                .as_ref()
                .and_then(|peer| peer.static_initial_topology.as_ref())
                .is_none()
        {
            return Err(ControlPlaneError::static_topology_failure(
                "static outer identity requires a certified initial topology",
            ));
        }

        // This must precede opening the authority: opening may replay or
        // publish durable Raft state, while an unsupported checkpoint format
        // is a hard startup incompatibility.
        let authority_clock = PreparedControlPlaneRaftAuthorityClock::load(
            artifact_path,
            &self.cluster_name,
            self.local_node_id,
        )?;
        let authority = Arc::new(
            self.open_durable_authority(artifact_path, outer_identity.is_established())
                .await?,
        );
        let durability = authority.durability_lifecycle(runtime)?;
        Ok(PreparedControlPlaneRaftAuthority {
            bootstrap: self.clone(),
            authority,
            durability,
            authority_clock,
            outer_identity,
        })
    }

    #[must_use]
    pub fn auth_diagnostics(&self) -> Option<String> {
        let peer = self.replicated.as_ref()?;
        Some(format_auth_diagnostics(&peer.policy))
    }
}

impl PreparedControlPlaneRaftAuthority<'_> {
    /// Publish peer listeners and complete startup as one storage-owned
    /// lifecycle.
    ///
    /// Storage owns authority-clock host initialization, checkpoint
    /// monitoring, membership initialization, startup convergence, certified
    /// topology establishment, and initial durability publication in that
    /// order.
    pub async fn start(
        self,
        listener_inputs: Vec<ControlPlaneRaftPeerServerListenerInput>,
        pre_auth_byte_budget: usize,
        startup_timeout: Duration,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<ControlPlaneRaftAuthorityService, ControlPlaneError> {
        let Self {
            bootstrap,
            authority,
            durability,
            authority_clock,
            outer_identity,
        } = self;
        let runtime = durability.runtime();
        let mut startup_failure_guard = AuthorityStartupFailureGuard::new(Arc::clone(&authority));
        let prepared_host = ControlPlaneRaftAuthorityHost::start_prepared(
            runtime.clone(),
            Arc::clone(&authority),
            authority_clock,
        )?;
        let peer_server_durability = durability.peer_server_durability()?;
        let peer_server = ControlPlaneRaftPeerServerBootstrap::for_authority(
            Arc::clone(&authority),
            listener_inputs,
            pre_auth_byte_budget,
        )
        .map_err(control_plane_peer_startup_error)?;

        // Monitoring starts before the peer endpoint is published, so every
        // remotely applied command is covered by the durability lifecycle.
        let checkpoint_monitor =
            durability.spawn_checkpoint_monitor(Arc::clone(&terminal_failure_handler))?;
        let peer_server_loops = peer_server
            .map(|server| {
                server.serve(
                    runtime.clone(),
                    peer_server_durability,
                    terminal_failure_handler,
                )
            })
            .transpose()
            .map_err(control_plane_peer_startup_error)?;

        let initialized_membership = authority
            .initialize_configured_membership_if_needed()
            .await?;
        if initialized_membership {
            durability.store_restart_artifact()?;
        }

        if bootstrap.startup_requires_local_leader() || initialized_membership {
            authority
                .wait_for_current_leader(
                    bootstrap.local_node_id,
                    startup_timeout,
                    "control-plane initial-membership startup leadership",
                )
                .await?;
            wait_for_local_authority_serving(
                &authority,
                startup_timeout,
                "control-plane initial-membership startup",
            )
            .await?;
        } else if !outer_identity.is_configured() {
            wait_for_startup_catch_up(
                &authority,
                startup_timeout,
                "control-plane startup committed replay",
            )
            .await?;
        }

        if outer_identity.is_configured() {
            let topology = bootstrap
                .replicated
                .as_ref()
                .and_then(|peer| peer.static_initial_topology.as_ref())
                .ok_or_else(|| {
                    ControlPlaneError::static_topology_failure(
                        "static outer identity requires a certified initial topology",
                    )
                })?;
            authority
                .establish_static_initial_topology(topology, !outer_identity.is_established())
                .await?;
        }

        match outer_identity {
            ControlPlaneRaftOuterIdentityStartup::Publish(publisher) => {
                durability.establish_static_outer_identity(publisher)?;
            }
            ControlPlaneRaftOuterIdentityStartup::NotConfigured
            | ControlPlaneRaftOuterIdentityStartup::Established => {
                durability.store_restart_artifact()?;
            }
        }

        let host = prepared_host.finish_startup()?;
        startup_failure_guard.disarm();
        Ok(ControlPlaneRaftAuthorityService {
            host,
            _checkpoint_monitor: checkpoint_monitor,
            _peer_server_loops: peer_server_loops,
            multi_node: bootstrap.is_multi_node(),
        })
    }
}

fn control_plane_peer_startup_error(
    error: ControlPlaneRaftPeerBootstrapError,
) -> ControlPlaneError {
    ControlPlaneError::invariant_failure(format!(
        "control-plane Raft peer startup configuration failed: {error}"
    ))
}

async fn wait_for_local_authority_serving(
    authority: &ControlPlaneRaftAuthority,
    timeout: Duration,
    message: &'static str,
) -> Result<(), ControlPlaneError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if authority.status().await?.linearized_authority_serving() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ControlPlaneError::startup_timeout(format!(
                "local OpenRaft authority did not become serving within {timeout:?}: {message}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_startup_catch_up(
    authority: &ControlPlaneRaftAuthority,
    timeout: Duration,
    message: &'static str,
) -> Result<(), ControlPlaneError> {
    wait_for_startup_catch_up_from(authority, timeout, message).await
}

trait StartupCatchUpSource {
    type Position: Copy + Eq;

    async fn committed_and_applied(
        &self,
    ) -> Result<(Option<Self::Position>, Option<Self::Position>), ControlPlaneError>;

    async fn wait_for_applied(
        &self,
        position: Self::Position,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError>;
}

impl StartupCatchUpSource for ControlPlaneRaftAuthority {
    type Position = ControlPlaneRaftLogId;

    async fn committed_and_applied(
        &self,
    ) -> Result<(Option<Self::Position>, Option<Self::Position>), ControlPlaneError> {
        let status = self.status().await?;
        Ok((status.committed(), status.applied()))
    }

    async fn wait_for_applied(
        &self,
        position: Self::Position,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.wait_for_applied_log_id(position, timeout, message)
            .await
    }
}

async fn wait_for_startup_catch_up_from<S>(
    source: &S,
    timeout: Duration,
    message: &'static str,
) -> Result<(), ControlPlaneError>
where
    S: StartupCatchUpSource,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let (committed, applied) = source.committed_and_applied().await?;
        let Some(committed) = committed else {
            return Ok(());
        };
        if applied == Some(committed) {
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(ControlPlaneError::startup_timeout(format!(
                "OpenRaft startup did not apply through committed state within {timeout:?}: {message}"
            )));
        }
        source
            .wait_for_applied(committed, deadline.saturating_duration_since(now), message)
            .await?;
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

pub(crate) struct ControlPlaneRaftPeerServerBootstrap {
    authority: Arc<ControlPlaneRaftAuthority>,
    listeners: Vec<ControlPlaneRaftPeerServerListener>,
    policy: ControlPlaneRaftPeerServerPolicy,
}

#[must_use = "dropping the handles detaches the Raft peer server loops"]
pub(crate) struct ControlPlaneRaftPeerServerLoops {
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
    pub(crate) fn for_authority(
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

    pub(crate) fn serve(
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
    use crate::control_plane::ControlPlaneRuntimeMapSource;
    use crate::control_plane_auth::{ControlPlaneAuthOperation, ControlPlaneAuthRejectionReason};
    use crate::control_plane_raft::ControlPlaneRaftTopologyIdentity;
    use crate::{
        derive_static_initial_control_plane_topology, derive_static_initial_pg_placement,
        StaticStorageFailureDomain, StaticStorageNodeEndpoint, StaticStoragePlacementNode,
        StaticStoragePlacementParameters,
    };
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    struct NoopPeerServerCheckpoint;

    impl ControlPlaneRaftPeerServerCheckpoint for NoopPeerServerCheckpoint {
        fn checkpoint_before_snapshot_response(
            &self,
            _authority: &ControlPlaneRaftAuthority,
        ) -> Result<(), ControlPlaneError> {
            Ok(())
        }
    }

    #[test]
    fn startup_catch_up_rechecks_an_advanced_committed_watermark() {
        struct ScriptedCatchUpSource {
            statuses: Mutex<VecDeque<(Option<u64>, Option<u64>)>>,
            waited_for: Mutex<Vec<u64>>,
        }

        impl StartupCatchUpSource for ScriptedCatchUpSource {
            type Position = u64;

            async fn committed_and_applied(
                &self,
            ) -> Result<(Option<Self::Position>, Option<Self::Position>), ControlPlaneError>
            {
                Ok(self
                    .statuses
                    .lock()
                    .expect("scripted status mutex should not be poisoned")
                    .pop_front()
                    .expect("catch-up loop requested an unexpected status"))
            }

            async fn wait_for_applied(
                &self,
                position: Self::Position,
                _timeout: Duration,
                _message: &'static str,
            ) -> Result<(), ControlPlaneError> {
                self.waited_for
                    .lock()
                    .expect("scripted wait mutex should not be poisoned")
                    .push(position);
                Ok(())
            }
        }

        let source = ScriptedCatchUpSource {
            statuses: Mutex::new(VecDeque::from([
                (Some(1), None),
                (Some(2), Some(1)),
                (Some(2), Some(2)),
            ])),
            waited_for: Mutex::new(Vec::new()),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("catch-up test runtime should build");

        runtime
            .block_on(wait_for_startup_catch_up_from(
                &source,
                Duration::from_secs(1),
                "scripted advancing committed watermark",
            ))
            .expect("catch-up should follow the advanced committed watermark");

        assert_eq!(
            *source
                .waited_for
                .lock()
                .expect("scripted wait mutex should not be poisoned"),
            vec![1, 2]
        );
        assert!(source
            .statuses
            .lock()
            .expect("scripted status mutex should not be poisoned")
            .is_empty());
    }

    #[test]
    fn single_node_startup_returns_only_after_membership_and_checkpoint_publication() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("startup test runtime should build");
        let bootstrap = ControlPlaneRaftPeerBootstrap::single_node("startup-owner", 1);
        let prepared = runtime
            .block_on(bootstrap.prepare_durable_authority(
                runtime.handle().clone(),
                &artifact_path,
                ControlPlaneRaftOuterIdentityStartup::NotConfigured,
            ))
            .expect("durable replay should prepare the authority");
        let authority = Arc::clone(&prepared.authority);

        let service = runtime
            .block_on(prepared.start(Vec::new(), 1024, Duration::from_secs(1), Arc::new(|| {})))
            .expect("storage-owned startup should complete");

        assert!(artifact_path.is_file());
        let status = runtime
            .block_on(authority.status())
            .expect("started authority status should load");
        assert!(status.linearized_authority_serving());
        assert!(runtime
            .block_on(authority.is_initialized())
            .expect("started authority membership should inspect"));
        service
            .host()
            .linearized_authority_serving()
            .expect("returned host should share the serving authority");
        ControlPlaneRuntimeMapSource::runtime_map_snapshot(service.host(), 0)
            .expect("fresh startup should bind its first leadership term on clock-backed access");

        authority
            .durability_publication()
            .expect("startup should retain one publication domain")
            .poison("stop storage-owned startup test");
        service
            ._checkpoint_monitor
            .join_for_test()
            .expect("checkpoint monitor should stop after poison");
        runtime
            .block_on(authority.shutdown())
            .expect("started authority should shut down");
    }

    #[test]
    fn restarted_single_node_binds_converged_term_before_clock_backed_access() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("restart test runtime should build");
        let bootstrap = ControlPlaneRaftPeerBootstrap::single_node("restart-owner", 1);

        let first_prepared = runtime
            .block_on(bootstrap.prepare_durable_authority(
                runtime.handle().clone(),
                &artifact_path,
                ControlPlaneRaftOuterIdentityStartup::NotConfigured,
            ))
            .expect("fresh authority should prepare");
        let first_authority = Arc::clone(&first_prepared.authority);
        let first_service = runtime
            .block_on(first_prepared.start(
                Vec::new(),
                1024,
                Duration::from_secs(1),
                Arc::new(|| {}),
            ))
            .expect("fresh authority should start");
        ControlPlaneRuntimeMapSource::runtime_map_snapshot(first_service.host(), 0)
            .expect("fresh authority clock-backed read should succeed");

        let mut checkpoint_path = artifact_path.as_os_str().to_os_string();
        checkpoint_path.push(".clock");
        assert!(
            PathBuf::from(checkpoint_path).is_file(),
            "fresh startup must persist a valid clock sidecar for restart"
        );
        first_authority
            .durability_publication()
            .expect("fresh authority should retain its publication domain")
            .poison("stop first restart-test authority");
        let ControlPlaneRaftAuthorityService {
            host: first_host,
            _checkpoint_monitor: first_checkpoint_monitor,
            _peer_server_loops: first_peer_server_loops,
            ..
        } = first_service;
        first_checkpoint_monitor
            .join_for_test()
            .expect("first checkpoint monitor should stop after poison");
        runtime
            .block_on(first_authority.shutdown())
            .expect("first authority should shut down");
        drop(first_host);
        drop(first_peer_server_loops);
        drop(first_authority);

        let restarted_prepared = runtime
            .block_on(bootstrap.prepare_durable_authority(
                runtime.handle().clone(),
                &artifact_path,
                ControlPlaneRaftOuterIdentityStartup::NotConfigured,
            ))
            .expect("valid restart artifact and clock sidecar should prepare");
        let restarted_authority = Arc::clone(&restarted_prepared.authority);
        assert!(runtime
            .block_on(restarted_authority.is_initialized())
            .expect("restored membership should inspect"));

        let restarted_service = runtime
            .block_on(restarted_prepared.start(
                Vec::new(),
                1024,
                Duration::from_secs(1),
                Arc::new(|| {}),
            ))
            .expect("restored authority should start");
        ControlPlaneRuntimeMapSource::runtime_map_snapshot(restarted_service.host(), 0)
            .expect("restart must bind its converged term before clock-backed access");

        restarted_authority
            .durability_publication()
            .expect("restored authority should retain its publication domain")
            .poison("stop second restart-test authority");
        restarted_service
            ._checkpoint_monitor
            .join_for_test()
            .expect("second checkpoint monitor should stop after poison");
        runtime
            .block_on(restarted_authority.shutdown())
            .expect("restored authority should shut down");
    }

    #[test]
    fn static_identity_is_rejected_before_open_without_a_certified_topology() {
        struct UnexpectedPublisher;

        impl ControlPlaneRaftOuterIdentityPublisher for UnexpectedPublisher {
            fn publish(
                &self,
                _authority_artifact_path: &Path,
            ) -> Result<
                (),
                crate::control_plane_raft_durability::ControlPlaneRaftOuterIdentityPublicationError,
            > {
                panic!("invalid static startup must not publish an outer identity")
            }
        }

        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("startup test runtime should build");
        let bootstrap = ControlPlaneRaftPeerBootstrap::single_node("invalid-static-startup", 1);
        let error = runtime
            .block_on(bootstrap.prepare_durable_authority(
                runtime.handle().clone(),
                &artifact_path,
                ControlPlaneRaftOuterIdentityStartup::Publish(&UnexpectedPublisher),
            ))
            .expect_err("static identity without certified topology must fail closed");

        assert!(matches!(
            error,
            ControlPlaneError::StaticTopologyFailure { .. }
        ));
        assert!(!artifact_path.exists());
    }

    #[test]
    fn prepared_startup_rejects_hard_clock_formats_before_open_or_outer_identity_publication() {
        struct RecordingPublisher(AtomicBool);

        impl ControlPlaneRaftOuterIdentityPublisher for RecordingPublisher {
            fn publish(
                &self,
                _authority_artifact_path: &Path,
            ) -> Result<
                (),
                crate::control_plane_raft_durability::ControlPlaneRaftOuterIdentityPublicationError,
            > {
                self.0.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("startup test runtime should build");

        // Seed a current restart artifact and matching clock sidecar. The
        // tested startup below uses the complete prepared-authority typestate,
        // rather than opening an authority and invoking the host directly.
        let seed_authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        "test-cluster",
                        1,
                        &artifact_path,
                    ),
                )
                .expect("seed authority should initialize"),
        );
        let seed_durability = seed_authority
            .durability_lifecycle(runtime.handle().clone())
            .expect("seed authority should issue durability");
        seed_durability
            .store_restart_artifact()
            .expect("seed restart artifacts should persist");
        runtime
            .block_on(seed_authority.shutdown())
            .expect("seed authority should shut down");
        drop(seed_durability);
        drop(seed_authority);

        let artifact_before = std::fs::read(&artifact_path).unwrap();
        let mut checkpoint_path = artifact_path.as_os_str().to_os_string();
        checkpoint_path.push(".clock");
        let checkpoint_path = PathBuf::from(checkpoint_path);
        let current_checkpoint = std::fs::read(&checkpoint_path).unwrap();

        let placement = derive_static_initial_pg_placement(
            1,
            StaticStoragePlacementParameters::new(1, 0, StaticStorageFailureDomain::None, 0),
            &["host-a".to_owned(), "host-b".to_owned()],
            &["disk-a".to_owned(), "disk-b".to_owned()],
            &[
                StaticStoragePlacementNode::new(1, "host-a", "disk-a"),
                StaticStoragePlacementNode::new(2, "host-b", "disk-b"),
            ],
        )
        .expect("static placement should derive");
        let topology = derive_static_initial_control_plane_topology(
            1,
            &"aa".repeat(32),
            &[1, 2],
            &[
                StaticStorageNodeEndpoint::new(1, "/tmp/node-1.sock"),
                StaticStorageNodeEndpoint::new(2, "/tmp/node-2.sock"),
            ],
            placement,
        )
        .expect("static topology should derive");
        let bootstrap = replicated_bootstrap(
            1,
            ControlPlaneRaftPeerTopologyBinding::StaticInitial(topology),
            Vec::new(),
            None,
        )
        .expect("static peer bootstrap should build");
        let publisher = RecordingPublisher(AtomicBool::new(false));

        let mut hard_failures = Vec::new();
        let mut bad_magic = current_checkpoint.clone();
        bad_magic[0] ^= 0xff;
        reseal_crc64_suffix_for_test(&mut bad_magic);
        hard_failures.push((bad_magic, "checkpoint magic mismatch".to_owned()));
        for version in [1u16, 3u16] {
            let mut unsupported = current_checkpoint.clone();
            unsupported[8..10].copy_from_slice(&version.to_be_bytes());
            reseal_crc64_suffix_for_test(&mut unsupported);
            hard_failures.push((
                unsupported,
                format!("unsupported checkpoint version {version}"),
            ));
        }

        for (bytes, expected_message) in hard_failures {
            std::fs::write(&checkpoint_path, &bytes).unwrap();
            let error = runtime
                .block_on(bootstrap.prepare_durable_authority(
                    runtime.handle().clone(),
                    &artifact_path,
                    ControlPlaneRaftOuterIdentityStartup::Publish(&publisher),
                ))
                .expect_err("hard checkpoint formats must prevent prepared startup");
            assert!(matches!(
                error,
                ControlPlaneError::AuthorityClockCheckpoint { message }
                    if message == expected_message
            ));
            assert!(!publisher.0.load(Ordering::SeqCst));
            assert_eq!(std::fs::read(&artifact_path).unwrap(), artifact_before);
            assert_eq!(std::fs::read(&checkpoint_path).unwrap(), bytes);
        }
    }

    #[test]
    fn failed_start_poisons_the_prepared_authority_before_returning() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let peer_socket = tmp.path().join("unexpected-peer.sock");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("startup test runtime should build");
        let bootstrap = ControlPlaneRaftPeerBootstrap::single_node("failed-start-owner", 1);
        let prepared = runtime
            .block_on(bootstrap.prepare_durable_authority(
                runtime.handle().clone(),
                &artifact_path,
                ControlPlaneRaftOuterIdentityStartup::NotConfigured,
            ))
            .expect("durable replay should prepare the authority");
        let authority = Arc::clone(&prepared.authority);
        let listeners = vec![ControlPlaneRaftPeerServerListenerInput::Unix {
            endpoint_id: "unexpected-peer".to_owned(),
            listener: UnixListener::bind(peer_socket).expect("test peer listener should bind"),
            max_connections: 1,
            io_timeout: Duration::from_secs(1),
        }];

        let error = runtime
            .block_on(prepared.start(listeners, 1024, Duration::from_secs(1), Arc::new(|| {})))
            .expect_err("single-node startup must reject a peer listener");

        assert!(matches!(error, ControlPlaneError::InvariantFailure { .. }));
        assert!(authority
            .durability_publication()
            .expect("prepared authority should retain its publication domain")
            .is_poisoned());
        runtime
            .block_on(authority.shutdown())
            .expect("failed authority should shut down");
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

    fn reseal_crc64_suffix_for_test(bytes: &mut [u8]) {
        let checksum_offset = bytes.len() - std::mem::size_of::<u64>();
        let checksum = checksum::crc64::checksum(&bytes[..checksum_offset]);
        bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
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
            CertifiedKey::from_der(certificates, private_key, &tls_provider::build_provider())
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
