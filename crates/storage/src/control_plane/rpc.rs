#[derive(Clone)]
pub struct UnixControlPlaneClient {
    endpoints: Arc<[ControlPlaneRpcClientEndpoint]>,
    socket_paths: Arc<[PathBuf]>,
    preferred_endpoint_index: Arc<AtomicUsize>,
}

/// A configured endpoint for the logical control-plane RPC client.
///
/// Framing, protocol limits, TLS profile construction, ALPN, deadlines, and
/// request-publication tracking remain owned by `storage`.
#[derive(Clone)]
pub struct ControlPlaneRpcClientEndpoint(ControlPlaneRpcClientEndpointKind);

#[derive(Clone)]
enum ControlPlaneRpcClientEndpointKind {
    Unix {
        socket_path: PathBuf,
    },
    TlsTcp {
        advertised_endpoint: String,
        host: String,
        port: u16,
        server_name: String,
        connect_timeout: Duration,
        tls_client_config: Arc<rustls::ClientConfig>,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneRpcClientEndpointError {
    #[error("control-plane TLS/TCP advertised endpoint must not be empty")]
    EmptyAdvertisedEndpoint,
    #[error("control-plane TLS/TCP host must not be empty")]
    EmptyHost,
    #[error("control-plane TLS/TCP endpoint has an invalid TLS server name")]
    InvalidServerName,
    #[error("failed to construct the control-plane TLS client profile")]
    TlsProfileUnavailable,
}

impl ControlPlaneRpcClientEndpoint {
    #[must_use]
    pub fn unix(socket_path: impl Into<PathBuf>) -> Self {
        Self(ControlPlaneRpcClientEndpointKind::Unix {
            socket_path: socket_path.into(),
        })
    }

    pub fn tls_tcp(
        advertised_endpoint: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        server_name: impl Into<String>,
        connect_timeout: Duration,
        trust_roots: rustls::RootCertStore,
    ) -> Result<Self, ControlPlaneRpcClientEndpointError> {
        let advertised_endpoint = advertised_endpoint.into();
        if advertised_endpoint.is_empty() {
            return Err(ControlPlaneRpcClientEndpointError::EmptyAdvertisedEndpoint);
        }
        let host = host.into();
        if host.is_empty() {
            return Err(ControlPlaneRpcClientEndpointError::EmptyHost);
        }
        let server_name = server_name.into();
        ServerName::try_from(server_name.clone())
            .map_err(|_| ControlPlaneRpcClientEndpointError::InvalidServerName)?;
        let mut tls_client_config =
            rustls::ClientConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| ControlPlaneRpcClientEndpointError::TlsProfileUnavailable)?
                .with_root_certificates(trust_roots)
                .with_no_client_auth();
        tls_client_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
        Ok(Self(ControlPlaneRpcClientEndpointKind::TlsTcp {
            advertised_endpoint,
            host,
            port,
            server_name,
            connect_timeout,
            tls_client_config: Arc::new(tls_client_config),
        }))
    }

    #[must_use]
    pub fn advertised_endpoint(&self) -> String {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => {
                format!("unix://{}", socket_path.display())
            }
            ControlPlaneRpcClientEndpointKind::TlsTcp {
                advertised_endpoint,
                ..
            } => advertised_endpoint.clone(),
        }
    }

    #[must_use]
    pub fn unix_socket_path(&self) -> Option<&Path> {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => Some(socket_path),
            ControlPlaneRpcClientEndpointKind::TlsTcp { .. } => None,
        }
    }
}

impl std::fmt::Debug for ControlPlaneRpcClientEndpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => formatter
                .debug_struct("ControlPlaneRpcClientEndpoint::Unix")
                .field("socket_path", socket_path)
                .finish(),
            ControlPlaneRpcClientEndpointKind::TlsTcp {
                advertised_endpoint,
                host,
                port,
                server_name,
                connect_timeout,
                ..
            } => formatter
                .debug_struct("ControlPlaneRpcClientEndpoint::TlsTcp")
                .field("advertised_endpoint", advertised_endpoint)
                .field("host", host)
                .field("port", port)
                .field("server_name", server_name)
                .field("connect_timeout", connect_timeout)
                .field("tls", &true)
                .finish(),
        }
    }
}

/// The operation class accepted by a control-plane RPC server endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlPlaneRpcServerRole {
    Ordinary,
    AuthorityClockRecovery,
}

impl ControlPlaneRpcServerRole {
    fn accepts(self, request: &VerifiedControlPlaneRpcRequest) -> bool {
        match self {
            Self::Ordinary => !request.is_authority_clock_admin(),
            Self::AuthorityClockRecovery => request.is_authority_clock_admin(),
        }
    }
}

/// Storage-owned gate around publication of a fully encoded RPC response.
///
/// Implementations may delay or reject publication to preserve an external
/// durability invariant, but never receive access to the transport or frame.
/// A successful implementation must invoke `publish` exactly once; the server
/// rejects implementations that return success without publishing or invoke
/// the callback repeatedly.
pub trait ControlPlaneRpcResponsePublication: Send + Sync {
    fn publish(
        &self,
        publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
    ) -> Result<(), ControlPlaneError>;
}

fn publish_control_plane_rpc_response(
    publication: Option<&dyn ControlPlaneRpcResponsePublication>,
    publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
) -> Result<(), ControlPlaneError> {
    let mut published = false;
    let result = {
        let mut publish_once = || {
            if std::mem::replace(&mut published, true) {
                return Err(ControlPlaneError::rpc_protocol(
                    "control-plane response publication attempted more than once".to_owned(),
                ));
            }
            publish()
        };
        match publication {
            Some(publication) => publication.publish(&mut publish_once),
            None => publish_once(),
        }
    };
    if result.is_ok() && !published {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane response publication completed without publishing".to_owned(),
        ));
    }
    result
}

/// Durable authority-clock checkpoint destination used by the RPC server and
/// the process-level lease-expiry loop.
pub struct ControlPlaneAuthorityClockCheckpointTarget {
    path: PathBuf,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
}

impl std::fmt::Debug for ControlPlaneAuthorityClockCheckpointTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPlaneAuthorityClockCheckpointTarget")
            .field("checkpoint", &"configured")
            .finish()
    }
}

impl ControlPlaneAuthorityClockCheckpointTarget {
    #[must_use]
    pub fn new(
        path: impl Into<PathBuf>,
        binding: ControlPlaneAuthorityClockCheckpointBinding,
    ) -> Self {
        Self {
            path: path.into(),
            binding,
        }
    }

    pub fn persist_established(
        &self,
        context: ControlPlaneAuthorityClockContext,
        authority_clock: &mut ControlPlaneAuthorityClock,
    ) -> Result<(), ControlPlaneError> {
        if !authority_clock.status(context).established() {
            return Ok(());
        }
        let persistence_started = Instant::now();
        let mut invalidate_elapsed = Duration::ZERO;
        let mut store_elapsed = Duration::ZERO;
        let persistence_result = (|| {
            let invalidate_started = Instant::now();
            invalidate_authority_clock_restart_checkpoint(&self.path)?;
            invalidate_elapsed = invalidate_started.elapsed();
            let store_started = Instant::now();
            store_validated_authority_clock_restart_checkpoint(
                &self.path,
                self.binding,
                context.committed_timestamp_high_water_ms(),
                authority_clock,
            )?;
            store_elapsed = store_started.elapsed();
            Ok::<(), ControlPlaneError>(())
        })();
        let persistence_elapsed = persistence_started.elapsed();
        if persistence_elapsed >= Duration::from_secs(1) {
            eprintln!(
                "control-plane authority-clock checkpoint persistence took {persistence_elapsed:?} \
                 (invalidation {invalidate_elapsed:?}, replacement {store_elapsed:?})"
            );
        }
        if let Err(error) = persistence_result {
            if authority_clock.status(context).established() {
                authority_clock.fail_closed_after_checkpoint_persistence_failure()?;
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn invalidate_if_blocked(
        &self,
        authority_clock: &ControlPlaneAuthorityClock,
    ) -> Result<(), ControlPlaneError> {
        if authority_clock.is_established() {
            return Ok(());
        }
        invalidate_authority_clock_restart_checkpoint(&self.path)
    }
}

#[derive(Clone)]
struct ControlPlaneRpcServerResources {
    active_workers: Arc<AtomicUsize>,
    worker_limit: usize,
    pre_auth_byte_budget: Arc<ControlPlaneRpcPreAuthByteBudget>,
}

/// Logical policy for one class of control-plane RPC endpoints.
///
/// Clones share the worker and pre-authentication memory budgets, allowing a
/// group of listeners to enforce one aggregate resource limit.
#[derive(Clone)]
pub(crate) struct ControlPlaneRpcServerPolicy {
    role: ControlPlaneRpcServerRole,
    resources: ControlPlaneRpcServerResources,
    gate_request_time_with_authority_clock: bool,
    auth_verifier: Option<Arc<ControlPlaneUnixAuthVerifier>>,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    authority_clock_checkpoint_target: Option<Arc<ControlPlaneAuthorityClockCheckpointTarget>>,
    authority_confirmation: Option<Arc<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>>,
    response_publication: Option<Arc<dyn ControlPlaneRpcResponsePublication>>,
    fatal_error_handler: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(any(test, feature = "test-hooks"))]
    test_authority_now_ms: Option<u64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ControlPlaneRpcServerConfigError {
    #[error("control-plane RPC worker limit must be positive")]
    ZeroWorkerLimit,
    #[error("control-plane RPC pre-authentication byte budget must be positive")]
    ZeroPreAuthByteBudget,
    #[error("control-plane RPC listener connection limit must be positive")]
    ZeroConnectionLimit,
    #[error("control-plane RPC listener frame limit must include the protocol envelope")]
    InvalidFrameLimit,
    #[error("control-plane RPC listener I/O timeout must be positive")]
    ZeroIoTimeout,
    #[error("control-plane RPC listener I/O timeout exceeds the supported operation bound")]
    IoTimeoutTooLarge,
    #[error("failed to construct the control-plane TLS server profile")]
    TlsProfileUnavailable,
}

impl ControlPlaneRpcServerPolicy {
    pub(crate) fn new(
        role: ControlPlaneRpcServerRole,
        worker_limit: usize,
        pre_auth_byte_budget: usize,
    ) -> Result<Self, ControlPlaneRpcServerConfigError> {
        if worker_limit == 0 {
            return Err(ControlPlaneRpcServerConfigError::ZeroWorkerLimit);
        }
        if pre_auth_byte_budget == 0 {
            return Err(ControlPlaneRpcServerConfigError::ZeroPreAuthByteBudget);
        }
        Ok(Self {
            role,
            resources: ControlPlaneRpcServerResources {
                active_workers: Arc::new(AtomicUsize::new(0)),
                worker_limit,
                pre_auth_byte_budget: Arc::new(ControlPlaneRpcPreAuthByteBudget::new(
                    pre_auth_byte_budget,
                )),
            },
            gate_request_time_with_authority_clock: false,
            auth_verifier: None,
            authority_clock: None,
            authority_clock_checkpoint_target: None,
            authority_confirmation: None,
            response_publication: None,
            fatal_error_handler: None,
            #[cfg(any(test, feature = "test-hooks"))]
            test_authority_now_ms: None,
        })
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_auth_verifier(
        mut self,
        auth_verifier: Arc<ControlPlaneUnixAuthVerifier>,
    ) -> Self {
        self.auth_verifier = Some(auth_verifier);
        self
    }

    #[must_use]
    pub(crate) fn with_server_auth(mut self, auth: &crate::ControlPlaneRpcServerAuth) -> Self {
        self.auth_verifier.clone_from(&auth.verifier);
        self
    }

    pub(crate) fn authentication_required(&self) -> bool {
        self.auth_verifier.is_some()
    }

    #[cfg(test)]
    pub(crate) fn role(&self) -> ControlPlaneRpcServerRole {
        self.role
    }

    #[must_use]
    pub(crate) fn with_authority_clock(
        mut self,
        authority_clock: Arc<Mutex<ControlPlaneAuthorityClock>>,
        checkpoint_target: Arc<ControlPlaneAuthorityClockCheckpointTarget>,
        gate_request_time: bool,
    ) -> Self {
        self.authority_clock = Some(authority_clock);
        self.authority_clock_checkpoint_target = Some(checkpoint_target);
        self.gate_request_time_with_authority_clock = gate_request_time;
        self
    }

    #[must_use]
    pub(crate) fn with_authority_confirmation(
        mut self,
        authority_confirmation: Arc<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>,
    ) -> Self {
        self.authority_confirmation = Some(authority_confirmation);
        self
    }

    #[must_use]
    pub(crate) fn with_response_publication(
        mut self,
        response_publication: Arc<dyn ControlPlaneRpcResponsePublication>,
    ) -> Self {
        self.response_publication = Some(response_publication);
        self
    }

    /// Installs the process-lifecycle action used after a fatal durable
    /// checkpoint failure has been diagnosed and logged by storage.
    #[must_use]
    pub(crate) fn with_fatal_error_handler(
        mut self,
        fatal_error_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        self.fatal_error_handler = Some(fatal_error_handler);
        self
    }

    #[must_use]
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn active_workers(&self) -> usize {
        self.resources.active_workers.load(Ordering::Acquire)
    }

    fn authority_now_ms(&self) -> u64 {
        #[cfg(any(test, feature = "test-hooks"))]
        if let Some(authority_now_ms) = self.test_authority_now_ms {
            return authority_now_ms;
        }
        crate::clock::current_time_millis()
    }
}

#[derive(Debug)]
struct ControlPlaneRpcPreAuthByteBudget {
    reserved_bytes: AtomicUsize,
    limit_bytes: usize,
}

impl ControlPlaneRpcPreAuthByteBudget {
    fn new(limit_bytes: usize) -> Self {
        Self {
            reserved_bytes: AtomicUsize::new(0),
            limit_bytes,
        }
    }

    fn reserve(
        self: &Arc<Self>,
        frame_bytes: usize,
    ) -> Result<ControlPlaneRpcPreAuthByteReservation, ControlPlaneError> {
        let result =
            self.reserved_bytes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                    reserved
                        .checked_add(frame_bytes)
                        .filter(|total| *total <= self.limit_bytes)
                });
        match result {
            Ok(_) => Ok(ControlPlaneRpcPreAuthByteReservation {
                budget: Arc::clone(self),
                frame_bytes,
            }),
            Err(reserved) => Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane RPC pre-authentication frame budget exhausted: requested {frame_bytes} bytes with {reserved} of {} bytes reserved",
                    self.limit_bytes
                ))),
        }
    }
}

#[derive(Debug)]
struct ControlPlaneRpcPreAuthByteReservation {
    budget: Arc<ControlPlaneRpcPreAuthByteBudget>,
    frame_bytes: usize,
}

impl Drop for ControlPlaneRpcPreAuthByteReservation {
    fn drop(&mut self) {
        self.budget
            .reserved_bytes
            .fetch_sub(self.frame_bytes, Ordering::AcqRel);
    }
}

struct ControlPlaneRpcTlsCertificateResolver {
    certified_key: Arc<CertifiedKey>,
}

impl std::fmt::Debug for ControlPlaneRpcTlsCertificateResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPlaneRpcTlsCertificateResolver")
            .field("certificate", &"configured")
            .finish()
    }
}

impl rustls::server::ResolvesServerCert for ControlPlaneRpcTlsCertificateResolver {
    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.certified_key))
    }
}

enum ControlPlaneRpcServerListenerKind {
    Unix(UnixListener),
    TlsTcp {
        listener: TcpListener,
        tls_server_config: Arc<rustls::ServerConfig>,
    },
}

/// Opaque bound listener for the storage-owned control-plane RPC server.
pub(crate) struct ControlPlaneRpcServerListener {
    kind: ControlPlaneRpcServerListenerKind,
    max_connections: usize,
    max_frame_bytes: usize,
    io_timeout: Duration,
}

impl std::fmt::Debug for ControlPlaneRpcServerListener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let transport = match self.kind {
            ControlPlaneRpcServerListenerKind::Unix(_) => "unix",
            ControlPlaneRpcServerListenerKind::TlsTcp { .. } => "tls-tcp",
        };
        formatter
            .debug_struct("ControlPlaneRpcServerListener")
            .field("transport", &transport)
            .field("max_connections", &self.max_connections)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("io_timeout", &self.io_timeout)
            .finish()
    }
}

fn validate_control_plane_rpc_server_listener_limits(
    max_connections: usize,
    max_frame_bytes: usize,
    io_timeout: Duration,
) -> Result<(), ControlPlaneRpcServerConfigError> {
    if max_connections == 0 {
        return Err(ControlPlaneRpcServerConfigError::ZeroConnectionLimit);
    }
    if max_frame_bytes < control_plane_rpc_frame_overhead() {
        return Err(ControlPlaneRpcServerConfigError::InvalidFrameLimit);
    }
    if io_timeout.is_zero() {
        return Err(ControlPlaneRpcServerConfigError::ZeroIoTimeout);
    }
    if io_timeout > CONTROL_PLANE_RPC_MAX_SERVER_OPERATION_TIMEOUT {
        return Err(ControlPlaneRpcServerConfigError::IoTimeoutTooLarge);
    }
    Ok(())
}

impl ControlPlaneRpcServerListener {
    pub(crate) fn unix(
        listener: UnixListener,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    ) -> Result<Self, ControlPlaneRpcServerConfigError> {
        validate_control_plane_rpc_server_listener_limits(
            max_connections,
            max_frame_bytes,
            io_timeout,
        )?;
        Ok(Self {
            kind: ControlPlaneRpcServerListenerKind::Unix(listener),
            max_connections,
            max_frame_bytes,
            io_timeout,
        })
    }

    pub(crate) fn tls_tcp(
        listener: TcpListener,
        certified_key: Arc<CertifiedKey>,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    ) -> Result<Self, ControlPlaneRpcServerConfigError> {
        validate_control_plane_rpc_server_listener_limits(
            max_connections,
            max_frame_bytes,
            io_timeout,
        )?;
        let resolver = ControlPlaneRpcTlsCertificateResolver { certified_key };
        let mut tls_server_config =
            rustls::ServerConfig::builder_with_provider(tls_provider::configured_provider())
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| ControlPlaneRpcServerConfigError::TlsProfileUnavailable)?
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(resolver));
        tls_server_config.alpn_protocols = vec![CONTROL_PLANE_RPC_TLS_ALPN.to_vec()];
        Ok(Self {
            kind: ControlPlaneRpcServerListenerKind::TlsTcp {
                listener,
                tls_server_config: Arc::new(tls_server_config),
            },
            max_connections,
            max_frame_bytes,
            io_timeout,
        })
    }
}

#[derive(Debug)]
struct ControlPlaneEndpointPass {
    endpoint_count: usize,
    next_endpoint_index: usize,
    remaining: usize,
    last_endpoint_index: Option<usize>,
}

impl ControlPlaneEndpointPass {
    fn new(start: usize, endpoint_count: usize) -> Self {
        debug_assert!(endpoint_count > 0);
        Self {
            endpoint_count,
            next_endpoint_index: start % endpoint_count,
            remaining: endpoint_count,
            last_endpoint_index: None,
        }
    }

    fn next(&mut self) -> Option<usize> {
        if self.remaining == 0 {
            return None;
        }
        let endpoint_index = self.next_endpoint_index;
        self.next_endpoint_index = (endpoint_index + 1) % self.endpoint_count;
        self.remaining -= 1;
        self.last_endpoint_index = Some(endpoint_index);
        Some(endpoint_index)
    }

    fn is_exhausted(&self) -> bool {
        self.remaining == 0
    }

    fn last_endpoint_index(&self) -> usize {
        self.last_endpoint_index
            .expect("endpoint pass records every attempted endpoint")
    }
}

impl std::fmt::Debug for UnixControlPlaneClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixControlPlaneClient")
            .field("endpoints", &self.endpoints)
            .field("socket_paths", &self.socket_paths)
            .field(
                "preferred_endpoint_index",
                &self.preferred_endpoint_index.load(Ordering::Acquire),
            )
            .finish()
    }
}

#[derive(Debug)]
struct ControlPlaneRpcFrameExchangeError {
    error: Box<ControlPlaneError>,
    request_may_have_been_sent: bool,
}

impl ControlPlaneRpcFrameExchangeError {
    #[must_use]
    fn before_request(error: ControlPlaneError) -> Self {
        Self {
            error: Box::new(error),
            request_may_have_been_sent: false,
        }
    }

    #[must_use]
    fn after_request_started(error: ControlPlaneError) -> Self {
        Self {
            error: Box::new(error),
            request_may_have_been_sent: true,
        }
    }

    #[must_use]
    fn request_may_have_been_sent(&self) -> bool {
        self.request_may_have_been_sent
    }

    #[must_use]
    fn into_error(self) -> ControlPlaneError {
        *self.error
    }
}

#[derive(Debug, Clone)]
pub struct AuthenticatedUnixControlPlaneClient {
    inner: UnixControlPlaneClient,
    credential: ControlPlaneScopedCredential,
}

#[derive(Debug, Clone)]
pub(crate) struct ControlPlaneUnixAuthVerifier {
    cluster_id: String,
    storage_node_credentials: BTreeMap<NodeId, Vec<ControlPlaneStorageNodeAuthCredential>>,
    frontend_credentials: BTreeMap<String, Vec<ControlPlaneFrontendAuthCredential>>,
    admin_credentials: BTreeMap<String, Vec<ControlPlaneAdminAuthCredential>>,
    metrics: Arc<ControlPlaneUnixAuthMetrics>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneStorageNodeAuthCredential {
    node_id: NodeId,
    credential_id: String,
    credential_version: u64,
    secret: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneStorageNodeAuthCredentialInput {
    pub node_id: NodeId,
    pub credential_id: String,
    pub credential_version: u64,
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for ControlPlaneStorageNodeAuthCredentialInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneStorageNodeAuthCredentialInput")
            .field("node_id", &self.node_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for ControlPlaneStorageNodeAuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneStorageNodeAuthCredential")
            .field("node_id", &self.node_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneFrontendAuthCredential {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
    secret: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneFrontendAuthCredentialInput {
    pub instance_id: String,
    pub credential_id: String,
    pub credential_version: u64,
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for ControlPlaneFrontendAuthCredentialInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneFrontendAuthCredentialInput")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for ControlPlaneFrontendAuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneFrontendAuthCredential")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneAdminAuthCredential {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
    secret: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ControlPlaneAdminAuthCredentialInput {
    pub instance_id: String,
    pub credential_id: String,
    pub credential_version: u64,
    pub secret: Vec<u8>,
}

impl std::fmt::Debug for ControlPlaneAdminAuthCredentialInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneAdminAuthCredentialInput")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for ControlPlaneAdminAuthCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneAdminAuthCredential")
            .field("instance_id", &self.instance_id)
            .field("credential_id", &self.credential_id)
            .field("credential_version", &self.credential_version)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAuthCredentialStatus {
    node_id: NodeId,
    credential_id: String,
    credential_version: u64,
}

impl ControlPlaneUnixAuthCredentialStatus {
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixFrontendAuthCredentialStatus {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
}

impl ControlPlaneUnixFrontendAuthCredentialStatus {
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAdminAuthCredentialStatus {
    instance_id: String,
    credential_id: String,
    credential_version: u64,
}

impl ControlPlaneUnixAdminAuthCredentialStatus {
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAuthStatusSnapshot {
    required: bool,
    storage_node_heartbeat_required: bool,
    frontend_runtime_map_required: bool,
    admin_control_plane_required: bool,
    cluster_id: String,
    storage_node_credentials: Vec<ControlPlaneUnixAuthCredentialStatus>,
    frontend_credentials: Vec<ControlPlaneUnixFrontendAuthCredentialStatus>,
    admin_credentials: Vec<ControlPlaneUnixAdminAuthCredentialStatus>,
    metrics: ControlPlaneUnixAuthMetricsSnapshot,
}

impl ControlPlaneUnixAuthStatusSnapshot {
    #[must_use]
    pub fn required(&self) -> bool {
        self.required
    }

    #[must_use]
    pub fn storage_node_heartbeat_required(&self) -> bool {
        self.storage_node_heartbeat_required
    }

    #[must_use]
    pub fn frontend_runtime_map_required(&self) -> bool {
        self.frontend_runtime_map_required
    }

    #[must_use]
    pub fn admin_control_plane_required(&self) -> bool {
        self.admin_control_plane_required
    }

    #[must_use]
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    #[must_use]
    pub fn storage_node_credentials(&self) -> &[ControlPlaneUnixAuthCredentialStatus] {
        &self.storage_node_credentials
    }

    #[must_use]
    pub fn frontend_credentials(&self) -> &[ControlPlaneUnixFrontendAuthCredentialStatus] {
        &self.frontend_credentials
    }

    #[must_use]
    pub fn admin_credentials(&self) -> &[ControlPlaneUnixAdminAuthCredentialStatus] {
        &self.admin_credentials
    }

    #[must_use]
    pub fn metrics(&self) -> &ControlPlaneUnixAuthMetricsSnapshot {
        &self.metrics
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControlPlaneUnixAuthMetricsSnapshot {
    accepted_total: u64,
    rejected_total: u64,
    accepted_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_reason: BTreeMap<ControlPlaneAuthRejectionReason, u64>,
}

impl ControlPlaneUnixAuthMetricsSnapshot {
    #[must_use]
    pub fn accepted_total(&self) -> u64 {
        self.accepted_total
    }

    #[must_use]
    pub fn rejected_total(&self) -> u64 {
        self.rejected_total
    }

    #[must_use]
    #[cfg(test)]
    pub fn accepted_for_operation(&self, operation: ControlPlaneAuthOperation) -> u64 {
        self.accepted_by_operation
            .get(&operation)
            .copied()
            .unwrap_or(0)
    }

    #[must_use]
    #[cfg(test)]
    pub fn rejected_for_operation(&self, operation: ControlPlaneAuthOperation) -> u64 {
        self.rejected_by_operation
            .get(&operation)
            .copied()
            .unwrap_or(0)
    }

    #[must_use]
    #[cfg(test)]
    pub fn rejected_for_reason(&self, reason: ControlPlaneAuthRejectionReason) -> u64 {
        self.rejected_by_reason.get(&reason).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn accepted_by_operation(&self) -> &BTreeMap<ControlPlaneAuthOperation, u64> {
        &self.accepted_by_operation
    }

    #[must_use]
    pub fn rejected_by_operation(&self) -> &BTreeMap<ControlPlaneAuthOperation, u64> {
        &self.rejected_by_operation
    }

    #[must_use]
    pub fn rejected_by_reason(&self) -> &BTreeMap<ControlPlaneAuthRejectionReason, u64> {
        &self.rejected_by_reason
    }
}

#[derive(Debug, Default)]
struct ControlPlaneUnixAuthMetrics {
    state: Mutex<ControlPlaneUnixAuthMetricsState>,
}

#[derive(Debug, Default)]
struct ControlPlaneUnixAuthMetricsState {
    accepted_total: u64,
    rejected_total: u64,
    accepted_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_operation: BTreeMap<ControlPlaneAuthOperation, u64>,
    rejected_by_reason: BTreeMap<ControlPlaneAuthRejectionReason, u64>,
}

struct VerifiedFrontendRuntimeMapRead {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

struct VerifiedAdminControlPlaneCommand {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

struct VerifiedAdminRuntimeMapRead {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

#[derive(Clone, Copy)]
struct AuthenticatedAdminRetryClock {
    authority_now_ms: u64,
    start: Instant,
    #[cfg(test)]
    elapsed_override_ms: Option<&'static std::sync::atomic::AtomicU64>,
}

impl AuthenticatedAdminRetryClock {
    fn new(authority_now_ms: u64) -> Self {
        Self {
            authority_now_ms,
            start: Instant::now(),
            #[cfg(test)]
            elapsed_override_ms: None,
        }
    }

    fn now_ms(self) -> u64 {
        #[cfg(test)]
        let elapsed_ms = self.elapsed_override_ms.map_or_else(
            || u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX),
            |elapsed_ms| elapsed_ms.load(std::sync::atomic::Ordering::SeqCst),
        );
        #[cfg(not(test))]
        let elapsed_ms = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.authority_now_ms.saturating_add(elapsed_ms)
    }

    #[cfg(test)]
    fn with_elapsed_source(authority_now_ms: u64) -> (Self, &'static std::sync::atomic::AtomicU64) {
        let elapsed_override_ms = Box::leak(Box::new(std::sync::atomic::AtomicU64::new(0)));
        (
            Self {
                authority_now_ms,
                start: Instant::now(),
                elapsed_override_ms: Some(elapsed_override_ms),
            },
            elapsed_override_ms,
        )
    }
}

struct VerifiedStorageNodeHeartbeatRefresh {
    payload: Vec<u8>,
    response_credential: ControlPlaneScopedCredential,
    response_target: ControlPlaneAuthPrincipal,
}

fn format_control_plane_auth_rejection(
    reason: ControlPlaneAuthRejectionReason,
    envelope: &ControlPlaneAuthEnvelope,
    authority_now_ms: u64,
) -> String {
    if reason != ControlPlaneAuthRejectionReason::ReplayFreshnessFailure {
        return format!("{reason:?}");
    }
    let issued_at_ms = envelope.header().issued_at_ms();
    let expires_at_ms = envelope.header().expires_at_ms();
    let issued_delta_ms = issued_at_ms
        .map(|issued_at_ms| i128::from(issued_at_ms).saturating_sub(i128::from(authority_now_ms)));
    let expiry_delta_ms = expires_at_ms.map(|expires_at_ms| {
        i128::from(expires_at_ms).saturating_sub(i128::from(authority_now_ms))
    });
    format!(
        "{reason:?} (issued_at_ms={issued_at_ms:?}, expires_at_ms={expires_at_ms:?}, authority_now_ms={authority_now_ms}, issued_delta_ms={issued_delta_ms:?}, expiry_delta_ms={expiry_delta_ms:?})"
    )
}

impl ControlPlaneUnixAuthMetrics {
    fn record_accepted(&self, operation: ControlPlaneAuthOperation) {
        let mut state = self
            .state
            .lock()
            .expect("control-plane Unix auth metrics mutex poisoned");
        state.accepted_total += 1;
        *state.accepted_by_operation.entry(operation).or_default() += 1;
    }

    fn record_rejected(
        &self,
        operation: ControlPlaneAuthOperation,
        reason: ControlPlaneAuthRejectionReason,
    ) {
        let mut state = self
            .state
            .lock()
            .expect("control-plane Unix auth metrics mutex poisoned");
        state.rejected_total += 1;
        *state.rejected_by_operation.entry(operation).or_default() += 1;
        *state.rejected_by_reason.entry(reason).or_default() += 1;
    }

    fn snapshot(&self) -> ControlPlaneUnixAuthMetricsSnapshot {
        let state = self
            .state
            .lock()
            .expect("control-plane Unix auth metrics mutex poisoned");
        ControlPlaneUnixAuthMetricsSnapshot {
            accepted_total: state.accepted_total,
            rejected_total: state.rejected_total,
            accepted_by_operation: state.accepted_by_operation.clone(),
            rejected_by_operation: state.rejected_by_operation.clone(),
            rejected_by_reason: state.rejected_by_reason.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FencedPgMetadataTransferRuntimeMap {
    runtime_map: ClusterRuntimeMapSnapshot,
    source_primary_lease_deadline_ms: Option<u64>,
}

impl FencedPgMetadataTransferRuntimeMap {
    #[must_use]
    pub fn new(
        runtime_map: ClusterRuntimeMapSnapshot,
        source_primary_lease_deadline_ms: Option<u64>,
    ) -> Self {
        Self {
            runtime_map,
            source_primary_lease_deadline_ms,
        }
    }

    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn source_primary_lease_deadline_ms(&self) -> Option<u64> {
        self.source_primary_lease_deadline_ms
    }

    #[must_use]
    pub fn into_parts(self) -> (ClusterRuntimeMapSnapshot, Option<u64>) {
        (self.runtime_map, self.source_primary_lease_deadline_ms)
    }
}

fn unix_io_timeout_error() -> std::io::Error {
    std::io::Error::new(
        ErrorKind::TimedOut,
        "control-plane Unix RPC deadline expired",
    )
}

fn unix_io_remaining(deadline: Instant) -> Result<Duration, std::io::Error> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(unix_io_timeout_error());
    }
    Ok(remaining)
}

/// Connects to a control-plane Unix socket within one absolute operation deadline.
pub(crate) fn connect_unix_stream_until(
    path: &Path,
    deadline: Instant,
) -> std::io::Result<UnixStream> {
    unix_io_remaining(deadline)?;
    let path_bytes = path.as_os_str().as_bytes();
    if path_bytes.contains(&0) {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "control-plane Unix socket path contains NUL",
        ));
    }

    // SAFETY: sockaddr_un is a plain C address structure and zero is a valid
    // initialization before its family and path fields are populated.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if path_bytes.len() >= address.sun_path.len() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "control-plane Unix socket path is too long",
        ));
    }
    address.sun_family = libc::sa_family_t::try_from(libc::AF_UNIX)
        .expect("AF_UNIX fits the platform socket-family field");
    // SAFETY: the length check above proves the source plus its zero terminator
    // fits sun_path, which was zero-initialized.
    unsafe {
        std::ptr::copy_nonoverlapping(
            path_bytes.as_ptr(),
            address.sun_path.as_mut_ptr().cast::<u8>(),
            path_bytes.len(),
        );
    }
    let address_len = std::mem::offset_of!(libc::sockaddr_un, sun_path)
        .checked_add(path_bytes.len())
        .and_then(|len| len.checked_add(1))
        .and_then(|len| libc::socklen_t::try_from(len).ok())
        .ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                "control-plane Unix socket address length overflowed",
            )
        })?;
    #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        address.sun_len = u8::try_from(address_len).map_err(|_| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                "control-plane Unix socket address is too long",
            )
        })?;
    }

    // SAFETY: AF_UNIX/SOCK_STREAM has no additional pointer arguments.
    let raw_fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: raw_fd was returned as a new owned descriptor above.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    set_unix_connect_descriptor_flags(&fd)?;

    // SAFETY: address points to an initialized sockaddr_un and address_len
    // covers exactly its family, path bytes, and zero terminator.
    let connect_result = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            (&raw const address).cast::<libc::sockaddr>(),
            address_len,
        )
    };
    if connect_result != 0 {
        let error = std::io::Error::last_os_error();
        let raw_error = error.raw_os_error();
        if raw_error != Some(libc::EINPROGRESS)
            && raw_error != Some(libc::EAGAIN)
            && raw_error != Some(libc::EWOULDBLOCK)
            && raw_error != Some(libc::EINTR)
        {
            return Err(error);
        }
        wait_for_unix_connect(&fd, deadline)?;
    }
    clear_unix_connect_nonblocking(&fd)?;
    Ok(UnixStream::from(fd))
}

fn set_unix_connect_descriptor_flags(fd: &OwnedFd) -> std::io::Result<()> {
    // SAFETY: fcntl operates on the live descriptor owned by fd.
    let descriptor_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
    if descriptor_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFD consumes an integer flag value, not a pointer.
    if unsafe {
        libc::fcntl(
            fd.as_raw_fd(),
            libc::F_SETFD,
            descriptor_flags | libc::FD_CLOEXEC,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fcntl operates on the live descriptor owned by fd.
    let status_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFL consumes an integer flag value, not a pointer.
    if unsafe {
        libc::fcntl(
            fd.as_raw_fd(),
            libc::F_SETFL,
            status_flags | libc::O_NONBLOCK,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn clear_unix_connect_nonblocking(fd: &OwnedFd) -> std::io::Result<()> {
    // SAFETY: fcntl operates on the live descriptor owned by fd.
    let status_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: F_SETFL consumes an integer flag value, not a pointer.
    if unsafe {
        libc::fcntl(
            fd.as_raw_fd(),
            libc::F_SETFL,
            status_flags & !libc::O_NONBLOCK,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn wait_for_unix_connect(fd: &OwnedFd, deadline: Instant) -> std::io::Result<()> {
    loop {
        let remaining = unix_io_remaining(deadline)?;
        let timeout_ms = remaining
            .as_nanos()
            .div_ceil(1_000_000)
            .min(u128::try_from(i32::MAX).expect("i32::MAX fits u128"));
        let timeout_ms = i32::try_from(timeout_ms).expect("poll timeout was clamped to i32::MAX");
        let mut poll_fd = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLOUT,
            revents: 0,
        };
        // SAFETY: poll_fd points to one initialized pollfd for the duration of
        // the call.
        let poll_result = unsafe { libc::poll(&raw mut poll_fd, 1, timeout_ms) };
        if poll_result == 0 {
            return Err(unix_io_timeout_error());
        }
        if poll_result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }

        let mut socket_error = 0;
        let mut socket_error_len = libc::socklen_t::try_from(std::mem::size_of_val(&socket_error))
            .expect("socket error length fits socklen_t");
        // SAFETY: both output pointers reference initialized writable values
        // of the lengths passed to getsockopt.
        if unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                (&raw mut socket_error).cast(),
                &raw mut socket_error_len,
            )
        } < 0
        {
            return Err(std::io::Error::last_os_error());
        }
        if socket_error != 0 {
            return Err(std::io::Error::from_raw_os_error(socket_error));
        }
        return Ok(());
    }
}

const CONTROL_PLANE_RPC_DEADLINE_EXPIRED: &str = "control-plane RPC operation deadline expired";
pub(crate) type DeadlineUnixStream<'a> = DeadlineStream<&'a mut UnixStream>;
type ControlPlaneDeadlineTcpSocket = DeadlineStream<TcpStream>;
type ControlPlaneDeadlineUnixSocket = DeadlineStream<UnixStream>;

pub(crate) async fn connect_tcp_stream_until_async(
    host: String,
    port: u16,
    deadline: Instant,
) -> std::io::Result<TcpStream> {
    let stream = match tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        tokio::net::TcpStream::connect((host.as_str(), port)),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            return Err(std::io::Error::new(
                ErrorKind::TimedOut,
                "TCP connect deadline expired",
            ));
        }
    };
    let stream = stream.into_std()?;
    stream.set_nonblocking(false)?;
    Ok(stream)
}

fn connect_control_plane_tcp_until(
    host: &str,
    port: u16,
    deadline: Instant,
) -> std::io::Result<TcpStream> {
    let future = connect_tcp_stream_until_async(host.to_owned(), port, deadline);
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) => std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(future)
        })
        .join()
        .map_err(|_| std::io::Error::other("control-plane TLS/TCP client runtime panicked"))?,
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(future),
    }
}

fn connect_control_plane_tls_tcp(
    host: &str,
    port: u16,
    server_name: &str,
    connect_timeout: Duration,
    tls_client_config: &Arc<rustls::ClientConfig>,
    deadline: Instant,
) -> Result<
    rustls::StreamOwned<rustls::ClientConnection, ControlPlaneDeadlineTcpSocket>,
    ControlPlaneError,
> {
    let connect_deadline = Instant::now()
        .checked_add(connect_timeout)
        .unwrap_or(deadline)
        .min(deadline);
    let tcp_stream =
        connect_control_plane_tcp_until(host, port, connect_deadline).map_err(|source| {
            ControlPlaneError::io("connect control-plane TLS/TCP endpoint", source)
        })?;
    tcp_stream.set_nodelay(true).map_err(|source| {
        ControlPlaneError::io("configure control-plane TLS/TCP endpoint", source)
    })?;
    let server_name = ServerName::try_from(server_name.to_owned()).map_err(|_| {
        ControlPlaneError::rpc_protocol(
            "control-plane TLS/TCP endpoint has an invalid TLS server name".to_owned(),
        )
    })?;
    let connection = rustls::ClientConnection::new(Arc::clone(tls_client_config), server_name)
        .map_err(|error| {
            ControlPlaneError::rpc_protocol(format!(
                "failed to initialize control-plane TLS client: {error}"
            ))
        })?;
    let socket = ControlPlaneDeadlineTcpSocket::new(
        tcp_stream,
        deadline,
        CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
    )
    .map_err(|source| {
        ControlPlaneError::io("configure control-plane TLS/TCP deadline I/O", source)
    })?;
    let mut stream = rustls::StreamOwned::new(connection, socket);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|source| {
                ControlPlaneError::io("complete control-plane TLS client handshake", source)
            })?;
    }
    if stream.conn.alpn_protocol() != Some(CONTROL_PLANE_RPC_TLS_ALPN) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane TLS peer did not negotiate the required protocol profile".to_owned(),
        ));
    }
    Ok(stream)
}

impl ControlPlaneRpcClientEndpoint {
    fn exchange(
        &self,
        request_frame: &[u8],
        deadline: Instant,
    ) -> Result<(ControlPlaneRpcKind, Vec<u8>), ControlPlaneRpcFrameExchangeError> {
        match &self.0 {
            ControlPlaneRpcClientEndpointKind::Unix { socket_path } => {
                let mut stream =
                    connect_unix_stream_until(socket_path, deadline).map_err(|source| {
                        ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::io(
                            "connect control-plane socket",
                            source,
                        ))
                    })?;
                let mut stream = DeadlineUnixStream::new(
                    &mut stream,
                    deadline,
                    CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
                )
                .map_err(|source| {
                    ControlPlaneRpcFrameExchangeError::before_request(ControlPlaneError::io(
                        "configure control-plane Unix deadline I/O",
                        source,
                    ))
                })?;
                stream.write_all(request_frame).map_err(|source| {
                    ControlPlaneRpcFrameExchangeError::after_request_started(ControlPlaneError::io(
                        "write control-plane RPC frame",
                        source,
                    ))
                })?;
                read_control_plane_rpc_frame(&mut stream)
                    .map_err(ControlPlaneRpcFrameExchangeError::after_request_started)
            }
            ControlPlaneRpcClientEndpointKind::TlsTcp {
                host,
                port,
                server_name,
                connect_timeout,
                tls_client_config,
                ..
            } => {
                let mut stream = connect_control_plane_tls_tcp(
                    host,
                    *port,
                    server_name,
                    *connect_timeout,
                    tls_client_config,
                    deadline,
                )
                .map_err(ControlPlaneRpcFrameExchangeError::before_request)?;
                stream.write_all(request_frame).map_err(|source| {
                    ControlPlaneRpcFrameExchangeError::after_request_started(ControlPlaneError::io(
                        "write control-plane TLS/TCP request frame",
                        source,
                    ))
                })?;
                read_control_plane_rpc_frame(&mut stream)
                    .map_err(ControlPlaneRpcFrameExchangeError::after_request_started)
            }
        }
    }
}

impl UnixControlPlaneClient {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        let socket_path = socket_path.into();
        Self {
            endpoints: Arc::from([ControlPlaneRpcClientEndpoint::unix(socket_path.clone())]),
            socket_paths: Arc::from([socket_path]),
            preferred_endpoint_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_socket_paths(
        socket_paths: impl IntoIterator<Item = PathBuf>,
    ) -> Result<Self, ControlPlaneError> {
        let socket_paths: Vec<PathBuf> = socket_paths.into_iter().collect();
        if socket_paths.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane Unix client requires at least one socket path".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for socket_path in &socket_paths {
            if !unique.insert(socket_path.clone()) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane Unix client contains duplicate socket path {}",
                    socket_path.display()
                )));
            }
        }
        Ok(Self {
            endpoints: socket_paths
                .iter()
                .cloned()
                .map(ControlPlaneRpcClientEndpoint::unix)
                .collect::<Vec<_>>()
                .into(),
            socket_paths: socket_paths.into(),
            preferred_endpoint_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn with_endpoints(
        endpoints: impl IntoIterator<Item = ControlPlaneRpcClientEndpoint>,
    ) -> Result<Self, ControlPlaneError> {
        let endpoints: Vec<ControlPlaneRpcClientEndpoint> = endpoints.into_iter().collect();
        if endpoints.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane client requires at least one endpoint".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for endpoint in &endpoints {
            let advertised_endpoint = endpoint.advertised_endpoint();
            if !unique.insert(advertised_endpoint.clone()) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane client contains duplicate endpoint {advertised_endpoint}"
                )));
            }
        }
        let socket_paths = endpoints
            .iter()
            .map(|endpoint| endpoint.unix_socket_path().map(Path::to_path_buf))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        Ok(Self {
            endpoints: endpoints.into(),
            socket_paths: socket_paths.into(),
            preferred_endpoint_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Returns the primary path for a client configured exclusively with Unix endpoints.
    ///
    /// # Panics
    ///
    /// Panics when the client contains a TLS/TCP endpoint.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_paths[0]
    }

    /// Returns all paths when the client is configured exclusively with Unix endpoints.
    /// Mixed or TLS/TCP-only clients return an empty slice.
    #[must_use]
    pub fn socket_paths(&self) -> &[PathBuf] {
        &self.socket_paths
    }

    fn preferred_endpoint_index(&self) -> usize {
        self.preferred_endpoint_index.load(Ordering::Acquire) % self.endpoint_count()
    }

    fn prefer_endpoint_index(&self, endpoint_index: usize) {
        self.preferred_endpoint_index
            .store(endpoint_index % self.endpoint_count(), Ordering::Release);
    }

    fn endpoint_pass(&self) -> ControlPlaneEndpointPass {
        ControlPlaneEndpointPass::new(self.preferred_endpoint_index(), self.endpoint_count())
    }

    fn prefer_next_endpoint_after_failure(&self, pass: &ControlPlaneEndpointPass) {
        let rejected = pass.last_endpoint_index();
        let _ = self.preferred_endpoint_index.compare_exchange(
            rejected,
            pass.next_endpoint_index,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn prefer_successful_endpoint(&self, pass: &ControlPlaneEndpointPass) {
        self.prefer_endpoint_index(pass.last_endpoint_index());
    }

    fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    fn send_request(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_request_with_read_timeout(kind, payload, CONTROL_PLANE_RPC_IO_TIMEOUT)
    }

    fn send_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_request_until(kind, payload, Instant::now() + read_timeout)
    }

    fn send_mutating_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        debug_assert!(kind.mutating_admin_operation().is_some());
        let deadline = Instant::now() + read_timeout;
        let mut last_routing_error = None;
        let mut endpoint_pass = self.endpoint_pass();
        while !endpoint_pass.is_exhausted() {
            let response = match self.send_request_raw_response_with_endpoint_pass_until_classified(
                kind,
                payload,
                deadline,
                &mut endpoint_pass,
            ) {
                Ok(response) => response,
                Err(error) if error.request_may_have_been_sent() => {
                    return Err(unconfirmed_admin_mutation_response(
                        kind,
                        error.into_error(),
                    ));
                }
                Err(error) => return Err(error.into_error()),
            };
            let response = decode_control_plane_rpc_response_frame(response)
                .map_err(|error| unconfirmed_admin_mutation_response(kind, error))?;
            match response {
                DecodedControlPlaneRpcResponse::Rejection(error)
                    if error.is_control_plane_leader_routing_rejection() =>
                {
                    last_routing_error = Some(error);
                    self.prefer_next_endpoint_after_failure(&endpoint_pass);
                }
                DecodedControlPlaneRpcResponse::Rejection(error) => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return Err(error);
                }
                DecodedControlPlaneRpcResponse::Success(payload) => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return Ok(payload);
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    fn send_request_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut last_routing_error = None;
        let mut endpoint_pass = self.endpoint_pass();
        while !endpoint_pass.is_exhausted() {
            let response_payload = self.send_request_raw_response_with_endpoint_pass_until(
                kind,
                payload,
                deadline,
                &mut endpoint_pass,
            )?;
            match decode_control_plane_rpc_response(response_payload) {
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    last_routing_error = Some(error);
                    self.prefer_next_endpoint_after_failure(&endpoint_pass);
                }
                result => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return result;
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    #[cfg(test)]
    fn send_request_raw_response_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut endpoint_pass = self.endpoint_pass();
        self.send_request_raw_response_with_endpoint_pass_until(
            kind,
            payload,
            deadline,
            &mut endpoint_pass,
        )
    }

    fn send_request_raw_response_with_endpoint_pass_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_request_raw_response_with_endpoint_pass_until_classified(
            kind,
            payload,
            deadline,
            endpoint_pass,
        )
        .map_err(ControlPlaneRpcFrameExchangeError::into_error)
    }

    fn send_request_raw_response_with_endpoint_pass_until_classified(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<Vec<u8>, ControlPlaneRpcFrameExchangeError> {
        let request_frame = encode_control_plane_rpc_frame(kind, payload)
            .map_err(ControlPlaneRpcFrameExchangeError::before_request)?;
        let mut response = None;
        let mut last_pre_request_error = None;
        while let Some(endpoint_index) = endpoint_pass.next() {
            match self.endpoints[endpoint_index].exchange(&request_frame, deadline) {
                Ok(result) => {
                    response = Some(result);
                    break;
                }
                Err(error) if !error.request_may_have_been_sent() => {
                    last_pre_request_error = Some(error);
                    self.prefer_next_endpoint_after_failure(endpoint_pass);
                }
                Err(error) => return Err(error),
            }
        }
        let (response_kind, response_payload) = response.ok_or_else(|| {
            last_pre_request_error.expect("endpoint set is non-empty and every connect failed")
        })?;
        if response_kind != kind {
            return Err(ControlPlaneRpcFrameExchangeError::after_request_started(
                ControlPlaneError::rpc_protocol(format!(
                    "response kind {:?} did not match request kind {:?}",
                    response_kind, kind
                )),
            ));
        }
        Ok(response_payload)
    }

    fn send_read_only_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_read_only_request_with_payload_factory(
            kind,
            read_timeout,
            || Ok(payload.to_vec()),
        )
    }

    fn send_read_only_request_with_payload_factory<F>(
        &self,
        kind: ControlPlaneRpcKind,
        read_timeout: Duration,
        mut build_payload: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
    {
        debug_assert!(matches!(
            kind,
            ControlPlaneRpcKind::RuntimeMapSnapshot
                | ControlPlaneRpcKind::RuntimeMapDiagnostics
                | ControlPlaneRpcKind::PgRuntimeMapSnapshot
                | ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot
                | ControlPlaneRpcKind::RuntimeMapStatus
                | ControlPlaneRpcKind::PendingMetadataCommandRecoveries
        ));
        let deadline = Instant::now() + CONTROL_PLANE_RPC_READ_ONLY_RETRY_DEADLINE;
        loop {
            let payload = build_payload()?;
            match self.send_request_with_read_timeout(kind, &payload, read_timeout) {
                Ok(payload) => return Ok(payload),
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn send_liveness_request(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        retry_budget: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let deadline = Instant::now() + retry_budget;
        let mut endpoint_pass = self.endpoint_pass();
        loop {
            let response_payload = self.send_liveness_request_raw_response_until(
                kind,
                payload,
                deadline,
                &mut endpoint_pass,
            )?;
            match decode_control_plane_rpc_response(response_payload) {
                Err(error)
                    if error.is_control_plane_leader_routing_rejection()
                        && Instant::now() < deadline =>
                {
                    self.prefer_next_endpoint_after_failure(&endpoint_pass);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let retry_sleep = CONTROL_PLANE_RPC_LIVENESS_RETRY_BACKOFF.min(remaining / 2);
                    if !retry_sleep.is_zero() {
                        std::thread::sleep(retry_sleep);
                    }
                }
                result => {
                    self.prefer_successful_endpoint(&endpoint_pass);
                    return result;
                }
            }
        }
    }

    fn send_liveness_request_raw_response_until(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.send_liveness_request_raw_response_with_payload_factory_until(
            kind,
            deadline,
            endpoint_pass,
            || Ok(payload.to_vec()),
        )
    }

    fn send_liveness_request_raw_response_with_payload_factory_until<F>(
        &self,
        kind: ControlPlaneRpcKind,
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
        mut build_payload: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
    {
        debug_assert_eq!(kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        let mut retry_started = false;
        let mut last_retryable_error = None;
        loop {
            if retry_started && Instant::now() >= deadline {
                return Err(last_retryable_error
                    .take()
                    .expect("heartbeat retry deadline reached after retryable error"));
            }
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                return match last_retryable_error.take() {
                    Some(error) => Err(error),
                    None => Err(ControlPlaneError::RpcUnconfirmed {
                        message: "heartbeat retry budget expired before the first request"
                            .to_owned(),
                    }),
                };
            }
            let read_timeout = remaining.min(CONTROL_PLANE_RPC_LIVENESS_IO_TIMEOUT);
            let attempt_deadline = now + read_timeout;
            if endpoint_pass.is_exhausted() {
                *endpoint_pass = self.endpoint_pass();
            }
            let payload = build_payload()?;
            if Instant::now() >= deadline {
                return match last_retryable_error.take() {
                    Some(error) => Err(error),
                    None => Err(ControlPlaneError::RpcUnconfirmed {
                        message: "heartbeat retry budget expired before the first request"
                            .to_owned(),
                    }),
                };
            }
            match self.send_request_raw_response_with_endpoint_pass_until(
                kind,
                &payload,
                attempt_deadline,
                endpoint_pass,
            ) {
                Ok(payload) => return Ok(payload),
                Err(error) if error.is_retryable_control_plane_rpc_transport_error() => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(error);
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    retry_started = true;
                    last_retryable_error = Some(error);
                    let retry_sleep = CONTROL_PLANE_RPC_LIVENESS_RETRY_BACKOFF.min(remaining / 2);
                    if !retry_sleep.is_zero() {
                        std::thread::sleep(retry_sleep);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn wait_for_metadata_transfer_install_applied(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        original_error: &ControlPlaneError,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_observation_error = None;
        loop {
            match self.pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                0,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    if metadata_transfer_install_applied(
                        &runtime_map,
                        pg_id,
                        acting_set,
                        transfer,
                        expected_destination_epoch,
                    ) {
                        return Ok(runtime_map);
                    }
                    last_observation_error = Some(
                        runtime_map
                            .pg_routes()
                            .iter()
                            .find(|route| route.pg_id() == pg_id)
                            .map_or_else(
                                || {
                                    format!(
                                        "runtime map at epoch {} has no route for PG {}",
                                        runtime_map.cluster_epoch().get(),
                                        pg_id.get()
                                    )
                                },
                                |route| {
                                    format!(
                                        "runtime map at epoch {} route epoch {} state {:?} acting set {:?} transfer {:?}",
                                        runtime_map.cluster_epoch().get(),
                                        route.cluster_epoch().get(),
                                        route.state(),
                                        route.acting_set(),
                                        route.peering_metadata_transfer()
                                    )
                                },
                            ),
                    );
                }
                Err(error) => {
                    if last_observation_error.is_none() {
                        last_observation_error = Some(error.to_string());
                    }
                }
            }
            if Instant::now() >= deadline {
                let mut message = format!(
                    "metadata-transfer acting-set install for PG {} was not observable after lost control-plane RPC response: {original_error}",
                    pg_id.get()
                );
                if let Some(error) = last_observation_error {
                    message.push_str("; last runtime-map observation error: ");
                    message.push_str(&error);
                }
                return Err(ControlPlaneError::RpcUnconfirmed { message });
            }
            std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
        }
    }

    fn validate_metadata_transfer_fence_response(
        &self,
        pg_id: PgId,
        fenced: FencedPgMetadataTransferRuntimeMap,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        if metadata_transfer_fence_observable(fenced.runtime_map(), pg_id) {
            return Ok(fenced);
        }
        Err(ControlPlaneError::RpcUnconfirmed {
            message: format!(
                "metadata-transfer fence for PG {} returned runtime map without an observable peering route",
                pg_id.get()
            ),
        })
    }

    fn retry_set_pg_acting_set_after_retryable_failure(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        pre_update_route: PgActingSetPreflightRoute,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_unconfirmed_message = None;
        loop {
            match self.pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                0,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    let Some(route) = runtime_map
                        .pg_routes()
                        .iter()
                        .find(|route| route.pg_id() == pg_id)
                    else {
                        return Err(ControlPlaneError::rpc_protocol(format!(
                            "PG-specific runtime map response omitted requested PG {}",
                            pg_id.get()
                        )));
                    };
                    if route.acting_set() == acting_set {
                        return Ok(route.cluster_epoch());
                    }
                    let message = format!(
                            "PG {} acting-set update was not confirmed after retryable control-plane failure: current route at epoch {} has acting set {:?}, expected {:?}",
                            pg_id.get(),
                            route.cluster_epoch().get(),
                            route.acting_set(),
                            acting_set
                        );
                    match &pre_update_route {
                        PgActingSetPreflightRoute::Absent => {
                            return Err(ControlPlaneError::RpcUnconfirmed { message });
                        }
                        PgActingSetPreflightRoute::Present(before) => {
                            match pg_acting_set_retry_route_disposition(before, route) {
                                PgActingSetRetryRouteDisposition::Conflict => {
                                    return Err(ControlPlaneError::RpcUnconfirmed { message });
                                }
                                PgActingSetRetryRouteDisposition::RetryReady => {
                                    match self.set_pg_acting_set(pg_id, acting_set.to_vec()) {
                                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                                        Err(error)
                                            if error.is_retryable_pg_acting_set_checked_error() => {
                                        }
                                        Err(error) => return Err(error),
                                    }
                                }
                                PgActingSetRetryRouteDisposition::Wait => {}
                            }
                        }
                    }
                    last_unconfirmed_message = Some(message);
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message
                                .expect("mismatched route message was recorded"),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error)
                    if error.is_maybe_applied_control_plane_rpc_response_loss()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message.unwrap_or_else(|| format!(
                                "PG {} acting-set update was not confirmed after retryable control-plane failure; runtime-map observation failed: {error}",
                                pg_id.get()
                            )),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get()
                    && matches!(&pre_update_route, PgActingSetPreflightRoute::Absent) =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: format!(
                                "PG {} acting-set update was not confirmed after retryable control-plane failure: PG remains absent",
                                pg_id.get()
                            ),
                        });
                    }
                    match self.set_pg_acting_set(pg_id, acting_set.to_vec()) {
                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                        Err(error) if error.is_retryable_pg_acting_set_checked_error() => {}
                        Err(error) => return Err(error),
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn set_pg_acting_set(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_request(&mut payload, pg_id, &acting_set)?;
        let payload = self.send_request(ControlPlaneRpcKind::SetPgActingSet, &payload)?;
        let mut reader = PayloadReader::new(&payload);
        let raw_cluster_epoch = reader.read_u64()?;
        let cluster_epoch = ClusterEpoch::new(raw_cluster_epoch).ok_or_else(|| {
            ControlPlaneError::rpc_protocol(format!("invalid cluster epoch {raw_cluster_epoch}"))
        })?;
        reader.finish()?;
        Ok(cluster_epoch)
    }

    pub fn set_pg_acting_set_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let pre_update_route = self.pg_acting_set_preflight_route(pg_id)?;
        match self.set_pg_acting_set(pg_id, acting_set.clone()) {
            Ok(cluster_epoch) => Ok(cluster_epoch),
            Err(error) if error.is_retryable_pg_acting_set_checked_error() => self
                .retry_set_pg_acting_set_after_retryable_failure(
                    pg_id,
                    &acting_set,
                    pre_update_route,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(pg_id)?
            .into_parts()
            .0)
    }

    #[cfg(test)]
    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease(
        &self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_read_timeout(
            pg_id,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )
    }

    #[cfg(test)]
    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_read_timeout(
        &self,
        pg_id: PgId,
        read_timeout: Duration,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
            pg_id,
            Instant::now() + read_timeout,
        )
    }

    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
        &self,
        pg_id: PgId,
        deadline: Instant,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_request_until(
            ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
            &payload,
            deadline,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        let source_primary_lease_deadline_ms = reader.read_option_u64()?;
        reader.finish()?;
        Ok(FencedPgMetadataTransferRuntimeMap::new(
            runtime_map,
            source_primary_lease_deadline_ms,
        ))
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
        &self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        retry_checked_metadata_transfer_fence(pg_id, |deadline| {
            self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(pg_id, deadline)
                .and_then(|fenced| self.validate_metadata_transfer_fence_response(pg_id, fenced))
        })
    }

    pub fn set_pg_acting_set_with_metadata_transfer(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_with_metadata_transfer_request(
            &mut payload,
            pg_id,
            &acting_set,
            transfer,
            expected_destination_epoch,
        )?;
        let payload = self.send_request(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer,
            &payload,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let raw_cluster_epoch = reader.read_u64()?;
        let cluster_epoch = ClusterEpoch::new(raw_cluster_epoch).ok_or_else(|| {
            ControlPlaneError::rpc_protocol(format!("invalid cluster epoch {raw_cluster_epoch}"))
        })?;
        reader.finish()?;
        Ok(cluster_epoch)
    }

    pub fn set_pg_acting_set_with_metadata_transfer_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        match self.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            acting_set.clone(),
            transfer,
            expected_destination_epoch,
        ) {
            Ok(cluster_epoch) => Ok(cluster_epoch),
            Err(error) if error.is_unconfirmed_control_plane_mutation() => self
                .wait_for_metadata_transfer_install_applied(
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                    &error,
                )
                .map(|runtime_map| runtime_map.cluster_epoch()),
            Err(error) => Err(error),
        }
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_with_metadata_transfer_request(
            &mut payload,
            pg_id,
            &acting_set,
            transfer,
            expected_destination_epoch,
        )?;
        let payload = self.send_request(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap,
            &payload,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    pub fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.pg_runtime_map_snapshot_with_read_timeout(
            pg_id,
            0,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )
    }

    pub fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot,
            &payload,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    pub fn pending_metadata_command_recoveries(
        &self,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PendingMetadataCommandRecoveries,
            &[],
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let listing = read_pending_metadata_command_recovery_listing(&mut reader)?;
        reader.finish()?;
        Ok(listing)
    }

    fn pg_runtime_map_snapshot_with_read_timeout(
        &self,
        pg_id: PgId,
        _authority_now_ms: u64,
        read_timeout: Duration,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PgRuntimeMapSnapshot,
            &payload,
            read_timeout,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn pg_acting_set_preflight_route(
        &self,
        pg_id: PgId,
    ) -> Result<PgActingSetPreflightRoute, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let mut last_retryable_error = None;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(last_retryable_error
                    .unwrap_or_else(|| pg_acting_set_preflight_deadline_error(pg_id)));
            }
            let attempt_deadline = (now + CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT).min(deadline);
            match self.send_request_until(
                ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                &payload,
                attempt_deadline,
            ) {
                Ok(response) => {
                    let mut reader = PayloadReader::new(&response);
                    let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                    reader.finish()?;
                    return pg_acting_set_preflight_route(runtime_map, pg_id);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get() => {
                    return Ok(PgActingSetPreflightRoute::Absent);
                }
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    last_retryable_error = Some(error);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let backoff = CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF.min(remaining);
                    if !backoff.is_zero() {
                        std::thread::sleep(backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match self.set_pg_acting_set_with_metadata_transfer_runtime_map(
            pg_id,
            acting_set.clone(),
            transfer,
            expected_destination_epoch,
        ) {
            Ok(runtime_map)
                if metadata_transfer_install_applied(
                    &runtime_map,
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                ) =>
            {
                Ok(runtime_map)
            }
            Ok(runtime_map) => Err(ControlPlaneError::RpcUnconfirmed {
                message: format!(
                    "metadata-transfer acting-set install for PG {} returned runtime map at epoch {} without the expected route/proof",
                    pg_id.get(),
                    runtime_map.cluster_epoch().get()
                ),
            }),
            Err(error) if error.is_unconfirmed_control_plane_mutation() => self
                .wait_for_metadata_transfer_install_applied(
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                    &error,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn transfer_raft_leadership_to(&self, node_id: u64) -> Result<(), ControlPlaneError> {
        let mut payload = Vec::new();
        write_u64(&mut payload, node_id);
        let payload = self.send_mutating_request_with_read_timeout(
            ControlPlaneRpcKind::TransferRaftLeadership,
            &payload,
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_admin_mutation_success(ControlPlaneRpcKind::TransferRaftLeadership, || {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            Ok(())
        })
    }

    pub fn trigger_raft_snapshot_and_purge(&self) -> Result<Option<u64>, ControlPlaneError> {
        let payload = self.send_mutating_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            &[],
            CONTROL_PLANE_RPC_SNAPSHOT_PURGE_TIMEOUT,
        )?;
        decode_admin_mutation_success(ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge, || {
            let mut reader = PayloadReader::new(&payload);
            let snapshot_index = reader.read_option_u64()?;
            reader.finish()?;
            Ok(snapshot_index)
        })
    }

    pub fn trigger_raft_election(&self) -> Result<(), ControlPlaneError> {
        let payload = self.send_mutating_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftElection,
            &[],
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_admin_mutation_success(ControlPlaneRpcKind::TriggerRaftElection, || {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            Ok(())
        })
    }
}

impl AuthenticatedUnixControlPlaneClient {
    pub fn runtime_map_diagnostics(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapDiagnostics, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapDiagnostics,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let diagnostics = read_control_plane_runtime_map_diagnostics(&mut reader)?;
        reader.finish()?;
        Ok(diagnostics)
    }

    #[must_use]
    pub fn new(inner: UnixControlPlaneClient, credential: ControlPlaneScopedCredential) -> Self {
        Self { inner, credential }
    }

    #[must_use]
    pub fn inner(&self) -> &UnixControlPlaneClient {
        &self.inner
    }

    #[must_use]
    pub fn credential(&self) -> &ControlPlaneScopedCredential {
        &self.credential
    }

    fn send_verified_request_with_endpoint_failover_until<B, V>(
        &self,
        kind: ControlPlaneRpcKind,
        deadline: Instant,
        build_payload: B,
        verify_response: V,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        B: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
        V: FnMut(&[u8]) -> Result<Vec<u8>, ControlPlaneError>,
    {
        let mut endpoint_pass = self.inner.endpoint_pass();
        self.send_verified_request_with_endpoint_pass_until(
            kind,
            deadline,
            &mut endpoint_pass,
            build_payload,
            verify_response,
        )
    }

    fn send_verified_request_with_endpoint_pass_until<B, V>(
        &self,
        kind: ControlPlaneRpcKind,
        deadline: Instant,
        endpoint_pass: &mut ControlPlaneEndpointPass,
        mut build_payload: B,
        mut verify_response: V,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        B: FnMut() -> Result<Vec<u8>, ControlPlaneError>,
        V: FnMut(&[u8]) -> Result<Vec<u8>, ControlPlaneError>,
    {
        let mut last_routing_error = None;
        while !endpoint_pass.is_exhausted() {
            let payload = build_payload()?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(last_routing_error.unwrap_or_else(|| {
                    ControlPlaneError::io(
                        "control-plane RPC endpoint failover deadline",
                        std::io::Error::new(
                            ErrorKind::TimedOut,
                            format!("{kind:?} endpoint failover deadline expired"),
                        ),
                    )
                }));
            }
            let response = self
                .inner
                .send_request_raw_response_with_endpoint_pass_until(
                    kind,
                    &payload,
                    deadline,
                    endpoint_pass,
                )?;
            let response = verify_response(&response)?;
            match decode_control_plane_rpc_response(response) {
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    last_routing_error = Some(error);
                    self.inner.prefer_next_endpoint_after_failure(endpoint_pass);
                }
                result => {
                    self.inner.prefer_successful_endpoint(endpoint_pass);
                    return result;
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    fn sign_read_only_request(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let expires_at_ms = authority_now_ms
            .checked_add(CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS)
            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
        let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
        let envelope = self.credential.sign_envelope(
            crate::control_plane_auth::ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Service(
                    crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
                ),
                operation: kind.auth_operation(),
                issued_at_ms: Some(authority_now_ms),
                expires_at_ms: Some(expires_at_ms),
                sequence: None,
                nonce: Vec::new(),
                payload,
            },
        )?;
        envelope.encode_frame()
    }

    fn sign_admin_control_plane_request(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        debug_assert!(
            kind.auth_operation() == ControlPlaneAuthOperation::AdminControlPlaneCommand
                || kind == ControlPlaneRpcKind::PgRuntimeMapSnapshot
        );
        let expires_at_ms = authority_now_ms
            .checked_add(CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS)
            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
        let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
        let envelope = self.credential.sign_envelope(
            crate::control_plane_auth::ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Service(
                    crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
                ),
                operation: ControlPlaneAuthOperation::AdminControlPlaneCommand,
                issued_at_ms: Some(authority_now_ms),
                expires_at_ms: Some(expires_at_ms),
                sequence: None,
                nonce: Vec::new(),
                payload,
            },
        )?;
        envelope.encode_frame()
    }

    fn send_admin_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        // The server signs after dispatch, so verify against a fresh receive-time wall sample.
        // Projecting the request timestamp with elapsed monotonic time diverges after a wall step.
        self.send_admin_request_until_and_clocks(
            kind,
            payload,
            Instant::now() + read_timeout,
            || Ok(authority_now_ms),
            || Ok(crate::clock::current_time_millis()),
        )
    }

    #[cfg(test)]
    fn send_admin_request_with_read_timeout_and_clock<F>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        read_timeout: Duration,
        mut authority_now_ms: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
    {
        let authority_now_ms = std::cell::RefCell::new(&mut authority_now_ms);
        self.send_admin_request_until_and_clocks(
            kind,
            payload,
            Instant::now() + read_timeout,
            || authority_now_ms.borrow_mut()(),
            || authority_now_ms.borrow_mut()(),
        )
    }

    #[cfg(test)]
    fn send_admin_request_with_read_timeout_and_clocks<R, S>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        read_timeout: Duration,
        request_authority_now_ms: R,
        response_authority_now_ms: S,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        R: FnMut() -> Result<u64, ControlPlaneError>,
        S: FnMut() -> Result<u64, ControlPlaneError>,
    {
        self.send_admin_request_until_and_clocks(
            kind,
            payload,
            Instant::now() + read_timeout,
            request_authority_now_ms,
            response_authority_now_ms,
        )
    }

    fn send_admin_request_until_and_clocks<R, S>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        deadline: Instant,
        mut request_authority_now_ms: R,
        mut response_authority_now_ms: S,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        R: FnMut() -> Result<u64, ControlPlaneError>,
        S: FnMut() -> Result<u64, ControlPlaneError>,
    {
        let mut last_routing_error = None;
        let mut endpoint_pass = self.inner.endpoint_pass();
        while !endpoint_pass.is_exhausted() {
            let request = self.sign_admin_control_plane_request(
                kind,
                request_authority_now_ms()?,
                payload.clone(),
            )?;
            let response = match self
                .inner
                .send_request_raw_response_with_endpoint_pass_until_classified(
                    kind,
                    &request,
                    deadline,
                    &mut endpoint_pass,
                ) {
                Ok(response) => response,
                Err(error) if error.request_may_have_been_sent() => {
                    return Err(classify_authenticated_admin_post_request_error(
                        kind,
                        error.into_error(),
                    ));
                }
                Err(error) => return Err(error.into_error()),
            };
            let response_now_ms = response_authority_now_ms()
                .map_err(|error| classify_authenticated_admin_post_request_error(kind, error))?;
            let response = self
                .verify_admin_control_plane_response(kind, response_now_ms, &response)
                .map_err(|error| classify_authenticated_admin_post_request_error(kind, error))?;
            let response = decode_control_plane_rpc_response_frame(response)
                .map_err(|error| classify_authenticated_admin_post_request_error(kind, error))?;
            match response {
                DecodedControlPlaneRpcResponse::Rejection(error)
                    if error.is_control_plane_leader_routing_rejection() =>
                {
                    last_routing_error = Some(error);
                    self.inner
                        .prefer_next_endpoint_after_failure(&endpoint_pass);
                }
                DecodedControlPlaneRpcResponse::Rejection(error) => {
                    self.inner.prefer_successful_endpoint(&endpoint_pass);
                    return Err(error);
                }
                DecodedControlPlaneRpcResponse::Success(payload) => {
                    self.inner.prefer_successful_endpoint(&endpoint_pass);
                    return Ok(payload);
                }
            }
        }
        Err(last_routing_error.expect("leader routing retry requires at least one endpoint"))
    }

    fn admin_pg_runtime_map_snapshot_with_read_timeout(
        &self,
        pg_id: PgId,
        retry_clock: AuthenticatedAdminRetryClock,
        read_timeout: Duration,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let deadline = Instant::now() + CONTROL_PLANE_RPC_READ_ONLY_RETRY_DEADLINE;
        let response = loop {
            match self.send_verified_request_with_endpoint_failover_until(
                ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                Instant::now() + read_timeout,
                || {
                    self.sign_admin_control_plane_request(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        payload.clone(),
                    )
                },
                |response| {
                    self.verify_admin_control_plane_response(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        response,
                    )
                },
            ) {
                Ok(response) => break response,
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        };
        let mut reader = PayloadReader::new(&response);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn pg_acting_set_preflight_route(
        &self,
        pg_id: PgId,
        retry_clock: AuthenticatedAdminRetryClock,
    ) -> Result<PgActingSetPreflightRoute, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let mut last_retryable_error = None;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(last_retryable_error
                    .unwrap_or_else(|| pg_acting_set_preflight_deadline_error(pg_id)));
            }
            let attempt_deadline = (now + CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT).min(deadline);
            match self.send_verified_request_with_endpoint_failover_until(
                ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                attempt_deadline,
                || {
                    self.sign_admin_control_plane_request(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        payload.clone(),
                    )
                },
                |response| {
                    self.verify_admin_control_plane_response(
                        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                        retry_clock.now_ms(),
                        response,
                    )
                },
            ) {
                Ok(response) => {
                    let mut reader = PayloadReader::new(&response);
                    let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                    reader.finish()?;
                    return pg_acting_set_preflight_route(runtime_map, pg_id);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get() => {
                    return Ok(PgActingSetPreflightRoute::Absent);
                }
                Err(error)
                    if error.is_retryable_read_only_rpc_transport_error()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    last_retryable_error = Some(error);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let backoff = CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF.min(remaining);
                    if !backoff.is_zero() {
                        std::thread::sleep(backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn retry_set_pg_acting_set_after_retryable_failure(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        pre_update_route: PgActingSetPreflightRoute,
        retry_clock: AuthenticatedAdminRetryClock,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_unconfirmed_message = None;
        loop {
            match self.admin_pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                retry_clock,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    let Some(route) = runtime_map
                        .pg_routes()
                        .iter()
                        .find(|route| route.pg_id() == pg_id)
                    else {
                        return Err(ControlPlaneError::rpc_protocol(format!(
                            "PG-specific runtime map response omitted requested PG {}",
                            pg_id.get()
                        )));
                    };
                    if route.acting_set() == acting_set {
                        return Ok(route.cluster_epoch());
                    }
                    let message = format!(
                        "PG {} authenticated acting-set update was not confirmed after retryable control-plane failure: current route at epoch {} has acting set {:?}, expected {:?}",
                        pg_id.get(),
                        route.cluster_epoch().get(),
                        route.acting_set(),
                        acting_set
                    );
                    match &pre_update_route {
                        PgActingSetPreflightRoute::Absent => {
                            return Err(ControlPlaneError::RpcUnconfirmed { message });
                        }
                        PgActingSetPreflightRoute::Present(before) => {
                            match pg_acting_set_retry_route_disposition(before, route) {
                                PgActingSetRetryRouteDisposition::Conflict => {
                                    return Err(ControlPlaneError::RpcUnconfirmed { message });
                                }
                                PgActingSetRetryRouteDisposition::RetryReady => {
                                    match self.set_pg_acting_set(
                                        pg_id,
                                        acting_set.to_vec(),
                                        retry_clock.now_ms(),
                                    ) {
                                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                                        Err(error)
                                            if error.is_retryable_pg_acting_set_checked_error() => {
                                        }
                                        Err(error) => return Err(error),
                                    }
                                }
                                PgActingSetRetryRouteDisposition::Wait => {}
                            }
                        }
                    }
                    last_unconfirmed_message = Some(message);
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message
                                .expect("mismatched route message was recorded"),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error)
                    if error.is_maybe_applied_control_plane_rpc_response_loss()
                        || error.is_transient_runtime_map_serving_gap() =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: last_unconfirmed_message.unwrap_or_else(|| format!(
                                "PG {} authenticated acting-set update was not confirmed after retryable control-plane failure; runtime-map observation failed: {error}",
                                pg_id.get()
                            )),
                        });
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(ControlPlaneError::UnknownPg {
                    pg_id: unknown_pg_id,
                }) if unknown_pg_id == pg_id.get()
                    && matches!(&pre_update_route, PgActingSetPreflightRoute::Absent) =>
                {
                    if Instant::now() >= deadline {
                        return Err(ControlPlaneError::RpcUnconfirmed {
                            message: format!(
                                "PG {} authenticated acting-set update was not confirmed after retryable control-plane failure: PG remains absent",
                                pg_id.get()
                            ),
                        });
                    }
                    match self.set_pg_acting_set(pg_id, acting_set.to_vec(), retry_clock.now_ms()) {
                        Ok(cluster_epoch) => return Ok(cluster_epoch),
                        Err(error) if error.is_retryable_pg_acting_set_checked_error() => {}
                        Err(error) => return Err(error),
                    }
                    std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn wait_for_metadata_transfer_install_applied(
        &self,
        pg_id: PgId,
        acting_set: &[NodeId],
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        retry_clock: AuthenticatedAdminRetryClock,
        original_error: &ControlPlaneError,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
        let mut last_observation_error = None;
        loop {
            match self.admin_pg_runtime_map_snapshot_with_read_timeout(
                pg_id,
                retry_clock,
                CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            ) {
                Ok(runtime_map) => {
                    if metadata_transfer_install_applied(
                        &runtime_map,
                        pg_id,
                        acting_set,
                        transfer,
                        expected_destination_epoch,
                    ) {
                        return Ok(runtime_map);
                    }
                    last_observation_error = Some(
                        runtime_map
                            .pg_routes()
                            .iter()
                            .find(|route| route.pg_id() == pg_id)
                            .map_or_else(
                                || {
                                    format!(
                                        "runtime map at epoch {} has no route for PG {}",
                                        runtime_map.cluster_epoch().get(),
                                        pg_id.get()
                                    )
                                },
                                |route| {
                                    format!(
                                        "runtime map at epoch {} route epoch {} state {:?} acting set {:?} transfer {:?}",
                                        runtime_map.cluster_epoch().get(),
                                        route.cluster_epoch().get(),
                                        route.state(),
                                        route.acting_set(),
                                        route.peering_metadata_transfer()
                                    )
                                },
                            ),
                    );
                }
                Err(error) => {
                    if last_observation_error.is_none() {
                        last_observation_error = Some(error.to_string());
                    }
                }
            }
            if Instant::now() >= deadline {
                let mut message = format!(
                    "metadata-transfer authenticated acting-set install for PG {} was not observable after lost control-plane RPC response: {original_error}",
                    pg_id.get()
                );
                if let Some(error) = last_observation_error {
                    message.push_str("; last runtime-map observation error: ");
                    message.push_str(&error);
                }
                return Err(ControlPlaneError::RpcUnconfirmed { message });
            }
            std::thread::sleep(CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF);
        }
    }

    fn verify_admin_control_plane_response(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::AdminControlPlaneResponse;
        let envelope =
            ControlPlaneAuthEnvelope::decode_frame(payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)?;
        let response_credential = self
            .credential
            .admin_control_plane_response_credential_for_admin()?;
        let verifier = ControlPlaneScopedCredentialStore::new(vec![response_credential])?;
        let expected_source = ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::Admin,
        };
        let expected_target =
            ControlPlaneAuthTarget::Principal(self.credential.principal().clone());
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: self.credential.cluster_id(),
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_response_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted { .. } => {
                read_authenticated_control_plane_rpc_payload(kind, envelope.payload())
            }
            ControlPlaneAuthDecision::Rejected { reason } => Err(ControlPlaneError::rpc_protocol(
                format!("control-plane admin response auth rejected: {reason:?}"),
            )),
        }
    }

    pub fn set_pg_acting_set(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        authority_now_ms: u64,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_request(&mut payload, pg_id, &acting_set)?;
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::SetPgActingSet,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(ControlPlaneRpcKind::SetPgActingSet, || {
            let mut reader = PayloadReader::new(&payload);
            let raw_cluster_epoch = reader.read_u64()?;
            let cluster_epoch = ClusterEpoch::new(raw_cluster_epoch).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(format!(
                    "invalid cluster epoch {raw_cluster_epoch}"
                ))
            })?;
            reader.finish()?;
            Ok(cluster_epoch)
        })
    }

    pub fn set_pg_acting_set_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        authority_now_ms: u64,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        self.set_pg_acting_set_checked_with_retry_clock(pg_id, acting_set, retry_clock)
    }

    fn set_pg_acting_set_checked_with_retry_clock(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        retry_clock: AuthenticatedAdminRetryClock,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        let pre_update_route = self.pg_acting_set_preflight_route(pg_id, retry_clock)?;
        match self.set_pg_acting_set(pg_id, acting_set.clone(), retry_clock.now_ms()) {
            Ok(cluster_epoch) => Ok(cluster_epoch),
            Err(error) if error.is_retryable_pg_acting_set_checked_error() => self
                .retry_set_pg_acting_set_after_retryable_failure(
                    pg_id,
                    &acting_set,
                    pre_update_route,
                    retry_clock,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
                pg_id,
                authority_now_ms,
            )?
            .into_parts()
            .0)
    }

    pub fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        retry_checked_metadata_transfer_fence(pg_id, |deadline| {
            self.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
                pg_id,
                retry_clock,
                deadline,
            )
        })
    }

    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_until(
        &self,
        pg_id: PgId,
        retry_clock: AuthenticatedAdminRetryClock,
        deadline: Instant,
    ) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_admin_request_until_and_clocks(
            ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
            payload,
            deadline,
            || Ok(retry_clock.now_ms()),
            || Ok(crate::clock::current_time_millis()),
        )?;
        let (runtime_map, source_primary_lease_deadline_ms) =
            decode_authenticated_admin_mutation_success(
                ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
                || {
                    let mut reader = PayloadReader::new(&payload);
                    let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                    let source_primary_lease_deadline_ms = reader.read_option_u64()?;
                    reader.finish()?;
                    Ok((runtime_map, source_primary_lease_deadline_ms))
                },
            )?;
        self.inner.validate_metadata_transfer_fence_response(
            pg_id,
            FencedPgMetadataTransferRuntimeMap::new(runtime_map, source_primary_lease_deadline_ms),
        )
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_acting_set_with_metadata_transfer_request(
            &mut payload,
            pg_id,
            &acting_set,
            transfer,
            expected_destination_epoch,
        )?;
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap,
            || {
                let mut reader = PayloadReader::new(&payload);
                let runtime_map = read_runtime_map_snapshot(&mut reader)?;
                reader.finish()?;
                Ok(runtime_map)
            },
        )
    }

    pub fn set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        match self.set_pg_acting_set_with_metadata_transfer_runtime_map(
            pg_id,
            acting_set.clone(),
            transfer,
            expected_destination_epoch,
            authority_now_ms,
        ) {
            Ok(runtime_map)
                if metadata_transfer_install_applied(
                    &runtime_map,
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                ) =>
            {
                Ok(runtime_map)
            }
            Ok(runtime_map) => Err(ControlPlaneError::RpcUnconfirmed {
                message: format!(
                    "metadata-transfer acting-set install for PG {} returned runtime map at epoch {} without the expected route/proof",
                    pg_id.get(),
                    runtime_map.cluster_epoch().get()
                ),
            }),
            Err(error) if error.is_unconfirmed_control_plane_mutation() => self
                .wait_for_metadata_transfer_install_applied(
                    pg_id,
                    &acting_set,
                    transfer,
                    expected_destination_epoch,
                    retry_clock,
                    &error,
                ),
            Err(error) => Err(error),
        }
    }

    pub fn set_pg_acting_set_with_metadata_transfer_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
        authority_now_ms: u64,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        self.set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
            authority_now_ms,
        )
        .map(|runtime_map| runtime_map.cluster_epoch())
    }

    pub fn transfer_raft_leadership_to(
        &self,
        node_id: u64,
        authority_now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        let mut payload = Vec::new();
        write_u64(&mut payload, node_id);
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::TransferRaftLeadership,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::TransferRaftLeadership,
            || {
                let reader = PayloadReader::new(&payload);
                reader.finish()?;
                Ok(())
            },
        )
    }

    pub fn trigger_raft_snapshot_and_purge(
        &self,
        authority_now_ms: u64,
    ) -> Result<Option<u64>, ControlPlaneError> {
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_SNAPSHOT_PURGE_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            || {
                let mut reader = PayloadReader::new(&payload);
                let snapshot_index = reader.read_option_u64()?;
                reader.finish()?;
                Ok(snapshot_index)
            },
        )
    }

    pub fn trigger_raft_election(&self, authority_now_ms: u64) -> Result<(), ControlPlaneError> {
        let payload = self.send_admin_request_with_read_timeout(
            ControlPlaneRpcKind::TriggerRaftElection,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_LEADERSHIP_TRANSFER_TIMEOUT,
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::TriggerRaftElection,
            || {
                let reader = PayloadReader::new(&payload);
                reader.finish()?;
                Ok(())
            },
        )
    }

    pub fn authority_clock_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.authority_clock_status_until(
            authority_now_ms,
            Instant::now() + CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ADMIN_TIMEOUT,
        )
    }

    fn authority_clock_status_until(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.authority_clock_status_until_with_attempt_timeout(
            authority_now_ms,
            deadline,
            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT,
        )
    }

    fn authority_clock_status_until_with_attempt_timeout(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let mut endpoint_pass = self.inner.endpoint_pass();
        self.authority_clock_status_with_endpoint_pass_until(
            authority_now_ms,
            deadline,
            attempt_timeout,
            &mut endpoint_pass,
        )
    }

    fn authority_clock_status_with_endpoint_pass_until(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
        endpoint_pass: &mut ControlPlaneEndpointPass,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        loop {
            let attempt_deadline =
                authority_clock_admin_attempt_deadline(deadline, attempt_timeout)?;
            let payload = self.send_verified_request_with_endpoint_pass_until(
                ControlPlaneRpcKind::AuthorityClockStatus,
                attempt_deadline,
                endpoint_pass,
                || {
                    self.sign_admin_control_plane_request(
                        ControlPlaneRpcKind::AuthorityClockStatus,
                        authority_now_ms,
                        Vec::new(),
                    )
                },
                |response| {
                    self.verify_admin_control_plane_response(
                        ControlPlaneRpcKind::AuthorityClockStatus,
                        crate::clock::current_time_millis(),
                        response,
                    )
                },
            )?;
            let mut reader = PayloadReader::new(&payload);
            let status = read_authority_clock_status(&mut reader)?;
            reader.finish()?;
            if status.current_raft_leadership_term().is_none()
                || status.local_raft_authority_leader()
                || endpoint_pass.is_exhausted()
            {
                return Ok(status);
            }
            self.inner.prefer_next_endpoint_after_failure(endpoint_pass);
        }
    }

    fn reestablish_authority_clock_from_status_with_attempt_timeout(
        &self,
        expected: ControlPlaneAuthorityClockStatus,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let mut payload = Vec::new();
        write_u64(&mut payload, expected.generation());
        write_option_u64(&mut payload, expected.committed_timestamp_high_water_ms());
        write_option_u64(&mut payload, expected.current_raft_leadership_term());
        let attempt_deadline = authority_clock_admin_attempt_deadline(deadline, attempt_timeout)?;
        let payload = self.send_admin_request_until_and_clocks(
            ControlPlaneRpcKind::ReestablishAuthorityClock,
            payload,
            attempt_deadline,
            || Ok(authority_now_ms),
            || Ok(crate::clock::current_time_millis()),
        )?;
        decode_authenticated_admin_mutation_success(
            ControlPlaneRpcKind::ReestablishAuthorityClock,
            || {
                let mut reader = PayloadReader::new(&payload);
                let status = read_authority_clock_status(&mut reader)?;
                reader.finish()?;
                Ok(status)
            },
        )
    }

    pub fn reestablish_authority_clock(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        self.reestablish_authority_clock_with_attempt_timeout(
            authority_now_ms,
            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT,
        )
    }

    fn reestablish_authority_clock_with_attempt_timeout(
        &self,
        authority_now_ms: u64,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let retry_clock = AuthenticatedAdminRetryClock::new(authority_now_ms);
        let retry_deadline = Instant::now() + CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ADMIN_TIMEOUT;
        'recovery: loop {
            let expected = self.retry_authority_clock_status_until(
                retry_clock.now_ms(),
                retry_deadline,
                attempt_timeout,
            )?;
            if expected.established() {
                return Ok(expected);
            }
            match self.reestablish_authority_clock_from_status_with_attempt_timeout(
                expected,
                retry_clock.now_ms(),
                retry_deadline,
                attempt_timeout,
            ) {
                Ok(status) => return Ok(status),
                Err(error) if error.is_control_plane_leader_routing_rejection() => {
                    let remaining =
                        authority_clock_admin_remaining(retry_deadline).map_err(|_| error)?;
                    std::thread::sleep(
                        CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF.min(remaining),
                    );
                }
                Err(error) if error.is_unconfirmed_control_plane_mutation() => {
                    let expected_generation = expected
                        .generation()
                        .checked_add(1)
                        .ok_or(ControlPlaneError::AuthorityClockGenerationOverflow)?;
                    let observed = self
                        .retry_authority_clock_status_until(
                            retry_clock.now_ms(),
                            retry_deadline,
                            attempt_timeout,
                        )
                        .map_err(|status_error| ControlPlaneError::RpcUnconfirmed {
                            message: format!(
                                "authority-clock re-establishment response was lost ({error}); status confirmation failed: {status_error}"
                            ),
                        })?;
                    if observed.established()
                        && observed.generation() == expected_generation
                        && observed.committed_timestamp_high_water_ms()
                            == expected.committed_timestamp_high_water_ms()
                        && observed.current_raft_leadership_term()
                            == expected.current_raft_leadership_term()
                    {
                        return Ok(observed);
                    }
                    if observed.generation() == expected.generation()
                        && !observed.established()
                        && observed.committed_timestamp_high_water_ms()
                            == expected.committed_timestamp_high_water_ms()
                        && observed.current_raft_leadership_term()
                            == expected.current_raft_leadership_term()
                    {
                        let remaining = authority_clock_admin_remaining(retry_deadline).map_err(
                            |_| ControlPlaneError::RpcUnconfirmed {
                                message: format!(
                                    "authority-clock re-establishment response was lost ({error}); status remained at the pre-operation generation until the confirmation deadline"
                                ),
                            },
                        )?;
                        std::thread::sleep(
                            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF.min(remaining),
                        );
                        // Re-establishment is a compare-and-swap over the
                        // expected generation and authority context. If a
                        // read-back proves those values are unchanged, a
                        // fresh status/mutation handshake cannot apply the
                        // operation twice and is safe after an ambiguous
                        // transport outcome.
                        continue 'recovery;
                    }
                    if observed.local_raft_authority_leader()
                        && observed.current_raft_leadership_term()
                            != expected.current_raft_leadership_term()
                    {
                        continue 'recovery;
                    }
                    return Err(ControlPlaneError::RpcUnconfirmed {
                        message: format!(
                            "authority-clock re-establishment response was lost ({error}); observed status did not confirm the expected generation and authority state"
                        ),
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn retry_authority_clock_status_until(
        &self,
        authority_now_ms: u64,
        deadline: Instant,
        attempt_timeout: Duration,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        let mut endpoint_pass = self.inner.endpoint_pass();
        loop {
            if endpoint_pass.is_exhausted() {
                endpoint_pass = self.inner.endpoint_pass();
            }
            match self.authority_clock_status_with_endpoint_pass_until(
                authority_now_ms,
                deadline,
                attempt_timeout,
                &mut endpoint_pass,
            ) {
                Ok(status) => return Ok(status),
                Err(error) if error.is_retryable_read_only_rpc_transport_error() => {
                    self.inner
                        .prefer_next_endpoint_after_failure(&endpoint_pass);
                    let remaining = authority_clock_admin_remaining(deadline).map_err(|_| error)?;
                    std::thread::sleep(
                        CONTROL_PLANE_RPC_AUTHORITY_CLOCK_RETRY_BACKOFF.min(remaining),
                    );
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn send_signed_read_only_request_with_read_timeout(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: Vec<u8>,
        read_timeout: Duration,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let start = Instant::now();
        self.send_signed_read_only_request_with_read_timeout_and_clock(
            kind,
            payload,
            read_timeout,
            || {
                let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                Ok(authority_now_ms.saturating_add(elapsed_ms))
            },
        )
    }

    fn send_signed_read_only_request_with_read_timeout_and_clock<F>(
        &self,
        kind: ControlPlaneRpcKind,
        payload: Vec<u8>,
        read_timeout: Duration,
        mut authority_now_ms: F,
    ) -> Result<Vec<u8>, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
    {
        let deadline = Instant::now() + CONTROL_PLANE_RPC_READ_ONLY_RETRY_DEADLINE;
        loop {
            let mut last_retryable_error = None;
            let mut endpoint_pass = self.inner.endpoint_pass();
            while !endpoint_pass.is_exhausted() {
                let request =
                    self.sign_read_only_request(kind, authority_now_ms()?, payload.clone())?;
                let response = self
                    .inner
                    .send_request_raw_response_with_endpoint_pass_until(
                        kind,
                        &request,
                        Instant::now() + read_timeout,
                        &mut endpoint_pass,
                    );
                let response = match response {
                    Ok(response) => response,
                    Err(error)
                        if error.is_retryable_read_only_rpc_transport_error()
                            && Instant::now() < deadline =>
                    {
                        last_retryable_error = Some(error);
                        self.inner
                            .prefer_next_endpoint_after_failure(&endpoint_pass);
                        std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let response =
                    self.verify_runtime_map_response(kind, authority_now_ms()?, &response)?;
                match decode_control_plane_rpc_response(response) {
                    Err(error) if error.is_control_plane_leader_routing_rejection() => {
                        last_retryable_error = Some(error);
                        self.inner
                            .prefer_next_endpoint_after_failure(&endpoint_pass);
                    }
                    result => {
                        self.inner.prefer_successful_endpoint(&endpoint_pass);
                        return result;
                    }
                }
            }
            let error = last_retryable_error
                .expect("authenticated read retry requires at least one retryable error");
            if Instant::now() >= deadline {
                return Err(error);
            }
            std::thread::sleep(CONTROL_PLANE_RPC_READ_ONLY_RETRY_BACKOFF);
        }
    }

    fn verify_runtime_map_response(
        &self,
        kind: ControlPlaneRpcKind,
        authority_now_ms: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::RuntimeMapResponse;
        let envelope =
            ControlPlaneAuthEnvelope::decode_frame(payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)?;
        let response_credential = match self.credential.principal() {
            ControlPlaneAuthPrincipal::Frontend { .. } => self
                .credential
                .runtime_map_response_credential_for_frontend()?,
            ControlPlaneAuthPrincipal::StorageNode { .. } => self
                .credential
                .runtime_map_response_credential_for_storage_node()?,
            _ => {
                return Err(ControlPlaneError::rpc_protocol("runtime-map response verification requires a frontend or storage-node credential"
                            .to_owned()));
            }
        };
        let verifier = ControlPlaneScopedCredentialStore::new(vec![response_credential])?;
        let expected_source = ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::RuntimeMap,
        };
        let expected_target =
            ControlPlaneAuthTarget::Principal(self.credential.principal().clone());
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: self.credential.cluster_id(),
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_response_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted { .. } => {
                read_authenticated_control_plane_rpc_payload(kind, envelope.payload())
            }
            ControlPlaneAuthDecision::Rejected { reason } => Err(ControlPlaneError::rpc_protocol(
                format!("control-plane runtime-map response auth rejected: {reason:?}"),
            )),
        }
    }
}

impl ControlPlaneStorageNodeAuthCredential {
    pub fn new(
        input: ControlPlaneStorageNodeAuthCredentialInput,
    ) -> Result<Self, ControlPlaneError> {
        let credential = Self {
            node_id: input.node_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            secret: input.secret,
        };
        credential.scoped_for_cluster_and_incarnation("validation-cluster", 1)?;
        Ok(credential)
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub fn scoped_for_cluster_and_incarnation(
        &self,
        cluster_id: &str,
        incarnation: u64,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::StorageNode {
                node_id: self.node_id,
                incarnation,
            },
            secret: self.secret.clone(),
        })
    }

    pub fn runtime_map_response_credential_for_cluster_and_incarnation(
        &self,
        cluster_id: &str,
        incarnation: u64,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        self.scoped_for_cluster_and_incarnation(cluster_id, incarnation)?
            .runtime_map_response_credential_for_storage_node()
    }
}

impl ControlPlaneFrontendAuthCredential {
    pub fn new(input: ControlPlaneFrontendAuthCredentialInput) -> Result<Self, ControlPlaneError> {
        let credential = Self {
            instance_id: input.instance_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            secret: input.secret,
        };
        credential.scoped_for_cluster("validation-cluster")?;
        Ok(credential)
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub fn scoped_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Frontend {
                instance_id: self.instance_id.clone(),
            },
            secret: self.secret.clone(),
        })
    }

    pub fn runtime_map_response_credential_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Service {
                service: ControlPlaneAuthService::RuntimeMap,
            },
            secret: self.secret.clone(),
        })
    }
}

impl ControlPlaneAdminAuthCredential {
    pub fn new(input: ControlPlaneAdminAuthCredentialInput) -> Result<Self, ControlPlaneError> {
        let credential = Self {
            instance_id: input.instance_id,
            credential_id: input.credential_id,
            credential_version: input.credential_version,
            secret: input.secret,
        };
        credential.scoped_for_cluster("validation-cluster")?;
        Ok(credential)
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }

    #[must_use]
    pub fn credential_version(&self) -> u64 {
        self.credential_version
    }

    pub fn scoped_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_id.to_owned(),
            credential_id: self.credential_id.clone(),
            credential_version: self.credential_version,
            principal: ControlPlaneAuthPrincipal::Admin {
                instance_id: self.instance_id.clone(),
            },
            secret: self.secret.clone(),
        })
    }

    pub fn admin_control_plane_response_credential_for_cluster(
        &self,
        cluster_id: &str,
    ) -> Result<ControlPlaneScopedCredential, ControlPlaneError> {
        self.scoped_for_cluster(cluster_id)?
            .admin_control_plane_response_credential_for_admin()
    }
}

fn insert_storage_node_auth_credential(
    credentials_by_node: &mut BTreeMap<NodeId, Vec<ControlPlaneStorageNodeAuthCredential>>,
    credential: ControlPlaneStorageNodeAuthCredential,
) -> Result<(), ControlPlaneError> {
    let credentials = credentials_by_node.entry(credential.node_id()).or_default();
    if credentials.iter().any(|existing| {
        existing.credential_id() == credential.credential_id()
            && existing.credential_version() == credential.credential_version()
    }) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane storage-node auth credential repeats credential identity".to_owned(),
        ));
    }
    credentials.push(credential);
    Ok(())
}

fn insert_frontend_auth_credential(
    credentials_by_instance: &mut BTreeMap<String, Vec<ControlPlaneFrontendAuthCredential>>,
    credential: ControlPlaneFrontendAuthCredential,
) -> Result<(), ControlPlaneError> {
    let credentials = credentials_by_instance
        .entry(credential.instance_id().to_owned())
        .or_default();
    if credentials.iter().any(|existing| {
        existing.credential_id() == credential.credential_id()
            && existing.credential_version() == credential.credential_version()
    }) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane frontend auth credential repeats credential identity".to_owned(),
        ));
    }
    credentials.push(credential);
    Ok(())
}

fn insert_admin_auth_credential(
    credentials_by_instance: &mut BTreeMap<String, Vec<ControlPlaneAdminAuthCredential>>,
    credential: ControlPlaneAdminAuthCredential,
) -> Result<(), ControlPlaneError> {
    let credentials = credentials_by_instance
        .entry(credential.instance_id().to_owned())
        .or_default();
    if credentials.iter().any(|existing| {
        existing.credential_id() == credential.credential_id()
            && existing.credential_version() == credential.credential_version()
    }) {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane admin auth credential repeats credential identity".to_owned(),
        ));
    }
    credentials.push(credential);
    Ok(())
}

fn matching_storage_node_auth_credential<'a>(
    credentials: &'a [ControlPlaneStorageNodeAuthCredential],
    credential_id: &str,
    credential_version: u64,
) -> Result<&'a ControlPlaneStorageNodeAuthCredential, ControlPlaneError> {
    credentials
        .iter()
        .find(|credential| {
            credential.credential_id() == credential_id
                && credential.credential_version() == credential_version
        })
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "accepted storage-node auth credential was not configured".to_owned(),
            )
        })
}

fn matching_frontend_auth_credential<'a>(
    credentials: &'a [ControlPlaneFrontendAuthCredential],
    credential_id: &str,
    credential_version: u64,
) -> Result<&'a ControlPlaneFrontendAuthCredential, ControlPlaneError> {
    credentials
        .iter()
        .find(|credential| {
            credential.credential_id() == credential_id
                && credential.credential_version() == credential_version
        })
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "accepted frontend auth credential was not configured".to_owned(),
            )
        })
}

fn matching_admin_auth_credential<'a>(
    credentials: &'a [ControlPlaneAdminAuthCredential],
    credential_id: &str,
    credential_version: u64,
) -> Result<&'a ControlPlaneAdminAuthCredential, ControlPlaneError> {
    credentials
        .iter()
        .find(|credential| {
            credential.credential_id() == credential_id
                && credential.credential_version() == credential_version
        })
        .ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "accepted admin auth credential was not configured".to_owned(),
            )
        })
}

impl ControlPlaneUnixAuthVerifier {
    pub fn new_empty(cluster_id: impl Into<String>) -> Result<Self, ControlPlaneError> {
        let cluster_id = cluster_id.into();
        if cluster_id.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane auth cluster id must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            cluster_id,
            storage_node_credentials: BTreeMap::new(),
            frontend_credentials: BTreeMap::new(),
            admin_credentials: BTreeMap::new(),
            metrics: Arc::new(ControlPlaneUnixAuthMetrics::default()),
        })
    }

    pub fn new(
        cluster_id: impl Into<String>,
        storage_node_credentials: Vec<ControlPlaneStorageNodeAuthCredential>,
    ) -> Result<Self, ControlPlaneError> {
        if storage_node_credentials.is_empty() {
            return Err(ControlPlaneError::rpc_protocol(
                "control-plane storage-node auth credential set is empty".to_owned(),
            ));
        }
        let mut verifier = Self::new_empty(cluster_id)?;
        let mut by_node = BTreeMap::new();
        for credential in storage_node_credentials {
            insert_storage_node_auth_credential(&mut by_node, credential)?;
        }
        verifier.storage_node_credentials = by_node;
        Ok(verifier)
    }

    pub fn with_frontend_credentials(
        mut self,
        frontend_credentials: Vec<ControlPlaneFrontendAuthCredential>,
    ) -> Result<Self, ControlPlaneError> {
        let mut by_instance = BTreeMap::new();
        for credential in frontend_credentials {
            insert_frontend_auth_credential(&mut by_instance, credential)?;
        }
        self.frontend_credentials = by_instance;
        Ok(self)
    }

    pub fn with_admin_credentials(
        mut self,
        admin_credentials: Vec<ControlPlaneAdminAuthCredential>,
    ) -> Result<Self, ControlPlaneError> {
        let mut by_instance = BTreeMap::new();
        for credential in admin_credentials {
            insert_admin_auth_credential(&mut by_instance, credential)?;
        }
        self.admin_credentials = by_instance;
        Ok(self)
    }

    #[cfg(test)]
    #[must_use]
    pub fn metrics_snapshot(&self) -> ControlPlaneUnixAuthMetricsSnapshot {
        self.metrics.snapshot()
    }

    #[must_use]
    pub fn status_snapshot(&self) -> ControlPlaneUnixAuthStatusSnapshot {
        let storage_node_heartbeat_required = self.requires_storage_node_heartbeat_auth();
        let frontend_runtime_map_required = self.requires_frontend_runtime_map_auth();
        let admin_control_plane_required = self.requires_admin_control_plane_auth();
        ControlPlaneUnixAuthStatusSnapshot {
            required: storage_node_heartbeat_required
                || frontend_runtime_map_required
                || admin_control_plane_required,
            storage_node_heartbeat_required,
            frontend_runtime_map_required,
            admin_control_plane_required,
            cluster_id: self.cluster_id.clone(),
            storage_node_credentials: self
                .storage_node_credentials
                .values()
                .flat_map(|credentials| {
                    credentials
                        .iter()
                        .map(|credential| ControlPlaneUnixAuthCredentialStatus {
                            node_id: credential.node_id(),
                            credential_id: credential.credential_id().to_owned(),
                            credential_version: credential.credential_version(),
                        })
                })
                .collect(),
            frontend_credentials: self
                .frontend_credentials
                .values()
                .flat_map(|credentials| {
                    credentials.iter().map(|credential| {
                        ControlPlaneUnixFrontendAuthCredentialStatus {
                            instance_id: credential.instance_id().to_owned(),
                            credential_id: credential.credential_id().to_owned(),
                            credential_version: credential.credential_version(),
                        }
                    })
                })
                .collect(),
            admin_credentials: self
                .admin_credentials
                .values()
                .flat_map(|credentials| {
                    credentials
                        .iter()
                        .map(|credential| ControlPlaneUnixAdminAuthCredentialStatus {
                            instance_id: credential.instance_id().to_owned(),
                            credential_id: credential.credential_id().to_owned(),
                            credential_version: credential.credential_version(),
                        })
                })
                .collect(),
            metrics: self.metrics.snapshot(),
        }
    }

    #[must_use]
    pub fn requires_frontend_runtime_map_auth(&self) -> bool {
        !self.frontend_credentials.is_empty()
    }

    #[must_use]
    pub fn requires_storage_node_heartbeat_auth(&self) -> bool {
        !self.storage_node_credentials.is_empty()
    }

    #[must_use]
    pub fn requires_admin_control_plane_auth(&self) -> bool {
        !self.admin_credentials.is_empty()
    }

    #[cfg(test)]
    pub fn verify_storage_node_heartbeat_request_payload(
        &self,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<Vec<u8>, ControlPlaneError> {
        self.verify_storage_node_heartbeat_payload(payload, authority_now_ms)
            .map(|verified| verified.payload)
    }

    fn verify_admin_control_plane_command_payload(
        &self,
        expected_kind: ControlPlaneRpcKind,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedAdminControlPlaneCommand, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::AdminControlPlaneCommand;
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                let reason = if control_plane_auth_payload_has_magic(payload) {
                    ControlPlaneAuthRejectionReason::Malformed
                } else {
                    ControlPlaneAuthRejectionReason::Missing
                };
                self.metrics.record_rejected(operation, reason);
                return Err(error);
            }
        };
        let expected_source = match envelope.header().source() {
            ControlPlaneAuthPrincipal::Admin { instance_id } => ControlPlaneAuthPrincipal::Admin {
                instance_id: instance_id.clone(),
            },
            _ => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                return Err(ControlPlaneError::rpc_protocol(
                    "control-plane admin command auth source is not an admin".to_owned(),
                ));
            }
        };
        let ControlPlaneAuthPrincipal::Admin { instance_id } = &expected_source else {
            unreachable!("admin source constructed above");
        };
        let Some(admin_credentials) = self.admin_credentials.get(instance_id) else {
            self.metrics.record_rejected(
                operation,
                ControlPlaneAuthRejectionReason::UnknownCredential,
            );
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane admin command auth has no credential for instance {instance_id}"
            )));
        };
        let credentials = match admin_credentials
            .iter()
            .map(|credential| credential.scoped_for_cluster(&self.cluster_id))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(credentials) => credentials,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let verifier = match ControlPlaneScopedCredentialStore::new(credentials) {
            Ok(verifier) => verifier,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let expected_target = ControlPlaneAuthTarget::Service(
            crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
        );
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: &self.cluster_id,
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted {
                credential_id,
                credential_version,
            } => {
                let payload = match read_authenticated_control_plane_rpc_payload(
                    expected_kind,
                    envelope.payload(),
                ) {
                    Ok(payload) => payload,
                    Err(error) => {
                        self.metrics
                            .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                        return Err(error);
                    }
                };
                let admin_credential = matching_admin_auth_credential(
                    admin_credentials,
                    &credential_id,
                    credential_version,
                )?;
                let response_credential = admin_credential
                    .admin_control_plane_response_credential_for_cluster(&self.cluster_id)?;
                self.metrics.record_accepted(operation);
                Ok(VerifiedAdminControlPlaneCommand {
                    payload,
                    response_credential,
                    response_target: expected_source,
                })
            }
            ControlPlaneAuthDecision::Rejected { reason } => {
                self.metrics.record_rejected(operation, reason);
                Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane admin command auth rejected: {}",
                    format_control_plane_auth_rejection(reason, &envelope, authority_now_ms)
                )))
            }
        }
    }

    fn verify_admin_runtime_map_read_payload(
        &self,
        expected_kind: ControlPlaneRpcKind,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedAdminRuntimeMapRead, ControlPlaneError> {
        let verified = self.verify_admin_control_plane_command_payload(
            expected_kind,
            payload,
            authority_now_ms,
        )?;
        Ok(VerifiedAdminRuntimeMapRead {
            payload: verified.payload,
            response_credential: verified.response_credential,
            response_target: verified.response_target,
        })
    }

    fn verify_frontend_runtime_map_read_payload(
        &self,
        expected_kind: ControlPlaneRpcKind,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedFrontendRuntimeMapRead, ControlPlaneError> {
        let operation = ControlPlaneAuthOperation::FrontendRuntimeMapRead;
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                let reason = if control_plane_auth_payload_has_magic(payload) {
                    ControlPlaneAuthRejectionReason::Malformed
                } else {
                    ControlPlaneAuthRejectionReason::Missing
                };
                self.metrics.record_rejected(operation, reason);
                return Err(error);
            }
        };
        let expected_source = match envelope.header().source() {
            ControlPlaneAuthPrincipal::Frontend { instance_id } => {
                ControlPlaneAuthPrincipal::Frontend {
                    instance_id: instance_id.clone(),
                }
            }
            _ => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                return Err(ControlPlaneError::rpc_protocol(
                    "control-plane frontend runtime-map read auth source is not a frontend"
                        .to_owned(),
                ));
            }
        };
        let ControlPlaneAuthPrincipal::Frontend { instance_id } = &expected_source else {
            unreachable!("frontend source constructed above");
        };
        let Some(frontend_credentials) = self.frontend_credentials.get(instance_id) else {
            self.metrics.record_rejected(
                operation,
                ControlPlaneAuthRejectionReason::UnknownCredential,
            );
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane frontend runtime-map read auth has no credential for instance {instance_id}"
                )));
        };
        let credentials = match frontend_credentials
            .iter()
            .map(|credential| credential.scoped_for_cluster(&self.cluster_id))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(credentials) => credentials,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let verifier = match ControlPlaneScopedCredentialStore::new(credentials) {
            Ok(verifier) => verifier,
            Err(error) => {
                self.metrics
                    .record_rejected(operation, ControlPlaneAuthRejectionReason::Malformed);
                return Err(error);
            }
        };
        let expected_target = ControlPlaneAuthTarget::Service(
            crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
        );
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: &self.cluster_id,
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: operation,
                replay_policy: control_plane_rpc_auth_replay_policy(authority_now_ms),
            },
        ) {
            ControlPlaneAuthDecision::Accepted {
                credential_id,
                credential_version,
            } => {
                let payload = match read_authenticated_control_plane_rpc_payload(
                    expected_kind,
                    envelope.payload(),
                ) {
                    Ok(payload) => payload,
                    Err(error) => {
                        self.metrics
                            .record_rejected(operation, ControlPlaneAuthRejectionReason::WrongRole);
                        return Err(error);
                    }
                };
                let frontend_credential = matching_frontend_auth_credential(
                    frontend_credentials,
                    &credential_id,
                    credential_version,
                )?;
                let response_credential = frontend_credential
                    .runtime_map_response_credential_for_cluster(&self.cluster_id)?;
                self.metrics.record_accepted(operation);
                Ok(VerifiedFrontendRuntimeMapRead {
                    payload,
                    response_credential,
                    response_target: expected_source,
                })
            }
            ControlPlaneAuthDecision::Rejected { reason } => {
                self.metrics.record_rejected(operation, reason);
                Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane frontend runtime-map read auth rejected: {}",
                    format_control_plane_auth_rejection(reason, &envelope, authority_now_ms)
                )))
            }
        }
    }

    fn verify_storage_node_heartbeat_payload(
        &self,
        payload: &[u8],
        authority_now_ms: u64,
    ) -> Result<VerifiedStorageNodeHeartbeatRefresh, ControlPlaneError> {
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                let reason = if control_plane_auth_payload_has_magic(payload) {
                    ControlPlaneAuthRejectionReason::Malformed
                } else {
                    ControlPlaneAuthRejectionReason::Missing
                };
                self.metrics
                    .record_rejected(ControlPlaneAuthOperation::StorageRuntimeMapRefresh, reason);
                return Err(error);
            }
        };
        let payload = match read_authenticated_control_plane_rpc_payload(
            ControlPlaneRpcKind::RefreshNodeHeartbeat,
            envelope.payload(),
        ) {
            Ok(payload) => payload,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::WrongRole,
                );
                return Err(error);
            }
        };
        let heartbeat = match read_node_heartbeat_payload(&payload) {
            Ok(heartbeat) => heartbeat,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(error);
            }
        };
        let expected_source = ControlPlaneAuthPrincipal::StorageNode {
            node_id: heartbeat.node_id,
            incarnation: heartbeat.node_incarnation,
        };
        let expected_target = ControlPlaneAuthTarget::Service(
            crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
        );
        let Some(node_credentials) = self.storage_node_credentials.get(&heartbeat.node_id) else {
            self.metrics.record_rejected(
                ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                ControlPlaneAuthRejectionReason::UnknownCredential,
            );
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane storage-node heartbeat auth has no credential for node {}",
                heartbeat.node_id.as_u32()
            )));
        };
        let credentials = match node_credentials
            .iter()
            .map(|credential| {
                credential.scoped_for_cluster_and_incarnation(
                    &self.cluster_id,
                    heartbeat.node_incarnation,
                )
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(credentials) => credentials,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(error);
            }
        };
        let verifier = match ControlPlaneScopedCredentialStore::new(credentials) {
            Ok(verifier) => verifier,
            Err(error) => {
                self.metrics.record_rejected(
                    ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(error);
            }
        };
        match verifier.verify_envelope(
            crate::control_plane_auth::ControlPlaneAuthVerificationInput {
                envelope: &envelope,
                expected_cluster_id: &self.cluster_id,
                expected_source: &expected_source,
                expected_target: &expected_target,
                expected_operation: ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                replay_policy: storage_node_heartbeat_auth_replay_policy(
                    &heartbeat,
                    authority_now_ms,
                ),
            },
        ) {
            ControlPlaneAuthDecision::Accepted {
                credential_id,
                credential_version,
            } => {
                let node_credential = matching_storage_node_auth_credential(
                    node_credentials,
                    &credential_id,
                    credential_version,
                )?;
                let response_credential = node_credential
                    .runtime_map_response_credential_for_cluster_and_incarnation(
                        &self.cluster_id,
                        heartbeat.node_incarnation,
                    )?;
                self.metrics
                    .record_accepted(ControlPlaneAuthOperation::StorageRuntimeMapRefresh);
                Ok(VerifiedStorageNodeHeartbeatRefresh {
                    payload,
                    response_credential,
                    response_target: expected_source,
                })
            }
            ControlPlaneAuthDecision::Rejected { reason } => {
                self.metrics
                    .record_rejected(ControlPlaneAuthOperation::StorageRuntimeMapRefresh, reason);
                Err(ControlPlaneError::rpc_protocol(format!(
                    "control-plane storage-node heartbeat auth rejected: {}",
                    format_control_plane_auth_rejection(reason, &envelope, authority_now_ms)
                )))
            }
        }
    }
}

fn storage_node_heartbeat_auth_replay_policy(
    heartbeat: &NodeHeartbeat,
    authority_now_ms: u64,
) -> ControlPlaneAuthReplayPolicy {
    ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms: authority_now_ms,
        max_window_ms: heartbeat
            .requested_lease_duration_ms
            .min(MAX_HEARTBEAT_LEASE_MS),
        allowed_future_skew_ms: CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS,
    }
}

fn control_plane_rpc_auth_replay_policy(authority_now_ms: u64) -> ControlPlaneAuthReplayPolicy {
    ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms: authority_now_ms,
        max_window_ms: CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS,
        allowed_future_skew_ms: CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS,
    }
}

fn control_plane_rpc_response_auth_replay_policy(
    authority_now_ms: u64,
) -> ControlPlaneAuthReplayPolicy {
    ControlPlaneAuthReplayPolicy::TimestampWindow {
        now_ms: authority_now_ms,
        max_window_ms: CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS,
        allowed_future_skew_ms: CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS,
    }
}

fn write_authenticated_control_plane_rpc_payload(
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Vec<u8> {
    let mut authenticated_payload = Vec::with_capacity(2 + payload.len());
    write_u16(&mut authenticated_payload, kind.as_u16());
    authenticated_payload.extend_from_slice(payload);
    authenticated_payload
}

fn read_authenticated_control_plane_rpc_payload(
    expected_kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<Vec<u8>, ControlPlaneError> {
    if payload.len() < 2 {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane authenticated RPC payload missing kind".to_owned(),
        ));
    }
    let raw_kind = u16::from_be_bytes([payload[0], payload[1]]);
    let actual_kind = ControlPlaneRpcKind::from_u16(raw_kind)?;
    if actual_kind != expected_kind {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane authenticated RPC kind {:?} did not match outer kind {:?}",
            actual_kind, expected_kind
        )));
    }
    Ok(payload[2..].to_vec())
}

fn sign_control_plane_response_payload(
    kind: ControlPlaneRpcKind,
    credential: &ControlPlaneScopedCredential,
    target: ControlPlaneAuthPrincipal,
    operation: ControlPlaneAuthOperation,
    authority_now_ms: u64,
    payload: Vec<u8>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let expires_at_ms = authority_now_ms
        .checked_add(CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS)
        .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
    let payload = write_authenticated_control_plane_rpc_payload(kind, &payload);
    let envelope =
        credential.sign_envelope(crate::control_plane_auth::ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(target),
            operation,
            issued_at_ms: Some(authority_now_ms),
            expires_at_ms: Some(expires_at_ms),
            sequence: None,
            nonce: Vec::new(),
            payload,
        })?;
    envelope.encode_frame()
}

impl ControlPlaneRuntimeMapSource for UnixControlPlaneClient {
    fn runtime_map_snapshot(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_snapshot_with_read_timeout(CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT)
    }

    fn runtime_map_status(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.runtime_map_status_with_check_applied_timeout()
    }

    fn pending_metadata_command_recoveries(
        &self,
        _authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        UnixControlPlaneClient::pending_metadata_command_recoveries(self)
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        UnixControlPlaneClient::pg_runtime_map_snapshot(self, pg_id, authority_now_ms)
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        UnixControlPlaneClient::serving_pg_runtime_map_snapshot(self, pg_id, authority_now_ms)
    }
}

impl ControlPlaneRuntimeMapSource for AuthenticatedUnixControlPlaneClient {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.runtime_map_status_with_check_applied_timeout(authority_now_ms)
    }

    fn pending_metadata_command_recoveries(
        &self,
        authority_now_ms: u64,
    ) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PendingMetadataCommandRecoveries,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let listing = read_pending_metadata_command_recovery_listing(&mut reader)?;
        reader.finish()?;
        Ok(listing)
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::PgRuntimeMapSnapshot,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut payload = Vec::new();
        write_pg_id_request(&mut payload, pg_id);
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot,
            authority_now_ms,
            payload,
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }
}

impl AuthenticatedUnixControlPlaneClient {
    pub fn runtime_map_status_with_check_applied_timeout(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        let payload = self.send_signed_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapStatus,
            authority_now_ms,
            Vec::new(),
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let status = read_runtime_map_status(&mut reader)?;
        reader.finish()?;
        Ok(status)
    }
}

impl UnixControlPlaneClient {
    pub fn runtime_map_diagnostics(
        &self,
    ) -> Result<ControlPlaneRuntimeMapDiagnostics, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapDiagnostics,
            &[],
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let diagnostics = read_control_plane_runtime_map_diagnostics(&mut reader)?;
        reader.finish()?;
        Ok(diagnostics)
    }

    fn runtime_map_snapshot_with_read_timeout(
        &self,
        read_timeout: Duration,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            &[],
            read_timeout,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }

    pub fn runtime_map_status_with_check_applied_timeout(
        &self,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.runtime_map_status_with_read_timeout(CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT)
    }

    fn runtime_map_status_with_read_timeout(
        &self,
        read_timeout: Duration,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        let payload = self.send_read_only_request_with_read_timeout(
            ControlPlaneRpcKind::RuntimeMapStatus,
            &[],
            read_timeout,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let status = read_runtime_map_status(&mut reader)?;
        reader.finish()?;
        Ok(status)
    }
}

impl ControlPlaneLinearizedRuntimeMapSource for UnixControlPlaneClient {
    fn linearized_runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.runtime_map_snapshot(authority_now_ms)
    }
}

fn metadata_transfer_install_applied(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
    transfer: PgMetadataTransferProof,
    expected_destination_epoch: ClusterEpoch,
) -> bool {
    if runtime_map.cluster_epoch() < expected_destination_epoch {
        return false;
    }
    runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .is_some_and(|route| {
            route.cluster_epoch() >= expected_destination_epoch
                && route.state() == PgState::Peering
                && route.acting_set() == acting_set
                && route.peering_metadata_transfer() == Some(transfer)
                && route.peering_metadata_transfer_destination_epoch()
                    == Some(expected_destination_epoch)
        })
}

fn unconfirmed_admin_mutation_response(
    kind: ControlPlaneRpcKind,
    error: ControlPlaneError,
) -> ControlPlaneError {
    let operation = kind
        .mutating_admin_operation()
        .expect("admin mutation response requires a mutating RPC kind");
    ControlPlaneError::RpcUnconfirmed {
        message: format!(
            "{operation} may have applied, but no valid operation result was received; \
             automatic retry requires an operation-specific confirmation predicate: {error}"
        ),
    }
}

fn unconfirmed_authenticated_admin_mutation_response(
    kind: ControlPlaneRpcKind,
    error: ControlPlaneError,
) -> ControlPlaneError {
    let operation = kind
        .mutating_admin_operation()
        .expect("authenticated admin mutation response requires a mutating RPC kind");
    ControlPlaneError::RpcUnconfirmed {
        message: format!(
            "{operation} may have applied, but no valid authenticated operation result was received; automatic retry requires an operation-specific confirmation predicate: {error}"
        ),
    }
}

fn decode_admin_mutation_success<T>(
    kind: ControlPlaneRpcKind,
    decode: impl FnOnce() -> Result<T, ControlPlaneError>,
) -> Result<T, ControlPlaneError> {
    decode().map_err(|error| unconfirmed_admin_mutation_response(kind, error))
}

fn classify_authenticated_admin_post_request_error(
    kind: ControlPlaneRpcKind,
    error: ControlPlaneError,
) -> ControlPlaneError {
    if kind.mutating_admin_operation().is_some() {
        unconfirmed_authenticated_admin_mutation_response(kind, error)
    } else {
        error
    }
}

fn decode_authenticated_admin_mutation_success<T>(
    kind: ControlPlaneRpcKind,
    decode: impl FnOnce() -> Result<T, ControlPlaneError>,
) -> Result<T, ControlPlaneError> {
    decode().map_err(|error| unconfirmed_authenticated_admin_mutation_response(kind, error))
}

fn authority_clock_admin_remaining(deadline: Instant) -> Result<Duration, ControlPlaneError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(ControlPlaneError::rpc_remote(
            "authority-clock admin operation deadline expired".to_owned(),
        ));
    }
    Ok(remaining)
}

fn authority_clock_admin_attempt_deadline(
    operation_deadline: Instant,
    attempt_timeout: Duration,
) -> Result<Instant, ControlPlaneError> {
    let remaining = authority_clock_admin_remaining(operation_deadline)?;
    Ok(Instant::now() + attempt_timeout.min(remaining))
}

fn metadata_transfer_fence_observable(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
) -> bool {
    runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .is_some_and(|route| route.state() == PgState::Peering)
}

fn retry_checked_metadata_transfer_fence(
    pg_id: PgId,
    mut attempt: impl FnMut(Instant) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError>,
) -> Result<FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
    let deadline = Instant::now() + CONTROL_PLANE_RPC_CHECK_APPLIED_DEADLINE;
    let mut last_retryable_error = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            let diagnostic = last_retryable_error
                .as_ref()
                .map_or_else(|| "no attempt completed".to_owned(), ToString::to_string);
            return Err(ControlPlaneError::RpcUnconfirmed {
                message: format!(
                    "metadata-transfer fence for PG {} was not confirmed before its retry deadline: {diagnostic}",
                    pg_id.get()
                ),
            });
        }
        let attempt_deadline = (now + CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT).min(deadline);
        match attempt(attempt_deadline) {
            Ok(fenced) => return Ok(fenced),
            Err(error)
                if error.is_unconfirmed_control_plane_mutation()
                    || error.is_retryable_read_only_rpc_transport_error()
                    || error.is_transient_runtime_map_serving_gap() =>
            {
                last_retryable_error = Some(error);
                let remaining = deadline.saturating_duration_since(Instant::now());
                let backoff = CONTROL_PLANE_RPC_CHECK_APPLIED_BACKOFF.min(remaining);
                if !backoff.is_zero() {
                    std::thread::sleep(backoff);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for UnixControlPlaneClient {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        _authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        if heartbeat.requested_lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        let payload = write_node_heartbeat_payload(&heartbeat)?;
        let retry_budget = Duration::from_millis(heartbeat.requested_lease_duration_ms);
        let payload = self.send_liveness_request(
            ControlPlaneRpcKind::RefreshNodeHeartbeat,
            &payload,
            retry_budget,
        )?;
        let mut reader = PayloadReader::new(&payload);
        let lease = read_heartbeat_lease_summary(&mut reader)?;
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        let history_reference_validation_epoch = runtime_map.cluster_epoch();
        Ok(ControlPlaneHeartbeatRefresh {
            lease,
            runtime_map,
            history_reference_validation_epoch,
        })
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for AuthenticatedUnixControlPlaneClient {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        initial_authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        // A retry must follow wall-clock corrections instead of projecting the
        // first sample forward with monotonic elapsed time.
        let mut first_attempt_now_ms = Some(initial_authority_now_ms);
        self.refresh_node_heartbeat_with_clock(heartbeat, || {
            Ok(first_attempt_now_ms
                .take()
                .unwrap_or_else(crate::clock::current_time_millis))
        })
    }
}

impl AuthenticatedUnixControlPlaneClient {
    fn refresh_node_heartbeat_with_clock<F>(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: F,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
    {
        self.refresh_node_heartbeat_with_clock_and_before_dispatch(
            heartbeat,
            authority_now_ms,
            || {},
        )
    }

    fn refresh_node_heartbeat_with_clock_and_before_dispatch<F, G>(
        &mut self,
        heartbeat: NodeHeartbeat,
        mut authority_now_ms: F,
        mut before_dispatch: G,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>
    where
        F: FnMut() -> Result<u64, ControlPlaneError>,
        G: FnMut(),
    {
        if heartbeat.requested_lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        let payload = write_node_heartbeat_payload(&heartbeat)?;
        let payload = write_authenticated_control_plane_rpc_payload(
            ControlPlaneRpcKind::RefreshNodeHeartbeat,
            &payload,
        );
        let retry_budget = Duration::from_millis(heartbeat.requested_lease_duration_ms);
        let deadline = Instant::now() + retry_budget;
        let mut endpoint_pass = self.inner.endpoint_pass();
        let payload = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ControlPlaneError::RpcUnconfirmed {
                    message: "heartbeat retry budget expired before leader routing completed"
                        .to_owned(),
                });
            }
            let response = self
                .inner
                .send_liveness_request_raw_response_with_payload_factory_until(
                    ControlPlaneRpcKind::RefreshNodeHeartbeat,
                    deadline,
                    &mut endpoint_pass,
                    || {
                        let issued_at_ms = authority_now_ms()?;
                        let expires_at_ms = issued_at_ms
                            .checked_add(heartbeat.requested_lease_duration_ms)
                            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
                        let envelope = self.credential.sign_envelope(
                            crate::control_plane_auth::ControlPlaneAuthSignInput {
                                target: ControlPlaneAuthTarget::Service(
                                    crate::control_plane_auth::ControlPlaneAuthService::ControlPlane,
                                ),
                                operation: ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
                                issued_at_ms: Some(issued_at_ms),
                                expires_at_ms: Some(expires_at_ms),
                                sequence: None,
                                nonce: Vec::new(),
                                payload: payload.clone(),
                            },
                        )?;
                        let request = envelope.encode_frame()?;
                        before_dispatch();
                        Ok(request)
                    },
                )?;
            let response = self.verify_runtime_map_response(
                ControlPlaneRpcKind::RefreshNodeHeartbeat,
                authority_now_ms()?,
                &response,
            )?;
            match decode_control_plane_rpc_response(response) {
                Err(error)
                    if error.is_control_plane_leader_routing_rejection()
                        && Instant::now() < deadline =>
                {
                    self.inner
                        .prefer_next_endpoint_after_failure(&endpoint_pass);
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let retry_sleep = CONTROL_PLANE_RPC_LIVENESS_RETRY_BACKOFF.min(remaining / 2);
                    if !retry_sleep.is_zero() {
                        std::thread::sleep(retry_sleep);
                    }
                }
                result => {
                    self.inner.prefer_successful_endpoint(&endpoint_pass);
                    break result?;
                }
            }
        };
        let mut reader = PayloadReader::new(&payload);
        let lease = read_heartbeat_lease_summary(&mut reader)?;
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        let history_reference_validation_epoch = runtime_map.cluster_epoch();
        Ok(ControlPlaneHeartbeatRefresh {
            lease,
            runtime_map,
            history_reference_validation_epoch,
        })
    }
}

#[cfg(test)]
fn handle_control_plane_unix_stream<T>(
    control_plane: &mut T,
    stream: &mut UnixStream,
    authority_now_ms: u64,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let request = read_control_plane_unix_request(stream)?;
    let response = build_control_plane_unix_response(control_plane, request, authority_now_ms)?;
    write_control_plane_unix_response(stream, response)
}

#[cfg(test)]
fn handle_control_plane_unix_stream_with_auth<T>(
    control_plane: &mut T,
    stream: &mut UnixStream,
    authority_now_ms: u64,
    auth_verifier: &ControlPlaneUnixAuthVerifier,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let request = read_control_plane_unix_request(stream)?;
    let response = build_control_plane_unix_response_with_auth(
        control_plane,
        request,
        authority_now_ms,
        Some(auth_verifier),
    )?;
    write_control_plane_unix_response(stream, response)
}

#[derive(Debug)]
struct ControlPlaneRpcRequest {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
}

impl ControlPlaneRpcRequest {
    #[must_use]
    fn metrics_kind(&self) -> observability::ControlPlaneRpcMetricKind {
        self.kind.metrics_kind()
    }
}

#[derive(Debug)]
struct ControlPlaneUnixResponseAuth {
    credential: ControlPlaneScopedCredential,
    target: ControlPlaneAuthPrincipal,
    operation: ControlPlaneAuthOperation,
}

struct VerifiedControlPlaneRpcRequest {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
    response_auth: Option<ControlPlaneUnixResponseAuth>,
}

impl VerifiedControlPlaneRpcRequest {
    #[must_use]
    fn is_refresh_node_heartbeat(&self) -> bool {
        self.kind == ControlPlaneRpcKind::RefreshNodeHeartbeat
    }

    #[must_use]
    fn is_authority_clock_admin(&self) -> bool {
        matches!(
            self.kind,
            ControlPlaneRpcKind::AuthorityClockStatus
                | ControlPlaneRpcKind::ReestablishAuthorityClock
        )
    }

    #[must_use]
    fn requires_raft_authority_confirmation(&self) -> bool {
        !matches!(
            self.kind,
            ControlPlaneRpcKind::AuthorityClockStatus | ControlPlaneRpcKind::TriggerRaftElection
        )
    }
}

#[derive(Debug)]
struct ControlPlaneRpcResponse {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct PreparedControlPlaneHeartbeatResponse {
    refresh: Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>,
    response_auth: Option<ControlPlaneUnixResponseAuth>,
}

#[cfg(test)]
fn read_control_plane_unix_request(
    stream: &mut impl std::io::Read,
) -> Result<ControlPlaneRpcRequest, ControlPlaneError> {
    let (kind, payload) = read_control_plane_rpc_frame(stream)?;
    Ok(ControlPlaneRpcRequest { kind, payload })
}

fn read_control_plane_request_with_reservation<R>(
    stream: &mut impl std::io::Read,
    reserve: impl FnOnce(usize) -> Result<R, ControlPlaneError>,
) -> Result<(ControlPlaneRpcRequest, R), ControlPlaneError> {
    let ((kind, payload), reservation) =
        read_control_plane_rpc_frame_with_reservation(stream, reserve)?;
    Ok((ControlPlaneRpcRequest { kind, payload }, reservation))
}

fn verify_control_plane_unix_request(
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    authority_now_ms: u64,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneError> {
    verify_control_plane_request(request, auth_verifier, authority_now_ms, false)
}

fn verify_control_plane_authenticated_request(
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    authority_now_ms: u64,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneError> {
    verify_control_plane_request(request, auth_verifier, authority_now_ms, true)
}

fn verify_control_plane_request(
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    authority_now_ms: u64,
    require_authentication: bool,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneError> {
    let ControlPlaneRpcRequest { kind, payload } = request;
    let (payload, response_auth) = match kind {
        _ if kind.auth_operation() == ControlPlaneAuthOperation::AdminControlPlaneCommand => {
            match auth_verifier.filter(|verifier| verifier.requires_admin_control_plane_auth()) {
                Some(auth_verifier) => {
                    let verified = auth_verifier.verify_admin_control_plane_command_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::AdminControlPlaneResponse,
                        }),
                    )
                }
                None => (payload, None),
            }
        }
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            match auth_verifier.filter(|verifier| verifier.requires_storage_node_heartbeat_auth()) {
                Some(auth_verifier) => {
                    let verified = auth_verifier
                        .verify_storage_node_heartbeat_payload(&payload, authority_now_ms)?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::RuntimeMapResponse,
                        }),
                    )
                }
                None => (payload, None),
            }
        }
        _ => {
            let auth_payload_operation = if control_plane_auth_payload_has_magic(&payload) {
                ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
                    .ok()
                    .map(|envelope| envelope.header().operation())
            } else {
                None
            };
            match auth_verifier {
                Some(auth_verifier)
                    if auth_verifier.requires_admin_control_plane_auth()
                        && auth_payload_operation
                            == Some(ControlPlaneAuthOperation::AdminControlPlaneCommand) =>
                {
                    let verified = auth_verifier.verify_admin_runtime_map_read_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::AdminControlPlaneResponse,
                        }),
                    )
                }
                Some(auth_verifier) if auth_verifier.requires_frontend_runtime_map_auth() => {
                    let verified = auth_verifier.verify_frontend_runtime_map_read_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::RuntimeMapResponse,
                        }),
                    )
                }
                Some(auth_verifier)
                    if auth_verifier.requires_admin_control_plane_auth()
                        && control_plane_auth_payload_has_magic(&payload) =>
                {
                    let verified = auth_verifier.verify_admin_runtime_map_read_payload(
                        kind,
                        &payload,
                        authority_now_ms,
                    )?;
                    (
                        verified.payload,
                        Some(ControlPlaneUnixResponseAuth {
                            credential: verified.response_credential,
                            target: verified.response_target,
                            operation: ControlPlaneAuthOperation::AdminControlPlaneResponse,
                        }),
                    )
                }
                _ => (payload, None),
            }
        }
    };
    if require_authentication && response_auth.is_none() {
        if let Some(auth_verifier) = auth_verifier {
            auth_verifier.metrics.record_rejected(
                kind.auth_operation(),
                ControlPlaneAuthRejectionReason::Missing,
            );
        }
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane RPC endpoint requires authenticated requests".to_owned(),
        ));
    }
    if matches!(
        kind,
        ControlPlaneRpcKind::AuthorityClockStatus | ControlPlaneRpcKind::ReestablishAuthorityClock
    ) && response_auth.is_none()
    {
        return Err(ControlPlaneError::rpc_protocol(
            "authority-clock administration requires configured admin authentication".to_owned(),
        ));
    }
    Ok(VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    })
}

#[cfg(test)]
fn build_control_plane_unix_response<T>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    build_control_plane_unix_response_with_auth(control_plane, request, authority_now_ms, None)
}

#[cfg(test)]
fn build_control_plane_unix_response_with_auth<T>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    build_control_plane_unix_response_with_auth_and_response_clock(
        control_plane,
        request,
        authority_now_ms,
        auth_verifier,
        || Ok(authority_now_ms),
    )
}

#[cfg(test)]
fn build_control_plane_unix_response_with_auth_and_response_clock<T, F>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let request = verify_control_plane_unix_request(request, auth_verifier, authority_now_ms)?;
    build_control_plane_unix_response_from_verified(
        control_plane,
        request,
        authority_now_ms,
        response_authority_now_ms,
    )
}

fn build_control_plane_unix_response_from_verified<T, F>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
    mut response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    } = request;
    let response = match kind {
        ControlPlaneRpcKind::RuntimeMapSnapshot => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = match control_plane.runtime_map_snapshot(authority_now_ms) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::RuntimeMapDiagnostics => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = match control_plane.runtime_map_diagnostics_snapshot(authority_now_ms) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_control_plane_runtime_map_diagnostics(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::RuntimeMapStatus => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = match control_plane.runtime_map_status(authority_now_ms) {
                Ok(status) => {
                    let mut response = Vec::new();
                    write_runtime_map_status(&mut response, status)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::PendingMetadataCommandRecoveries => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let response = control_plane
                .pending_metadata_command_recoveries(authority_now_ms)
                .and_then(|listing| {
                    let mut response = Vec::new();
                    write_pending_metadata_command_recovery_listing(&mut response, &listing)?;
                    Ok(response)
                });
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::PgRuntimeMapSnapshot => {
            let mut reader = PayloadReader::new(&payload);
            let pg_id = read_pg_id_request(&mut reader)?;
            reader.finish()?;
            let response = match control_plane.pg_runtime_map_snapshot(pg_id, authority_now_ms) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot => {
            let mut reader = PayloadReader::new(&payload);
            let pg_id = read_pg_id_request(&mut reader)?;
            reader.finish()?;
            let response =
                match control_plane.serving_pg_runtime_map_snapshot(pg_id, authority_now_ms) {
                    Ok(snapshot) => {
                        let mut response = Vec::new();
                        write_runtime_map_snapshot(&mut response, &snapshot)?;
                        Ok(response)
                    }
                    Err(error) => Err(error),
                };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            debug_assert_eq!(
                kind.auth_operation(),
                ControlPlaneAuthOperation::StorageRuntimeMapRefresh
            );
            let mut reader = PayloadReader::new(&payload);
            let heartbeat = read_node_heartbeat(&mut reader)?;
            reader.finish()?;
            let response = match control_plane.refresh_node_heartbeat(heartbeat, authority_now_ms) {
                Ok(refresh) => {
                    let mut response = Vec::new();
                    write_heartbeat_lease_summary(&mut response, refresh.lease());
                    write_runtime_map_snapshot(&mut response, refresh.runtime_map())?;
                    Ok(response)
                }
                Err(error) => Err(error),
            };
            let response_authority_now_ms = response_authority_now_ms()?;
            return build_control_plane_verified_response(
                kind,
                response,
                response_auth,
                response_authority_now_ms,
            );
        }
        ControlPlaneRpcKind::SetPgActingSet => {
            let mut reader = PayloadReader::new(&payload);
            let (pg_id, acting_set) = read_pg_acting_set_request(&mut reader)?;
            reader.finish()?;
            match control_plane.set_pg_acting_set(pg_id, acting_set) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_u64(&mut response, snapshot.cluster_epoch().get());
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap => {
            let mut reader = PayloadReader::new(&payload);
            let pg_id = read_pg_id_request(&mut reader)?;
            reader.finish()?;
            match control_plane
                .fence_pg_for_metadata_transfer_with_source_lease(pg_id)
                .and_then(|fenced| {
                    let (snapshot, source_primary_lease_deadline_ms) = fenced.into_parts();
                    snapshot
                        .reconstructed_runtime_map_for_pg_with_fallback_validity(
                            pg_id,
                            non_serving_runtime_map_validity(authority_now_ms),
                        )
                        .map(|runtime_map| (runtime_map, source_primary_lease_deadline_ms))
                }) {
                Ok((snapshot, source_primary_lease_deadline_ms)) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    write_option_u64(&mut response, source_primary_lease_deadline_ms);
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer => {
            let mut reader = PayloadReader::new(&payload);
            let (pg_id, acting_set, transfer, expected_destination_epoch) =
                read_pg_acting_set_with_metadata_transfer_request(&mut reader)?;
            reader.finish()?;
            match control_plane.set_pg_acting_set_with_metadata_transfer(
                pg_id,
                acting_set,
                transfer,
                expected_destination_epoch,
            ) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_u64(&mut response, snapshot.cluster_epoch().get());
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap => {
            let mut reader = PayloadReader::new(&payload);
            let (pg_id, acting_set, transfer, expected_destination_epoch) =
                read_pg_acting_set_with_metadata_transfer_request(&mut reader)?;
            reader.finish()?;
            match control_plane
                .set_pg_acting_set_with_metadata_transfer(
                    pg_id,
                    acting_set,
                    transfer,
                    expected_destination_epoch,
                )
                .and_then(|snapshot| {
                    snapshot.reconstructed_runtime_map_for_pg_with_fallback_validity(
                        pg_id,
                        non_serving_runtime_map_validity(authority_now_ms),
                    )
                }) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::TransferRaftLeadership => {
            let mut reader = PayloadReader::new(&payload);
            let node_id = reader.read_u64()?;
            reader.finish()?;
            match control_plane.transfer_raft_leadership_to(node_id) {
                Ok(()) => Ok(Vec::new()),
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            match control_plane.trigger_raft_snapshot_and_purge() {
                Ok(snapshot_index) => {
                    let mut response = Vec::new();
                    write_option_u64(&mut response, snapshot_index);
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::TriggerRaftElection => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            match control_plane.trigger_raft_election() {
                Ok(()) => Ok(Vec::new()),
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::AuthorityClockStatus
        | ControlPlaneRpcKind::ReestablishAuthorityClock => {
            return Err(ControlPlaneError::rpc_protocol(
                "authority-clock admin RPC requires the process-local clock handler".to_owned(),
            ));
        }
    };
    build_control_plane_verified_response(
        kind,
        response,
        response_auth,
        response_authority_now_ms()?,
    )
}

fn build_control_plane_unix_admission_error_response(
    request: VerifiedControlPlaneRpcRequest,
    error: ControlPlaneError,
    authority_now_ms: u64,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError> {
    build_control_plane_verified_response(
        request.kind,
        Err(error),
        request.response_auth,
        authority_now_ms,
    )
}

fn build_control_plane_verified_response(
    kind: ControlPlaneRpcKind,
    response: Result<Vec<u8>, ControlPlaneError>,
    response_auth: Option<ControlPlaneUnixResponseAuth>,
    authority_now_ms: u64,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError> {
    let mut payload = encode_control_plane_rpc_response(response)?;
    if let Some(response_auth) = response_auth {
        payload = sign_control_plane_response_payload(
            kind,
            &response_auth.credential,
            response_auth.target,
            response_auth.operation,
            authority_now_ms,
            payload,
        )?;
    }
    Ok(ControlPlaneRpcResponse { kind, payload })
}

#[cfg(test)]
fn build_control_plane_authority_clock_admin_response<T, P, F>(
    control_plane: &T,
    authority_clock: &mut ControlPlaneAuthorityClock,
    request: ControlPlaneRpcRequest,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
    sample: ControlPlaneAuthorityClockAdminSample,
    before_response_sign: P,
    response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin,
    P: FnOnce(&T, &mut ControlPlaneAuthorityClock) -> Result<(), ControlPlaneError>,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let request =
        verify_control_plane_unix_request(request, auth_verifier, sample.auth_authority_now_ms)?;
    build_control_plane_authority_clock_admin_response_from_verified(
        control_plane,
        authority_clock,
        request,
        sample,
        before_response_sign,
        response_authority_now_ms,
    )
}

#[cfg(test)]
fn build_control_plane_authority_clock_admin_response_from_verified<T, P, F>(
    control_plane: &T,
    authority_clock: &mut ControlPlaneAuthorityClock,
    request: VerifiedControlPlaneRpcRequest,
    sample: ControlPlaneAuthorityClockAdminSample,
    before_response_sign: P,
    response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneAdmin,
    P: FnOnce(&T, &mut ControlPlaneAuthorityClock) -> Result<(), ControlPlaneError>,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let context = control_plane.authority_clock_context();
    build_control_plane_authority_clock_admin_response_from_verified_with_context(
        authority_clock,
        request,
        sample,
        context,
        |_, authority_clock| before_response_sign(control_plane, authority_clock),
        response_authority_now_ms,
    )
}

fn build_control_plane_authority_clock_admin_response_from_verified_with_context<P, F>(
    authority_clock: &mut ControlPlaneAuthorityClock,
    request: VerifiedControlPlaneRpcRequest,
    sample: ControlPlaneAuthorityClockAdminSample,
    context: Result<ControlPlaneAuthorityClockContext, ControlPlaneError>,
    before_response_sign: P,
    mut response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    P: FnOnce(
        ControlPlaneAuthorityClockContext,
        &mut ControlPlaneAuthorityClock,
    ) -> Result<(), ControlPlaneError>,
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    } = request;
    if !matches!(
        kind,
        ControlPlaneRpcKind::AuthorityClockStatus | ControlPlaneRpcKind::ReestablishAuthorityClock
    ) {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "expected authority-clock admin RPC, got {kind:?}"
        )));
    }
    let Some(response_auth) = response_auth else {
        return Err(ControlPlaneError::rpc_protocol(
            "authority-clock administration requires configured admin authentication".to_owned(),
        ));
    };
    let mut successful_reestablishment_context = None;
    let response = match kind {
        ControlPlaneRpcKind::AuthorityClockStatus => context.and_then(|context| {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            let mut response = Vec::new();
            let status =
                authority_clock.observe_status(context, sample.wall_ms, sample.clock_health_ms)?;
            write_authority_clock_status(&mut response, status);
            Ok(response)
        }),
        ControlPlaneRpcKind::ReestablishAuthorityClock => context.and_then(|context| {
            let mut reader = PayloadReader::new(&payload);
            let expected_generation = reader.read_u64()?;
            let expected_committed_timestamp_high_water_ms = reader.read_option_u64()?;
            let expected_raft_leadership_term = reader.read_option_u64()?;
            reader.finish()?;
            let status = authority_clock.reestablish(
                expected_generation,
                expected_committed_timestamp_high_water_ms,
                expected_raft_leadership_term,
                context,
                sample.wall_ms,
                sample.clock_health_ms,
            )?;
            successful_reestablishment_context = Some(context);
            let mut response = Vec::new();
            write_authority_clock_status(&mut response, status);
            Ok(response)
        }),
        _ => unreachable!("authority-clock RPC kind checked above"),
    };
    if kind == ControlPlaneRpcKind::ReestablishAuthorityClock && response.is_ok() {
        before_response_sign(
            successful_reestablishment_context
                .expect("successful authority-clock response requires a valid context"),
            authority_clock,
        )?;
    }
    let response_authority_now_ms = response_authority_now_ms()?;
    let payload = sign_control_plane_response_payload(
        kind,
        &response_auth.credential,
        response_auth.target,
        response_auth.operation,
        response_authority_now_ms,
        encode_control_plane_rpc_response(response)?,
    )?;
    Ok(ControlPlaneRpcResponse { kind, payload })
}

fn write_control_plane_unix_response(
    stream: &mut impl std::io::Write,
    response: ControlPlaneRpcResponse,
) -> Result<(), ControlPlaneError> {
    write_control_plane_rpc_frame(stream, response.kind, &response.payload)
}

#[cfg(test)]
fn respond_control_plane_unix_request<T>(
    control_plane: &mut T,
    stream: &mut impl std::io::Write,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneAdmin + ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let response = build_control_plane_unix_response(control_plane, request, authority_now_ms)?;
    write_control_plane_unix_response(stream, response)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRpcKind {
    RuntimeMapSnapshot = 1,
    RefreshNodeHeartbeat = 2,
    SetPgActingSet = 3,
    SetPgActingSetWithMetadataTransfer = 4,
    SetPgActingSetWithMetadataTransferRuntimeMap = 6,
    FencePgForMetadataTransferRuntimeMap = 7,
    TransferRaftLeadership = 8,
    PgRuntimeMapSnapshot = 9,
    TriggerRaftSnapshotAndPurge = 10,
    TriggerRaftElection = 11,
    RuntimeMapStatus = 12,
    PendingMetadataCommandRecoveries = 13,
    AuthorityClockStatus = 14,
    ReestablishAuthorityClock = 15,
    RuntimeMapDiagnostics = 16,
    ServingPgRuntimeMapSnapshot = 17,
}

impl ControlPlaneRpcKind {
    #[cfg(test)]
    const ALL: [Self; 16] = [
        Self::RuntimeMapSnapshot,
        Self::RefreshNodeHeartbeat,
        Self::SetPgActingSet,
        Self::SetPgActingSetWithMetadataTransfer,
        Self::SetPgActingSetWithMetadataTransferRuntimeMap,
        Self::FencePgForMetadataTransferRuntimeMap,
        Self::TransferRaftLeadership,
        Self::PgRuntimeMapSnapshot,
        Self::TriggerRaftSnapshotAndPurge,
        Self::TriggerRaftElection,
        Self::RuntimeMapStatus,
        Self::PendingMetadataCommandRecoveries,
        Self::AuthorityClockStatus,
        Self::ReestablishAuthorityClock,
        Self::RuntimeMapDiagnostics,
        Self::ServingPgRuntimeMapSnapshot,
    ];

    fn as_u16(self) -> u16 {
        self as u16
    }

    fn from_u16(value: u16) -> Result<Self, ControlPlaneError> {
        match value {
            1 => Ok(Self::RuntimeMapSnapshot),
            2 => Ok(Self::RefreshNodeHeartbeat),
            3 => Ok(Self::SetPgActingSet),
            4 => Ok(Self::SetPgActingSetWithMetadataTransfer),
            6 => Ok(Self::SetPgActingSetWithMetadataTransferRuntimeMap),
            7 => Ok(Self::FencePgForMetadataTransferRuntimeMap),
            8 => Ok(Self::TransferRaftLeadership),
            9 => Ok(Self::PgRuntimeMapSnapshot),
            10 => Ok(Self::TriggerRaftSnapshotAndPurge),
            11 => Ok(Self::TriggerRaftElection),
            12 => Ok(Self::RuntimeMapStatus),
            13 => Ok(Self::PendingMetadataCommandRecoveries),
            14 => Ok(Self::AuthorityClockStatus),
            15 => Ok(Self::ReestablishAuthorityClock),
            16 => Ok(Self::RuntimeMapDiagnostics),
            17 => Ok(Self::ServingPgRuntimeMapSnapshot),
            _ => Err(ControlPlaneError::rpc_protocol(format!(
                "unknown control-plane RPC kind {value}"
            ))),
        }
    }

    fn auth_operation(self) -> ControlPlaneAuthOperation {
        match self {
            Self::RuntimeMapSnapshot
            | Self::PgRuntimeMapSnapshot
            | Self::ServingPgRuntimeMapSnapshot
            | Self::RuntimeMapStatus
            | Self::RuntimeMapDiagnostics
            | Self::PendingMetadataCommandRecoveries => {
                ControlPlaneAuthOperation::FrontendRuntimeMapRead
            }
            Self::RefreshNodeHeartbeat => ControlPlaneAuthOperation::StorageRuntimeMapRefresh,
            Self::SetPgActingSet
            | Self::SetPgActingSetWithMetadataTransfer
            | Self::SetPgActingSetWithMetadataTransferRuntimeMap
            | Self::FencePgForMetadataTransferRuntimeMap
            | Self::TransferRaftLeadership
            | Self::TriggerRaftSnapshotAndPurge
            | Self::TriggerRaftElection
            | Self::AuthorityClockStatus
            | Self::ReestablishAuthorityClock => {
                ControlPlaneAuthOperation::AdminControlPlaneCommand
            }
        }
    }

    fn mutating_admin_operation(self) -> Option<&'static str> {
        match self {
            Self::SetPgActingSet => Some("control-plane PG acting-set update"),
            Self::SetPgActingSetWithMetadataTransfer => {
                Some("control-plane PG metadata-transfer acting-set update")
            }
            Self::SetPgActingSetWithMetadataTransferRuntimeMap => {
                Some("control-plane PG metadata-transfer acting-set update")
            }
            Self::FencePgForMetadataTransferRuntimeMap => {
                Some("control-plane PG metadata-transfer fence")
            }
            Self::TransferRaftLeadership => Some("control-plane Raft leadership transfer"),
            Self::TriggerRaftSnapshotAndPurge => Some("control-plane Raft snapshot/purge trigger"),
            Self::TriggerRaftElection => Some("control-plane Raft election trigger"),
            Self::ReestablishAuthorityClock => {
                Some("control-plane authority-clock re-establishment")
            }
            Self::RuntimeMapSnapshot
            | Self::RefreshNodeHeartbeat
            | Self::PgRuntimeMapSnapshot
            | Self::RuntimeMapStatus
            | Self::PendingMetadataCommandRecoveries
            | Self::AuthorityClockStatus
            | Self::RuntimeMapDiagnostics
            | Self::ServingPgRuntimeMapSnapshot => None,
        }
    }

    fn metrics_kind(self) -> observability::ControlPlaneRpcMetricKind {
        use observability::ControlPlaneRpcMetricKind as MetricKind;

        match self {
            Self::RuntimeMapSnapshot => MetricKind::RuntimeMapSnapshot,
            Self::RefreshNodeHeartbeat => MetricKind::RefreshNodeHeartbeat,
            Self::SetPgActingSet => MetricKind::SetPgActingSet,
            Self::SetPgActingSetWithMetadataTransfer => {
                MetricKind::SetPgActingSetWithMetadataTransfer
            }
            Self::SetPgActingSetWithMetadataTransferRuntimeMap => {
                MetricKind::SetPgActingSetWithMetadataTransferRuntimeMap
            }
            Self::FencePgForMetadataTransferRuntimeMap => {
                MetricKind::FencePgForMetadataTransferRuntimeMap
            }
            Self::TransferRaftLeadership => MetricKind::TransferRaftLeadership,
            Self::PgRuntimeMapSnapshot => MetricKind::PgRuntimeMapSnapshot,
            Self::ServingPgRuntimeMapSnapshot => MetricKind::ServingPgRuntimeMapSnapshot,
            Self::TriggerRaftSnapshotAndPurge => MetricKind::TriggerRaftSnapshotAndPurge,
            Self::TriggerRaftElection => MetricKind::TriggerRaftElection,
            Self::RuntimeMapStatus => MetricKind::RuntimeMapStatus,
            Self::PendingMetadataCommandRecoveries => MetricKind::PendingMetadataCommandRecoveries,
            Self::AuthorityClockStatus => MetricKind::AuthorityClockStatus,
            Self::ReestablishAuthorityClock => MetricKind::ReestablishAuthorityClock,
            Self::RuntimeMapDiagnostics => MetricKind::RuntimeMapDiagnostics,
        }
    }
}

#[cfg(test)]
fn prepare_control_plane_heartbeat_response<T>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
    auth_verifier: Option<&ControlPlaneUnixAuthVerifier>,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    let request = verify_control_plane_unix_request(request, auth_verifier, authority_now_ms)?;
    prepare_control_plane_heartbeat_response_from_verified(control_plane, request, authority_now_ms)
}

fn prepare_control_plane_heartbeat_response_from_verified<T>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    prepare_control_plane_heartbeat_response_internal(
        control_plane,
        request,
        authority_now_ms,
        None,
    )
}

fn prepare_control_plane_heartbeat_response_with_lease_horizon_authority_from_verified<T>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
    lease_horizon_authority: LeaseHorizonAuthorityBinding,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    prepare_control_plane_heartbeat_response_internal(
        control_plane,
        request,
        authority_now_ms,
        Some(lease_horizon_authority),
    )
}

fn prepare_control_plane_heartbeat_response_internal<T>(
    control_plane: &mut T,
    request: VerifiedControlPlaneRpcRequest,
    authority_now_ms: u64,
    lease_horizon_authority: Option<LeaseHorizonAuthorityBinding>,
) -> Result<PreparedControlPlaneHeartbeatResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource,
{
    let VerifiedControlPlaneRpcRequest {
        kind,
        payload,
        response_auth,
    } = request;
    if kind != ControlPlaneRpcKind::RefreshNodeHeartbeat {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "expected RefreshNodeHeartbeat RPC, got {kind:?}"
        )));
    }
    debug_assert_eq!(
        kind.auth_operation(),
        ControlPlaneAuthOperation::StorageRuntimeMapRefresh
    );
    let mut reader = PayloadReader::new(&payload);
    let heartbeat = read_node_heartbeat(&mut reader)?;
    reader.finish()?;
    let history_reference_summary = heartbeat.cluster_map_history_route_references.summary();
    let mut history_reference_sample = observability::ControlPlaneHistoryReferenceSample {
        node_id: heartbeat.node_id.as_u32(),
        observed_epoch: heartbeat.observed_epoch.get(),
        validation_epoch: heartbeat.observed_epoch.get(),
        observed_at_ms: authority_now_ms,
        oldest_live_placement_epoch: history_reference_summary
            .oldest_live_placement_epoch
            .map(ClusterEpoch::get),
        oldest_durable_backfill_epoch: history_reference_summary
            .oldest_durable_backfill_epoch
            .map(ClusterEpoch::get),
        oldest_pending_metadata_command_epoch: history_reference_summary
            .oldest_pending_metadata_command_epoch
            .map(ClusterEpoch::get),
        oldest_object_payload_reclaim_claim_epoch: history_reference_summary
            .oldest_object_payload_reclaim_claim_epoch
            .map(ClusterEpoch::get),
    };
    let refresh = match lease_horizon_authority {
        Some(lease_horizon_authority) => control_plane
            .refresh_node_heartbeat_with_lease_horizon_authority(
                heartbeat,
                authority_now_ms,
                lease_horizon_authority,
            ),
        None => control_plane.refresh_node_heartbeat(heartbeat, authority_now_ms),
    };
    if let Ok(refresh) = &refresh {
        history_reference_sample.validation_epoch =
            refresh.history_reference_validation_epoch.get();
        observability::record_control_plane_history_reference_sample(history_reference_sample);
    }
    Ok(PreparedControlPlaneHeartbeatResponse {
        refresh,
        response_auth,
    })
}

fn finish_control_plane_heartbeat_response<F>(
    prepared: PreparedControlPlaneHeartbeatResponse,
    mut response_authority_now_ms: F,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    F: FnMut() -> Result<u64, ControlPlaneError>,
{
    let response = match prepared.refresh {
        Ok(refresh) => {
            let mut response = Vec::new();
            write_heartbeat_lease_summary(&mut response, refresh.lease());
            write_runtime_map_snapshot(&mut response, refresh.runtime_map())?;
            Ok(response)
        }
        Err(error) => Err(error),
    };
    let response_authority_now_ms = response_authority_now_ms()?;
    build_control_plane_verified_response(
        ControlPlaneRpcKind::RefreshNodeHeartbeat,
        response,
        prepared.response_auth,
        response_authority_now_ms,
    )
}

/// Opaque failure returned when a control-plane server can no longer accept
/// connections. The concrete transport diagnostic is logged inside storage.
pub(crate) struct ControlPlaneRpcServerError;

impl std::fmt::Debug for ControlPlaneRpcServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ControlPlaneRpcServerError")
    }
}

impl std::fmt::Display for ControlPlaneRpcServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("control-plane RPC server stopped accepting connections")
    }
}

impl std::error::Error for ControlPlaneRpcServerError {}

enum ControlPlaneRpcServerAuthority<T> {
    Shared(Arc<Mutex<T>>),
    PerWorker(T),
}

impl<T> ControlPlaneRpcServerAuthority<T> {
    fn with_mut<R>(
        &mut self,
        metrics_kind: observability::ControlPlaneRpcMetricKind,
        operation: impl FnOnce(&mut T) -> R,
    ) -> R {
        match self {
            Self::Shared(authority) => {
                let lock_started = Instant::now();
                let mut authority = authority
                    .lock()
                    .expect("control-plane authority mutex poisoned");
                observability::record_control_plane_rpc_lock_wait(
                    metrics_kind,
                    lock_started.elapsed(),
                );
                operation(&mut authority)
            }
            Self::PerWorker(authority) => {
                observability::record_control_plane_rpc_lock_wait(metrics_kind, Duration::ZERO);
                operation(authority)
            }
        }
    }
}

enum ControlPlaneRpcAdmissionFailure {
    Unauthenticated(Box<ControlPlaneError>),
    Authenticated {
        request: Box<VerifiedControlPlaneRpcRequest>,
        error: Box<ControlPlaneError>,
    },
}

fn authenticate_and_admit_control_plane_rpc(
    request: ControlPlaneRpcRequest,
    policy: &ControlPlaneRpcServerPolicy,
    require_authentication: bool,
    authority_now_ms: u64,
) -> Result<VerifiedControlPlaneRpcRequest, ControlPlaneRpcAdmissionFailure> {
    let verify = if require_authentication {
        verify_control_plane_authenticated_request
    } else {
        verify_control_plane_unix_request
    };
    let request = verify(request, policy.auth_verifier.as_deref(), authority_now_ms)
        .map_err(|error| ControlPlaneRpcAdmissionFailure::Unauthenticated(Box::new(error)))?;
    if !policy.role.accepts(&request) {
        let error = ControlPlaneError::rpc_protocol(match policy.role {
            ControlPlaneRpcServerRole::Ordinary => {
                "authority-clock administration requires the dedicated recovery endpoint".to_owned()
            }
            ControlPlaneRpcServerRole::AuthorityClockRecovery => {
                "dedicated authority-clock recovery endpoint rejects ordinary control-plane RPCs"
                    .to_owned()
            }
        });
        return Err(ControlPlaneRpcAdmissionFailure::Authenticated {
            request: Box::new(request),
            error: Box::new(error),
        });
    }
    if request.requires_raft_authority_confirmation() {
        if let Some(confirm) = &policy.authority_confirmation {
            if let Err(error) = confirm() {
                return Err(ControlPlaneRpcAdmissionFailure::Authenticated {
                    request: Box::new(request),
                    error: Box::new(error),
                });
            }
        }
    }
    Ok(request)
}

trait ControlPlaneRpcServerStream: std::io::Read + std::io::Write + Send {
    fn begin_response(&mut self, timeout: Duration);
    fn finish_response(&mut self) -> std::io::Result<()>;
}

impl ControlPlaneRpcServerStream for ControlPlaneDeadlineUnixSocket {
    fn begin_response(&mut self, timeout: Duration) {
        self.set_deadline(Instant::now() + timeout);
    }

    fn finish_response(&mut self) -> std::io::Result<()> {
        self.flush()
    }
}

impl ControlPlaneRpcServerStream
    for rustls::StreamOwned<rustls::ServerConnection, ControlPlaneDeadlineTcpSocket>
{
    fn begin_response(&mut self, timeout: Duration) {
        self.sock.set_deadline(Instant::now() + timeout);
    }

    fn finish_response(&mut self) -> std::io::Result<()> {
        self.conn.send_close_notify();
        self.flush()
    }
}

struct ControlPlaneRpcWorkerGuard {
    active_workers: Arc<AtomicUsize>,
}

impl Drop for ControlPlaneRpcWorkerGuard {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ControlPlaneRpcServerListener {
    /// Serves this listener using one shared, mutex-protected authority.
    ///
    /// This method blocks until the listener encounters an unrecoverable
    /// accept error. Individual connection failures remain isolated to their
    /// worker.
    pub(crate) fn serve_shared<T>(
        self,
        authority: Arc<Mutex<T>>,
        policy: ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        self.serve_with(
            move || ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
            policy,
        )
    }

    /// Serves this listener with an independently cloned authority per worker.
    pub(crate) fn serve_cloned<T>(
        self,
        authority: T,
        policy: ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: Clone
            + ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        self.serve_with(
            move || ControlPlaneRpcServerAuthority::PerWorker(authority.clone()),
            policy,
        )
    }

    /// Serves a bounded sequence of requests for a cross-crate process test.
    ///
    /// This test-only facility retains the complete production facade while
    /// allowing deterministic authority timestamps and a joinable server.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn serve_shared_requests_for_test<T>(
        self,
        authority: Arc<Mutex<T>>,
        mut policy: ControlPlaneRpcServerPolicy,
        authority_times_ms: impl IntoIterator<Item = u64>,
        mut after_request: impl FnMut(&T),
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let configure_result = match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => listener.set_nonblocking(false),
            ControlPlaneRpcServerListenerKind::TlsTcp { listener, .. } => {
                listener.set_nonblocking(false)
            }
        };
        if let Err(error) = configure_result {
            eprintln!("control-plane RPC test listener setup failed: {error}");
            return Err(ControlPlaneRpcServerError);
        }
        for authority_now_ms in authority_times_ms {
            policy.test_authority_now_ms = Some(authority_now_ms);
            self.accept_one(
                &|| ControlPlaneRpcServerAuthority::Shared(Arc::clone(&authority)),
                &policy,
            )?;
            while policy.active_workers() != 0 {
                std::thread::yield_now();
            }
            let authority = authority
                .lock()
                .expect("control-plane test authority mutex poisoned");
            after_request(&authority);
        }
        Ok(())
    }

    /// Injects one pre-dispatch connection loss, then serves bounded requests.
    #[cfg(feature = "test-hooks")]
    pub(crate) fn serve_shared_requests_after_dropped_connection_for_test<T>(
        self,
        authority: Arc<Mutex<T>>,
        policy: ControlPlaneRpcServerPolicy,
        authority_times_ms: impl IntoIterator<Item = u64>,
        after_request: impl FnMut(&T),
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => listener.accept().map(drop),
            ControlPlaneRpcServerListenerKind::TlsTcp { listener, .. } => {
                listener.accept().map(drop)
            }
        }
        .map_err(|error| {
            eprintln!("control-plane RPC test connection-loss injection failed: {error}");
            ControlPlaneRpcServerError
        })?;
        self.serve_shared_requests_for_test(authority, policy, authority_times_ms, after_request)
    }

    fn serve_with<T>(
        self,
        authority: impl Fn() -> ControlPlaneRpcServerAuthority<T>,
        policy: ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let configure_result = match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => listener.set_nonblocking(false),
            ControlPlaneRpcServerListenerKind::TlsTcp { listener, .. } => {
                listener.set_nonblocking(false)
            }
        };
        if let Err(error) = configure_result {
            eprintln!("control-plane RPC listener setup failed: {error}");
            return Err(ControlPlaneRpcServerError);
        }
        loop {
            self.accept_one(&authority, &policy)?;
        }
    }

    fn accept_one<T>(
        &self,
        authority: &impl Fn() -> ControlPlaneRpcServerAuthority<T>,
        policy: &ControlPlaneRpcServerPolicy,
    ) -> Result<(), ControlPlaneRpcServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let worker_limit = policy.resources.worker_limit.min(self.max_connections);
        match &self.kind {
            ControlPlaneRpcServerListenerKind::Unix(listener) => match listener.accept() {
                Ok((stream, _)) => spawn_control_plane_rpc_server_worker(
                    stream,
                    authority(),
                    policy.clone(),
                    policy.authentication_required(),
                    self.max_frame_bytes,
                    worker_limit,
                    self.io_timeout,
                    move |stream, deadline| {
                        stream.set_nonblocking(false).map_err(|source| {
                            ControlPlaneError::io(
                                "set control-plane Unix RPC blocking mode",
                                source,
                            )
                        })?;
                        ControlPlaneDeadlineUnixSocket::new(
                            stream,
                            deadline,
                            CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
                        )
                        .map(|stream| Box::new(stream) as Box<dyn ControlPlaneRpcServerStream>)
                        .map_err(|source| {
                            ControlPlaneError::io(
                                "configure control-plane Unix RPC deadline I/O",
                                source,
                            )
                        })
                    },
                ),
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => {
                    eprintln!("control-plane Unix socket accept failed: {error}");
                    return Err(ControlPlaneRpcServerError);
                }
            },
            ControlPlaneRpcServerListenerKind::TlsTcp {
                listener,
                tls_server_config,
            } => match listener.accept() {
                Ok((stream, _)) => {
                    let tls_server_config = Arc::clone(tls_server_config);
                    spawn_control_plane_rpc_server_worker(
                        stream,
                        authority(),
                        policy.clone(),
                        true,
                        self.max_frame_bytes,
                        worker_limit,
                        self.io_timeout,
                        move |stream, deadline| {
                            let socket = ControlPlaneDeadlineTcpSocket::new(
                                stream,
                                deadline,
                                CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
                            )
                            .map_err(|source| {
                                ControlPlaneError::io(
                                    "configure control-plane TLS/TCP deadline I/O",
                                    source,
                                )
                            })?;
                            let connection = rustls::ServerConnection::new(tls_server_config)
                                .map_err(|_| {
                                    ControlPlaneError::rpc_protocol(
                                        "failed to initialize control-plane TLS server connection"
                                            .to_owned(),
                                    )
                                })?;
                            let mut stream = rustls::StreamOwned::new(connection, socket);
                            while stream.conn.is_handshaking() {
                                stream
                                    .conn
                                    .complete_io(&mut stream.sock)
                                    .map_err(|source| {
                                        ControlPlaneError::io(
                                            "complete control-plane TLS server handshake",
                                            source,
                                        )
                                    })?;
                            }
                            if stream.conn.alpn_protocol() != Some(CONTROL_PLANE_RPC_TLS_ALPN) {
                                return Err(ControlPlaneError::rpc_protocol("control-plane TLS peer did not negotiate the required protocol profile".to_owned()));
                            }
                            Ok(Box::new(stream) as Box<dyn ControlPlaneRpcServerStream>)
                        },
                    );
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => {
                    eprintln!("control-plane TCP socket accept failed: {error}");
                    return Err(ControlPlaneRpcServerError);
                }
            },
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_control_plane_rpc_server_worker<T, RawStream, Prepare>(
    stream: RawStream,
    mut authority: ControlPlaneRpcServerAuthority<T>,
    policy: ControlPlaneRpcServerPolicy,
    require_authentication: bool,
    max_frame_bytes: usize,
    worker_limit: usize,
    io_timeout: Duration,
    prepare: Prepare,
) where
    T: ControlPlaneAdmin
        + ControlPlaneHeartbeatRuntimeMapSource
        + ControlPlaneRuntimeMapSource
        + Send
        + 'static,
    RawStream: Send + 'static,
    Prepare: FnOnce(
            RawStream,
            Instant,
        ) -> Result<Box<dyn ControlPlaneRpcServerStream>, ControlPlaneError>
        + Send
        + 'static,
{
    if !reserve_control_plane_rpc_worker(&policy.resources.active_workers, worker_limit) {
        eprintln!("control-plane RPC rejected: worker limit reached");
        return;
    }
    let connection_deadline = Instant::now()
        .checked_add(io_timeout)
        .unwrap_or(Instant::now());
    std::thread::spawn(move || {
        let _guard = ControlPlaneRpcWorkerGuard {
            active_workers: Arc::clone(&policy.resources.active_workers),
        };
        let mut stream = match prepare(stream, connection_deadline) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!("control-plane RPC transport setup failed: {error}");
                return;
            }
        };
        let request = match read_control_plane_request_with_reservation(
            &mut stream,
            |frame_bytes| {
                if frame_bytes > max_frame_bytes {
                    return Err(ControlPlaneError::rpc_protocol(format!(
                            "control-plane RPC frame size {frame_bytes} bytes exceeds listener limit {max_frame_bytes}"
                        )));
                }
                policy.resources.pre_auth_byte_budget.reserve(frame_bytes)
            },
        ) {
            Ok((request, reservation)) => (request, reservation),
            Err(error) => {
                eprintln!("control-plane RPC request read failed: {error}");
                return;
            }
        };
        let (request, _pre_auth_byte_reservation) = request;
        let metrics_kind = request.metrics_kind();
        let request = match authenticate_and_admit_control_plane_rpc(
            request,
            &policy,
            require_authentication,
            policy.authority_now_ms(),
        ) {
            Ok(request) => request,
            Err(ControlPlaneRpcAdmissionFailure::Unauthenticated(error)) => {
                eprintln!("control-plane RPC authentication failed: {error}");
                return;
            }
            Err(ControlPlaneRpcAdmissionFailure::Authenticated { request, error }) => {
                let response = build_control_plane_unix_admission_error_response(
                    *request,
                    *error,
                    policy.authority_now_ms(),
                );
                stream.begin_response(io_timeout);
                write_control_plane_rpc_admission_response(&mut stream, metrics_kind, response);
                if let Err(error) = stream.finish_response() {
                    eprintln!("control-plane RPC response finalization failed: {error}");
                }
                return;
            }
        };
        let response = (|| {
            if request.is_authority_clock_admin() {
                let _operation_timer =
                    observability::control_plane_rpc_operation_timer(metrics_kind);
                let authority_clock = policy.authority_clock.as_ref().ok_or_else(|| {
                    ControlPlaneError::rpc_protocol(
                        "authority-clock administration requires a process-local clock gate"
                            .to_owned(),
                    )
                })?;
                let context = authority.with_mut(metrics_kind, |authority| {
                    authority.authority_clock_context()
                });
                let mut authority_clock = authority_clock
                    .lock()
                    .expect("control-plane authority clock mutex poisoned");
                let response =
                    build_control_plane_authority_clock_admin_response_from_verified_with_context(
                        &mut authority_clock,
                        request,
                        ControlPlaneAuthorityClockAdminSample::from_process_clock()?,
                        context,
                        |context, authority_clock| {
                            policy
                                .authority_clock_checkpoint_target
                                .as_ref()
                                .ok_or_else(|| ControlPlaneError::rpc_protocol("authority-clock administration requires a durable checkpoint target".to_owned()))?
                                .persist_established(context, authority_clock)
                        },
                        || Ok(policy.authority_now_ms()),
                    );
                policy
                    .authority_clock_checkpoint_target
                    .as_ref()
                    .ok_or_else(|| {
                        ControlPlaneError::rpc_protocol(
                            "authority-clock administration requires a durable checkpoint target"
                                .to_owned(),
                        )
                    })?
                    .invalidate_if_blocked(&authority_clock)?;
                response
            } else if request.is_refresh_node_heartbeat() {
                let prepared = {
                    let _operation_timer =
                        observability::control_plane_rpc_operation_timer(metrics_kind);
                    authority.with_mut(metrics_kind, |authority| {
                        let (now_ms, lease_horizon_authority) = match &policy.authority_clock {
                            Some(authority_clock)
                                if policy.gate_request_time_with_authority_clock =>
                            {
                                let mut authority_clock = authority_clock
                                    .lock()
                                    .expect("control-plane authority clock mutex poisoned");
                                let now_ms = authority_clock.effective_process_now_ms();
                                policy
                                    .authority_clock_checkpoint_target
                                    .as_ref()
                                    .expect("configured authority clock has checkpoint target")
                                    .invalidate_if_blocked(&authority_clock)?;
                                let now_ms = now_ms?;
                                let lease_horizon_authority =
                                    authority_clock.lease_horizon_authority_binding(None)?;
                                (now_ms, Some(lease_horizon_authority))
                            }
                            _ => (policy.authority_now_ms(), None),
                        };
                        match lease_horizon_authority {
                            Some(lease_horizon_authority) => {
                                prepare_control_plane_heartbeat_response_with_lease_horizon_authority_from_verified(
                                    authority,
                                    request,
                                    now_ms,
                                    lease_horizon_authority,
                                )
                            }
                            None => prepare_control_plane_heartbeat_response_from_verified(
                                authority, request, now_ms,
                            ),
                        }
                    })
                };
                prepared.and_then(|prepared| {
                    finish_control_plane_heartbeat_response(prepared, || {
                        Ok(policy.authority_now_ms())
                    })
                })
            } else {
                let _operation_timer =
                    observability::control_plane_rpc_operation_timer(metrics_kind);
                authority.with_mut(metrics_kind, |authority| {
                    let now_ms = match &policy.authority_clock {
                        Some(authority_clock) if policy.gate_request_time_with_authority_clock => {
                            let mut authority_clock = authority_clock
                                .lock()
                                .expect("control-plane authority clock mutex poisoned");
                            let now_ms = authority_clock.effective_process_now_ms();
                            policy
                                .authority_clock_checkpoint_target
                                .as_ref()
                                .expect("configured authority clock has checkpoint target")
                                .invalidate_if_blocked(&authority_clock)?;
                            now_ms?
                        }
                        _ => policy.authority_now_ms(),
                    };
                    build_control_plane_unix_response_from_verified(
                        authority,
                        request,
                        now_ms,
                        || Ok(policy.authority_now_ms()),
                    )
                })
            }
        })();
        if let Some(authority_clock) = &policy.authority_clock {
            let authority_clock = authority_clock
                .lock()
                .expect("control-plane authority clock mutex poisoned");
            let checkpoint_result = policy
                .authority_clock_checkpoint_target
                .as_ref()
                .expect("configured authority clock has checkpoint target")
                .invalidate_if_blocked(&authority_clock);
            if let Err(error) = checkpoint_result {
                eprintln!(
                    "failed to invalidate blocked control-plane authority-clock checkpoint: {error}"
                );
                if let Some(fatal_error_handler) = &policy.fatal_error_handler {
                    fatal_error_handler();
                }
                return;
            }
        }
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                eprintln!("control-plane RPC response build failed: {error}");
                return;
            }
        };
        let response_write_started = Instant::now();
        stream.begin_response(io_timeout);
        let mut response = Some(response);
        let mut write_response = || {
            let response = response.take().ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "control-plane response publication attempted more than once".to_owned(),
                )
            })?;
            stream.begin_response(io_timeout);
            write_control_plane_rpc_response_and_flush(&mut stream, response)
        };
        let response_result = publish_control_plane_rpc_response(
            policy.response_publication.as_deref(),
            &mut write_response,
        );
        observability::record_control_plane_rpc_response_write(
            metrics_kind,
            response_write_started.elapsed(),
        );
        if let Err(error) = response_result {
            observability::record_control_plane_rpc_response_write_error(
                metrics_kind,
                control_plane_rpc_response_write_error_kind(&error),
            );
            eprintln!("control-plane RPC response failed: {error}");
        } else if let Err(error) = stream.finish_response() {
            eprintln!("control-plane RPC response finalization failed: {error}");
        }
    });
}

fn reserve_control_plane_rpc_worker(active_workers: &AtomicUsize, worker_limit: usize) -> bool {
    active_workers
        .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
            (active < worker_limit).then_some(active + 1)
        })
        .is_ok()
}

fn write_control_plane_rpc_admission_response(
    stream: &mut impl std::io::Write,
    metrics_kind: observability::ControlPlaneRpcMetricKind,
    response: Result<ControlPlaneRpcResponse, ControlPlaneError>,
) {
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            eprintln!("control-plane RPC admission response build failed: {error}");
            return;
        }
    };
    let response_write_started = Instant::now();
    let response_result = write_control_plane_rpc_response_and_flush(stream, response);
    observability::record_control_plane_rpc_response_write(
        metrics_kind,
        response_write_started.elapsed(),
    );
    if let Err(error) = response_result {
        observability::record_control_plane_rpc_response_write_error(
            metrics_kind,
            control_plane_rpc_response_write_error_kind(&error),
        );
        eprintln!("control-plane RPC admission response failed: {error}");
    }
}

fn write_control_plane_rpc_response_and_flush(
    stream: &mut impl std::io::Write,
    response: ControlPlaneRpcResponse,
) -> Result<(), ControlPlaneError> {
    write_control_plane_unix_response(stream, response)?;
    stream
        .flush()
        .map_err(|source| ControlPlaneError::io("flush control-plane RPC response", source))
}

fn control_plane_rpc_response_write_error_kind(
    error: &ControlPlaneError,
) -> observability::ControlPlaneRpcResponseWriteErrorKind {
    let ControlPlaneError::Io { diagnostic: source } = error else {
        return observability::ControlPlaneRpcResponseWriteErrorKind::Other;
    };
    match source.kind() {
        ErrorKind::BrokenPipe => observability::ControlPlaneRpcResponseWriteErrorKind::BrokenPipe,
        ErrorKind::ConnectionReset => {
            observability::ControlPlaneRpcResponseWriteErrorKind::ConnectionReset
        }
        ErrorKind::TimedOut | ErrorKind::WouldBlock => {
            observability::ControlPlaneRpcResponseWriteErrorKind::Timeout
        }
        _ => observability::ControlPlaneRpcResponseWriteErrorKind::Other,
    }
}

fn write_control_plane_rpc_frame(
    stream: &mut impl std::io::Write,
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<(), ControlPlaneError> {
    let frame = encode_control_plane_rpc_frame(kind, payload)?;
    let magic_len = CONTROL_PLANE_RPC_MAGIC.len();
    stream
        .write_all(&frame[..magic_len])
        .map_err(|source| ControlPlaneError::io("write control-plane RPC magic", source))?;
    stream
        .write_all(&frame[magic_len..])
        .map_err(|source| ControlPlaneError::io("write control-plane RPC frame", source))
}

fn encode_control_plane_rpc_frame(
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<Vec<u8>, ControlPlaneError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        ControlPlaneError::rpc_protocol(format!(
            "control-plane RPC payload too large: {}",
            payload.len()
        ))
    })?;
    let mut frame = Vec::with_capacity(control_plane_rpc_frame_overhead() + payload.len());
    frame.extend_from_slice(CONTROL_PLANE_RPC_MAGIC);
    write_u16(&mut frame, CONTROL_PLANE_RPC_VERSION);
    write_u16(&mut frame, kind as u16);
    write_u32(&mut frame, payload_len);
    write_u64(
        &mut frame,
        control_plane_rpc_frame_checksum(
            CONTROL_PLANE_RPC_VERSION,
            kind as u16,
            payload_len,
            payload,
        ),
    );
    frame.extend_from_slice(payload);
    Ok(frame)
}

const fn control_plane_rpc_frame_overhead() -> usize {
    CONTROL_PLANE_RPC_MAGIC.len() + 16
}

fn read_control_plane_rpc_frame(
    stream: &mut impl std::io::Read,
) -> Result<(ControlPlaneRpcKind, Vec<u8>), ControlPlaneError> {
    read_control_plane_rpc_frame_with_reservation(stream, |_| Ok(())).map(|(frame, ())| frame)
}

fn read_control_plane_rpc_frame_with_reservation<R>(
    stream: &mut impl std::io::Read,
    reserve: impl FnOnce(usize) -> Result<R, ControlPlaneError>,
) -> Result<((ControlPlaneRpcKind, Vec<u8>), R), ControlPlaneError> {
    let mut magic = vec![0; CONTROL_PLANE_RPC_MAGIC.len()];
    stream
        .read_exact(&mut magic)
        .map_err(|source| ControlPlaneError::io("read control-plane RPC magic", source))?;
    if magic != CONTROL_PLANE_RPC_MAGIC {
        return Err(ControlPlaneError::rpc_protocol(
            "invalid control-plane RPC magic".to_owned(),
        ));
    }
    let mut header = [0; 16];
    stream
        .read_exact(&mut header)
        .map_err(|source| ControlPlaneError::io("read control-plane RPC header", source))?;
    let mut reader = PayloadReader::new(&header);
    let version = reader.read_u16()?;
    if version != CONTROL_PLANE_RPC_VERSION {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "unsupported control-plane RPC version {version}"
        )));
    }
    let kind = ControlPlaneRpcKind::from_u16(reader.read_u16()?)?;
    let raw_kind = kind as u16;
    let payload_len_u32 = reader.read_u32()?;
    let payload_len = usize::try_from(payload_len_u32).map_err(|_| {
        ControlPlaneError::rpc_protocol(
            "control-plane RPC payload length does not fit usize".to_owned(),
        )
    })?;
    let expected_checksum = reader.read_u64()?;
    reader.finish()?;
    if payload_len > CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane RPC payload too large: {payload_len}"
        )));
    }
    let reservation = reserve(control_plane_rpc_frame_overhead() + payload_len)?;
    let mut payload = vec![0; payload_len];
    stream
        .read_exact(&mut payload)
        .map_err(|source| ControlPlaneError::io("read control-plane RPC payload", source))?;
    if control_plane_rpc_frame_checksum(version, raw_kind, payload_len_u32, &payload)
        != expected_checksum
    {
        return Err(ControlPlaneError::rpc_protocol(
            "control-plane RPC frame checksum mismatch".to_owned(),
        ));
    }
    Ok(((kind, payload), reservation))
}

fn control_plane_rpc_frame_checksum(
    version: u16,
    raw_kind: u16,
    payload_len: u32,
    payload: &[u8],
) -> u64 {
    let mut hasher = checksum::crc64::Hasher::new();
    hasher.update(CONTROL_PLANE_RPC_MAGIC);
    hasher.update(&version.to_le_bytes());
    hasher.update(&raw_kind.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

fn encode_control_plane_rpc_response(
    response: Result<Vec<u8>, ControlPlaneError>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut payload = Vec::new();
    match response {
        Ok(response) => {
            write_u8(&mut payload, 0);
            write_bytes(&mut payload, &response)?;
        }
        Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id,
            node_id,
            cluster_epoch,
            pending,
        }) => {
            write_u8(&mut payload, 2);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
            write_u64(&mut payload, cluster_epoch.get());
            write_u64(&mut payload, pending.cluster_epoch().get());
            write_u64(&mut payload, pending.log_index());
            write_u64(&mut payload, pending.command_checksum());
        }
        Err(ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id,
            cluster_epoch,
        }) => {
            write_u8(&mut payload, 3);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, cluster_epoch.get());
        }
        Err(ControlPlaneError::PgHasNoServingPrimary {
            pg_id,
            cluster_epoch,
        }) => {
            write_u8(&mut payload, 4);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, cluster_epoch.get());
        }
        Err(ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id,
            node_id,
            cluster_epoch,
        }) => {
            write_u8(&mut payload, 5);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
            write_u64(&mut payload, cluster_epoch.get());
        }
        Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id,
            node_id,
            cluster_epoch,
            state,
        }) => {
            write_u8(&mut payload, 6);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
            write_u64(&mut payload, cluster_epoch.get());
            write_pg_state(&mut payload, state);
        }
        Err(ControlPlaneError::PgActingSetChangeNotReady {
            pg_id,
            cluster_epoch,
            state,
        }) => {
            write_u8(&mut payload, 7);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, cluster_epoch.get());
            write_pg_state(&mut payload, state);
        }
        Err(ControlPlaneError::UnknownPg { pg_id }) => {
            write_u8(&mut payload, 8);
            write_u32(&mut payload, pg_id);
        }
        Err(ControlPlaneError::OpenRaftOperation { kind, message }) => {
            write_u8(&mut payload, 9);
            write_u8(&mut payload, kind.wire_tag());
            write_string(&mut payload, &message)?;
        }
        Err(ControlPlaneError::AuthorityNotServing) => {
            write_u8(&mut payload, 10);
        }
        Err(ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority) => {
            write_u8(&mut payload, 11);
        }
        Err(ControlPlaneError::UnknownNode { node_id }) => {
            write_u8(&mut payload, 12);
            write_u32(&mut payload, node_id);
        }
        Err(ControlPlaneError::UnknownActingSetNode { pg_id, node_id }) => {
            write_u8(&mut payload, 13);
            write_u32(&mut payload, pg_id);
            write_u32(&mut payload, node_id);
        }
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term,
            current_term,
        }) => {
            write_u8(&mut payload, 14);
            write_option_u64(&mut payload, established_term);
            write_u64(&mut payload, current_term);
        }
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id,
            expected_destination_epoch,
            actual_destination_epoch,
        }) => {
            write_u8(&mut payload, 15);
            write_u32(&mut payload, pg_id);
            write_u64(&mut payload, expected_destination_epoch.get());
            write_u64(&mut payload, actual_destination_epoch.get());
        }
        Err(error) => {
            write_u8(&mut payload, 1);
            write_string(&mut payload, &error.rpc_wire_error_message())?;
        }
    }
    Ok(payload)
}

enum DecodedControlPlaneRpcResponse {
    Success(Vec<u8>),
    Rejection(ControlPlaneError),
}

fn decode_control_plane_rpc_response(payload: Vec<u8>) -> Result<Vec<u8>, ControlPlaneError> {
    match decode_control_plane_rpc_response_frame(payload)? {
        DecodedControlPlaneRpcResponse::Success(payload) => Ok(payload),
        DecodedControlPlaneRpcResponse::Rejection(error) => Err(error),
    }
}

fn decode_control_plane_rpc_response_frame(
    payload: Vec<u8>,
) -> Result<DecodedControlPlaneRpcResponse, ControlPlaneError> {
    let mut reader = PayloadReader::new(&payload);
    let status = reader.read_u8()?;
    match status {
        0 => {
            let response = reader.read_bytes()?.to_vec();
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Success(response))
        }
        1 => {
            let message = reader.read_string()?.to_owned();
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::rpc_remote(message),
            ))
        }
        2 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            let cluster_epoch = read_cluster_epoch(&mut reader, "pending blocker cluster epoch")?;
            let pending_cluster_epoch =
                read_cluster_epoch(&mut reader, "pending command cluster epoch")?;
            let pending_log_index = NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "pending command log index must be nonzero".to_owned(),
                )
            })?;
            let pending_command_checksum = reader.read_u64()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgPeeringPendingMetadataCommand {
                    pg_id,
                    node_id,
                    cluster_epoch,
                    pending: PendingMetadataCommandObservation::new(
                        pending_cluster_epoch,
                        pending_log_index,
                        pending_command_checksum,
                    ),
                },
            ))
        }
        3 => {
            let pg_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "metadata migration source cluster epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgMetadataMigrationSourceNotReady {
                    pg_id,
                    cluster_epoch,
                },
            ))
        }
        4 => {
            let pg_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG serving-primary cluster epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgHasNoServingPrimary {
                    pg_id,
                    cluster_epoch,
                },
            ))
        }
        5 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG primary-observation cluster epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgPrimaryMissingActiveObservation {
                    pg_id,
                    node_id,
                    cluster_epoch,
                },
            ))
        }
        6 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG primary-observation cluster epoch")?;
            let state = read_pg_state(&mut reader)?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgPrimaryObservationNotActive {
                    pg_id,
                    node_id,
                    cluster_epoch,
                    state,
                },
            ))
        }
        7 => {
            let pg_id = reader.read_u32()?;
            let cluster_epoch =
                read_cluster_epoch(&mut reader, "PG acting-set readiness cluster epoch")?;
            let state = read_pg_state(&mut reader)?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgActingSetChangeNotReady {
                    pg_id,
                    cluster_epoch,
                    state,
                },
            ))
        }
        8 => {
            let pg_id = reader.read_u32()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::UnknownPg { pg_id },
            ))
        }
        9 => {
            let kind = ControlPlaneRaftOperationErrorKind::from_wire_tag(reader.read_u8()?)?;
            let message = reader.read_string()?.to_owned();
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::OpenRaftOperation { kind, message },
            ))
        }
        10 => {
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::AuthorityNotServing,
            ))
        }
        11 => {
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority,
            ))
        }
        12 => {
            let node_id = reader.read_u32()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::UnknownNode { node_id },
            ))
        }
        13 => {
            let pg_id = reader.read_u32()?;
            let node_id = reader.read_u32()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::UnknownActingSetNode { pg_id, node_id },
            ))
        }
        14 => {
            let established_term = reader.read_option_u64()?;
            let current_term = reader.read_u64()?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::AuthorityClockLeadershipChanged {
                    established_term,
                    current_term,
                },
            ))
        }
        15 => {
            let pg_id = reader.read_u32()?;
            let expected_destination_epoch =
                read_cluster_epoch(&mut reader, "expected metadata transfer destination epoch")?;
            let actual_destination_epoch =
                read_cluster_epoch(&mut reader, "actual metadata transfer destination epoch")?;
            reader.finish()?;
            Ok(DecodedControlPlaneRpcResponse::Rejection(
                ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
                    pg_id,
                    expected_destination_epoch,
                    actual_destination_epoch,
                },
            ))
        }
        _ => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid control-plane RPC response status {status}"
        ))),
    }
}

fn write_node_heartbeat_payload(heartbeat: &NodeHeartbeat) -> Result<Vec<u8>, ControlPlaneError> {
    let mut payload = Vec::new();
    write_node_heartbeat(&mut payload, heartbeat)?;
    Ok(payload)
}

fn read_node_heartbeat_payload(payload: &[u8]) -> Result<NodeHeartbeat, ControlPlaneError> {
    let mut reader = PayloadReader::new(payload);
    let heartbeat = read_node_heartbeat(&mut reader)?;
    reader.finish()?;
    Ok(heartbeat)
}

fn write_node_heartbeat(
    out: &mut Vec<u8>,
    heartbeat: &NodeHeartbeat,
) -> Result<(), ControlPlaneError> {
    write_u32(out, heartbeat.node_id.as_u32());
    write_u64(out, heartbeat.node_incarnation);
    write_string(out, &heartbeat.endpoint)?;
    write_u64(out, heartbeat.observed_epoch.get());
    write_u64(out, heartbeat.requested_lease_duration_ms);
    write_cluster_map_history_route_references(
        out,
        &heartbeat.cluster_map_history_route_references,
    )?;
    write_u32(
        out,
        len_as_u32(heartbeat.pg_observations.len(), "PG observations")?,
    );
    for observation in &heartbeat.pg_observations {
        write_u32(out, observation.pg_id.get());
        write_pg_state(out, observation.state);
        write_pg_metadata_proof(out, observation.metadata_proof);
        write_pending_metadata_command_observation(out, observation.pending_metadata_command);
    }
    Ok(())
}

fn read_node_heartbeat(reader: &mut PayloadReader<'_>) -> Result<NodeHeartbeat, ControlPlaneError> {
    let node_id = NodeId::new(reader.read_u32()?);
    let node_incarnation = reader.read_u64()?;
    let endpoint = reader.read_string()?.to_owned();
    let observed_epoch = read_cluster_epoch(reader, "heartbeat observed epoch")?;
    let requested_lease_duration_ms = reader.read_u64()?;
    let cluster_map_history_route_references = read_cluster_map_history_route_references(reader)?;
    let observation_count = reader.read_collection_len(
        "PG observations",
        CONTROL_PLANE_RPC_HEARTBEAT_OBSERVATION_MIN_LEN,
    )?;
    let mut pg_observations = Vec::with_capacity(observation_count);
    for _ in 0..observation_count {
        pg_observations.push(NodePgHeartbeatObservation {
            pg_id: PgId::new(reader.read_u32()?),
            state: read_pg_state(reader)?,
            metadata_proof: read_pg_metadata_proof(reader)?,
            pending_metadata_command: read_pending_metadata_command_observation(reader)?,
        });
    }
    Ok(NodeHeartbeat {
        node_id,
        node_incarnation,
        endpoint,
        observed_epoch,
        requested_lease_duration_ms,
        cluster_map_history_route_references,
        pg_observations,
    })
}

fn write_cluster_map_history_route_references(
    out: &mut Vec<u8>,
    references: &PgClusterMapHistoryRouteReferences,
) -> Result<(), ControlPlaneError> {
    write_u32(
        out,
        len_as_u32(references.len(), "cluster-map history route references")?,
    );
    for reference in references.iter() {
        write_u8(
            out,
            cluster_map_history_route_reference_kind_code(reference.kind()),
        );
        write_u64(out, reference.cluster_epoch().get());
        write_u32(out, reference.pg_id().get());
    }
    Ok(())
}

fn read_cluster_map_history_route_references(
    reader: &mut PayloadReader<'_>,
) -> Result<PgClusterMapHistoryRouteReferences, ControlPlaneError> {
    let count = reader.read_collection_len(
        "cluster-map history route references",
        CONTROL_PLANE_RPC_HISTORY_ROUTE_REFERENCE_MIN_LEN,
    )?;
    if count > MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "cluster-map history route reference count {count} exceeds {}",
            MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES
        )));
    }
    let mut decoded = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let reference = PgClusterMapHistoryRouteReference::new(
            read_cluster_map_history_route_reference_kind(reader)?,
            read_cluster_epoch(reader, "cluster-map history route reference epoch")?,
            PgId::new(reader.read_u32()?),
        );
        if previous.is_some_and(|previous| reference <= previous) {
            return Err(ControlPlaneError::rpc_protocol(
                "cluster-map history route references are not in canonical order".to_owned(),
            ));
        }
        previous = Some(reference);
        decoded.push(reference);
    }
    PgClusterMapHistoryRouteReferences::try_from_iter(decoded).map_err(|error| {
        ControlPlaneError::rpc_protocol(format!(
            "invalid cluster-map history route references: {error}"
        ))
    })
}

const fn cluster_map_history_route_reference_kind_code(
    kind: PgClusterMapHistoryRouteReferenceKind,
) -> u8 {
    match kind {
        PgClusterMapHistoryRouteReferenceKind::LivePlacement => 1,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource => 2,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired => 3,
        PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand => 4,
        PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim => 5,
    }
}

fn read_cluster_map_history_route_reference_kind(
    reader: &mut PayloadReader<'_>,
) -> Result<PgClusterMapHistoryRouteReferenceKind, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(PgClusterMapHistoryRouteReferenceKind::LivePlacement),
        2 => Ok(PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource),
        3 => Ok(PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired),
        4 => Ok(PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand),
        5 => Ok(PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim),
        value => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid cluster-map history route reference kind {value}"
        ))),
    }
}

fn write_pg_acting_set_request(
    out: &mut Vec<u8>,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<(), ControlPlaneError> {
    write_u32(out, pg_id.get());
    write_u32(out, len_as_u32(acting_set.len(), "acting set")?);
    for node_id in acting_set {
        write_u32(out, node_id.as_u32());
    }
    Ok(())
}

fn read_pg_acting_set_request(
    reader: &mut PayloadReader<'_>,
) -> Result<(PgId, Vec<NodeId>), ControlPlaneError> {
    let pg_id = PgId::new(reader.read_u32()?);
    let node_count =
        reader.read_collection_len("acting set", CONTROL_PLANE_RPC_ACTING_SET_NODE_MIN_LEN)?;
    let mut acting_set = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        acting_set.push(NodeId::new(reader.read_u32()?));
    }
    Ok((pg_id, acting_set))
}

fn write_pg_id_request(out: &mut Vec<u8>, pg_id: PgId) {
    write_u32(out, pg_id.get());
}

fn read_pg_id_request(reader: &mut PayloadReader<'_>) -> Result<PgId, ControlPlaneError> {
    Ok(PgId::new(reader.read_u32()?))
}

fn write_pg_acting_set_with_metadata_transfer_request(
    out: &mut Vec<u8>,
    pg_id: PgId,
    acting_set: &[NodeId],
    transfer: PgMetadataTransferProof,
    expected_destination_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    write_pg_acting_set_request(out, pg_id, acting_set)?;
    write_u64(out, transfer.source_epoch().get());
    write_pg_metadata_proof(out, transfer.source_metadata_proof());
    write_pg_metadata_proof(out, transfer.metadata_proof());
    write_u64(out, expected_destination_epoch.get());
    Ok(())
}

fn read_pg_acting_set_with_metadata_transfer_request(
    reader: &mut PayloadReader<'_>,
) -> Result<(PgId, Vec<NodeId>, PgMetadataTransferProof, ClusterEpoch), ControlPlaneError> {
    let (pg_id, acting_set) = read_pg_acting_set_request(reader)?;
    let source_epoch = read_cluster_epoch(reader, "metadata transfer source epoch")?;
    let source_metadata_proof = read_pg_metadata_proof(reader)?;
    let imported_metadata_proof = read_pg_metadata_proof(reader)?;
    let expected_destination_epoch =
        read_cluster_epoch(reader, "metadata transfer destination epoch")?;
    Ok((
        pg_id,
        acting_set,
        PgMetadataTransferProof::new_with_imported_metadata_proof(
            source_epoch,
            source_metadata_proof,
            imported_metadata_proof,
        ),
        expected_destination_epoch,
    ))
}

fn write_heartbeat_lease_summary(out: &mut Vec<u8>, lease: &HeartbeatLease) {
    write_u64(out, lease.authority_incarnation().get());
    write_u64(out, lease.cluster_epoch().get());
    write_u32(out, lease.node_id().as_u32());
    write_u64(out, lease.lease_deadline_ms());
    write_u8(out, u8::from(lease.serving()));
}

fn read_heartbeat_lease_summary(
    reader: &mut PayloadReader<'_>,
) -> Result<HeartbeatLease, ControlPlaneError> {
    let authority_incarnation = AuthorityIncarnation::new(reader.read_u64()?).ok_or_else(|| {
        ControlPlaneError::rpc_protocol(
            "heartbeat lease authority incarnation must be nonzero".to_owned(),
        )
    })?;
    let cluster_epoch = read_cluster_epoch(reader, "heartbeat lease cluster epoch")?;
    let node_id = NodeId::new(reader.read_u32()?);
    let lease_deadline_ms = reader.read_u64()?;
    let serving = reader.read_bool()?;
    Ok(HeartbeatLease {
        authority_incarnation,
        cluster_epoch,
        node_id,
        lease_deadline_ms,
        serving,
        snapshot: ClusterControlSnapshot::empty(),
    })
}

fn write_runtime_map_status(
    out: &mut Vec<u8>,
    status: ControlPlaneRuntimeMapStatus,
) -> Result<(), ControlPlaneError> {
    write_u64(out, status.cluster_epoch().get());
    write_u32(
        out,
        len_as_u32(status.pg_routes(), "runtime map status PG routes")?,
    );
    write_u32(
        out,
        len_as_u32(
            status.active_serving_pg_routes(),
            "runtime map status active serving PG routes",
        )?,
    );
    match status.lease_renewal() {
        Some(renewal) => {
            write_u8(out, 1);
            out.extend_from_slice(&renewal.content_digest().as_bytes());
            let Some(valid_until_ms) = renewal.validity().valid_until_ms() else {
                return Err(ControlPlaneError::rpc_protocol(
                    "runtime map status renewal validity must be bounded".to_owned(),
                ));
            };
            if !renewal.freshness_proof().is_serving_authority_read() {
                return Err(ControlPlaneError::rpc_protocol(
                    "runtime map status renewal requires a serving-authority freshness proof"
                        .to_owned(),
                ));
            }
            write_u64(out, valid_until_ms);
            write_runtime_map_freshness_proof(out, &renewal.freshness_proof());
        }
        None => write_u8(out, 0),
    }
    Ok(())
}

fn read_runtime_map_status(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
    let cluster_epoch = read_cluster_epoch(reader, "runtime map status cluster epoch")?;
    let pg_routes = reader.read_u32()? as usize;
    let active_serving_pg_routes = reader.read_u32()? as usize;
    let lease_renewal = match reader.read_u8()? {
        0 => None,
        1 => {
            let content_digest = RuntimeMapContentDigest::from_bytes(
                reader
                    .read_exact(RUNTIME_MAP_CONTENT_DIGEST_LEN)?
                    .try_into()
                    .expect("runtime-map digest read must return 32 bytes"),
            );
            let valid_until_ms = reader.read_u64()?;
            let validity =
                RouteMapValidity::from_valid_until_ms(Some(valid_until_ms)).ok_or_else(|| {
                    ControlPlaneError::rpc_protocol(
                        "runtime map status renewal validity uses reserved unbounded sentinel"
                            .to_owned(),
                    )
                })?;
            let freshness_proof = read_runtime_map_freshness_proof(reader)?;
            if !freshness_proof.is_serving_authority_read() {
                return Err(ControlPlaneError::rpc_protocol(
                    "runtime map status renewal requires a serving-authority freshness proof"
                        .to_owned(),
                ));
            }
            Some(ControlPlaneRuntimeMapLeaseRenewal {
                content_digest,
                validity,
                freshness_proof,
            })
        }
        value => {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "invalid runtime map status renewal tag {value}"
            )));
        }
    };
    Ok(ControlPlaneRuntimeMapStatus {
        cluster_epoch,
        pg_routes,
        active_serving_pg_routes,
        lease_renewal,
    })
}

fn write_control_plane_runtime_map_diagnostics(
    out: &mut Vec<u8>,
    diagnostics: &ControlPlaneRuntimeMapDiagnosticSnapshot,
) -> Result<(), ControlPlaneError> {
    let runtime_map = diagnostics.runtime_map();
    write_runtime_map_snapshot(out, runtime_map)?;
    let rpc_metrics = observability::control_plane_rpc_metrics_snapshot();
    write_u32(
        out,
        len_as_u32(rpc_metrics.len(), "control-plane RPC metric samples")?,
    );
    for sample in rpc_metrics {
        write_u8(out, control_plane_rpc_metric_kind_code(sample.kind));
        write_u64(out, sample.total);
        write_u64(out, sample.lock_wait_us_total);
        write_u64(out, sample.lock_wait_us_max);
        write_u64(out, sample.operation_us_total);
        write_u64(out, sample.operation_us_max);
        write_u64(out, sample.response_write_us_total);
        write_u64(out, sample.response_write_us_max);
        write_u64(out, sample.response_write_error_total);
        write_u64(out, sample.response_write_broken_pipe_total);
        write_u64(out, sample.response_write_connection_reset_total);
        write_u64(out, sample.response_write_timeout_total);
        write_u64(out, sample.response_write_other_error_total);
    }
    let snapshot = observability::control_plane_snapshot_metrics_snapshot();
    write_u64(out, snapshot.serialize_total);
    write_u64(out, snapshot.serialize_us_total);
    write_u64(out, snapshot.serialize_us_max);
    write_u64(out, snapshot.save_total);
    write_u64(out, snapshot.save_error_total);
    write_u64(out, snapshot.save_us_total);
    write_u64(out, snapshot.save_us_max);
    write_u64(out, snapshot.sync_total);
    write_u64(out, snapshot.sync_us_total);
    write_u64(out, snapshot.sync_us_max);
    write_u64(out, snapshot.bytes_total);
    write_u64(out, snapshot.bytes_last);
    write_u64(out, snapshot.bytes_max);
    let journal = observability::control_plane_journal_metrics_snapshot();
    write_u64(out, journal.append_total);
    write_u64(out, journal.append_error_total);
    write_u64(out, journal.append_us_total);
    write_u64(out, journal.append_us_max);
    write_u64(out, journal.lock_wait_us_total);
    write_u64(out, journal.lock_wait_us_max);
    write_u64(out, journal.frame_bytes_total);
    write_u64(out, journal.frame_bytes_last);
    write_u64(out, journal.frame_bytes_max);
    write_u64(out, journal.file_sync_total);
    write_u64(out, journal.file_sync_us_total);
    write_u64(out, journal.file_sync_us_max);
    write_u64(out, journal.directory_sync_total);
    write_u64(out, journal.directory_sync_us_total);
    write_u64(out, journal.directory_sync_us_max);
    write_u64(out, journal.compaction_total);
    write_u64(out, journal.compaction_error_total);
    write_u64(out, journal.compaction_us_total);
    write_u64(out, journal.compaction_us_max);
    write_u64(out, journal.compaction_lock_wait_us_total);
    write_u64(out, journal.compaction_lock_wait_us_max);
    write_u64(out, journal.compaction_bytes_total);
    write_u64(out, journal.compaction_bytes_last);
    write_u64(out, journal.compaction_bytes_max);
    write_u64(out, journal.compaction_file_sync_total);
    write_u64(out, journal.compaction_file_sync_us_total);
    write_u64(out, journal.compaction_file_sync_us_max);
    write_u64(out, journal.compaction_directory_sync_total);
    write_u64(out, journal.compaction_directory_sync_us_total);
    write_u64(out, journal.compaction_directory_sync_us_max);
    let raft_checkpoint = observability::control_plane_raft_checkpoint_metrics_snapshot();
    write_u64(out, raft_checkpoint.encode_total);
    write_u64(out, raft_checkpoint.encode_us_total);
    write_u64(out, raft_checkpoint.encode_us_max);
    write_u64(out, raft_checkpoint.store_total);
    write_u64(out, raft_checkpoint.store_error_total);
    write_u64(out, raft_checkpoint.store_us_total);
    write_u64(out, raft_checkpoint.store_us_max);
    write_u64(out, raft_checkpoint.file_sync_total);
    write_u64(out, raft_checkpoint.file_sync_us_total);
    write_u64(out, raft_checkpoint.file_sync_us_max);
    write_u64(out, raft_checkpoint.directory_sync_total);
    write_u64(out, raft_checkpoint.directory_sync_us_total);
    write_u64(out, raft_checkpoint.directory_sync_us_max);
    write_u64(out, raft_checkpoint.bytes_total);
    write_u64(out, raft_checkpoint.bytes_last);
    write_u64(out, raft_checkpoint.bytes_max);
    write_u64(out, raft_checkpoint.compaction_total);
    write_u64(out, raft_checkpoint.compaction_error_total);
    write_u64(out, raft_checkpoint.compaction_us_total);
    write_u64(out, raft_checkpoint.compaction_us_max);
    let raft_wal = observability::control_plane_raft_wal_metrics_snapshot();
    write_u64(out, raft_wal.append_total);
    write_u64(out, raft_wal.append_error_total);
    write_u64(out, raft_wal.append_us_total);
    write_u64(out, raft_wal.append_us_max);
    write_u64(out, raft_wal.lock_wait_us_total);
    write_u64(out, raft_wal.lock_wait_us_max);
    write_u64(out, raft_wal.frame_bytes_total);
    write_u64(out, raft_wal.frame_bytes_last);
    write_u64(out, raft_wal.frame_bytes_max);
    write_u64(out, raft_wal.file_sync_total);
    write_u64(out, raft_wal.file_sync_us_total);
    write_u64(out, raft_wal.file_sync_us_max);
    write_u64(out, raft_wal.directory_sync_total);
    write_u64(out, raft_wal.directory_sync_us_total);
    write_u64(out, raft_wal.directory_sync_us_max);
    write_u64(out, raft_wal.durability_queue_depth);
    write_u64(out, raft_wal.durability_queue_depth_max);
    write_u64(out, raft_wal.durability_queue_wait_us_total);
    write_u64(out, raft_wal.durability_queue_wait_us_max);
    write_u64(out, raft_wal.append_accept_us_total);
    write_u64(out, raft_wal.append_accept_us_max);
    write_u64(out, raft_wal.durability_operation_us_total);
    write_u64(out, raft_wal.durability_operation_us_max);
    let raft_command = observability::control_plane_raft_command_metrics_snapshot();
    write_u64(out, raft_command.submit_total);
    write_u64(out, raft_command.submit_error_total);
    write_u64(out, raft_command.queue_wait_us_total);
    write_u64(out, raft_command.queue_wait_us_max);
    write_u64(out, raft_command.operation_us_total);
    write_u64(out, raft_command.operation_us_max);
    let runtime_node_ids = runtime_map
        .nodes()
        .iter()
        .map(|node| node.node_id().as_u32())
        .collect::<BTreeSet<_>>();
    let history_reference_samples = observability::control_plane_history_reference_samples()
        .into_iter()
        .filter(|sample| runtime_node_ids.contains(&sample.node_id))
        .collect::<Vec<_>>();
    write_u32(
        out,
        len_as_u32(
            history_reference_samples.len(),
            "control-plane history reference samples",
        )?,
    );
    for sample in history_reference_samples {
        write_u32(out, sample.node_id);
        write_u64(out, sample.observed_epoch);
        write_u64(out, sample.validation_epoch);
        write_u64(out, sample.observed_at_ms);
        write_option_u64(out, sample.oldest_live_placement_epoch);
        write_option_u64(out, sample.oldest_durable_backfill_epoch);
        write_option_u64(out, sample.oldest_pending_metadata_command_epoch);
        write_option_u64(out, sample.oldest_object_payload_reclaim_claim_epoch);
    }
    write_u32(
        out,
        len_as_u32(
            diagnostics.node_leases().len(),
            "control-plane diagnostic node leases",
        )?,
    );
    for node_lease in diagnostics.node_leases() {
        write_u32(out, node_lease.node_id().as_u32());
        write_option_u64(out, node_lease.lease_deadline_ms());
    }
    Ok(())
}

fn read_control_plane_runtime_map_diagnostics(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneRuntimeMapDiagnostics, ControlPlaneError> {
    let runtime_map = read_runtime_map_snapshot(reader)?;
    let metric_count = reader.read_collection_len("control-plane RPC metrics", 97)?;
    if metric_count > observability::ControlPlaneRpcMetricKind::COUNT {
        return Err(ControlPlaneError::rpc_protocol(format!(
            "control-plane RPC metric count {metric_count} exceeds {}",
            observability::ControlPlaneRpcMetricKind::COUNT
        )));
    }
    let mut rpc_metrics = Vec::with_capacity(metric_count);
    let mut seen = BTreeSet::new();
    for _ in 0..metric_count {
        let kind = read_control_plane_rpc_metric_kind(reader.read_u8()?)?;
        if !seen.insert(control_plane_rpc_metric_kind_code(kind)) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "duplicate control-plane RPC metric kind {}",
                kind.as_str()
            )));
        }
        rpc_metrics.push(observability::ControlPlaneRpcMetricSample {
            kind,
            total: reader.read_u64()?,
            lock_wait_us_total: reader.read_u64()?,
            lock_wait_us_max: reader.read_u64()?,
            operation_us_total: reader.read_u64()?,
            operation_us_max: reader.read_u64()?,
            response_write_us_total: reader.read_u64()?,
            response_write_us_max: reader.read_u64()?,
            response_write_error_total: reader.read_u64()?,
            response_write_broken_pipe_total: reader.read_u64()?,
            response_write_connection_reset_total: reader.read_u64()?,
            response_write_timeout_total: reader.read_u64()?,
            response_write_other_error_total: reader.read_u64()?,
        });
    }
    let snapshot_metrics = observability::ControlPlaneSnapshotMetricSnapshot {
        serialize_total: reader.read_u64()?,
        serialize_us_total: reader.read_u64()?,
        serialize_us_max: reader.read_u64()?,
        save_total: reader.read_u64()?,
        save_error_total: reader.read_u64()?,
        save_us_total: reader.read_u64()?,
        save_us_max: reader.read_u64()?,
        sync_total: reader.read_u64()?,
        sync_us_total: reader.read_u64()?,
        sync_us_max: reader.read_u64()?,
        bytes_total: reader.read_u64()?,
        bytes_last: reader.read_u64()?,
        bytes_max: reader.read_u64()?,
    };
    let journal_metrics = observability::ControlPlaneJournalMetricSnapshot {
        append_total: reader.read_u64()?,
        append_error_total: reader.read_u64()?,
        append_us_total: reader.read_u64()?,
        append_us_max: reader.read_u64()?,
        lock_wait_us_total: reader.read_u64()?,
        lock_wait_us_max: reader.read_u64()?,
        frame_bytes_total: reader.read_u64()?,
        frame_bytes_last: reader.read_u64()?,
        frame_bytes_max: reader.read_u64()?,
        file_sync_total: reader.read_u64()?,
        file_sync_us_total: reader.read_u64()?,
        file_sync_us_max: reader.read_u64()?,
        directory_sync_total: reader.read_u64()?,
        directory_sync_us_total: reader.read_u64()?,
        directory_sync_us_max: reader.read_u64()?,
        compaction_total: reader.read_u64()?,
        compaction_error_total: reader.read_u64()?,
        compaction_us_total: reader.read_u64()?,
        compaction_us_max: reader.read_u64()?,
        compaction_lock_wait_us_total: reader.read_u64()?,
        compaction_lock_wait_us_max: reader.read_u64()?,
        compaction_bytes_total: reader.read_u64()?,
        compaction_bytes_last: reader.read_u64()?,
        compaction_bytes_max: reader.read_u64()?,
        compaction_file_sync_total: reader.read_u64()?,
        compaction_file_sync_us_total: reader.read_u64()?,
        compaction_file_sync_us_max: reader.read_u64()?,
        compaction_directory_sync_total: reader.read_u64()?,
        compaction_directory_sync_us_total: reader.read_u64()?,
        compaction_directory_sync_us_max: reader.read_u64()?,
    };
    let raft_checkpoint_metrics = observability::ControlPlaneRaftCheckpointMetricSnapshot {
        encode_total: reader.read_u64()?,
        encode_us_total: reader.read_u64()?,
        encode_us_max: reader.read_u64()?,
        store_total: reader.read_u64()?,
        store_error_total: reader.read_u64()?,
        store_us_total: reader.read_u64()?,
        store_us_max: reader.read_u64()?,
        file_sync_total: reader.read_u64()?,
        file_sync_us_total: reader.read_u64()?,
        file_sync_us_max: reader.read_u64()?,
        directory_sync_total: reader.read_u64()?,
        directory_sync_us_total: reader.read_u64()?,
        directory_sync_us_max: reader.read_u64()?,
        bytes_total: reader.read_u64()?,
        bytes_last: reader.read_u64()?,
        bytes_max: reader.read_u64()?,
        compaction_total: reader.read_u64()?,
        compaction_error_total: reader.read_u64()?,
        compaction_us_total: reader.read_u64()?,
        compaction_us_max: reader.read_u64()?,
    };
    let raft_wal_metrics = observability::ControlPlaneRaftWalMetricSnapshot {
        append_total: reader.read_u64()?,
        append_error_total: reader.read_u64()?,
        append_us_total: reader.read_u64()?,
        append_us_max: reader.read_u64()?,
        lock_wait_us_total: reader.read_u64()?,
        lock_wait_us_max: reader.read_u64()?,
        frame_bytes_total: reader.read_u64()?,
        frame_bytes_last: reader.read_u64()?,
        frame_bytes_max: reader.read_u64()?,
        file_sync_total: reader.read_u64()?,
        file_sync_us_total: reader.read_u64()?,
        file_sync_us_max: reader.read_u64()?,
        directory_sync_total: reader.read_u64()?,
        directory_sync_us_total: reader.read_u64()?,
        directory_sync_us_max: reader.read_u64()?,
        durability_queue_depth: reader.read_u64()?,
        durability_queue_depth_max: reader.read_u64()?,
        durability_queue_wait_us_total: reader.read_u64()?,
        durability_queue_wait_us_max: reader.read_u64()?,
        append_accept_us_total: reader.read_u64()?,
        append_accept_us_max: reader.read_u64()?,
        durability_operation_us_total: reader.read_u64()?,
        durability_operation_us_max: reader.read_u64()?,
    };
    let raft_command_metrics = observability::ControlPlaneRaftCommandMetricSnapshot {
        submit_total: reader.read_u64()?,
        submit_error_total: reader.read_u64()?,
        queue_wait_us_total: reader.read_u64()?,
        queue_wait_us_max: reader.read_u64()?,
        operation_us_total: reader.read_u64()?,
        operation_us_max: reader.read_u64()?,
    };
    let history_reference_count =
        reader.read_collection_len("control-plane history reference samples", 31)?;
    if history_reference_count > runtime_map.nodes().len() {
        return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane history reference sample count {history_reference_count} exceeds runtime node count {}",
                runtime_map.nodes().len()
            )));
    }
    let runtime_node_ids = runtime_map
        .nodes()
        .iter()
        .map(|node| node.node_id().as_u32())
        .collect::<BTreeSet<_>>();
    let mut history_reference_samples = Vec::with_capacity(history_reference_count);
    let mut seen_nodes = BTreeSet::new();
    for _ in 0..history_reference_count {
        let node_id = reader.read_u32()?;
        if !runtime_node_ids.contains(&node_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane history reference sample names unknown node {node_id}"
            )));
        }
        if !seen_nodes.insert(node_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "duplicate control-plane history reference sample for node {node_id}"
            )));
        }
        let observed_epoch = read_cluster_epoch(reader, "history reference observed epoch")?;
        let validation_epoch = read_cluster_epoch(reader, "history reference validation epoch")?;
        let observed_at_ms = reader.read_u64()?;
        let oldest_live_placement_epoch =
            read_option_cluster_epoch(reader, "history reference live placement epoch")?;
        let oldest_durable_backfill_epoch =
            read_option_cluster_epoch(reader, "history reference durable backfill epoch")?;
        let oldest_pending_metadata_command_epoch =
            read_option_cluster_epoch(reader, "history reference pending metadata command epoch")?;
        let oldest_object_payload_reclaim_claim_epoch = read_option_cluster_epoch(
            reader,
            "history reference object payload reclaim claim epoch",
        )?;
        if observed_epoch > validation_epoch {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "history reference sample for node {node_id} observed epoch {observed_epoch} beyond validation epoch {validation_epoch}"
                )));
        }
        if validation_epoch > runtime_map.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "history reference sample for node {node_id} has future validation epoch {validation_epoch} beyond runtime-map epoch {}",
                    runtime_map.cluster_epoch()
                )));
        }
        for (field, component_epoch) in [
            ("live placement", oldest_live_placement_epoch),
            ("durable backfill", oldest_durable_backfill_epoch),
            (
                "pending metadata command",
                oldest_pending_metadata_command_epoch,
            ),
            (
                "object payload reclaim claim",
                oldest_object_payload_reclaim_claim_epoch,
            ),
        ] {
            let Some(component_epoch) = component_epoch else {
                continue;
            };
            if component_epoch > validation_epoch {
                return Err(ControlPlaneError::rpc_protocol(format!(
                        "history reference sample for node {node_id} has future {field} epoch {component_epoch} beyond validation epoch {validation_epoch}"
                    )));
            }
        }
        history_reference_samples.push(observability::ControlPlaneHistoryReferenceSample {
            node_id,
            observed_epoch: observed_epoch.get(),
            validation_epoch: validation_epoch.get(),
            observed_at_ms,
            oldest_live_placement_epoch: oldest_live_placement_epoch.map(ClusterEpoch::get),
            oldest_durable_backfill_epoch: oldest_durable_backfill_epoch.map(ClusterEpoch::get),
            oldest_pending_metadata_command_epoch: oldest_pending_metadata_command_epoch
                .map(ClusterEpoch::get),
            oldest_object_payload_reclaim_claim_epoch: oldest_object_payload_reclaim_claim_epoch
                .map(ClusterEpoch::get),
        });
    }
    let node_lease_count = reader.read_collection_len(
        "control-plane diagnostic node leases",
        CONTROL_PLANE_RPC_NODE_LEASE_DIAGNOSTIC_MIN_LEN,
    )?;
    if node_lease_count != runtime_map.nodes().len() {
        return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane diagnostic node lease count {node_lease_count} does not match runtime node count {}",
                runtime_map.nodes().len()
            )));
    }
    let mut node_leases = Vec::with_capacity(node_lease_count);
    for expected_node in runtime_map.nodes() {
        let node_id = NodeId::new(reader.read_u32()?);
        if node_id != expected_node.node_id() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane diagnostic node lease names node {}, expected canonical node {}",
                node_id.as_u32(),
                expected_node.node_id().as_u32()
            )));
        }
        node_leases.push(ControlPlaneRuntimeMapNodeLeaseDiagnostic {
            node_id,
            lease_deadline_ms: reader.read_option_u64()?,
        });
    }
    Ok(ControlPlaneRuntimeMapDiagnostics {
        runtime_map,
        rpc_metrics,
        snapshot_metrics,
        journal_metrics,
        raft_checkpoint_metrics,
        raft_wal_metrics,
        raft_command_metrics,
        history_reference_samples,
        node_leases,
    })
}

fn control_plane_rpc_metric_kind_code(kind: observability::ControlPlaneRpcMetricKind) -> u8 {
    use observability::ControlPlaneRpcMetricKind as Kind;
    match kind {
        Kind::RuntimeMapSnapshot => 1,
        Kind::RefreshNodeHeartbeat => 2,
        Kind::SetPgActingSet => 3,
        Kind::SetPgActingSetWithMetadataTransfer => 4,
        Kind::SetPgActingSetWithMetadataTransferRuntimeMap => 5,
        Kind::FencePgForMetadataTransferRuntimeMap => 6,
        Kind::TransferRaftLeadership => 7,
        Kind::PgRuntimeMapSnapshot => 8,
        Kind::TriggerRaftSnapshotAndPurge => 9,
        Kind::TriggerRaftElection => 10,
        Kind::RuntimeMapStatus => 11,
        Kind::PendingMetadataCommandRecoveries => 12,
        Kind::AuthorityClockStatus => 13,
        Kind::ReestablishAuthorityClock => 14,
        Kind::RuntimeMapDiagnostics => 15,
        Kind::Unknown => 16,
        Kind::ServingPgRuntimeMapSnapshot => 17,
    }
}

fn read_control_plane_rpc_metric_kind(
    code: u8,
) -> Result<observability::ControlPlaneRpcMetricKind, ControlPlaneError> {
    use observability::ControlPlaneRpcMetricKind as Kind;
    match code {
        1 => Ok(Kind::RuntimeMapSnapshot),
        2 => Ok(Kind::RefreshNodeHeartbeat),
        3 => Ok(Kind::SetPgActingSet),
        4 => Ok(Kind::SetPgActingSetWithMetadataTransfer),
        5 => Ok(Kind::SetPgActingSetWithMetadataTransferRuntimeMap),
        6 => Ok(Kind::FencePgForMetadataTransferRuntimeMap),
        7 => Ok(Kind::TransferRaftLeadership),
        8 => Ok(Kind::PgRuntimeMapSnapshot),
        9 => Ok(Kind::TriggerRaftSnapshotAndPurge),
        10 => Ok(Kind::TriggerRaftElection),
        11 => Ok(Kind::RuntimeMapStatus),
        12 => Ok(Kind::PendingMetadataCommandRecoveries),
        13 => Ok(Kind::AuthorityClockStatus),
        14 => Ok(Kind::ReestablishAuthorityClock),
        15 => Ok(Kind::RuntimeMapDiagnostics),
        16 => Ok(Kind::Unknown),
        17 => Ok(Kind::ServingPgRuntimeMapSnapshot),
        _ => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid control-plane RPC metric kind {code}"
        ))),
    }
}

fn write_pending_metadata_command_recovery_listing(
    out: &mut Vec<u8>,
    listing: &PendingMetadataCommandRecoveryListing,
) -> Result<(), ControlPlaneError> {
    write_u32(
        out,
        len_as_u32(listing.tasks().len(), "pending metadata command recoveries")?,
    );
    for task in listing.tasks() {
        write_u32(out, task.pg_id().get());
        write_u32(out, task.recovery().reporting_node_id().as_u32());
        write_u64(out, task.recovery().pending().cluster_epoch().get());
        write_u64(out, task.recovery().pending().log_index());
        write_u64(out, task.recovery().pending().command_checksum());
    }
    write_u32(
        out,
        len_as_u32(
            listing.failures().len(),
            "pending metadata command recovery discovery failures",
        )?,
    );
    for failure in listing.failures() {
        write_u32(out, failure.pg_id().get());
        out.push(failure.kind().as_u8());
        write_string(out, failure.detail())?;
    }
    Ok(())
}

fn read_pending_metadata_command_recovery_listing(
    reader: &mut PayloadReader<'_>,
) -> Result<PendingMetadataCommandRecoveryListing, ControlPlaneError> {
    let count = reader.read_collection_len(
        "pending metadata command recoveries",
        CONTROL_PLANE_RPC_PENDING_RECOVERY_TASK_MIN_LEN,
    )?;
    let mut tasks = Vec::with_capacity(count);
    let mut pg_ids = BTreeSet::new();
    for _ in 0..count {
        let pg_id = PgId::new(reader.read_u32()?);
        if !pg_ids.insert(pg_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "pending metadata command recoveries repeat PG {}",
                pg_id.get()
            )));
        }
        let reporting_node_id = NodeId::new(reader.read_u32()?);
        let pending = PendingMetadataCommandObservation::new(
            read_cluster_epoch(reader, "pending recovery command epoch")?,
            NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "pending recovery command log index must be nonzero".to_owned(),
                )
            })?,
            reader.read_u64()?,
        );
        tasks.push(PendingMetadataCommandRecoveryTask::new(
            pg_id,
            PendingMetadataCommandRecovery::new(reporting_node_id, pending),
        ));
    }
    let failure_count = reader.read_collection_len(
        "pending metadata command recovery discovery failures",
        CONTROL_PLANE_RPC_PENDING_RECOVERY_FAILURE_MIN_LEN,
    )?;
    let mut failures = Vec::with_capacity(failure_count);
    for _ in 0..failure_count {
        let pg_id = PgId::new(reader.read_u32()?);
        if !pg_ids.insert(pg_id) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "pending metadata command recovery listing repeats PG {}",
                pg_id.get()
            )));
        }
        let kind = PendingMetadataCommandRecoveryDiscoveryFailureKind::from_u8(reader.read_u8()?)?;
        let detail = reader.read_string()?.to_owned();
        failures.push(PendingMetadataCommandRecoveryDiscoveryFailure::new(
            pg_id, kind, detail,
        ));
    }
    Ok(PendingMetadataCommandRecoveryListing::new(tasks, failures))
}

fn write_authority_clock_blocked_reason(
    out: &mut Vec<u8>,
    reason: Option<ControlPlaneAuthorityClockBlockedReason>,
) {
    write_u8(
        out,
        match reason {
            None => 0,
            Some(ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity) => 1,
            Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged) => 2,
            Some(ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable) => 3,
            Some(ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression) => 4,
            Some(ControlPlaneAuthorityClockBlockedReason::WallClockRegression) => 5,
            Some(ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump) => 6,
            Some(ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure) => 7,
        },
    );
}

fn read_authority_clock_blocked_reason(
    reader: &mut PayloadReader<'_>,
) -> Result<Option<ControlPlaneAuthorityClockBlockedReason>, ControlPlaneError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity,
        )),
        2 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged,
        )),
        3 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable,
        )),
        4 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression,
        )),
        5 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::WallClockRegression,
        )),
        6 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump,
        )),
        7 => Ok(Some(
            ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure,
        )),
        tag => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid authority-clock blocked-reason tag {tag}"
        ))),
    }
}

fn write_authority_clock_status(out: &mut Vec<u8>, status: ControlPlaneAuthorityClockStatus) {
    write_u64(out, status.generation());
    write_u8(out, u8::from(status.established()));
    write_authority_clock_blocked_reason(out, status.blocked_reason());
    write_option_u64(out, status.committed_timestamp_high_water_ms());
    write_option_u64(out, status.bound_raft_leadership_term());
    write_option_u64(out, status.current_raft_leadership_term());
    write_u8(out, u8::from(status.local_raft_authority_leader()));
    write_u8(out, u8::from(status.local_raft_authority_serving()));
}

fn read_authority_clock_status(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
    Ok(ControlPlaneAuthorityClockStatus {
        generation: reader.read_u64()?,
        established: reader.read_bool()?,
        blocked_reason: read_authority_clock_blocked_reason(reader)?,
        committed_timestamp_high_water_ms: reader.read_option_u64()?,
        bound_raft_leadership_term: reader.read_option_u64()?,
        current_raft_leadership_term: reader.read_option_u64()?,
        local_raft_authority_leader: reader.read_bool()?,
        local_raft_authority_serving: reader.read_bool()?,
    })
}

fn write_runtime_map_snapshot(
    out: &mut Vec<u8>,
    snapshot: &ClusterRuntimeMapSnapshot,
) -> Result<(), ControlPlaneError> {
    write_u64(out, snapshot.cluster_epoch().get());
    write_option_u64(out, snapshot.valid_until_ms());
    write_runtime_map_freshness_proof(out, snapshot.freshness_proof());
    write_u32(out, len_as_u32(snapshot.nodes().len(), "runtime nodes")?);
    for node in snapshot.nodes() {
        write_u32(out, node.node_id().as_u32());
        write_u64(out, node.node_incarnation());
        write_string(out, node.endpoint())?;
        write_option_u64(
            out,
            node.cluster_map_history_floor_epoch()
                .map(ClusterEpoch::get),
        );
    }
    write_pg_route_snapshots(out, "PG routes", snapshot.pg_routes())?;
    write_pg_route_snapshots(out, "historical PG routes", snapshot.historical_pg_routes())?;
    write_u32(
        out,
        len_as_u32(
            snapshot.historical_cluster_epochs().len(),
            "historical cluster epochs",
        )?,
    );
    for epoch in snapshot.historical_cluster_epochs() {
        write_u64(out, epoch.get());
    }
    Ok(())
}

fn write_runtime_map_freshness_proof(out: &mut Vec<u8>, proof: &RuntimeMapFreshnessProof) {
    match proof {
        RuntimeMapFreshnessProof::SingleAuthority {
            authority_incarnation,
            issued_at_ms,
        } => {
            write_u8(out, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_SINGLE_AUTHORITY);
            write_u64(out, authority_incarnation.get());
            write_u64(out, *issued_at_ms);
        }
        RuntimeMapFreshnessProof::ReadIndex {
            authority_incarnation,
            read_index,
            issued_at_ms,
        } => {
            write_u8(out, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_READ_INDEX);
            write_u64(out, authority_incarnation.get());
            write_u64(out, read_index.term());
            write_u64(out, read_index.index());
            write_u64(out, *issued_at_ms);
        }
        RuntimeMapFreshnessProof::Reconstructed {
            authority_incarnation,
        } => {
            write_u8(out, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_RECONSTRUCTED);
            write_u64(out, authority_incarnation.get());
        }
    }
}

fn write_pg_route_snapshots(
    out: &mut Vec<u8>,
    label: &'static str,
    routes: &[PgRouteSnapshot],
) -> Result<(), ControlPlaneError> {
    write_u32(out, len_as_u32(routes.len(), label)?);
    for route in routes {
        write_u64(out, route.cluster_epoch().get());
        write_u32(out, route.pg_id().get());
        write_u32(out, route.primary_node_id().as_u32());
        write_pg_state(out, route.state());
        match route.active_metadata_proof() {
            Some(proof) => {
                write_u8(out, 1);
                write_pg_metadata_proof(out, proof);
            }
            None => write_u8(out, 0),
        }
        match route.metadata_read_route() {
            Some(read_route) => {
                write_u8(out, 1);
                write_u32(out, read_route.node_id().as_u32());
                write_pg_metadata_proof(out, read_route.proof());
            }
            None => write_u8(out, 0),
        }
        write_option_u64(out, route.primary_lease_deadline_ms());
        match route.peering_metadata_transfer() {
            Some(transfer) => {
                write_u8(out, 1);
                write_u64(out, transfer.source_epoch().get());
                write_pg_metadata_proof(out, transfer.source_metadata_proof());
                write_pg_metadata_proof(out, transfer.metadata_proof());
                write_option_u64(
                    out,
                    route
                        .peering_metadata_transfer_destination_epoch()
                        .map(ClusterEpoch::get),
                );
                write_option_u64(
                    out,
                    route
                        .peering_metadata_transfer_source_route_epoch()
                        .map(ClusterEpoch::get),
                );
                write_option_u32(
                    out,
                    route
                        .peering_metadata_transfer_source_node_id()
                        .map(NodeId::as_u32),
                );
            }
            None => write_u8(out, 0),
        }
        match route.pending_metadata_command_recovery() {
            Some(recovery) => {
                write_u8(out, 1);
                write_u32(out, recovery.reporting_node_id().as_u32());
                write_u64(out, recovery.pending().cluster_epoch().get());
                write_u64(out, recovery.pending().log_index());
                write_u64(out, recovery.pending().command_checksum());
            }
            None => write_u8(out, 0),
        }
        write_u32(out, len_as_u32(route.acting_set().len(), "acting set")?);
        for node_id in route.acting_set() {
            write_u32(out, node_id.as_u32());
        }
    }
    Ok(())
}

fn read_runtime_map_snapshot(
    reader: &mut PayloadReader<'_>,
) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
    let cluster_epoch = read_cluster_epoch(reader, "runtime map cluster epoch")?;
    let valid_until_ms = reader.read_option_u64()?;
    let freshness_proof = read_runtime_map_freshness_proof(reader)?;
    let node_count =
        reader.read_collection_len("runtime nodes", CONTROL_PLANE_RPC_RUNTIME_NODE_MIN_LEN)?;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        nodes.push(NodeRouteSnapshot {
            node_id: NodeId::new(reader.read_u32()?),
            node_incarnation: reader.read_u64()?,
            endpoint: reader.read_string()?.to_owned(),
            cluster_map_history_floor_epoch: read_option_cluster_epoch(
                reader,
                "runtime node cluster-map history floor epoch",
            )?,
        });
    }
    let pg_routes = read_pg_route_snapshots(reader, "PG routes")?;
    let historical_pg_routes = read_pg_route_snapshots(reader, "historical PG routes")?;
    let historical_epoch_count =
        reader.read_collection_len("historical cluster epochs", std::mem::size_of::<u64>())?;
    let mut historical_cluster_epochs = Vec::with_capacity(historical_epoch_count);
    for _ in 0..historical_epoch_count {
        historical_cluster_epochs.push(read_cluster_epoch(
            reader,
            "runtime map historical cluster epoch",
        )?);
    }
    let Some(valid_until_ms) = valid_until_ms else {
        return Err(ControlPlaneError::rpc_protocol(
            "runtime map validity must be bounded on the wire".to_owned(),
        ));
    };
    let snapshot = ClusterRuntimeMapSnapshot {
        cluster_epoch,
        validity: RouteMapValidity::from_valid_until_ms(Some(valid_until_ms)).ok_or_else(|| {
            ControlPlaneError::rpc_protocol(
                "runtime map validity deadline uses reserved unbounded sentinel".to_owned(),
            )
        })?,
        freshness_proof,
        nodes,
        pg_routes,
        historical_pg_routes,
        historical_cluster_epochs,
    };
    validate_runtime_map_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn validate_runtime_map_snapshot(
    snapshot: &ClusterRuntimeMapSnapshot,
) -> Result<(), ControlPlaneError> {
    let mut previous_historical_epoch = None;
    for epoch in snapshot.historical_cluster_epochs() {
        if *epoch >= snapshot.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map historical epoch {} is not older than current epoch {}",
                epoch.get(),
                snapshot.cluster_epoch().get()
            )));
        }
        if previous_historical_epoch.is_some_and(|previous| previous >= *epoch) {
            return Err(ControlPlaneError::rpc_protocol(
                "runtime map historical epochs are not strictly increasing".to_owned(),
            ));
        }
        previous_historical_epoch = Some(*epoch);
    }
    let mut node_ids = BTreeSet::new();
    for node in snapshot.nodes() {
        if !node_ids.insert(node.node_id()) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map contains duplicate node {}",
                node.node_id().as_u32()
            )));
        }
        if node.endpoint().is_empty() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map node {} has an empty endpoint",
                node.node_id().as_u32()
            )));
        }
    }

    validate_runtime_map_routes(
        snapshot.cluster_epoch(),
        &node_ids,
        "runtime map",
        true,
        snapshot.pg_routes(),
    )?;
    validate_runtime_map_routes(
        snapshot.cluster_epoch(),
        &node_ids,
        "runtime map historical",
        false,
        snapshot.historical_pg_routes(),
    )?;
    for route in snapshot.historical_pg_routes() {
        if snapshot
            .historical_cluster_epochs()
            .binary_search(&route.cluster_epoch())
            .is_err()
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map historical route epoch {} is not retained",
                route.cluster_epoch().get()
            )));
        }
    }
    for route in snapshot.pg_routes() {
        let Some(recovery) = route.pending_metadata_command_recovery() else {
            continue;
        };
        if route.state() != PgState::Peering {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map route for PG {} authorizes pending command recovery while {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if recovery.pending().cluster_epoch() >= snapshot.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} pending command epoch {} is not older than current epoch {}",
                    route.pg_id().get(),
                    recovery.pending().cluster_epoch().get(),
                    snapshot.cluster_epoch().get()
                )));
        }
        let historical = snapshot
            .reconstructed_pg_route_at_epoch(route.pg_id(), recovery.pending().cluster_epoch())?;
        if historical.state() != PgState::Active
            || historical.primary_node_id() != recovery.reporting_node_id()
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} pending command recovery reporter {} is not the Active historical primary at epoch {}",
                    route.pg_id().get(),
                    recovery.reporting_node_id().as_u32(),
                    recovery.pending().cluster_epoch().get()
                )));
        }
    }
    validate_runtime_map_transfer_sources(snapshot)
}

fn validate_runtime_map_transfer_sources(
    snapshot: &ClusterRuntimeMapSnapshot,
) -> Result<(), ControlPlaneError> {
    for route in snapshot
        .pg_routes()
        .iter()
        .chain(snapshot.historical_pg_routes())
    {
        let Some(source_route_epoch) = route.peering_metadata_transfer_source_route_epoch() else {
            continue;
        };
        let source_node_id = route
            .peering_metadata_transfer_source_node_id()
            .expect("metadata transfer source route fields validated as complete");
        if source_route_epoch >= route.cluster_epoch() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} metadata transfer source route epoch {} must be older than transfer route epoch {}",
                    route.pg_id().get(),
                    source_route_epoch.get(),
                    route.cluster_epoch().get()
                )));
        }
        let source_route =
            runtime_map_route_at_epoch(snapshot, route.pg_id(), source_route_epoch).ok_or_else(
                || ControlPlaneError::rpc_protocol(format!(
                        "runtime map route for PG {} references missing metadata transfer source route epoch {}",
                        route.pg_id().get(),
                        source_route_epoch.get()
                    )),
            )?;
        if source_route.primary_node_id() != source_node_id {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "runtime map route for PG {} metadata transfer source node {} does not match source route primary {} at epoch {}",
                    route.pg_id().get(),
                    source_node_id.as_u32(),
                    source_route.primary_node_id().as_u32(),
                    source_route_epoch.get()
                )));
        }
    }
    Ok(())
}

fn runtime_map_route_at_epoch(
    snapshot: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
) -> Option<PgRouteSnapshot> {
    if cluster_epoch == snapshot.cluster_epoch() {
        snapshot
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
            .map(PgRouteSnapshot::without_serving_authority)
    } else {
        snapshot
            .reconstructed_pg_route_at_epoch(pg_id, cluster_epoch)
            .ok()
    }
}

fn validate_runtime_map_routes(
    current_cluster_epoch: ClusterEpoch,
    node_ids: &BTreeSet<NodeId>,
    label: &'static str,
    is_current_route_set: bool,
    routes: &[PgRouteSnapshot],
) -> Result<(), ControlPlaneError> {
    let mut seen_routes = BTreeSet::new();
    for route in routes {
        let route_key = (route.cluster_epoch(), route.pg_id());
        if !seen_routes.insert(route_key) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} contains duplicate route for PG {} at epoch {}",
                route.pg_id().get(),
                route.cluster_epoch().get()
            )));
        }
        if is_current_route_set && route.cluster_epoch() != current_cluster_epoch {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "runtime map route for PG {} has epoch {}, expected {}",
                route.pg_id().get(),
                route.cluster_epoch().get(),
                current_cluster_epoch.get()
            )));
        }
        if !is_current_route_set && route.cluster_epoch() >= current_cluster_epoch {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has epoch {}, expected an epoch older than {}",
                route.pg_id().get(),
                route.cluster_epoch().get(),
                current_cluster_epoch.get()
            )));
        }
        if route.acting_set().is_empty() {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has an empty acting set",
                route.pg_id().get()
            )));
        }
        let mut acting_set = BTreeSet::new();
        for &node_id in route.acting_set() {
            if !acting_set.insert(node_id) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} repeats acting-set node {}",
                    route.pg_id().get(),
                    node_id.as_u32()
                )));
            }
            if !node_ids.contains(&node_id) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} references unknown acting-set node {}",
                    route.pg_id().get(),
                    node_id.as_u32()
                )));
            }
        }
        if !acting_set.contains(&route.primary_node_id()) {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} primary {} is outside the acting set",
                route.pg_id().get(),
                route.primary_node_id().as_u32()
            )));
        }
        if route.primary_lease_deadline_ms().is_some() && route.state() != PgState::Active {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has serving authority but is {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if route.active_metadata_proof().is_some() && route.state() != PgState::Active {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has an active metadata proof but is {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if let Some(read_route) = route.metadata_read_route() {
            if !is_current_route_set {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} grants metadata read authority on a historical route",
                    route.pg_id().get()
                )));
            }
            if !acting_set.contains(&read_route.node_id()) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} metadata read node {} is outside the acting set",
                    route.pg_id().get(),
                    read_route.node_id().as_u32()
                )));
            }
            if route.state() != PgState::Peering {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} grants metadata read authority while {:?}",
                    route.pg_id().get(),
                    route.state()
                )));
            }
        }
        if route.peering_metadata_transfer().is_some()
            && (route
                .peering_metadata_transfer_destination_epoch()
                .is_none()
                || route
                    .peering_metadata_transfer_source_route_epoch()
                    .is_none()
                || route.peering_metadata_transfer_source_node_id().is_none())
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has incomplete metadata transfer source route",
                route.pg_id().get()
            )));
        }
        if let Some(source_node_id) = route.peering_metadata_transfer_source_node_id() {
            if !node_ids.contains(&source_node_id) {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} references unknown metadata transfer source node {}",
                    route.pg_id().get(),
                    source_node_id.as_u32()
                )));
            }
        }
        if route.peering_metadata_transfer().is_some() && route.state() != PgState::Peering {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has metadata transfer state but is {:?}",
                route.pg_id().get(),
                route.state()
            )));
        }
        if route.peering_metadata_transfer().is_none()
            && (route
                .peering_metadata_transfer_destination_epoch()
                .is_some()
                || route
                    .peering_metadata_transfer_source_route_epoch()
                    .is_some()
                || route.peering_metadata_transfer_source_node_id().is_some())
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                "{label} route for PG {} has source route fields without metadata transfer",
                route.pg_id().get()
            )));
        }
        if route.pending_metadata_command_recovery().is_some()
            && (!is_current_route_set || route.state() != PgState::Peering)
        {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "{label} route for PG {} has pending metadata command recovery outside a current Peering route",
                    route.pg_id().get()
                )));
        }
    }
    Ok(())
}

fn read_runtime_map_freshness_proof(
    reader: &mut PayloadReader<'_>,
) -> Result<RuntimeMapFreshnessProof, ControlPlaneError> {
    let tag = reader.read_u8()?;
    match tag {
        CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_SINGLE_AUTHORITY => {
            let authority_incarnation = read_runtime_map_proof_authority_incarnation(reader)?;
            Ok(RuntimeMapFreshnessProof::SingleAuthority {
                authority_incarnation,
                issued_at_ms: reader.read_u64()?,
            })
        }
        CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_RECONSTRUCTED => {
            let authority_incarnation = read_runtime_map_proof_authority_incarnation(reader)?;
            Ok(RuntimeMapFreshnessProof::Reconstructed {
                authority_incarnation,
            })
        }
        CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_READ_INDEX => {
            let authority_incarnation = read_runtime_map_proof_authority_incarnation(reader)?;
            Ok(RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation,
                read_index: read_runtime_map_proof_log_id(reader)?,
                issued_at_ms: reader.read_u64()?,
            })
        }
        other => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid runtime map freshness proof tag {other}"
        ))),
    }
}

fn read_runtime_map_proof_authority_incarnation(
    reader: &mut PayloadReader<'_>,
) -> Result<AuthorityIncarnation, ControlPlaneError> {
    AuthorityIncarnation::new(reader.read_u64()?).ok_or_else(|| {
        ControlPlaneError::rpc_protocol(
            "runtime map freshness proof authority incarnation must be nonzero".to_owned(),
        )
    })
}

fn read_runtime_map_proof_log_id(
    reader: &mut PayloadReader<'_>,
) -> Result<ControlPlaneLogId, ControlPlaneError> {
    let term = reader.read_u64()?;
    let index = reader.read_u64()?;
    if term == 0 {
        return Err(ControlPlaneError::rpc_protocol(
            "runtime map freshness proof read-index term must be nonzero".to_owned(),
        ));
    }
    ControlPlaneLogId::new(term, index).ok_or_else(|| {
        ControlPlaneError::rpc_protocol(
            "runtime map freshness proof read-index index must be nonzero".to_owned(),
        )
    })
}

fn read_pg_route_snapshots(
    reader: &mut PayloadReader<'_>,
    label: &'static str,
) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
    let route_count = reader.read_collection_len(label, CONTROL_PLANE_RPC_PG_ROUTE_MIN_LEN)?;
    let mut routes = Vec::with_capacity(route_count);
    for _ in 0..route_count {
        let route_epoch = read_cluster_epoch(reader, "PG route cluster epoch")?;
        let pg_id = PgId::new(reader.read_u32()?);
        let primary_node_id = NodeId::new(reader.read_u32()?);
        let state = read_pg_state(reader)?;
        let active_metadata_proof = match reader.read_u8()? {
            0 => None,
            1 => Some(read_pg_metadata_proof(reader)?),
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid PG route active metadata proof tag {tag}"
                )));
            }
        };
        let metadata_read_route = match reader.read_u8()? {
            0 => None,
            1 => Some(PgMetadataReadRoute::new(
                NodeId::new(reader.read_u32()?),
                read_pg_metadata_proof(reader)?,
            )),
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid PG metadata read route tag {tag}"
                )));
            }
        };
        let primary_lease_deadline_ms = reader.read_option_u64()?;
        let (
            peering_metadata_transfer,
            peering_metadata_transfer_destination_epoch,
            peering_metadata_transfer_source_route_epoch,
            peering_metadata_transfer_source_node_id,
        ) = match reader.read_u8()? {
            0 => (None, None, None, None),
            1 => {
                let source_epoch = read_cluster_epoch(reader, "metadata transfer source epoch")?;
                let source_metadata_proof = read_pg_metadata_proof(reader)?;
                let imported_metadata_proof = read_pg_metadata_proof(reader)?;
                let destination_epoch = reader
                    .read_option_u64()?
                    .map(|epoch| {
                        ClusterEpoch::new(epoch).ok_or_else(|| {
                            ControlPlaneError::rpc_protocol(
                                "metadata transfer destination epoch must be nonzero".to_owned(),
                            )
                        })
                    })
                    .transpose()?;
                let source_route_epoch = reader
                    .read_option_u64()?
                    .map(|epoch| {
                        ClusterEpoch::new(epoch).ok_or_else(|| {
                            ControlPlaneError::rpc_protocol(
                                "metadata transfer source route epoch must be nonzero".to_owned(),
                            )
                        })
                    })
                    .transpose()?;
                let source_node_id = reader.read_option_u32()?.map(NodeId::new);
                (
                    Some(PgMetadataTransferProof::new_with_imported_metadata_proof(
                        source_epoch,
                        source_metadata_proof,
                        imported_metadata_proof,
                    )),
                    destination_epoch,
                    source_route_epoch,
                    source_node_id,
                )
            }
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid PG route metadata transfer tag {tag}"
                )));
            }
        };
        let pending_metadata_command_recovery = match reader.read_u8()? {
            0 => None,
            1 => Some(PendingMetadataCommandRecovery {
                reporting_node_id: NodeId::new(reader.read_u32()?),
                pending: PendingMetadataCommandObservation::new(
                    read_cluster_epoch(reader, "pending metadata command recovery epoch")?,
                    NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                        ControlPlaneError::rpc_protocol(
                            "pending metadata command recovery log index must be nonzero"
                                .to_owned(),
                        )
                    })?,
                    reader.read_u64()?,
                ),
            }),
            tag => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "invalid pending metadata command recovery tag {tag}"
                )));
            }
        };
        let acting_set_len = reader.read_collection_len(
            "PG route acting set",
            CONTROL_PLANE_RPC_ACTING_SET_NODE_MIN_LEN,
        )?;
        let mut acting_set = Vec::with_capacity(acting_set_len);
        for _ in 0..acting_set_len {
            acting_set.push(NodeId::new(reader.read_u32()?));
        }
        match (
            peering_metadata_transfer,
            peering_metadata_transfer_destination_epoch,
        ) {
            (Some(transfer), Some(destination_epoch)) => {
                if destination_epoch > route_epoch || destination_epoch <= transfer.source_epoch() {
                    return Err(ControlPlaneError::rpc_protocol(format!(
                        "PG {} metadata transfer destination epoch {} must be newer than source epoch {} and no newer than route epoch {}",
                        pg_id.get(),
                        destination_epoch.get(),
                        transfer.source_epoch().get(),
                        route_epoch.get(),
                    )));
                }
            }
            (Some(_), None) => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "PG {} metadata transfer is missing its destination epoch",
                    pg_id.get()
                )));
            }
            (None, Some(_)) => {
                return Err(ControlPlaneError::rpc_protocol(format!(
                    "PG {} metadata transfer destination epoch requires a transfer marker",
                    pg_id.get()
                )));
            }
            (None, None) => {}
        }
        routes.push(PgRouteSnapshot {
            cluster_epoch: route_epoch,
            pg_id,
            primary_node_id,
            acting_set,
            state,
            active_metadata_proof,
            metadata_read_route,
            primary_lease_deadline_ms,
            peering_metadata_transfer,
            peering_metadata_transfer_destination_epoch,
            peering_metadata_transfer_source_route_epoch,
            peering_metadata_transfer_source_node_id,
            pending_metadata_command_recovery,
        });
    }
    Ok(routes)
}

fn write_pg_metadata_proof(out: &mut Vec<u8>, proof: PgMetadataProof) {
    write_u64(out, proof.applied_log_index);
    write_u64(out, proof.applied_log_hash);
    write_u64(out, proof.state_digest);
}

fn write_pending_metadata_command_observation(
    out: &mut Vec<u8>,
    pending: Option<PendingMetadataCommandObservation>,
) {
    match pending {
        Some(pending) => {
            write_u8(out, 1);
            write_u64(out, pending.cluster_epoch().get());
            write_u64(out, pending.log_index());
            write_u64(out, pending.command_checksum());
        }
        None => write_u8(out, 0),
    }
}

fn read_pending_metadata_command_observation(
    reader: &mut PayloadReader<'_>,
) -> Result<Option<PendingMetadataCommandObservation>, ControlPlaneError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => {
            let cluster_epoch =
                read_cluster_epoch(reader, "pending metadata command cluster epoch")?;
            let log_index = NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                ControlPlaneError::rpc_protocol(
                    "pending metadata command log index must be nonzero".to_owned(),
                )
            })?;
            let command_checksum = reader.read_u64()?;
            Ok(Some(PendingMetadataCommandObservation::new(
                cluster_epoch,
                log_index,
                command_checksum,
            )))
        }
        present => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid pending metadata command presence code {present}"
        ))),
    }
}

fn read_pg_metadata_proof(
    reader: &mut PayloadReader<'_>,
) -> Result<PgMetadataProof, ControlPlaneError> {
    Ok(PgMetadataProof {
        applied_log_index: reader.read_u64()?,
        applied_log_hash: reader.read_u64()?,
        state_digest: reader.read_u64()?,
    })
}

fn write_pg_state(out: &mut Vec<u8>, state: PgState) {
    write_u8(
        out,
        match state {
            PgState::Active => 1,
            PgState::Peering => 2,
            PgState::Degraded => 3,
            PgState::Backfilling => 4,
            PgState::Inconsistent => 5,
        },
    );
}

fn read_pg_state(reader: &mut PayloadReader<'_>) -> Result<PgState, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(PgState::Active),
        2 => Ok(PgState::Peering),
        3 => Ok(PgState::Degraded),
        4 => Ok(PgState::Backfilling),
        5 => Ok(PgState::Inconsistent),
        state => Err(ControlPlaneError::rpc_protocol(format!(
            "invalid PG state code {state}"
        ))),
    }
}

fn read_cluster_epoch(
    reader: &mut PayloadReader<'_>,
    field: &'static str,
) -> Result<ClusterEpoch, ControlPlaneError> {
    ClusterEpoch::new(reader.read_u64()?)
        .ok_or_else(|| ControlPlaneError::rpc_protocol(format!("{field} must be nonzero")))
}

fn read_option_cluster_epoch(
    reader: &mut PayloadReader<'_>,
    field: &'static str,
) -> Result<Option<ClusterEpoch>, ControlPlaneError> {
    reader
        .read_option_u64()?
        .map(|epoch| {
            ClusterEpoch::new(epoch)
                .ok_or_else(|| ControlPlaneError::rpc_protocol(format!("{field} must be nonzero")))
        })
        .transpose()
}

fn write_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_u64(out, value);
        }
        None => write_u8(out, 0),
    }
}

fn write_option_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_u32(out, value);
        }
        None => write_u8(out, 0),
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneError> {
    write_bytes(out, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), ControlPlaneError> {
    write_u32(out, len_as_u32(value.len(), "byte field")?);
    out.extend_from_slice(value);
    Ok(())
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn len_as_u32(len: usize, field: &'static str) -> Result<u32, ControlPlaneError> {
    u32::try_from(len).map_err(|_| {
        ControlPlaneError::rpc_protocol(format!("{field} length {len} exceeds u32::MAX"))
    })
}

struct PayloadReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn finish(&self) -> Result<(), ControlPlaneError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(ControlPlaneError::rpc_protocol(format!(
                "control-plane RPC payload has {} trailing bytes",
                self.payload.len() - self.offset
            )))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            ControlPlaneError::rpc_protocol("control-plane RPC payload offset overflow".to_owned())
        })?;
        let bytes = self.payload.get(self.offset..end).ok_or_else(|| {
            ControlPlaneError::rpc_protocol("truncated control-plane RPC payload".to_owned())
        })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, ControlPlaneError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ControlPlaneError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, ControlPlaneError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, ControlPlaneError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_bool(&mut self) -> Result<bool, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid boolean value {value}"
            ))),
        }
    }

    fn read_option_u64(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            value => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid optional u64 tag {value}"
            ))),
        }
    }

    fn read_option_u32(&mut self) -> Result<Option<u32>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u32()?)),
            value => Err(ControlPlaneError::rpc_protocol(format!(
                "invalid optional u32 tag {value}"
            ))),
        }
    }

    fn read_len(&mut self, field: &'static str) -> Result<usize, ControlPlaneError> {
        usize::try_from(self.read_u32()?).map_err(|_| {
            ControlPlaneError::rpc_protocol(format!("{field} length does not fit usize"))
        })
    }

    fn read_collection_len(
        &mut self,
        field: &'static str,
        min_item_len: usize,
    ) -> Result<usize, ControlPlaneError> {
        assert!(min_item_len > 0);
        let len = self.read_len(field)?;
        let max_items = self.remaining_len() / min_item_len;
        if len > max_items {
            return Err(ControlPlaneError::rpc_protocol(format!(
                    "{field} count {len} exceeds remaining control-plane RPC payload capacity {max_items}",
                )));
        }
        Ok(len)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len("byte field")?;
        self.read_exact(len)
    }

    fn read_string(&mut self) -> Result<&'a str, ControlPlaneError> {
        std::str::from_utf8(self.read_bytes()?).map_err(|source| {
            ControlPlaneError::rpc_protocol(format!(
                "control-plane RPC string is not UTF-8: {source}"
            ))
        })
    }

    fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}
