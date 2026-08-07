use std::collections::HashMap;
use std::fmt;
use std::io;
use std::mem::MaybeUninit;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use rustls::sign::CertifiedKey;
use thiserror::Error;

use crate::control_plane::{
    ControlPlaneAdmin, ControlPlaneAuthorityClock, ControlPlaneAuthorityClockCheckpointTarget,
    ControlPlaneError, ControlPlaneHeartbeatRuntimeMapSource, ControlPlaneRpcResponsePublication,
    ControlPlaneRpcServerListener, ControlPlaneRpcServerPolicy, ControlPlaneRpcServerRole,
    ControlPlaneRuntimeMapSource,
};
use crate::ControlPlaneRpcServerAuth;

/// A process-owned socket binding handed to the storage-owned control-plane
/// server bootstrap.
pub enum ControlPlaneRpcServerListenerInput {
    Unix {
        listener: UnixListener,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    },
    TlsTcp {
        listener: TcpListener,
        certified_key: Arc<CertifiedKey>,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
    },
}

impl fmt::Debug for ControlPlaneRpcServerListenerInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (transport, max_connections, max_frame_bytes, io_timeout) = match self {
            Self::Unix {
                max_connections,
                max_frame_bytes,
                io_timeout,
                ..
            } => ("unix", max_connections, max_frame_bytes, io_timeout),
            Self::TlsTcp {
                max_connections,
                max_frame_bytes,
                io_timeout,
                ..
            } => ("tls-tcp", max_connections, max_frame_bytes, io_timeout),
        };
        formatter
            .debug_struct("ControlPlaneRpcServerListenerInput")
            .field("transport", &transport)
            .field("max_connections", max_connections)
            .field("max_frame_bytes", max_frame_bytes)
            .field("io_timeout", io_timeout)
            .finish()
    }
}

/// Opaque ordinary and authority-clock-recovery listener/policy pairing.
pub struct ControlPlaneRpcServerBootstrap {
    ordinary_listeners: Vec<ControlPlaneRpcServerListener>,
    recovery_listeners: Vec<ControlPlaneRpcServerListener>,
    ordinary_policy: ControlPlaneRpcServerPolicy,
    recovery_policy: ControlPlaneRpcServerPolicy,
}

/// Opaque ownership token for active control-plane RPC listener loops.
#[must_use = "dropping the token detaches but does not stop the listener loops"]
pub struct ControlPlaneRpcServerLoops {
    handles: Vec<thread::JoinHandle<()>>,
}

impl fmt::Debug for ControlPlaneRpcServerBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRpcServerBootstrap")
            .field("ordinary_listeners", &self.ordinary_listeners.len())
            .field("recovery_listeners", &self.recovery_listeners.len())
            .field(
                "authenticated",
                &self.ordinary_policy.authentication_required(),
            )
            .finish()
    }
}

impl fmt::Debug for ControlPlaneRpcServerLoops {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRpcServerLoops")
            .field("listeners", &self.handles.len())
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneRpcServerBootstrapError {
    #[error("control-plane RPC server requires at least one ordinary listener")]
    MissingOrdinaryListener,
    #[error("control-plane RPC server requires at least one authority-clock recovery listener")]
    MissingRecoveryListener,
    #[error("ordinary control-plane RPC listener {index} is invalid")]
    InvalidOrdinaryListener { index: usize },
    #[error("authority-clock recovery RPC listener {index} is invalid")]
    InvalidRecoveryListener { index: usize },
    #[error(
        "ordinary control-plane RPC listener {ordinary_index} and authority-clock recovery RPC listener {recovery_index} refer to the same socket"
    )]
    AliasedRoleListener {
        ordinary_index: usize,
        recovery_index: usize,
    },
    #[error("ordinary control-plane RPC resource policy is invalid")]
    InvalidOrdinaryPolicy,
    #[error("authority-clock recovery RPC resource policy is invalid")]
    InvalidRecoveryPolicy,
}

/// Narrow ordinary-listener facility for cross-crate process tests.
///
/// Production assembly must use [`ControlPlaneRpcServerBootstrap`]. This
/// feature-gated facility preserves deterministic bounded-request tests
/// without exposing raw resource policies or listener dispatch.
#[cfg(feature = "test-hooks")]
pub struct ControlPlaneRpcOrdinaryTestServer {
    listener: ControlPlaneRpcServerListener,
    policy: ControlPlaneRpcServerPolicy,
}

#[cfg(feature = "test-hooks")]
impl fmt::Debug for ControlPlaneRpcOrdinaryTestServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRpcOrdinaryTestServer")
            .field("authenticated", &self.policy.authentication_required())
            .finish()
    }
}

#[cfg(feature = "test-hooks")]
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlPlaneRpcOrdinaryTestServerError {
    #[error("invalid ordinary control-plane RPC test server configuration")]
    InvalidConfiguration,
    #[error("ordinary control-plane RPC test server stopped")]
    Stopped,
}

#[cfg(feature = "test-hooks")]
impl ControlPlaneRpcOrdinaryTestServer {
    pub fn unix(
        listener: UnixListener,
        max_connections: usize,
        max_frame_bytes: usize,
        io_timeout: Duration,
        pre_auth_byte_budget: usize,
        auth: Option<&ControlPlaneRpcServerAuth>,
    ) -> Result<Self, ControlPlaneRpcOrdinaryTestServerError> {
        let listener = ControlPlaneRpcServerListener::unix(
            listener,
            max_connections,
            max_frame_bytes,
            io_timeout,
        )
        .map_err(|_| ControlPlaneRpcOrdinaryTestServerError::InvalidConfiguration)?;
        let policy = ControlPlaneRpcServerPolicy::new(
            ControlPlaneRpcServerRole::Ordinary,
            max_connections,
            pre_auth_byte_budget,
        )
        .map_err(|_| ControlPlaneRpcOrdinaryTestServerError::InvalidConfiguration)?;
        let policy = match auth {
            Some(auth) => policy.with_server_auth(auth),
            None => policy,
        };
        Ok(Self { listener, policy })
    }

    pub fn serve_shared_requests<T>(
        self,
        authority: Arc<Mutex<T>>,
        authority_times_ms: impl IntoIterator<Item = u64>,
        after_request: impl FnMut(&T),
    ) -> Result<(), ControlPlaneRpcOrdinaryTestServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        self.listener
            .serve_shared_requests_for_test(
                authority,
                self.policy,
                authority_times_ms,
                after_request,
            )
            .map_err(|_| ControlPlaneRpcOrdinaryTestServerError::Stopped)
    }

    pub fn serve_shared_requests_after_dropped_connection<T>(
        self,
        authority: Arc<Mutex<T>>,
        authority_times_ms: impl IntoIterator<Item = u64>,
        after_request: impl FnMut(&T),
    ) -> Result<(), ControlPlaneRpcOrdinaryTestServerError>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        self.listener
            .serve_shared_requests_after_dropped_connection_for_test(
                authority,
                self.policy,
                authority_times_ms,
                after_request,
            )
            .map_err(|_| ControlPlaneRpcOrdinaryTestServerError::Stopped)
    }
}

impl ControlPlaneRpcServerBootstrap {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ordinary_listeners: Vec<ControlPlaneRpcServerListenerInput>,
        recovery_listeners: Vec<ControlPlaneRpcServerListenerInput>,
        ordinary_worker_limit: usize,
        ordinary_pre_auth_byte_budget: usize,
        recovery_worker_limit: usize,
        recovery_pre_auth_byte_budget: usize,
        auth: &ControlPlaneRpcServerAuth,
    ) -> Result<Self, ControlPlaneRpcServerBootstrapError> {
        if ordinary_listeners.is_empty() {
            return Err(ControlPlaneRpcServerBootstrapError::MissingOrdinaryListener);
        }
        if recovery_listeners.is_empty() {
            return Err(ControlPlaneRpcServerBootstrapError::MissingRecoveryListener);
        }
        reject_cross_role_listener_aliases(&ordinary_listeners, &recovery_listeners)?;
        let ordinary_listeners = build_listeners(ordinary_listeners, |index| {
            ControlPlaneRpcServerBootstrapError::InvalidOrdinaryListener { index }
        })?;
        let recovery_listeners = build_listeners(recovery_listeners, |index| {
            ControlPlaneRpcServerBootstrapError::InvalidRecoveryListener { index }
        })?;
        let ordinary_policy = ControlPlaneRpcServerPolicy::new(
            ControlPlaneRpcServerRole::Ordinary,
            ordinary_worker_limit,
            ordinary_pre_auth_byte_budget,
        )
        .map_err(|_| ControlPlaneRpcServerBootstrapError::InvalidOrdinaryPolicy)?
        .with_server_auth(auth);
        let recovery_policy = ControlPlaneRpcServerPolicy::new(
            ControlPlaneRpcServerRole::AuthorityClockRecovery,
            recovery_worker_limit,
            recovery_pre_auth_byte_budget,
        )
        .map_err(|_| ControlPlaneRpcServerBootstrapError::InvalidRecoveryPolicy)?
        .with_server_auth(auth);
        Ok(Self {
            ordinary_listeners,
            recovery_listeners,
            ordinary_policy,
            recovery_policy,
        })
    }

    pub fn serve_shared_single_authority<T>(
        self,
        authority: Arc<Mutex<T>>,
        authority_clock: Arc<Mutex<ControlPlaneAuthorityClock>>,
        checkpoint_target: Arc<ControlPlaneAuthorityClockCheckpointTarget>,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> ControlPlaneRpcServerLoops
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let ordinary_policy = self
            .ordinary_policy
            .with_authority_clock(
                Arc::clone(&authority_clock),
                Arc::clone(&checkpoint_target),
                true,
            )
            .with_fatal_error_handler(Arc::clone(&terminal_failure_handler));
        let recovery_policy = self
            .recovery_policy
            .with_authority_clock(authority_clock, checkpoint_target, true)
            .with_fatal_error_handler(Arc::clone(&terminal_failure_handler));
        let mut handles =
            Vec::with_capacity(self.ordinary_listeners.len() + self.recovery_listeners.len());
        for listener in self.ordinary_listeners {
            handles.push(spawn_shared_listener(
                listener,
                Arc::clone(&authority),
                ordinary_policy.clone(),
                Arc::clone(&terminal_failure_handler),
            ));
        }
        for listener in self.recovery_listeners {
            handles.push(spawn_shared_listener(
                listener,
                Arc::clone(&authority),
                recovery_policy.clone(),
                Arc::clone(&terminal_failure_handler),
            ));
        }
        ControlPlaneRpcServerLoops { handles }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn serve_cloned_raft_authority<T>(
        self,
        authority: T,
        authority_clock: Arc<Mutex<ControlPlaneAuthorityClock>>,
        checkpoint_target: Arc<ControlPlaneAuthorityClockCheckpointTarget>,
        authority_confirmation: Arc<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>,
        response_publication: Arc<dyn ControlPlaneRpcResponsePublication>,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> ControlPlaneRpcServerLoops
    where
        T: Clone
            + ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let ordinary_policy = self
            .ordinary_policy
            .with_authority_clock(
                Arc::clone(&authority_clock),
                Arc::clone(&checkpoint_target),
                false,
            )
            .with_authority_confirmation(Arc::clone(&authority_confirmation))
            .with_response_publication(Arc::clone(&response_publication))
            .with_fatal_error_handler(Arc::clone(&terminal_failure_handler));
        let recovery_policy = self
            .recovery_policy
            .with_authority_clock(authority_clock, checkpoint_target, false)
            .with_authority_confirmation(authority_confirmation)
            .with_response_publication(response_publication)
            .with_fatal_error_handler(Arc::clone(&terminal_failure_handler));
        let mut handles =
            Vec::with_capacity(self.ordinary_listeners.len() + self.recovery_listeners.len());
        for listener in self.ordinary_listeners {
            handles.push(spawn_cloned_listener(
                listener,
                authority.clone(),
                ordinary_policy.clone(),
                Arc::clone(&terminal_failure_handler),
            ));
        }
        for listener in self.recovery_listeners {
            handles.push(spawn_cloned_listener(
                listener,
                authority.clone(),
                recovery_policy.clone(),
                Arc::clone(&terminal_failure_handler),
            ));
        }
        ControlPlaneRpcServerLoops { handles }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct ListenerIdentity {
    device: libc::dev_t,
    inode: libc::ino_t,
}

fn listener_identity(
    listener: &ControlPlaneRpcServerListenerInput,
) -> io::Result<ListenerIdentity> {
    let file_descriptor = match listener {
        ControlPlaneRpcServerListenerInput::Unix { listener, .. } => listener.as_raw_fd(),
        ControlPlaneRpcServerListenerInput::TlsTcp { listener, .. } => listener.as_raw_fd(),
    };
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `metadata` points to writable storage for one `libc::stat`, and
    // `file_descriptor` remains owned by the borrowed listener for the call.
    if unsafe { libc::fstat(file_descriptor, metadata.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful `fstat` initialized the complete `libc::stat`.
    let metadata = unsafe { metadata.assume_init() };
    Ok(ListenerIdentity {
        device: metadata.st_dev,
        inode: metadata.st_ino,
    })
}

fn reject_cross_role_listener_aliases(
    ordinary_listeners: &[ControlPlaneRpcServerListenerInput],
    recovery_listeners: &[ControlPlaneRpcServerListenerInput],
) -> Result<(), ControlPlaneRpcServerBootstrapError> {
    let mut ordinary_identities = HashMap::with_capacity(ordinary_listeners.len());
    for (ordinary_index, listener) in ordinary_listeners.iter().enumerate() {
        let identity = listener_identity(listener).map_err(|_| {
            ControlPlaneRpcServerBootstrapError::InvalidOrdinaryListener {
                index: ordinary_index,
            }
        })?;
        ordinary_identities
            .entry(identity)
            .or_insert(ordinary_index);
    }
    for (recovery_index, listener) in recovery_listeners.iter().enumerate() {
        let identity = listener_identity(listener).map_err(|_| {
            ControlPlaneRpcServerBootstrapError::InvalidRecoveryListener {
                index: recovery_index,
            }
        })?;
        if let Some(ordinary_index) = ordinary_identities.get(&identity) {
            return Err(ControlPlaneRpcServerBootstrapError::AliasedRoleListener {
                ordinary_index: *ordinary_index,
                recovery_index,
            });
        }
    }
    Ok(())
}

fn build_listeners(
    listeners: Vec<ControlPlaneRpcServerListenerInput>,
    error: impl Fn(usize) -> ControlPlaneRpcServerBootstrapError,
) -> Result<Vec<ControlPlaneRpcServerListener>, ControlPlaneRpcServerBootstrapError> {
    listeners
        .into_iter()
        .enumerate()
        .map(|(index, listener)| {
            match listener {
                ControlPlaneRpcServerListenerInput::Unix {
                    listener,
                    max_connections,
                    max_frame_bytes,
                    io_timeout,
                } => ControlPlaneRpcServerListener::unix(
                    listener,
                    max_connections,
                    max_frame_bytes,
                    io_timeout,
                ),
                ControlPlaneRpcServerListenerInput::TlsTcp {
                    listener,
                    certified_key,
                    max_connections,
                    max_frame_bytes,
                    io_timeout,
                } => ControlPlaneRpcServerListener::tls_tcp(
                    listener,
                    certified_key,
                    max_connections,
                    max_frame_bytes,
                    io_timeout,
                ),
            }
            .map_err(|_| error(index))
        })
        .collect()
}

fn spawn_shared_listener<T>(
    listener: ControlPlaneRpcServerListener,
    authority: Arc<Mutex<T>>,
    policy: ControlPlaneRpcServerPolicy,
    terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
) -> thread::JoinHandle<()>
where
    T: ControlPlaneAdmin
        + ControlPlaneHeartbeatRuntimeMapSource
        + ControlPlaneRuntimeMapSource
        + Send
        + 'static,
{
    thread::spawn(move || {
        if listener.serve_shared(authority, policy).is_err() {
            eprintln!("control-plane RPC listener stopped");
            terminal_failure_handler();
        }
    })
}

fn spawn_cloned_listener<T>(
    listener: ControlPlaneRpcServerListener,
    authority: T,
    policy: ControlPlaneRpcServerPolicy,
    terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
) -> thread::JoinHandle<()>
where
    T: Clone
        + ControlPlaneAdmin
        + ControlPlaneHeartbeatRuntimeMapSource
        + ControlPlaneRuntimeMapSource
        + Send
        + 'static,
{
    thread::spawn(move || {
        if listener.serve_cloned(authority, policy).is_err() {
            eprintln!("control-plane RPC listener stopped");
            terminal_failure_handler();
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unix_input(listener: UnixListener) -> ControlPlaneRpcServerListenerInput {
        ControlPlaneRpcServerListenerInput::Unix {
            listener,
            max_connections: 2,
            max_frame_bytes: 1024 * 1024,
            io_timeout: Duration::from_secs(1),
        }
    }

    fn unix_pair(
        directory: &std::path::Path,
        name: &str,
    ) -> (
        ControlPlaneRpcServerListenerInput,
        ControlPlaneRpcServerListenerInput,
    ) {
        let ordinary = UnixListener::bind(directory.join(format!("{name}-ordinary.sock"))).unwrap();
        let recovery = UnixListener::bind(directory.join(format!("{name}-recovery.sock"))).unwrap();
        (unix_input(ordinary), unix_input(recovery))
    }

    #[test]
    fn bootstrap_pairs_both_listener_roles_and_redacts_bindings() {
        let tmp = test_util::tempdir();
        let ordinary = UnixListener::bind(tmp.path().join("ordinary.sock")).unwrap();
        let recovery = UnixListener::bind(tmp.path().join("recovery.sock")).unwrap();
        let ordinary = unix_input(ordinary);
        let listener_diagnostic = format!("{ordinary:?}");
        assert!(listener_diagnostic.contains("transport: \"unix\""));
        assert!(!listener_diagnostic.contains(tmp.path().to_str().unwrap()));
        let auth = ControlPlaneRpcServerAuth::new(
            Some("secret-cluster"),
            Some("admin-1"),
            Vec::new(),
            Vec::new(),
            vec![crate::control_plane::ControlPlaneAdminAuthCredentialInput {
                instance_id: "admin-1".to_owned(),
                credential_id: "secret-credential".to_owned(),
                credential_version: 1,
                secret: b"secret-key-material".to_vec(),
            }],
        )
        .unwrap();
        let bootstrap = ControlPlaneRpcServerBootstrap::new(
            vec![ordinary],
            vec![unix_input(recovery)],
            2,
            1024 * 1024,
            1,
            1024 * 1024,
            &auth,
        )
        .unwrap();
        assert_eq!(
            bootstrap.ordinary_policy.role(),
            ControlPlaneRpcServerRole::Ordinary
        );
        assert_eq!(
            bootstrap.recovery_policy.role(),
            ControlPlaneRpcServerRole::AuthorityClockRecovery
        );
        assert_eq!(
            format!("{bootstrap:?}"),
            "ControlPlaneRpcServerBootstrap { ordinary_listeners: 1, recovery_listeners: 1, authenticated: true }"
        );
        let diagnostic = format!("{bootstrap:?}");
        assert!(!diagnostic.contains("secret-cluster"));
        assert!(!diagnostic.contains("secret-credential"));
        assert!(!diagnostic.contains("secret-key-material"));
    }

    #[test]
    fn bootstrap_rejects_missing_or_invalid_listener_and_policy_configuration() {
        let auth =
            ControlPlaneRpcServerAuth::new(None, None, Vec::new(), Vec::new(), Vec::new()).unwrap();
        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(Vec::new(), Vec::new(), 1, 1, 1, 1, &auth,)
                .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::MissingOrdinaryListener
        );

        let tmp = test_util::tempdir();
        let (ordinary, _) = unix_pair(tmp.path(), "missing-recovery");
        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(vec![ordinary], Vec::new(), 1, 1, 1, 1, &auth,)
                .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::MissingRecoveryListener
        );

        let (ordinary, recovery) = unix_pair(tmp.path(), "invalid-ordinary");
        let ControlPlaneRpcServerListenerInput::Unix {
            listener: ordinary,
            max_frame_bytes,
            io_timeout,
            ..
        } = ordinary
        else {
            unreachable!();
        };
        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(
                vec![ControlPlaneRpcServerListenerInput::Unix {
                    listener: ordinary,
                    max_connections: 0,
                    max_frame_bytes,
                    io_timeout,
                }],
                vec![recovery],
                1,
                1,
                1,
                1,
                &auth,
            )
            .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::InvalidOrdinaryListener { index: 0 }
        );

        let (ordinary, recovery) = unix_pair(tmp.path(), "invalid-recovery");
        let ControlPlaneRpcServerListenerInput::Unix {
            listener: recovery,
            max_frame_bytes,
            io_timeout,
            ..
        } = recovery
        else {
            unreachable!();
        };
        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(
                vec![ordinary],
                vec![ControlPlaneRpcServerListenerInput::Unix {
                    listener: recovery,
                    max_connections: 0,
                    max_frame_bytes,
                    io_timeout,
                }],
                1,
                1,
                1,
                1,
                &auth,
            )
            .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::InvalidRecoveryListener { index: 0 }
        );

        let (ordinary, recovery) = unix_pair(tmp.path(), "invalid-ordinary-policy");
        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(vec![ordinary], vec![recovery], 0, 1, 1, 1, &auth,)
                .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::InvalidOrdinaryPolicy
        );

        let (ordinary, recovery) = unix_pair(tmp.path(), "invalid-recovery-policy");
        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(vec![ordinary], vec![recovery], 1, 1, 1, 0, &auth,)
                .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::InvalidRecoveryPolicy
        );
    }

    #[test]
    fn bootstrap_rejects_cloned_unix_listener_across_roles() {
        let tmp = test_util::tempdir();
        let ordinary = UnixListener::bind(tmp.path().join("aliased.sock")).unwrap();
        let recovery = ordinary.try_clone().unwrap();
        let auth =
            ControlPlaneRpcServerAuth::new(None, None, Vec::new(), Vec::new(), Vec::new()).unwrap();

        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(
                vec![unix_input(ordinary)],
                vec![unix_input(recovery)],
                1,
                1,
                1,
                1,
                &auth,
            )
            .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::AliasedRoleListener {
                ordinary_index: 0,
                recovery_index: 0,
            }
        );
    }

    #[test]
    fn bootstrap_rejects_cloned_tcp_listener_across_roles() {
        fn tcp_input(listener: TcpListener) -> ControlPlaneRpcServerListenerInput {
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
            let provider = tls_provider::build_provider();
            ControlPlaneRpcServerListenerInput::TlsTcp {
                listener,
                certified_key: Arc::new(
                    CertifiedKey::from_der(certificates, private_key, &provider).unwrap(),
                ),
                max_connections: 2,
                max_frame_bytes: 1024 * 1024,
                io_timeout: Duration::from_secs(1),
            }
        }

        let ordinary = TcpListener::bind("127.0.0.1:0").unwrap();
        let recovery = ordinary.try_clone().unwrap();
        let auth =
            ControlPlaneRpcServerAuth::new(None, None, Vec::new(), Vec::new(), Vec::new()).unwrap();

        assert_eq!(
            ControlPlaneRpcServerBootstrap::new(
                vec![tcp_input(ordinary)],
                vec![tcp_input(recovery)],
                1,
                1,
                1,
                1,
                &auth,
            )
            .unwrap_err(),
            ControlPlaneRpcServerBootstrapError::AliasedRoleListener {
                ordinary_index: 0,
                recovery_index: 0,
            }
        );
    }
}
