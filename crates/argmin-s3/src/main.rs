// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

mod config;
mod static_cluster_config;
mod static_cluster_state;

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io;
use std::net::TcpListener as StdTcpListener;
use std::os::fd::AsRawFd;
#[cfg(test)]
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::Condvar;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use auth::{AccountIdentity, ConfiguredPrincipalIdentity, CredentialStore, StoredCredential};
use ec::EcConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use server_core::coordinator::Coordinator;
use server_core::sse::{
    ManagedWrappingKeyConfig, SseCustomerValidatorConfig, StaticManagedKeyProvider,
};
use storage::control_plane::{
    ensure_control_plane_state_parent_directory, invalidate_authority_clock_restart_checkpoint,
    load_authority_clock_restart_checkpoint, ClusterRuntimeMapSnapshot,
    ControlPlaneAdminAuthCredentialInput, ControlPlaneAuthorityClock,
    ControlPlaneAuthorityClockCheckpointBinding, ControlPlaneAuthorityClockCheckpointTarget,
    ControlPlaneError, ControlPlaneFrontendAuthCredentialInput,
    ControlPlaneHeartbeatRuntimeMapSource, ControlPlaneRuntimeMapSource,
    ControlPlaneStorageNodeAuthCredentialInput, FileControlPlaneStore, SingleAuthorityControlPlane,
    CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
};
#[cfg(test)]
use storage::control_plane::{
    store_authority_clock_restart_checkpoint, ClusterControlSnapshot, ControlPlaneAdmin,
    ControlPlaneAuthorityClockContext, ControlPlaneRpcResponsePublication,
    LeaseHorizonAuthorityBinding, PgMetadataTransferProof, UnixControlPlaneClient,
};
#[cfg(test)]
use storage::control_plane_command::{ControlPlaneCommand, ControlPlaneCommandResponse};
#[cfg(test)]
use storage::control_plane_raft::ControlPlaneRaftAuthority;
use storage::control_plane_raft::ControlPlaneRaftNodeId;
use storage::storage_node_server::{
    PreparedStorageNodeServer, StorageNodeBootstrap, StorageNodeControlPlaneRefreshLoop,
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts, StorageNodeServer,
};
use storage::{
    CanonicalUserId, ClusterEpoch, ControlPlaneRaftAuthorityHost,
    ControlPlaneRaftOuterIdentityPublicationError, ControlPlaneRaftOuterIdentityPublisher,
    ControlPlaneRaftOuterIdentityStartup, ControlPlaneRaftPeerAuthCredentialInput,
    ControlPlaneRaftPeerBootstrap, ControlPlaneRaftPeerServerListenerInput,
    ControlPlaneRaftPeerTopologyBinding, ControlPlaneRpcServerBootstrap,
    ControlPlaneRpcServerListenerInput, EcShape, LocalClusterMap,
    LocalUnixStorageNodeClientAdmissionSettings, LocalUnixStorageNodeClientConfig, NodeId, PgState,
    RouteMapValidity, StorageCluster, StorageClusterRouteHandle, StorageClusterRuntimeMapHandle,
};
#[cfg(test)]
use storage::{
    ControlPlaneRaftAuthorityDurability, ControlPlaneRaftCheckpointMonitorForTest,
    ControlPlaneRaftPeerTestServer, ControlPlaneRpcOrdinaryTestServer, PgId,
};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio_rustls::TlsAcceptor;

use config::{
    ConfiguredControlPlaneAdminAuthCredential, ConfiguredControlPlaneAdminCommandAuth,
    ConfiguredControlPlaneFrontendAuthCredential, ConfiguredControlPlaneFrontendRuntimeMapAuth,
    ConfiguredControlPlaneRaftPeerListener, ConfiguredControlPlaneRpcListener,
    ConfiguredControlPlaneStorageAuthCredential, ConfiguredCredential, ConfiguredCredentialProfile,
    ConfiguredStaticClusterIdentity, ProcessRole, ServerConfig,
};
use server_http::http::HttpFrontend;

const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;
const CONTROL_PLANE_RPC_WORKER_LIMIT: usize = 64;
const CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT: usize = 8;
const CONTROL_PLANE_RPC_PRE_AUTH_BYTE_BUDGET: usize = 64 * 1024 * 1024;
const CONTROL_PLANE_CLOCK_RECOVERY_RPC_PRE_AUTH_BYTE_BUDGET: usize = 16 * 1024 * 1024;
const CONTROL_PLANE_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);
#[cfg(test)]
const CONTROL_PLANE_RAFT_PEER_RPC_WORKER_LIMIT: usize = 64;
const CONTROL_PLANE_RAFT_PEER_PRE_AUTH_BYTE_BUDGET: usize = 64 * 1024 * 1024;
const CONTROL_PLANE_STANDALONE_CHECKPOINT_POLL_INTERVAL: Duration = Duration::from_millis(100);

macro_rules! process_info {
    ($($arg:tt)*) => {{
        #[cfg(not(test))]
        {
            eprintln!($($arg)*);
        }
        #[cfg(test)]
        {
            let _ = format_args!($($arg)*);
        }
    }};
}

extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
    fn getuid() -> u32;
    fn geteuid() -> u32;
}

const ROOT_PROCESS_ERROR: &str =
    "argmin-s3 must not be run as root; configure a dedicated non-root service user";

fn reject_root_process(effective_uid: u32) -> Result<(), &'static str> {
    if effective_uid == 0 {
        Err(ROOT_PROCESS_ERROR)
    } else {
        Ok(())
    }
}

fn reject_current_root_process() -> Result<(), &'static str> {
    // SAFETY: geteuid has no preconditions and does not mutate memory.
    let effective_uid = unsafe { geteuid() };
    reject_root_process(effective_uid)
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    CertificateDer::pem_file_iter(path)
        .map_err(|e| format!("failed to open TLS cert {path}: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to read TLS cert {path}: {e}"))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| match e {
        rustls::pki_types::pem::Error::NoItemsFound => {
            format!("no private key found in {path}")
        }
        _ => format!("failed to read TLS key {path}: {e}"),
    })
}

fn build_tls_acceptor(config: &ServerConfig) -> Result<Option<TlsAcceptor>, String> {
    if config.tls_certified_key.is_none()
        && config.tls_cert_path.is_none()
        && config.tls_key_path.is_none()
    {
        return Ok(None);
    }
    if config.tls_certified_key.is_some()
        && (config.tls_cert_path.is_some() || config.tls_key_path.is_some())
    {
        return Err("public TLS cannot use both resolved and path-based credentials".to_string());
    }

    let builder = rustls::ServerConfig::builder_with_provider(tls_provider::configured_provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("failed to select TLS protocol versions: {e}"))?
        .with_no_client_auth();
    let mut server_config = if let Some(certified_key) = &config.tls_certified_key {
        builder.with_cert_resolver(Arc::new(SingleTlsCertificateResolver(
            certified_key.certified_key(),
        )))
    } else {
        let (Some(cert_path), Some(key_path)) = (&config.tls_cert_path, &config.tls_key_path)
        else {
            return Err("public TLS certificate and key paths must be configured together".into());
        };
        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        builder
            .with_single_cert(certs, key)
            .map_err(|e| format!("failed to build TLS config: {e}"))?
    };
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Some(TlsAcceptor::from(Arc::new(server_config))))
}

#[derive(Debug)]
struct SingleTlsCertificateResolver(Arc<CertifiedKey>);

impl ResolvesServerCert for SingleTlsCertificateResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.0))
    }
}

fn authorization_profile(profile: ConfiguredCredentialProfile) -> auth::AuthorizationProfile {
    match profile {
        ConfiguredCredentialProfile::Standard => auth::AuthorizationProfile::Standard,
        ConfiguredCredentialProfile::OwnerAccountAdmin => {
            auth::AuthorizationProfile::OwnerAccountAdmin
        }
    }
}

fn add_configured_credential(credentials: &mut CredentialStore, credential: &ConfiguredCredential) {
    let account = AccountIdentity::new(
        credential.account_id.clone(),
        CanonicalUserId::from_principal(&credential.account_id),
        credential.display_name.clone(),
    );
    credentials
        .add_record(StoredCredential::configured(
            credential.access_key_id.clone(),
            credential.secret_access_key.clone(),
            account,
            ConfiguredPrincipalIdentity::new(credential.principal.clone()),
            authorization_profile(credential.authorization_profile),
            None,
            true,
        ))
        .expect("server configuration rejected reserved session access key namespace");
}

fn build_credential_store(config: &ServerConfig) -> CredentialStore {
    let mut credentials = CredentialStore::new();
    let account = AccountIdentity::new(
        config.account_id.clone(),
        CanonicalUserId::from_principal(&config.account_id),
        config.account_id.clone(),
    );
    credentials
        .add_record(StoredCredential::configured(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            account,
            ConfiguredPrincipalIdentity::new(config.account_id.clone()),
            auth::AuthorizationProfile::OwnerAccountAdmin,
            None,
            true,
        ))
        .expect("server configuration rejected reserved session access key namespace");
    for credential in &config.uat_credentials {
        add_configured_credential(&mut credentials, credential);
    }
    credentials
}

#[derive(Debug)]
struct ControlPlaneStateLock {
    _file: File,
}

fn acquire_control_plane_state_lock(state_path: &Path) -> Result<ControlPlaneStateLock, String> {
    let lock_path = control_plane_state_lock_path(state_path)?;
    ensure_control_plane_state_parent_directory(state_path).map_err(|error| {
        format!(
            "failed to durably create control-plane state directory for {}: {error}",
            state_path.display()
        )
    })?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|error| {
            format!(
                "failed to open control-plane state lock {}: {error}",
                lock_path.display()
            )
        })?;
    // SAFETY: flock operates on a valid file descriptor owned by `file`.
    // The descriptor stays open for the lifetime of `ControlPlaneStateLock`.
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc != 0 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::WouldBlock {
            Err(format!(
                "control-plane state {} is already locked by another manager",
                state_path.display()
            ))
        } else {
            Err(format!(
                "failed to lock control-plane state {} using {}: {error}",
                state_path.display(),
                lock_path.display()
            ))
        };
    }
    Ok(ControlPlaneStateLock { _file: file })
}

fn control_plane_state_lock_path(state_path: &Path) -> Result<PathBuf, String> {
    let file_name = state_path.file_name().ok_or_else(|| {
        format!(
            "ARGMIN_CONTROL_PLANE_STATE_PATH {} is missing a file name",
            state_path.display()
        )
    })?;
    let mut lock_name = OsString::from(file_name);
    lock_name.push(".lock");
    let mut lock_path = state_path.to_path_buf();
    lock_path.set_file_name(lock_name);
    Ok(lock_path)
}

fn main() {
    if let Err(error) = reject_current_root_process() {
        eprintln!("{error}");
        std::process::exit(1);
    }
    tls_provider::install_default().unwrap_or_else(|error| {
        eprintln!(
            "failed to initialize {} TLS provider: {error}",
            tls_provider::provider_name()
        );
        std::process::exit(1);
    });
    if let Some(exit_code) = maybe_run_control_plane_admin_command() {
        std::process::exit(exit_code);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| {
            eprintln!("failed to initialize Tokio runtime: {error}");
            std::process::exit(1);
        });
    runtime.block_on(async_main());
}

async fn async_main() {
    let config = match static_cluster_config::load_server_config_from_environment() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(1);
        }
    };
    observability::configure_with_options(
        config.trace_enabled,
        config.trace_filter.as_deref(),
        config.trace_file.as_deref(),
        config.trace_sync,
    );
    observability::install_panic_flight_recorder_hook();
    let host_id = config
        .host_id
        .clone()
        .unwrap_or_else(server_http::http::new_host_id);

    // EC self-test
    if let Err(e) = ec::self_test() {
        eprintln!("EC self-test failed: {e}");
        std::process::exit(1);
    }

    let ec_config = match EcConfig::new(config.ec_k, config.ec_m) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("invalid EC config: {e}");
            std::process::exit(1);
        }
    };

    let _standalone_remote_route_identity_lock =
        bind_no_control_plane_remote_route_identity(&config, &ec_config).unwrap_or_else(|error| {
            eprintln!("failed to bind standalone storage route identity: {error}");
            std::process::exit(1);
        });

    match config.process_role {
        ProcessRole::ControlPlane => run_control_plane_process(&config),
        ProcessRole::StorageNode => run_storage_node_process(&config, &ec_config),
        ProcessRole::Combined => {
            let _storage_node_thread = start_storage_node_process(&config, &ec_config);
            run_remote_frontend(config, host_id, ec_config).await;
        }
        ProcessRole::Frontend => {
            run_remote_frontend(config, host_id, ec_config).await;
        }
        ProcessRole::AllInOne => {
            run_all_in_one_frontend(config, host_id, ec_config).await;
        }
    }
}

fn bind_no_control_plane_remote_route_identity(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<Option<storage::StandaloneRouteIdentityLock>, String> {
    if config.control_plane_socket_path.is_some() {
        return Ok(None);
    }
    let route_identity = match config.process_role {
        ProcessRole::Frontend => build_remote_frontend_storage_cluster(config, ec_config)?
            .standalone_route_identity()
            .map_err(|error| error.to_string())?,
        ProcessRole::Combined => {
            let frontend = build_remote_frontend_storage_cluster(config, ec_config)?
                .standalone_route_identity()
                .map_err(|error| error.to_string())?;
            let storage_node = standalone_storage_node_process_config(config, ec_config)?
                .standalone_route_identity()
                .map_err(|error| error.to_string())?;
            frontend.combined_with(storage_node)
        }
        ProcessRole::StorageNode => standalone_storage_node_process_config(config, ec_config)?
            .standalone_route_identity()
            .map_err(|error| error.to_string())?,
        ProcessRole::AllInOne | ProcessRole::ControlPlane => return Ok(None),
    };
    let preparation =
        storage::StandaloneRouteIdentityPreparation::acquire(Path::new(&config.data_dir))
            .map_err(|error| error.to_string())?;
    preparation
        .bind(route_identity)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn maybe_run_control_plane_admin_command() -> Option<i32> {
    let mut args = std::env::args_os();
    let _program = args.next();
    let command = args.next()?;
    if command == "validate-cluster-config" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some(process_id) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        let Some(process_id) = process_id.to_str() else {
            eprintln!("cluster manifest process id must contain valid UTF-8");
            return Some(2);
        };
        return match static_cluster_config::load_static_cluster_manifest(
            Path::new(&path),
            process_id,
        ) {
            Ok(manifest) => {
                println!(
                    "valid cluster manifest cluster_id={} topology_generation={} process_id={} deployment_mode={} topology_digest={} process_identity_digest={} full_config_fingerprint={}",
                    manifest.cluster_id(),
                    manifest.topology_generation(),
                    manifest.selected_process_id(),
                    manifest.deployment_mode(),
                    manifest.topology_digest(),
                    manifest.process_identity_digest(),
                    manifest.full_config_fingerprint()
                );
                Some(0)
            }
            Err(error) => {
                eprintln!("cluster manifest validation failed: {error}");
                Some(1)
            }
        };
    }
    if command == "validate-cluster-material" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some(process_id) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        let Some(process_id) = process_id.to_str() else {
            eprintln!("cluster manifest process id must contain valid UTF-8");
            return Some(2);
        };
        return match static_cluster_config::load_static_cluster_manifest(
            Path::new(&path),
            process_id,
        )
        .and_then(|manifest| {
            manifest
                .resolve_selected_process_material()
                .map(|material| (manifest, material))
        }) {
            Ok((manifest, material)) => {
                println!(
                    "valid cluster material cluster_id={} topology_generation={} process_id={} auth_credentials={} tls_identities={} tls_trust_bundles={}",
                    manifest.cluster_id(),
                    manifest.topology_generation(),
                    manifest.selected_process_id(),
                    material.auth_credential_count(),
                    material.tls_identity_count(),
                    material.tls_trust_bundle_count()
                );
                Some(0)
            }
            Err(error) => {
                eprintln!("cluster material validation failed: {error}");
                Some(1)
            }
        };
    }
    if command == "initialize-cluster-state" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some(process_id) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <absolute-manifest-path> <process-id>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        let Some(process_id) = process_id.to_str() else {
            eprintln!("cluster manifest process id must contain valid UTF-8");
            return Some(2);
        };
        return match static_cluster_config::load_static_cluster_manifest(
            Path::new(&path),
            process_id,
        )
        .and_then(|manifest| manifest.initialize_selected_process_state())
        {
            Ok(()) => {
                println!("initialized static cluster process state");
                Some(0)
            }
            Err(error) => {
                eprintln!("static cluster state initialization failed: {error}");
                Some(1)
            }
        };
    }
    if command == "control-plane-runtime-map-ready" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match control_plane_runtime_map_ready(Path::new(&path)) {
            Ok((epoch, pg_routes, active_serving_pg_routes)) => {
                println!("{} {} {}", epoch.get(), pg_routes, active_serving_pg_routes);
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-runtime-map-diagnostics" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match control_plane_runtime_map_diagnostics(Path::new(&path)) {
            Ok(diagnostics) => {
                println!("{diagnostics}");
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-pg-runtime-map-ready" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some((pg_id, acting_set)) = parse_control_plane_pg_acting_set_args(args) else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        return match control_plane_pg_runtime_map_ready(Path::new(&path), pg_id, &acting_set) {
            Ok((runtime_epoch, route_epoch)) => {
                println!("{runtime_epoch} {route_epoch}");
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-authority-clock-status" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match control_plane_authority_clock_status(Path::new(&path)) {
            Ok(status) => {
                println!("{status}");
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-reestablish-authority-clock" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match reestablish_control_plane_authority_clock(Path::new(&path)) {
            Ok(status) => {
                println!("{status}");
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-transfer-raft-leadership" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <raft-node-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some(node_id) = args.next().and_then(|arg| {
            arg.to_string_lossy()
                .parse::<u64>()
                .ok()
                .filter(|node_id| *node_id != 0)
        }) else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <raft-node-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <raft-node-id>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match transfer_control_plane_raft_leadership(Path::new(&path), node_id) {
            Ok(()) => {
                eprintln!("control-plane transferred Raft leadership to node {node_id}");
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-trigger-raft-snapshot-purge" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match trigger_control_plane_raft_snapshot_and_purge(Path::new(&path)) {
            Ok(Some(snapshot_index)) => {
                println!("{snapshot_index}");
                Some(0)
            }
            Ok(None) => {
                println!("-");
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-trigger-raft-election" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match trigger_control_plane_raft_election(Path::new(&path)) {
            Ok(()) => {
                eprintln!("control-plane triggered Raft election");
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-set-pg-acting-set-with-metadata-transfer-live" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id> <source-epoch> <expected-destination-epoch> <source-applied-log-index> <source-applied-log-hash> <source-state-digest> <imported-applied-log-index> <imported-applied-log-hash> <imported-state-digest> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some((pg_id, install)) =
            parse_control_plane_pg_acting_set_with_metadata_transfer_args(args)
        else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id> <source-epoch> <expected-destination-epoch> <source-applied-log-index> <source-applied-log-hash> <source-state-digest> <imported-applied-log-index> <imported-applied-log-hash> <imported-state-digest> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        return match set_control_plane_pg_acting_set_with_metadata_transfer_live(
            Path::new(&path),
            install,
        ) {
            Ok(epoch) => {
                eprintln!(
                    "control-plane set PG {} acting set with metadata transfer at epoch {}",
                    pg_id, epoch
                );
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-fence-pg-for-metadata-transfer-live" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some(pg_id) = args.next().and_then(parse_pg_id_arg) else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id>",
                command.to_string_lossy()
            );
            return Some(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id>",
                command.to_string_lossy()
            );
            return Some(2);
        }
        return match fence_control_plane_pg_for_metadata_transfer_live(Path::new(&path), pg_id) {
            Ok(epoch) => {
                eprintln!(
                    "control-plane fenced PG {} for metadata transfer at epoch {}",
                    pg_id, epoch
                );
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
    }

    if command == "control-plane-transfer-pg-metadata-live" {
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some((pg_id, acting_set)) = parse_control_plane_pg_acting_set_args(args) else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        return Some(run_control_plane_pg_metadata_transfer_command(
            Path::new(&path),
            pg_id,
            acting_set,
            None,
        ));
    }

    #[cfg(feature = "test-live-metadata-transfer-failpoints")]
    if command == "test-control-plane-transfer-pg-metadata-live-with-failpoint" {
        let Some(failpoint) = args
            .next()
            .as_deref()
            .and_then(parse_live_metadata_transfer_failpoint)
        else {
            eprintln!(
                "usage: argmin-s3 {} <after-fence|after-transfer-install|after-import> <socket-path> <pg-id> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some(path) = args.next() else {
            eprintln!(
                "usage: argmin-s3 {} <after-fence|after-transfer-install|after-import> <socket-path> <pg-id> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some((pg_id, acting_set)) = parse_control_plane_pg_acting_set_args(args) else {
            eprintln!(
                "usage: argmin-s3 {} <after-fence|after-transfer-install|after-import> <socket-path> <pg-id> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        return Some(run_control_plane_pg_metadata_transfer_command(
            Path::new(&path),
            pg_id,
            acting_set,
            Some(failpoint),
        ));
    }

    #[cfg(not(feature = "test-live-metadata-transfer-failpoints"))]
    if command == "test-control-plane-transfer-pg-metadata-live-with-failpoint" {
        eprintln!(
            "{} requires an argmin-s3 build with the test-live-metadata-transfer-failpoints feature",
            command.to_string_lossy()
        );
        return Some(2);
    }

    let live = if command == "control-plane-set-pg-acting-set" {
        false
    } else if command == "control-plane-set-pg-acting-set-live" {
        true
    } else {
        return None;
    };

    let usage_path = if live { "socket-path" } else { "state-path" };
    let Some(path) = args.next() else {
        eprintln!(
            "usage: argmin-s3 {} <{}> <pg-id> <node-id>...",
            command.to_string_lossy(),
            usage_path
        );
        return Some(2);
    };
    let Some((pg_id, acting_set)) = parse_control_plane_pg_acting_set_args(args) else {
        eprintln!(
            "usage: argmin-s3 {} <{}> <pg-id> <node-id>...",
            command.to_string_lossy(),
            usage_path
        );
        return Some(2);
    };

    let result = if live {
        set_control_plane_pg_acting_set_live(Path::new(&path), pg_id, acting_set)
    } else {
        set_control_plane_pg_acting_set(Path::new(&path), pg_id, acting_set)
    };
    match result {
        Ok(epoch) => {
            eprintln!(
                "control-plane set PG {} acting set at epoch {}",
                pg_id, epoch
            );
            Some(0)
        }
        Err(error) => {
            eprintln!("{error}");
            Some(1)
        }
    }
}

fn run_control_plane_pg_metadata_transfer_command(
    socket_path: &Path,
    pg_id: u32,
    acting_set: Vec<u32>,
    failpoint: Option<storage::LivePgMetadataTransferFailpoint>,
) -> i32 {
    match transfer_control_plane_pg_metadata_live(socket_path, pg_id, acting_set, failpoint) {
        Ok(summary) => {
            if summary.already_completed() {
                eprintln!(
                    "control-plane PG {} metadata transfer already completed at epoch {} with acting primary node {}",
                    pg_id,
                    summary.destination_epoch(),
                    summary.source_node_id()
                );
            } else {
                eprintln!(
                    "control-plane transferred PG {} metadata from node {} epoch {} to epoch {} with imported proof {}:{}:{}",
                    pg_id,
                    summary.source_node_id(),
                    summary.source_epoch(),
                    summary.destination_epoch(),
                    summary.imported_log_index(),
                    summary.imported_log_hash(),
                    summary.imported_state_digest()
                );
            }
            0
        }
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

#[cfg(feature = "test-live-metadata-transfer-failpoints")]
fn parse_live_metadata_transfer_failpoint(
    value: &std::ffi::OsStr,
) -> Option<storage::LivePgMetadataTransferFailpoint> {
    match value.to_str()? {
        "after-fence" => Some(storage::LivePgMetadataTransferFailpoint::AfterFence),
        "after-transfer-install" => {
            Some(storage::LivePgMetadataTransferFailpoint::AfterTransferInstall)
        }
        "after-import" => Some(storage::LivePgMetadataTransferFailpoint::AfterImport),
        _ => None,
    }
}

fn parse_control_plane_pg_acting_set_args(
    mut args: impl Iterator<Item = OsString>,
) -> Option<(u32, Vec<u32>)> {
    let pg_id = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<u32>().ok())?;
    let mut acting_set = Vec::new();
    for node_id in args {
        let node_id = node_id
            .into_string()
            .ok()
            .and_then(|value| value.parse::<u32>().ok())?;
        acting_set.push(node_id);
    }
    if acting_set.is_empty() {
        return None;
    }
    Some((pg_id, acting_set))
}

fn parse_pg_id_arg(value: OsString) -> Option<u32> {
    value.into_string().ok()?.parse().ok()
}

fn parse_control_plane_pg_acting_set_with_metadata_transfer_args(
    mut args: impl Iterator<Item = OsString>,
) -> Option<(u32, storage::ControlPlanePgMetadataTransferInstall)> {
    let pg_id = parse_next_u32(&mut args)?;
    let source_epoch = parse_next_u64(&mut args)?;
    let expected_destination_epoch = parse_next_u64(&mut args)?;
    let source_applied_log_index = parse_next_u64(&mut args)?;
    let source_applied_log_hash = parse_next_u64(&mut args)?;
    let source_state_digest = parse_next_u64(&mut args)?;
    let imported_applied_log_index = parse_next_u64(&mut args)?;
    let imported_applied_log_hash = parse_next_u64(&mut args)?;
    let imported_state_digest = parse_next_u64(&mut args)?;
    let mut acting_set = Vec::new();
    for node_id in args {
        let node_id = node_id
            .into_string()
            .ok()
            .and_then(|value| value.parse::<u32>().ok())?;
        acting_set.push(node_id);
    }
    if acting_set.is_empty() {
        return None;
    }
    let install = storage::ControlPlanePgMetadataTransferInstall::new(
        pg_id,
        acting_set,
        source_epoch,
        expected_destination_epoch,
        source_applied_log_index,
        source_applied_log_hash,
        source_state_digest,
        imported_applied_log_index,
        imported_applied_log_hash,
        imported_state_digest,
    )
    .ok()?;
    Some((pg_id, install))
}

fn parse_next_u32(args: &mut impl Iterator<Item = OsString>) -> Option<u32> {
    args.next()?.into_string().ok()?.parse().ok()
}

fn parse_next_u64(args: &mut impl Iterator<Item = OsString>) -> Option<u64> {
    args.next()?.into_string().ok()?.parse().ok()
}

fn set_control_plane_pg_acting_set(
    state_path: &Path,
    pg_id: u32,
    acting_set: Vec<u32>,
) -> Result<u64, String> {
    let _state_lock = acquire_control_plane_state_lock(state_path)?;
    storage::set_offline_control_plane_pg_acting_set(state_path, pg_id, acting_set)
        .map_err(|error| error.to_string())
}

fn set_control_plane_pg_acting_set_live(
    socket_path: &Path,
    pg_id: u32,
    acting_set: Vec<u32>,
) -> Result<u64, String> {
    build_pg_admin_control_plane_client_from_command_auth_env(socket_path)?
        .set_acting_set(pg_id, acting_set)
        .map_err(|error| error.to_string())
}

fn transfer_control_plane_raft_leadership(socket_path: &Path, node_id: u64) -> Result<(), String> {
    build_raft_admin_control_plane_client_from_command_auth_env(socket_path)?
        .transfer_leadership_to(node_id)
        .map_err(|error| error.to_string())
}

fn trigger_control_plane_raft_snapshot_and_purge(
    socket_path: &Path,
) -> Result<Option<u64>, String> {
    build_raft_admin_control_plane_client_from_command_auth_env(socket_path)?
        .trigger_snapshot_and_purge()
        .map_err(|error| error.to_string())
}

fn trigger_control_plane_raft_election(socket_path: &Path) -> Result<(), String> {
    build_raft_admin_control_plane_client_from_command_auth_env(socket_path)?
        .trigger_election()
        .map_err(|error| error.to_string())
}

fn control_plane_authority_clock_status(
    socket_path: &Path,
) -> Result<storage::ControlPlaneAuthorityClockAdminStatus, String> {
    build_authority_clock_admin_client_from_command_auth_env(socket_path)?
        .status()
        .map_err(|error| error.to_string())
}

fn reestablish_control_plane_authority_clock(
    socket_path: &Path,
) -> Result<storage::ControlPlaneAuthorityClockAdminStatus, String> {
    build_authority_clock_admin_client_from_command_auth_env(socket_path)?
        .reestablish()
        .map_err(|error| error.to_string())
}

fn fence_control_plane_pg_for_metadata_transfer_live(
    socket_path: &Path,
    pg_id: u32,
) -> Result<u64, String> {
    build_pg_admin_control_plane_client_from_command_auth_env(socket_path)?
        .fence_for_metadata_transfer(pg_id)
        .map_err(|error| error.to_string())
}

fn set_control_plane_pg_acting_set_with_metadata_transfer_live(
    socket_path: &Path,
    install: storage::ControlPlanePgMetadataTransferInstall,
) -> Result<u64, String> {
    build_pg_admin_control_plane_client_from_command_auth_env(socket_path)?
        .install_metadata_transfer(install)
        .map_err(|error| error.to_string())
}

fn transfer_control_plane_pg_metadata_live(
    socket_path: &Path,
    pg_id: u32,
    acting_set: Vec<u32>,
    failpoint: Option<storage::LivePgMetadataTransferFailpoint>,
) -> Result<storage::LivePgMetadataTransferSummary, String> {
    let config = static_cluster_config::load_server_config_from_environment()
        .map_err(|error| format!("configuration error: {error}"))?;
    let ec_config = EcConfig::new(config.ec_k, config.ec_m)
        .map_err(|error| format!("invalid EC config: {error}"))?;
    let frontend = build_frontend_control_plane_client_from_runtime_map_auth_env(socket_path)?;
    let admin_credential = if static_cluster_command_configured() {
        build_admin_credential_binding_from_config(&config)?
    } else {
        build_admin_credential_binding_from_command_auth_env()?
    };
    let control_plane = storage::LivePgMetadataTransferControlPlaneClient::with_frontend_client(
        &frontend,
        &admin_credential,
    )
    .map_err(|error| error.to_string())?;
    let ec_shape = EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };
    let admission_settings = unix_storage_node_client_admission_settings(&config);
    let transfer = if config.storage_rpc_client_endpoints.is_empty() {
        if config.storage_rpc_frontend_client_auth.is_none()
            && !config.allow_unauthenticated_internal_rpc_for_tests
        {
            return Err("live metadata transfer requires authenticated storage RPC".to_string());
        }
        storage::LivePgMetadataTransferAdmin::with_unix_storage_nodes(
            control_plane,
            ec_shape,
            admission_settings,
            config.storage_rpc_frontend_client_auth.clone(),
        )
    } else {
        let auth = config
            .storage_rpc_frontend_client_auth
            .clone()
            .ok_or_else(|| {
                "configured storage RPC endpoints require frontend authentication".to_owned()
            })?;
        storage::LivePgMetadataTransferAdmin::with_storage_rpc_endpoints(
            control_plane,
            ec_shape,
            admission_settings,
            config.storage_rpc_client_endpoints.clone(),
            auth,
        )
    }
    .with_failpoint(failpoint);
    transfer
        .transfer(pg_id, acting_set)
        .map_err(|error| error.to_string())
}

fn control_plane_runtime_map_ready(
    socket_path: &Path,
) -> Result<(ClusterEpoch, usize, usize), String> {
    let control_plane = build_frontend_control_plane_client_from_runtime_map_auth_env(socket_path)?;
    let status = control_plane
        .runtime_map_status_with_check_applied_timeout()
        .map_err(|error| format!("control-plane runtime map is not ready: {error}"))?;
    Ok((
        status.cluster_epoch(),
        status.pg_routes(),
        status.active_serving_pg_routes(),
    ))
}

fn control_plane_runtime_map_diagnostics(socket_path: &Path) -> Result<String, String> {
    let control_plane = build_frontend_control_plane_client_from_runtime_map_auth_env(socket_path)?;
    let diagnostics = control_plane
        .runtime_map_diagnostics()
        .map_err(|error| format!("control-plane runtime map is not ready: {error}"))?;
    Ok(format_control_plane_runtime_map_diagnostics(&diagnostics))
}

fn control_plane_pg_runtime_map_ready(
    socket_path: &Path,
    pg_id: u32,
    expected_acting_set: &[u32],
) -> Result<(u64, u64), String> {
    build_pg_status_control_plane_client_from_runtime_map_auth_env(socket_path)?
        .serving_epochs(pg_id, expected_acting_set)
        .map_err(|error| format!("control-plane PG runtime map is not ready: {error}"))
}

fn format_control_plane_runtime_map_diagnostics(
    diagnostics: &storage::control_plane::ControlPlaneRuntimeMapDiagnostics,
) -> String {
    format_control_plane_runtime_map_diagnostics_parts(
        (diagnostics.runtime_map(), diagnostics.node_leases()),
        diagnostics.rpc_metrics(),
        (
            diagnostics.snapshot_metrics(),
            diagnostics.journal_metrics(),
            diagnostics.raft_checkpoint_metrics(),
            diagnostics.raft_wal_metrics(),
            diagnostics.raft_command_metrics(),
        ),
        diagnostics.history_reference_samples(),
    )
}

fn format_control_plane_runtime_map_diagnostics_parts(
    runtime_map: (
        &ClusterRuntimeMapSnapshot,
        &[storage::control_plane::ControlPlaneRuntimeMapNodeLeaseDiagnostic],
    ),
    rpc_metrics: &[observability::ControlPlaneRpcMetricSample],
    durability_metrics: (
        observability::ControlPlaneSnapshotMetricSnapshot,
        observability::ControlPlaneJournalMetricSnapshot,
        observability::ControlPlaneRaftCheckpointMetricSnapshot,
        observability::ControlPlaneRaftWalMetricSnapshot,
        observability::ControlPlaneRaftCommandMetricSnapshot,
    ),
    history_reference_samples: &[observability::ControlPlaneHistoryReferenceSample],
) -> String {
    let (runtime_map, node_leases) = runtime_map;
    let (snapshot, journal, raft_checkpoint, raft_wal, raft_command) = durability_metrics;
    let active_serving_pg_routes = runtime_map
        .pg_routes()
        .iter()
        .filter(|route| {
            route.state() == PgState::Active && route.primary_lease_deadline_ms().is_some()
        })
        .count();
    let floor_nodes = runtime_map
        .nodes()
        .iter()
        .filter(|node| node.cluster_map_history_floor_epoch().is_some())
        .count();
    let oldest_floor = runtime_map
        .nodes()
        .iter()
        .filter_map(|node| node.cluster_map_history_floor_epoch())
        .min();
    let mut output = format!(
        "epoch={} nodes={} pg_routes={} active_serving_pg_routes={} historical_pg_routes={} storage_history_floor_nodes={} oldest_storage_history_floor_epoch={}",
        runtime_map.cluster_epoch().get(),
        runtime_map.nodes().len(),
        runtime_map.pg_routes().len(),
        active_serving_pg_routes,
        runtime_map.historical_pg_routes().len(),
        floor_nodes,
        format_optional_epoch(oldest_floor),
    );
    for node in runtime_map.nodes() {
        let history_references = history_reference_samples
            .iter()
            .find(|sample| sample.node_id == node.node_id().as_u32());
        let lease_deadline_ms = node_leases
            .iter()
            .find(|lease| lease.node_id() == node.node_id())
            .and_then(|lease| lease.lease_deadline_ms());
        output.push('\n');
        output.push_str(&format!(
            "node_id={} incarnation={} endpoint={} lease_deadline_ms={} storage_history_floor_epoch={} history_report_observed_epoch={} history_report_validation_epoch={} history_report_accepted_at_ms={} history_live_payload_epoch={} history_durable_backfill_epoch={} history_pending_metadata_command_epoch={} history_object_payload_reclaim_claim_epoch={}",
            node.node_id().as_u32(),
            node.node_incarnation(),
            node.endpoint(),
            format_optional_u64(lease_deadline_ms),
            format_optional_epoch(node.cluster_map_history_floor_epoch()),
            format_optional_u64(history_references.map(|sample| sample.observed_epoch)),
            format_optional_u64(history_references.map(|sample| sample.validation_epoch)),
            format_optional_u64(history_references.map(|sample| sample.observed_at_ms)),
            format_optional_u64(
                history_references.and_then(|sample| sample.oldest_live_placement_epoch)
            ),
            format_optional_u64(
                history_references.and_then(|sample| sample.oldest_durable_backfill_epoch)
            ),
            format_optional_u64(
                history_references
                    .and_then(|sample| sample.oldest_pending_metadata_command_epoch)
            ),
            format_optional_u64(
                history_references
                    .and_then(|sample| sample.oldest_object_payload_reclaim_claim_epoch)
            ),
        ));
    }
    for metric in rpc_metrics {
        output.push('\n');
        output.push_str(&format!(
            "control_plane_rpc kind={} total={} lock_wait_us_total={} lock_wait_us_max={} operation_us_total={} operation_us_max={} response_write_us_total={} response_write_us_max={} response_write_error_total={} response_write_broken_pipe_total={} response_write_connection_reset_total={} response_write_timeout_total={} response_write_other_error_total={}",
            metric.kind.as_str(),
            metric.total,
            metric.lock_wait_us_total,
            metric.lock_wait_us_max,
            metric.operation_us_total,
            metric.operation_us_max,
            metric.response_write_us_total,
            metric.response_write_us_max,
            metric.response_write_error_total,
            metric.response_write_broken_pipe_total,
            metric.response_write_connection_reset_total,
            metric.response_write_timeout_total,
            metric.response_write_other_error_total,
        ));
    }
    output.push('\n');
    output.push_str(&format!(
        "control_plane_snapshot serialize_total={} serialize_us_total={} serialize_us_max={} save_total={} save_error_total={} save_us_total={} save_us_max={} sync_total={} sync_us_total={} sync_us_max={} bytes_total={} bytes_last={} bytes_max={}",
        snapshot.serialize_total,
        snapshot.serialize_us_total,
        snapshot.serialize_us_max,
        snapshot.save_total,
        snapshot.save_error_total,
        snapshot.save_us_total,
        snapshot.save_us_max,
        snapshot.sync_total,
        snapshot.sync_us_total,
        snapshot.sync_us_max,
        snapshot.bytes_total,
        snapshot.bytes_last,
        snapshot.bytes_max,
    ));
    output.push('\n');
    output.push_str(&format!(
        "control_plane_journal append_total={} append_error_total={} append_us_total={} append_us_max={} lock_wait_us_total={} lock_wait_us_max={} frame_bytes_total={} frame_bytes_last={} frame_bytes_max={} file_sync_total={} file_sync_us_total={} file_sync_us_max={} directory_sync_total={} directory_sync_us_total={} directory_sync_us_max={} compaction_total={} compaction_error_total={} compaction_us_total={} compaction_us_max={} compaction_lock_wait_us_total={} compaction_lock_wait_us_max={} compaction_bytes_total={} compaction_bytes_last={} compaction_bytes_max={} compaction_file_sync_total={} compaction_file_sync_us_total={} compaction_file_sync_us_max={} compaction_directory_sync_total={} compaction_directory_sync_us_total={} compaction_directory_sync_us_max={}",
        journal.append_total,
        journal.append_error_total,
        journal.append_us_total,
        journal.append_us_max,
        journal.lock_wait_us_total,
        journal.lock_wait_us_max,
        journal.frame_bytes_total,
        journal.frame_bytes_last,
        journal.frame_bytes_max,
        journal.file_sync_total,
        journal.file_sync_us_total,
        journal.file_sync_us_max,
        journal.directory_sync_total,
        journal.directory_sync_us_total,
        journal.directory_sync_us_max,
        journal.compaction_total,
        journal.compaction_error_total,
        journal.compaction_us_total,
        journal.compaction_us_max,
        journal.compaction_lock_wait_us_total,
        journal.compaction_lock_wait_us_max,
        journal.compaction_bytes_total,
        journal.compaction_bytes_last,
        journal.compaction_bytes_max,
        journal.compaction_file_sync_total,
        journal.compaction_file_sync_us_total,
        journal.compaction_file_sync_us_max,
        journal.compaction_directory_sync_total,
        journal.compaction_directory_sync_us_total,
        journal.compaction_directory_sync_us_max,
    ));
    output.push('\n');
    output.push_str(&format!(
        "control_plane_raft_checkpoint encode_total={} encode_us_total={} encode_us_max={} store_total={} store_error_total={} store_us_total={} store_us_max={} file_sync_total={} file_sync_us_total={} file_sync_us_max={} directory_sync_total={} directory_sync_us_total={} directory_sync_us_max={} bytes_total={} bytes_last={} bytes_max={} compaction_total={} compaction_error_total={} compaction_us_total={} compaction_us_max={}",
        raft_checkpoint.encode_total,
        raft_checkpoint.encode_us_total,
        raft_checkpoint.encode_us_max,
        raft_checkpoint.store_total,
        raft_checkpoint.store_error_total,
        raft_checkpoint.store_us_total,
        raft_checkpoint.store_us_max,
        raft_checkpoint.file_sync_total,
        raft_checkpoint.file_sync_us_total,
        raft_checkpoint.file_sync_us_max,
        raft_checkpoint.directory_sync_total,
        raft_checkpoint.directory_sync_us_total,
        raft_checkpoint.directory_sync_us_max,
        raft_checkpoint.bytes_total,
        raft_checkpoint.bytes_last,
        raft_checkpoint.bytes_max,
        raft_checkpoint.compaction_total,
        raft_checkpoint.compaction_error_total,
        raft_checkpoint.compaction_us_total,
        raft_checkpoint.compaction_us_max,
    ));
    output.push('\n');
    output.push_str(&format!(
        "control_plane_raft_wal append_total={} append_error_total={} append_us_total={} append_us_max={} lock_wait_us_total={} lock_wait_us_max={} frame_bytes_total={} frame_bytes_last={} frame_bytes_max={} file_sync_total={} file_sync_us_total={} file_sync_us_max={} directory_sync_total={} directory_sync_us_total={} directory_sync_us_max={} durability_queue_depth={} durability_queue_depth_max={} durability_queue_wait_us_total={} durability_queue_wait_us_max={} append_accept_us_total={} append_accept_us_max={} durability_operation_us_total={} durability_operation_us_max={}",
        raft_wal.append_total,
        raft_wal.append_error_total,
        raft_wal.append_us_total,
        raft_wal.append_us_max,
        raft_wal.lock_wait_us_total,
        raft_wal.lock_wait_us_max,
        raft_wal.frame_bytes_total,
        raft_wal.frame_bytes_last,
        raft_wal.frame_bytes_max,
        raft_wal.file_sync_total,
        raft_wal.file_sync_us_total,
        raft_wal.file_sync_us_max,
        raft_wal.directory_sync_total,
        raft_wal.directory_sync_us_total,
        raft_wal.directory_sync_us_max,
        raft_wal.durability_queue_depth,
        raft_wal.durability_queue_depth_max,
        raft_wal.durability_queue_wait_us_total,
        raft_wal.durability_queue_wait_us_max,
        raft_wal.append_accept_us_total,
        raft_wal.append_accept_us_max,
        raft_wal.durability_operation_us_total,
        raft_wal.durability_operation_us_max,
    ));
    output.push('\n');
    output.push_str(&format!(
        "control_plane_raft_command submit_total={} submit_error_total={} queue_wait_us_total={} queue_wait_us_max={} operation_us_total={} operation_us_max={}",
        raft_command.submit_total,
        raft_command.submit_error_total,
        raft_command.queue_wait_us_total,
        raft_command.queue_wait_us_max,
        raft_command.operation_us_total,
        raft_command.operation_us_max,
    ));
    output
}

fn format_optional_epoch(epoch: Option<ClusterEpoch>) -> String {
    epoch
        .map(|epoch| epoch.get().to_string())
        .unwrap_or_else(|| "-".to_owned())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn checkpoint_standalone_control_plane_if_due(
    authority: &Arc<Mutex<SingleAuthorityControlPlane<FileControlPlaneStore>>>,
    now: Instant,
) -> Result<bool, ControlPlaneError> {
    let checkpoint = authority
        .lock()
        .map_err(|_| ControlPlaneError::CommandDecode {
            message: "standalone control-plane authority mutex poisoned".to_owned(),
        })?
        .capture_durable_checkpoint_if_due(now)?;
    let Some(checkpoint) = checkpoint else {
        return Ok(false);
    };
    checkpoint.persist()?;
    Ok(true)
}

fn spawn_standalone_control_plane_checkpoint_loop(
    authority: Arc<Mutex<SingleAuthorityControlPlane<FileControlPlaneStore>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        if let Err(error) = checkpoint_standalone_control_plane_if_due(&authority, Instant::now()) {
            eprintln!(
                "standalone control-plane bounded journal checkpoint failed; exiting to avoid serving after durability failure: {error}"
            );
            std::process::exit(1);
        }
        thread::sleep(CONTROL_PLANE_STANDALONE_CHECKPOINT_POLL_INTERVAL);
    })
}

fn initialize_standalone_control_plane_before_binding<Ready, Listeners>(
    initialize: impl FnOnce() -> Ready,
    bind_listeners: impl FnOnce() -> Listeners,
) -> (Ready, Listeners) {
    let ready = initialize();
    let listeners = bind_listeners();
    (ready, listeners)
}

fn run_control_plane_process(config: &ServerConfig) -> ! {
    if config.control_plane_experimental_raft {
        run_experimental_raft_control_plane_process(config);
    }

    let state_path = config
        .control_plane_state_path
        .as_deref()
        .expect("control-plane role requires state path");
    let socket_path = config
        .control_plane_socket_path
        .as_deref()
        .expect("control-plane role requires socket path");
    let _state_lock =
        acquire_control_plane_state_lock(Path::new(state_path)).unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });
    let recovery_socket_path = config
        .control_plane_clock_recovery_socket_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            storage::control_plane_clock_recovery_socket_path(Path::new(socket_path))
        });
    let (
        (authority, authority_clock, authority_clock_checkpoint_target, server_auth),
        (listeners, recovery_listeners),
    ) = initialize_standalone_control_plane_before_binding(
        || {
            let store = FileControlPlaneStore::new(state_path);
            let authority_clock_checkpoint_binding = store
                .load_or_create_authority_clock_checkpoint_binding()
                .unwrap_or_else(|error| {
                    eprintln!("failed to load control-plane durable identity: {error}");
                    std::process::exit(1);
                });
            let restart_clock_checkpoint = load_process_authority_clock_restart_checkpoint(
                store.path(),
                authority_clock_checkpoint_binding,
            )
            .unwrap_or_else(|error| {
                eprintln!("failed to load control-plane authority clock checkpoint: {error}");
                std::process::exit(1);
            });
            let mut authority = SingleAuthorityControlPlane::open(store).unwrap_or_else(|error| {
                eprintln!("failed to open control-plane state {state_path}: {error}");
                std::process::exit(1);
            });
            bootstrap_empty_control_plane(&mut authority, config).unwrap_or_else(|error| {
                eprintln!("failed to bootstrap control-plane state: {error}");
                std::process::exit(1);
            });
            let mut authority_clock =
                ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
                    authority.snapshot().max_committed_timestamp_ms(),
                    restart_clock_checkpoint,
                )
                .unwrap_or_else(|error| {
                    eprintln!("failed to initialize control-plane authority clock: {error}");
                    std::process::exit(1);
                });
            if !authority_clock.is_established() {
                invalidate_authority_clock_restart_checkpoint(Path::new(state_path))
                    .unwrap_or_else(|error| {
                        eprintln!(
                            "failed to invalidate blocked authority clock checkpoint: {error}"
                        );
                        std::process::exit(1);
                    });
            }
            if let Some(previous_authority) = authority.snapshot().lease_grant_horizon_authority() {
                if !authority_clock
                    .resume_single_authority_lease_horizon_generation(previous_authority)
                {
                    authority_clock
                        .advance_generation_past_lease_horizon(previous_authority)
                        .unwrap_or_else(|error| {
                            eprintln!(
                                "failed to advance restarted control-plane clock generation: {error}"
                            );
                            std::process::exit(1);
                        });
                }
            }
            let server_auth = build_control_plane_rpc_server_auth(config).unwrap_or_else(|error| {
                eprintln!("failed to configure control-plane auth verifier: {error}");
                std::process::exit(1);
            });
            (
                Arc::new(Mutex::new(authority)),
                Arc::new(Mutex::new(authority_clock)),
                Arc::new(ControlPlaneAuthorityClockCheckpointTarget::new(
                    state_path,
                    authority_clock_checkpoint_binding,
                )),
                server_auth,
            )
        },
        || {
            // Requests are timestamped before connect. Binding before replay completes would
            // accumulate stale authenticated requests in the accept backlog.
            let listeners = bind_configured_control_plane_rpc_listeners(
                &config.control_plane_rpc_listeners,
                Path::new(socket_path),
                "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
                CONTROL_PLANE_RPC_WORKER_LIMIT,
            )
            .unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(1);
            });
            let recovery_listeners = bind_configured_control_plane_rpc_listeners(
                &config.control_plane_clock_recovery_rpc_listeners,
                &recovery_socket_path,
                "derived control-plane clock recovery socket",
                CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
            )
            .unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(1);
            });
            (listeners, recovery_listeners)
        },
    );
    let _checkpoint_loop = spawn_standalone_control_plane_checkpoint_loop(Arc::clone(&authority));
    process_info!(
        "argmin-s3 control-plane manager using state {} on {} (clock recovery {}, lease scan {} ms)",
        state_path,
        socket_path,
        recovery_socket_path.display(),
        config.control_plane_lease_scan_interval.as_millis()
    );
    if let Some(diagnostics) = server_auth.diagnostics() {
        process_info!("{}", diagnostics);
    }

    let fatal_error_handler: Arc<dyn Fn() + Send + Sync> = Arc::new(|| std::process::exit(1));
    let rpc_server = ControlPlaneRpcServerBootstrap::new(
        listeners,
        recovery_listeners,
        CONTROL_PLANE_RPC_WORKER_LIMIT,
        CONTROL_PLANE_RPC_PRE_AUTH_BYTE_BUDGET,
        CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
        CONTROL_PLANE_CLOCK_RECOVERY_RPC_PRE_AUTH_BYTE_BUDGET,
        &server_auth,
    )
    .unwrap_or_else(|error| {
        eprintln!("failed to configure control-plane RPC server: {error}");
        std::process::exit(1);
    });
    let _rpc_listener_loops = rpc_server.serve_shared_single_authority(
        Arc::clone(&authority),
        Arc::clone(&authority_clock),
        Arc::clone(&authority_clock_checkpoint_target),
        fatal_error_handler,
    );

    loop {
        let expiry = (|| {
            let mut authority = authority
                .lock()
                .expect("control-plane authority mutex poisoned");
            let mut authority_clock = authority_clock
                .lock()
                .expect("control-plane authority clock mutex poisoned");
            let effective_now_ms = authority_clock.effective_process_now_ms();
            authority_clock_checkpoint_target.invalidate_if_blocked(&authority_clock)?;
            authority.expire_heartbeat_leases(effective_now_ms?)
        })();
        match expiry {
            Ok(expiry) if !expiry.expired_nodes().is_empty() => {
                process_info!(
                    "control-plane expired {} node leases at epoch {} and moved {} PGs to peering",
                    expiry.expired_nodes().len(),
                    expiry.cluster_epoch(),
                    expiry.peering_pgs().len()
                );
            }
            Ok(_) => {}
            Err(error @ ControlPlaneError::AuthorityClockSampleWindowTooWide { .. }) => {
                eprintln!(
                    "control-plane lease expiry deferred because a coherent clock sample was unavailable: {error}"
                );
            }
            Err(error) if control_plane_lease_expiry_error_is_clock_wait(&error) => {
                eprintln!(
                    "control-plane lease expiry deferred while local clock catches up to committed timestamp: {error}"
                );
            }
            Err(error) => {
                eprintln!("control-plane lease expiry failed: {error}");
                std::process::exit(1);
            }
        }
        thread::sleep(config.control_plane_lease_scan_interval);
    }
}

fn block_on_control_plane_raft<F: Future>(runtime: &Handle, future: F) -> F::Output {
    if Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(future))
    } else {
        runtime.block_on(future)
    }
}

fn build_experimental_raft_peer_bootstrap(
    config: &ServerConfig,
    cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<ControlPlaneRaftPeerBootstrap, String> {
    if config.control_plane_raft_peer_socket_path.is_none()
        && config.control_plane_raft_peer_listeners.is_empty()
    {
        return Ok(ControlPlaneRaftPeerBootstrap::single_node(
            cluster_name,
            local_node_id,
        ));
    }
    let peer_endpoints: Vec<_> = if config.control_plane_raft_peer_sockets.is_empty() {
        let local_peer_socket_path = config
            .control_plane_raft_peer_socket_path
            .as_deref()
            .ok_or_else(|| {
                "configured OpenRaft peer listeners require explicit peer endpoints".to_string()
            })?;
        vec![(local_node_id, local_peer_socket_path.to_string())]
    } else {
        config
            .control_plane_raft_peer_sockets
            .iter()
            .map(|entry| (entry.node_id, entry.socket_path.clone()))
            .collect()
    };
    let topology = if let Some(initial) = &config.static_initial_cluster_map {
        ControlPlaneRaftPeerTopologyBinding::StaticInitial(initial.clone())
    } else if let Some(identity) = &config.static_cluster_identity {
        ControlPlaneRaftPeerTopologyBinding::Established {
            generation: identity.topology_generation,
            digest: identity.topology_digest.clone(),
        }
    } else {
        ControlPlaneRaftPeerTopologyBinding::Unbound
    };
    let credentials = config
        .control_plane_raft_auth_credentials
        .iter()
        .map(|credential| {
            ControlPlaneRaftPeerAuthCredentialInput::new(
                credential.node_id,
                credential.credential_id.clone(),
                credential.credential_version,
                credential.secret.as_bytes().to_vec(),
            )
        })
        .collect();
    ControlPlaneRaftPeerBootstrap::replicated(
        cluster_name,
        local_node_id,
        peer_endpoints,
        config.control_plane_raft_peer_client_endpoints.clone(),
        config.control_plane_raft_peer_transport_limits,
        config.control_plane_raft_peer_connect_timeout,
        config.control_plane_raft_peer_io_timeout,
        topology,
        credentials,
        config.control_plane_raft_auth_signing_credential.clone(),
    )
    .map_err(|error| format!("invalid control-plane OpenRaft peer bootstrap: {error}"))
}

#[cfg(test)]
async fn experimental_raft_local_authority_serving_within(
    authority: &ControlPlaneRaftAuthority,
    timeout: Duration,
) -> Result<bool, ControlPlaneError> {
    let deadline = Instant::now() + timeout;
    loop {
        if authority.status().await?.linearized_authority_serving() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(test)]
async fn wait_for_experimental_raft_local_authority_serving(
    authority: &ControlPlaneRaftAuthority,
    timeout: Duration,
    message: &'static str,
) -> Result<(), ControlPlaneError> {
    if experimental_raft_local_authority_serving_within(authority, timeout).await? {
        return Ok(());
    }
    Err(ControlPlaneError::startup_timeout(format!(
        "local OpenRaft authority did not become serving within {timeout:?}: {message}"
    )))
}

#[cfg(test)]
fn bind_experimental_raft_peer_listener(
    config: &ServerConfig,
    cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<Vec<ControlPlaneRaftPeerServerListenerInput>, String> {
    let _bootstrap = build_experimental_raft_peer_bootstrap(config, cluster_name, local_node_id)?;
    bind_experimental_raft_peer_listener_inputs(config)
}

fn bind_experimental_raft_peer_listener_inputs(
    config: &ServerConfig,
) -> Result<Vec<ControlPlaneRaftPeerServerListenerInput>, String> {
    let configured_listeners = if config.control_plane_raft_peer_listeners.is_empty() {
        config
            .control_plane_raft_peer_socket_path
            .as_ref()
            .map(|socket_path| {
                vec![ConfiguredControlPlaneRaftPeerListener::Unix {
                    endpoint_id: "legacy-raft-peer".to_string(),
                    socket_path: socket_path.clone(),
                    max_connections: config.control_plane_raft_peer_max_connections,
                    io_timeout: config.control_plane_raft_peer_io_timeout,
                }]
            })
            .unwrap_or_default()
    } else {
        config.control_plane_raft_peer_listeners.clone()
    };
    if configured_listeners.is_empty() {
        return Ok(Vec::new());
    }
    configured_listeners
        .into_iter()
        .map(|listener| match listener {
            ConfiguredControlPlaneRaftPeerListener::Unix {
                endpoint_id,
                socket_path,
                max_connections,
                io_timeout,
            } => Ok(ControlPlaneRaftPeerServerListenerInput::Unix {
                endpoint_id,
                listener: bind_control_plane_raft_peer_socket(Path::new(&socket_path))?,
                max_connections,
                io_timeout,
            }),
            ConfiguredControlPlaneRaftPeerListener::Tcp {
                endpoint_id,
                bind_addr,
                certified_key,
                max_connections,
                io_timeout,
            } => {
                let listener = StdTcpListener::bind(&bind_addr).map_err(|error| {
                    format!(
                        "bind control-plane OpenRaft TCP peer listener {endpoint_id} at {bind_addr}: {error}"
                    )
                })?;
                Ok(ControlPlaneRaftPeerServerListenerInput::TlsTcp {
                    endpoint_id,
                    listener,
                    certified_key,
                    max_connections,
                    io_timeout,
                })
            }
        })
        .collect()
}

struct StaticRaftOuterIdentityPublisher<'a> {
    identity: &'a ConfiguredStaticClusterIdentity,
    node_id: ControlPlaneRaftNodeId,
}

impl ControlPlaneRaftOuterIdentityPublisher for StaticRaftOuterIdentityPublisher<'_> {
    fn publish(
        &self,
        authority_artifact_path: &Path,
    ) -> Result<(), ControlPlaneRaftOuterIdentityPublicationError> {
        static_cluster_state::mark_static_control_plane_identity_established(
            self.identity,
            self.node_id,
            authority_artifact_path,
        )
        .map_err(classify_static_control_plane_identity_establishment_error)
    }
}

fn classify_static_control_plane_identity_establishment_error(
    error: static_cluster_state::StaticControlPlaneIdentityEstablishmentError,
) -> ControlPlaneRaftOuterIdentityPublicationError {
    match error {
        static_cluster_state::StaticControlPlaneIdentityEstablishmentError::Validation(_) => {
            ControlPlaneRaftOuterIdentityPublicationError::InvalidIdentity
        }
        static_cluster_state::StaticControlPlaneIdentityEstablishmentError::Persistence(_) => {
            ControlPlaneRaftOuterIdentityPublicationError::PersistenceFailure
        }
    }
}

fn run_experimental_raft_control_plane_process(config: &ServerConfig) -> ! {
    let state_path = config
        .control_plane_state_path
        .as_deref()
        .expect("control-plane role requires state path");
    let socket_path = config
        .control_plane_socket_path
        .as_deref()
        .expect("control-plane role requires socket path");
    let _state_lock =
        acquire_control_plane_state_lock(Path::new(state_path)).unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });
    let node_id: ControlPlaneRaftNodeId = config.control_plane_raft_node_id.unwrap_or(1);
    let static_cluster_identity_established =
        if let Some(identity) = &config.static_cluster_identity {
            static_cluster_state::bind_static_control_plane_identity(
                identity,
                node_id,
                Path::new(state_path),
            )
            .unwrap_or_else(|error| {
                eprintln!("failed to bind static control-plane identity: {error}");
                std::process::exit(1);
            })
        } else {
            false
        };
    let recovery_socket_path = config
        .control_plane_clock_recovery_socket_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            storage::control_plane_clock_recovery_socket_path(Path::new(socket_path))
        });

    let runtime = Handle::current();
    let cluster_name = config
        .control_plane_raft_cluster_name
        .clone()
        .unwrap_or_else(|| format!("argmin-s3-experimental-control-plane-{socket_path}"));
    let raft_peer_bootstrap =
        build_experimental_raft_peer_bootstrap(config, &cluster_name, node_id).unwrap_or_else(
            |error| {
                eprintln!("{error}");
                std::process::exit(1);
            },
        );
    let raft_peer_auth_diagnostics = raft_peer_bootstrap.auth_diagnostics();
    let outer_identity_publisher = config
        .static_cluster_identity
        .as_ref()
        .map(|identity| StaticRaftOuterIdentityPublisher { identity, node_id });
    let outer_identity = match outer_identity_publisher.as_ref() {
        None => ControlPlaneRaftOuterIdentityStartup::NotConfigured,
        Some(_) if static_cluster_identity_established => {
            ControlPlaneRaftOuterIdentityStartup::Established
        }
        Some(publisher) => ControlPlaneRaftOuterIdentityStartup::Publish(publisher),
    };
    let prepared_authority = block_on_control_plane_raft(
        &runtime,
        raft_peer_bootstrap.prepare_durable_authority(
            runtime.clone(),
            Path::new(state_path),
            outer_identity,
        ),
    )
    .unwrap_or_else(|error| {
        eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
        std::process::exit(1);
    });
    // Durable replay and authority validation must finish before the process
    // publishes any inbound peer endpoint.
    let raft_peer_listener_inputs = bind_experimental_raft_peer_listener_inputs(config)
        .unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });
    let fatal_error_handler: Arc<dyn Fn() + Send + Sync> = Arc::new(|| std::process::exit(1));
    let mut authority_service = block_on_control_plane_raft(
        &runtime,
        prepared_authority.start(
            raft_peer_listener_inputs,
            CONTROL_PLANE_RAFT_PEER_PRE_AUTH_BYTE_BUDGET,
            Duration::from_secs(1),
            Arc::clone(&fatal_error_handler),
        ),
    )
    .unwrap_or_else(|error| {
        eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
        std::process::exit(1);
    });
    let multi_node_raft_peer_mode = authority_service.is_multi_node();
    if config.static_initial_cluster_map.is_none() {
        bootstrap_empty_experimental_raft_control_plane(authority_service.host(), config)
            .unwrap_or_else(|error| {
                eprintln!("failed to bootstrap experimental OpenRaft control-plane state: {error}");
                std::process::exit(1);
            });
    }
    let listeners = bind_configured_control_plane_rpc_listeners(
        &config.control_plane_rpc_listeners,
        Path::new(socket_path),
        "ARGMIN_CONTROL_PLANE_SOCKET_PATH",
        CONTROL_PLANE_RPC_WORKER_LIMIT,
    )
    .unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let recovery_listeners = bind_configured_control_plane_rpc_listeners(
        &config.control_plane_clock_recovery_rpc_listeners,
        &recovery_socket_path,
        "derived control-plane clock recovery socket",
        CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
    )
    .unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let server_auth = build_control_plane_rpc_server_auth(config).unwrap_or_else(|error| {
        eprintln!("failed to configure control-plane auth verifier: {error}");
        std::process::exit(1);
    });
    let raft_peer_socket_path = config
        .control_plane_raft_peer_socket_path
        .as_deref()
        .unwrap_or("-");
    process_info!(
        "argmin-s3 experimental durable OpenRaft control-plane manager using state {} on {} (clock recovery {}, raft node {}, peer socket {}, configured peers {}, configured auth credentials {}, lease scan {} ms)",
        state_path,
        socket_path,
        recovery_socket_path.display(),
        node_id,
        raft_peer_socket_path,
        config.control_plane_raft_peer_sockets.len(),
        config.control_plane_raft_auth_credentials.len(),
        config.control_plane_lease_scan_interval.as_millis()
    );
    if let Some(diagnostics) = raft_peer_auth_diagnostics {
        process_info!("{}", diagnostics);
    }
    if let Some(diagnostics) = server_auth.diagnostics() {
        process_info!("{}", diagnostics);
    }

    let rpc_server = ControlPlaneRpcServerBootstrap::new(
        listeners,
        recovery_listeners,
        CONTROL_PLANE_RPC_WORKER_LIMIT,
        CONTROL_PLANE_RPC_PRE_AUTH_BYTE_BUDGET,
        CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
        CONTROL_PLANE_CLOCK_RECOVERY_RPC_PRE_AUTH_BYTE_BUDGET,
        &server_auth,
    )
    .unwrap_or_else(|error| {
        eprintln!("failed to configure control-plane RPC server: {error}");
        std::process::exit(1);
    });
    let _rpc_listener_loops = authority_service
        .host()
        .serve_rpc(rpc_server, fatal_error_handler)
        .unwrap_or_else(|error| {
            eprintln!("failed to start experimental OpenRaft RPC server: {error}");
            std::process::exit(1);
        });

    let mut lease_expiry_not_before_ms = None;
    loop {
        let local_raft_authority_serving = if multi_node_raft_peer_mode {
            authority_service
                .host()
                .linearized_authority_serving()
                .unwrap_or_else(|error| {
                    eprintln!("experimental OpenRaft control-plane status check failed: {error}");
                    std::process::exit(1);
                })
        } else {
            true
        };
        let expiry_now_ms = storage::clock::current_time_millis();
        let expiry = if local_raft_authority_serving {
            if multi_node_raft_peer_mode && config.static_initial_cluster_map.is_none() {
                bootstrap_empty_experimental_raft_control_plane(authority_service.host(), config)
                    .unwrap_or_else(|error| {
                        eprintln!(
                            "failed to bootstrap experimental OpenRaft control-plane state: {error}"
                        );
                        std::process::exit(1);
                    });
            }
            if lease_expiry_not_before_ms.is_some_and(|not_before_ms| expiry_now_ms < not_before_ms)
            {
                Ok(None)
            } else {
                authority_service
                    .host_mut()
                    .expire_heartbeat_leases(expiry_now_ms)
                    .map(Some)
            }
        } else {
            Ok(None)
        };
        authority_service
            .host()
            .invalidate_blocked_authority_clock_checkpoint()
            .unwrap_or_else(|error| {
                eprintln!(
                    "failed to invalidate blocked experimental OpenRaft authority clock checkpoint: {error}"
                );
                std::process::exit(1);
            });
        match expiry {
            Ok(Some(expiry)) if expiry.expired_nodes() > 0 => {
                process_info!(
                    "experimental OpenRaft control-plane expired {} node leases at epoch {} and moved {} PGs to peering",
                    expiry.expired_nodes(),
                    expiry.cluster_epoch(),
                    expiry.peering_pgs()
                );
            }
            Ok(_) => {}
            Err(error @ ControlPlaneError::AuthorityClockSampleWindowTooWide { .. }) => {
                eprintln!(
                    "experimental OpenRaft control-plane lease expiry deferred because a coherent clock sample was unavailable: {error}"
                );
            }
            Err(
                error @ ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
                    fenced_until_ms,
                    ..
                },
            ) => {
                let renewal_not_before_ms = successor_heartbeat_renewal_not_before_ms(
                    fenced_until_ms,
                )
                .unwrap_or_else(|error| {
                    eprintln!(
                        "experimental OpenRaft successor heartbeat renewal window failed: {error}"
                    );
                    std::process::exit(1);
                });
                lease_expiry_not_before_ms = Some(
                    lease_expiry_not_before_ms.map_or(renewal_not_before_ms, |existing: u64| {
                        existing.max(renewal_not_before_ms)
                    }),
                );
                eprintln!(
                    "experimental OpenRaft control-plane lease expiry deferred while local clock catches up to committed timestamp: {error}"
                );
            }
            Err(error) if experimental_raft_lease_expiry_error_is_transient(&error) => {}
            Err(error) if control_plane_lease_expiry_error_is_clock_wait(&error) => {
                eprintln!(
                    "experimental OpenRaft control-plane lease expiry deferred while local clock catches up to committed timestamp: {error}"
                );
            }
            Err(error) => {
                eprintln!("experimental OpenRaft control-plane lease expiry failed: {error}");
                std::process::exit(1);
            }
        }
        thread::sleep(config.control_plane_lease_scan_interval);
    }
}

fn successor_heartbeat_renewal_not_before_ms(
    predecessor_fenced_until_ms: u64,
) -> Result<u64, ControlPlaneError> {
    let rpc_timeout_ms = u64::try_from(CONTROL_PLANE_RPC_IO_TIMEOUT.as_millis())
        .map_err(|_| ControlPlaneError::LeaseDeadlineOverflow)?;
    predecessor_fenced_until_ms
        .checked_add(
            storage::storage_node_server::STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MAX_INTERVAL_MS,
        )
        .and_then(|deadline_ms| deadline_ms.checked_add(rpc_timeout_ms))
        .ok_or(ControlPlaneError::LeaseDeadlineOverflow)
}

fn control_plane_lease_expiry_error_is_clock_wait(error: &ControlPlaneError) -> bool {
    matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression { .. }
            | ControlPlaneError::CommittedTimestampTooFarAhead { .. }
            | ControlPlaneError::PreviousLeaseGrantHorizonStillActive { .. }
            | ControlPlaneError::AuthorityClockLeadershipChanged { .. }
            | ControlPlaneError::AuthorityClockSourceUnavailable
            | ControlPlaneError::AuthorityClockNotEstablished { .. }
    )
}

fn experimental_raft_error_is_non_local_leader(error: &ControlPlaneError) -> bool {
    error.is_control_plane_leader_routing_rejection()
}

fn experimental_raft_lease_expiry_error_is_transient(error: &ControlPlaneError) -> bool {
    experimental_raft_error_is_non_local_leader(error)
        || matches!(
            error,
            ControlPlaneError::LeaseGrantHorizonAuthorityTermMismatch { .. }
        )
}

fn bootstrap_empty_experimental_raft_control_plane(
    authority: &ControlPlaneRaftAuthorityHost,
    config: &ServerConfig,
) -> Result<(), ControlPlaneError> {
    if config.static_initial_cluster_map.is_some() {
        return Ok(());
    }
    let topology = uncertified_initial_control_plane_topology(config)
        .map_err(storage::StaticStorageTopologyError::into_control_plane_error)?;
    let Some(epoch) = authority.establish_uncertified_initial_topology(&topology)? else {
        return Ok(());
    };
    process_info!(
        "experimental OpenRaft control-plane bootstrapped {} nodes and {} PG acting sets at epoch {}",
        topology.node_count(),
        topology.pg_count(),
        epoch
    );
    Ok(())
}

fn uncertified_initial_control_plane_topology(
    config: &ServerConfig,
) -> Result<storage::UncertifiedInitialControlPlaneTopology, storage::StaticStorageTopologyError> {
    let endpoints = config
        .storage_node_sockets
        .iter()
        .map(|entry| {
            storage::StaticStorageNodeEndpoint::new(entry.node_id, entry.socket_path.clone())
        })
        .collect::<Vec<_>>();
    storage::derive_uncertified_initial_control_plane_topology(&endpoints, &config.storage_pg_ids)
}

fn bootstrap_empty_control_plane(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    config: &ServerConfig,
) -> Result<(), String> {
    let topology =
        uncertified_initial_control_plane_topology(config).map_err(|error| error.to_string())?;
    let Some(epoch) = authority
        .establish_uncertified_initial_control_plane_topology(&topology)
        .map_err(|error| error.to_string())?
    else {
        return Ok(());
    };
    process_info!(
        "control-plane bootstrapped {} nodes and {} PG acting sets at epoch {}",
        topology.node_count(),
        topology.pg_count(),
        epoch
    );
    Ok(())
}

fn load_process_authority_clock_restart_checkpoint(
    durable_state_path: &Path,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
) -> Result<
    Option<storage::control_plane::ControlPlaneAuthorityClockRestartCheckpoint>,
    ControlPlaneError,
> {
    match load_authority_clock_restart_checkpoint(durable_state_path, binding) {
        Ok(checkpoint) => Ok(checkpoint),
        Err(error @ ControlPlaneError::AuthorityClockCheckpoint { .. }) => {
            eprintln!(
                "control-plane authority clock checkpoint is invalid; starting non-serving until authenticated recovery: {error}"
            );
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn configured_storage_node_auth_credential_input(
    configured: &ConfiguredControlPlaneStorageAuthCredential,
) -> ControlPlaneStorageNodeAuthCredentialInput {
    ControlPlaneStorageNodeAuthCredentialInput {
        node_id: NodeId::new(configured.node_id),
        credential_id: configured.credential_id.clone(),
        credential_version: configured.credential_version,
        secret: configured.secret.as_bytes().to_vec(),
    }
}

fn configured_frontend_auth_credential_input(
    configured: &ConfiguredControlPlaneFrontendAuthCredential,
) -> ControlPlaneFrontendAuthCredentialInput {
    ControlPlaneFrontendAuthCredentialInput {
        instance_id: configured.instance_id.clone(),
        credential_id: configured.credential_id.clone(),
        credential_version: configured.credential_version,
        secret: configured.secret.as_bytes().to_vec(),
    }
}

fn configured_admin_auth_credential_input(
    configured: &ConfiguredControlPlaneAdminAuthCredential,
) -> ControlPlaneAdminAuthCredentialInput {
    ControlPlaneAdminAuthCredentialInput {
        instance_id: configured.instance_id.clone(),
        credential_id: configured.credential_id.clone(),
        credential_version: configured.credential_version,
        secret: configured.secret.as_bytes().to_vec(),
    }
}

fn build_control_plane_rpc_server_auth(
    config: &ServerConfig,
) -> Result<storage::ControlPlaneRpcServerAuth, String> {
    let storage_credentials = config
        .control_plane_storage_auth_credentials
        .iter()
        .map(configured_storage_node_auth_credential_input)
        .collect();
    let frontend_credentials = config
        .control_plane_frontend_auth_credentials
        .iter()
        .map(configured_frontend_auth_credential_input)
        .collect();
    let admin_credentials = config
        .control_plane_admin_auth_credentials
        .iter()
        .map(configured_admin_auth_credential_input)
        .collect();
    let auth = storage::ControlPlaneRpcServerAuth::new(
        config.control_plane_auth_cluster_id.as_deref(),
        config.control_plane_admin_auth_instance_id.as_deref(),
        storage_credentials,
        frontend_credentials,
        admin_credentials,
    )
    .map_err(|error| error.to_string())?;
    if auth.diagnostics().is_none() && !config.allow_unauthenticated_internal_rpc_for_tests {
        return Err("control-plane listeners require authentication".to_string());
    }
    Ok(auth)
}

fn build_frontend_control_plane_client(
    config: &ServerConfig,
    control_plane_socket_path: &str,
) -> Result<storage::ControlPlaneFrontendClient, String> {
    let credentials = config
        .control_plane_frontend_auth_credentials
        .iter()
        .map(configured_frontend_auth_credential_input)
        .collect();
    if !config.control_plane_rpc_client_endpoints.is_empty() {
        return storage::ControlPlaneFrontendClient::with_endpoints(
            config.control_plane_rpc_client_endpoints.clone(),
            config.control_plane_auth_cluster_id.as_deref(),
            config.control_plane_frontend_auth_instance_id.as_deref(),
            credentials,
            config
                .control_plane_frontend_auth_signing_credential
                .clone(),
        )
        .map_err(|error| error.to_string());
    }
    let socket_paths = if config.control_plane_client_socket_paths.is_empty() {
        vec![PathBuf::from(control_plane_socket_path)]
    } else {
        config
            .control_plane_client_socket_paths
            .iter()
            .map(PathBuf::from)
            .collect()
    };
    storage::ControlPlaneFrontendClient::with_socket_paths(
        socket_paths,
        config.control_plane_auth_cluster_id.as_deref(),
        config.control_plane_frontend_auth_instance_id.as_deref(),
        credentials,
        config
            .control_plane_frontend_auth_signing_credential
            .clone(),
    )
    .map_err(|error| error.to_string())
}

fn build_frontend_control_plane_client_from_runtime_map_auth_env(
    control_plane_socket_path: &Path,
) -> Result<storage::ControlPlaneFrontendClient, String> {
    if static_cluster_command_configured() {
        let config = static_cluster_config::load_server_config_from_environment()
            .map_err(|error| format!("configuration error: {error}"))?;
        let fallback = control_plane_socket_path.to_str().ok_or_else(|| {
            "control-plane command socket path must contain valid UTF-8".to_string()
        })?;
        return build_frontend_control_plane_client(&config, fallback);
    }
    let auth_config = ConfiguredControlPlaneFrontendRuntimeMapAuth::from_env()?;
    if auth_config.is_none() && !unauthenticated_internal_rpc_tests_enabled() {
        return Err(
            "control-plane runtime-map commands require frontend authentication".to_string(),
        );
    }
    let (cluster_id, instance_id, credentials) = match &auth_config {
        Some(auth_config) => (
            Some(auth_config.cluster_id.as_str()),
            Some(auth_config.instance_id.as_str()),
            auth_config
                .credentials
                .iter()
                .map(configured_frontend_auth_credential_input)
                .collect(),
        ),
        None => (None, None, Vec::new()),
    };
    storage::ControlPlaneFrontendClient::with_socket_paths(
        command_control_plane_socket_paths(control_plane_socket_path)?,
        cluster_id,
        instance_id,
        credentials,
        None,
    )
    .map_err(|error| error.to_string())
}

fn build_pg_status_control_plane_client_from_runtime_map_auth_env(
    control_plane_socket_path: &Path,
) -> Result<storage::ControlPlanePgStatusClient, String> {
    let frontend =
        build_frontend_control_plane_client_from_runtime_map_auth_env(control_plane_socket_path)?;
    Ok(storage::ControlPlanePgStatusClient::from_frontend_client(
        &frontend,
    ))
}

fn command_control_plane_socket_paths(primary_socket_path: &Path) -> Result<Vec<PathBuf>, String> {
    let configured = std::env::var("ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS").ok();
    let Some(configured) = configured else {
        return Ok(vec![primary_socket_path.to_path_buf()]);
    };
    let primary_socket_path = primary_socket_path.to_str().ok_or_else(|| {
        "control-plane command socket path must be UTF-8 when ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS is set"
            .to_owned()
    })?;
    config::parse_control_plane_client_socket_paths(Some(configured), Some(primary_socket_path))
        .map(|paths| paths.into_iter().map(PathBuf::from).collect())
}

fn build_admin_control_plane_client_from_command_auth_env(
    control_plane_socket_path: &Path,
) -> Result<storage::ControlPlaneAdminClientBootstrap, String> {
    if static_cluster_command_configured() {
        let config = static_cluster_config::load_server_config_from_environment()
            .map_err(|error| format!("configuration error: {error}"))?;
        return build_admin_control_plane_client_from_config(&config, control_plane_socket_path);
    }
    storage::ControlPlaneAdminClientBootstrap::with_socket_paths(
        command_control_plane_socket_paths(control_plane_socket_path)?,
        build_admin_credential_binding_from_command_auth_env()?,
    )
    .map_err(|error| error.to_string())
}

fn build_pg_admin_control_plane_client_from_command_auth_env(
    control_plane_socket_path: &Path,
) -> Result<storage::ControlPlanePgAdminClient, String> {
    let admin = build_admin_control_plane_client_from_command_auth_env(control_plane_socket_path)?;
    Ok(storage::ControlPlanePgAdminClient::from_bootstrap(&admin))
}

fn build_raft_admin_control_plane_client_from_command_auth_env(
    control_plane_socket_path: &Path,
) -> Result<storage::ControlPlaneRaftAdminClient, String> {
    let admin = build_admin_control_plane_client_from_command_auth_env(control_plane_socket_path)?;
    Ok(storage::ControlPlaneRaftAdminClient::from_bootstrap(&admin))
}

fn build_authority_clock_admin_client_from_command_auth_env(
    control_plane_socket_path: &Path,
) -> Result<storage::ControlPlaneAuthorityClockAdminClient, String> {
    let admin = build_admin_clock_recovery_client_from_command_auth_env(control_plane_socket_path)?;
    storage::ControlPlaneAuthorityClockAdminClient::from_bootstrap(&admin)
        .map_err(|error| error.to_string())
}

fn static_cluster_command_configured() -> bool {
    std::env::var_os("ARGMIN_CLUSTER_CONFIG_PATH").is_some()
        || std::env::var_os("ARGMIN_PROCESS_ID").is_some()
}

fn build_admin_clock_recovery_client_from_command_auth_env(
    control_plane_socket_path: &Path,
) -> Result<storage::ControlPlaneAdminClientBootstrap, String> {
    if static_cluster_command_configured() {
        let config = static_cluster_config::load_server_config_from_environment()
            .map_err(|error| format!("configuration error: {error}"))?;
        return build_admin_clock_recovery_client_from_config(&config, control_plane_socket_path);
    }
    storage::ControlPlaneAdminClientBootstrap::with_derived_clock_recovery_socket_paths(
        command_control_plane_socket_paths(control_plane_socket_path)?,
        build_admin_credential_binding_from_command_auth_env()?,
    )
    .map_err(|error| error.to_string())
}

fn build_admin_clock_recovery_client_from_config(
    config: &ServerConfig,
    fallback_control_plane_socket_path: &Path,
) -> Result<storage::ControlPlaneAdminClientBootstrap, String> {
    let credential = build_admin_credential_binding_from_config(config)?;
    if !config
        .control_plane_clock_recovery_rpc_client_endpoints
        .is_empty()
    {
        return storage::ControlPlaneAdminClientBootstrap::with_endpoints(
            config
                .control_plane_clock_recovery_rpc_client_endpoints
                .clone(),
            credential,
        )
        .map_err(|error| error.to_string());
    }
    storage::ControlPlaneAdminClientBootstrap::with_derived_clock_recovery_socket_paths(
        command_control_plane_socket_paths(fallback_control_plane_socket_path)?,
        credential,
    )
    .map_err(|error| error.to_string())
}

fn build_admin_control_plane_client_from_config(
    config: &ServerConfig,
    fallback_control_plane_socket_path: &Path,
) -> Result<storage::ControlPlaneAdminClientBootstrap, String> {
    let credential = build_admin_credential_binding_from_config(config)?;
    if !config.control_plane_rpc_client_endpoints.is_empty() {
        return storage::ControlPlaneAdminClientBootstrap::with_endpoints(
            config.control_plane_rpc_client_endpoints.clone(),
            credential,
        )
        .map_err(|error| error.to_string());
    }
    let socket_paths = if config.control_plane_client_socket_paths.is_empty() {
        vec![fallback_control_plane_socket_path.to_path_buf()]
    } else {
        config
            .control_plane_client_socket_paths
            .iter()
            .map(PathBuf::from)
            .collect()
    };
    storage::ControlPlaneAdminClientBootstrap::with_socket_paths(socket_paths, credential)
        .map_err(|error| error.to_string())
}

fn build_admin_credential_binding_from_command_auth_env(
) -> Result<storage::ControlPlaneAdminCredentialBinding, String> {
    match ConfiguredControlPlaneAdminCommandAuth::from_env()? {
        Some(auth_config) => build_admin_credential_binding(
            Some(&auth_config.cluster_id),
            Some(&auth_config.instance_id),
            &auth_config.credentials,
        ),
        None if unauthenticated_internal_rpc_tests_enabled() => {
            build_admin_credential_binding(None, None, &[])
        }
        None => Err("control-plane admin commands require authentication".to_string()),
    }
}

const fn unauthenticated_internal_rpc_tests_enabled() -> bool {
    cfg!(any(test, feature = "test-unauthenticated-internal-rpc"))
}

fn build_admin_credential_binding_from_config(
    config: &ServerConfig,
) -> Result<storage::ControlPlaneAdminCredentialBinding, String> {
    build_admin_credential_binding(
        config.control_plane_auth_cluster_id.as_deref(),
        config.control_plane_admin_auth_instance_id.as_deref(),
        &config.control_plane_admin_auth_credentials,
    )
}

fn build_admin_credential_binding(
    cluster_id: Option<&str>,
    instance_id: Option<&str>,
    configured: &[ConfiguredControlPlaneAdminAuthCredential],
) -> Result<storage::ControlPlaneAdminCredentialBinding, String> {
    let credentials = configured
        .iter()
        .map(|credential| ControlPlaneAdminAuthCredentialInput {
            instance_id: credential.instance_id.clone(),
            credential_id: credential.credential_id.clone(),
            credential_version: credential.credential_version,
            secret: credential.secret.as_bytes().to_vec(),
        })
        .collect();
    storage::ControlPlaneAdminCredentialBinding::new(cluster_id, instance_id, credentials)
        .map_err(|error| error.to_string())
}

fn build_storage_node_control_plane_client(
    config: &ServerConfig,
    control_plane_socket_path: &str,
    node_id: NodeId,
    node_incarnation: u64,
) -> Result<storage::ControlPlaneStorageNodeClient, String> {
    let credentials = config
        .control_plane_storage_auth_credentials
        .iter()
        .map(configured_storage_node_auth_credential_input)
        .collect();
    if !config.control_plane_rpc_client_endpoints.is_empty() {
        return storage::ControlPlaneStorageNodeClient::with_endpoints(
            config.control_plane_rpc_client_endpoints.clone(),
            config.control_plane_auth_cluster_id.as_deref(),
            node_id.as_u32(),
            node_incarnation,
            credentials,
            config.control_plane_storage_auth_signing_credential.clone(),
        )
        .map_err(|error| error.to_string());
    }
    let socket_paths = if config.control_plane_client_socket_paths.is_empty() {
        vec![PathBuf::from(control_plane_socket_path)]
    } else {
        config
            .control_plane_client_socket_paths
            .iter()
            .map(PathBuf::from)
            .collect()
    };
    storage::ControlPlaneStorageNodeClient::with_socket_paths(
        socket_paths,
        config.control_plane_auth_cluster_id.as_deref(),
        node_id.as_u32(),
        node_incarnation,
        credentials,
        config.control_plane_storage_auth_signing_credential.clone(),
    )
    .map_err(|error| error.to_string())
}

fn bind_configured_control_plane_rpc_listeners(
    configured: &[ConfiguredControlPlaneRpcListener],
    fallback_socket_path: &Path,
    fallback_config_name: &'static str,
    fallback_worker_limit: usize,
) -> Result<Vec<ControlPlaneRpcServerListenerInput>, String> {
    if configured.is_empty() {
        return Ok(vec![ControlPlaneRpcServerListenerInput::Unix {
            listener: bind_control_plane_unix_socket(fallback_socket_path, fallback_config_name)?,
            max_connections: fallback_worker_limit,
            max_frame_bytes: CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            io_timeout: CONTROL_PLANE_RPC_IO_TIMEOUT,
        }]);
    }
    configured
        .iter()
        .map(|listener| match listener {
            ConfiguredControlPlaneRpcListener::Unix {
                endpoint_id,
                socket_path,
                max_connections,
                max_frame_bytes,
                io_timeout,
            } => Ok(ControlPlaneRpcServerListenerInput::Unix {
                listener: bind_control_plane_unix_socket(
                    Path::new(socket_path),
                    "static control-plane Unix endpoint",
                )
                .map_err(|error| format!("failed to bind endpoint {endpoint_id}: {error}"))?,
                max_connections: *max_connections,
                max_frame_bytes: *max_frame_bytes,
                io_timeout: *io_timeout,
            }),
            ConfiguredControlPlaneRpcListener::Tcp {
                endpoint_id,
                bind_addr,
                certified_key,
                max_connections,
                max_frame_bytes,
                io_timeout,
            } => {
                let listener = StdTcpListener::bind(bind_addr).map_err(|error| {
                    format!(
                        "failed to bind static control-plane TCP endpoint {endpoint_id} at {bind_addr}: {error}"
                    )
                })?;
                listener.set_nonblocking(false).map_err(|error| {
                    format!(
                        "failed to configure static control-plane TCP endpoint {endpoint_id}: {error}"
                    )
                })?;
                Ok(ControlPlaneRpcServerListenerInput::TlsTcp {
                    listener,
                    certified_key: Arc::clone(certified_key),
                    max_connections: *max_connections,
                    max_frame_bytes: *max_frame_bytes,
                    io_timeout: *io_timeout,
                })
            }
        })
        .collect()
}

#[cfg(test)]
fn bind_control_plane_socket(socket_path: &Path) -> Result<UnixListener, String> {
    bind_control_plane_unix_socket(socket_path, "ARGMIN_CONTROL_PLANE_SOCKET_PATH")
}

fn bind_control_plane_raft_peer_socket(socket_path: &Path) -> Result<UnixListener, String> {
    bind_control_plane_unix_socket(socket_path, "ARGMIN_CONTROL_PLANE_RAFT_PEER_SOCKET_PATH")
}

fn bind_control_plane_unix_socket(
    socket_path: &Path,
    config_name: &'static str,
) -> Result<UnixListener, String> {
    if !socket_path.is_absolute() {
        return Err(format!(
            "{config_name} {} must be absolute",
            socket_path.display()
        ));
    }
    let parent = socket_path.parent().ok_or_else(|| {
        format!(
            "{config_name} {} is missing a parent directory",
            socket_path.display()
        )
    })?;
    socket_path.file_name().ok_or_else(|| {
        format!(
            "{config_name} {} is missing a file name",
            socket_path.display()
        )
    })?;
    let parent_existed = parent.exists();
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "failed to create control-plane socket directory {}: {error}",
            parent.display()
        )
    })?;
    if !parent_existed {
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(|error| {
            format!(
                "failed to make control-plane socket directory {} private: {error}",
                parent.display()
            )
        })?;
    }
    validate_control_plane_socket_directory(parent)?;
    cleanup_stale_control_plane_socket(socket_path)?;
    let listener = UnixListener::bind(socket_path).map_err(|error| {
        format!(
            "failed to bind control-plane socket {}: {error}",
            socket_path.display()
        )
    })?;
    listener.set_nonblocking(true).map_err(|error| {
        format!(
            "failed to set control-plane socket {} nonblocking: {error}",
            socket_path.display()
        )
    })?;
    Ok(listener)
}

fn validate_control_plane_socket_directory(parent: &Path) -> Result<(), String> {
    let metadata = fs::metadata(parent).map_err(|error| {
        format!(
            "failed to stat control-plane socket directory {}: {error}",
            parent.display()
        )
    })?;
    let mode = metadata.permissions().mode() & 0o777;
    if !metadata.is_dir() || mode & 0o077 != 0 {
        return Err(format!(
            "control-plane socket directory {} must be private; mode is {mode:o}",
            parent.display()
        ));
    }
    // SAFETY: getuid has no preconditions and does not mutate memory.
    let uid = unsafe { getuid() };
    if metadata.uid() != uid {
        return Err(format!(
            "control-plane socket directory {} must be owned by uid {uid}; owner is {}",
            parent.display(),
            metadata.uid()
        ));
    }
    Ok(())
}

fn cleanup_stale_control_plane_socket(socket_path: &Path) -> Result<(), String> {
    let metadata = match fs::symlink_metadata(socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "failed to stat control-plane socket {}: {error}",
                socket_path.display()
            ));
        }
    };
    if !metadata.file_type().is_socket() {
        return Err(format!(
            "control-plane socket path {} already exists and is not a socket",
            socket_path.display()
        ));
    }
    match UnixStream::connect(socket_path) {
        Ok(_) => Err(format!(
            "control-plane socket path {} is already in use",
            socket_path.display()
        )),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            fs::remove_file(socket_path).map_err(|error| {
                format!(
                    "failed to remove stale control-plane socket {}: {error}",
                    socket_path.display()
                )
            })
        }
        Err(error) => Err(format!(
            "failed to connect existing control-plane socket {}: {error}",
            socket_path.display()
        )),
    }
}

fn run_storage_node_process(config: &ServerConfig, ec_config: &EcConfig) -> ! {
    let bound = bind_storage_node_process(config, ec_config);
    let control_plane_node_incarnation = bound.control_plane_node_incarnation;
    let _static_storage_runtime_lock = bound.static_storage_runtime_lock;
    let server = Arc::new(bound.server);
    let _control_plane_refresh_loop = maybe_spawn_storage_node_control_plane_refresh_loop(
        Arc::clone(&server),
        config,
        control_plane_node_incarnation,
    );
    if let Err(error) = server.serve_forever() {
        eprintln!("storage-node server failed: {error}");
        std::process::exit(1);
    }
    unreachable!("storage-node serve loop should not return successfully")
}

fn start_storage_node_process(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> std::thread::JoinHandle<()> {
    let bound = bind_storage_node_process(config, ec_config);
    let control_plane_node_incarnation = bound.control_plane_node_incarnation;
    let static_storage_runtime_lock = bound.static_storage_runtime_lock;
    let server = Arc::new(bound.server);
    let control_plane_refresh_loop = maybe_spawn_storage_node_control_plane_refresh_loop(
        Arc::clone(&server),
        config,
        control_plane_node_incarnation,
    );
    std::thread::spawn(move || {
        let _static_storage_runtime_lock = static_storage_runtime_lock;
        let _control_plane_refresh_loop = control_plane_refresh_loop;
        if let Err(error) = server.serve_forever() {
            eprintln!("storage-node server failed: {error}");
            std::process::exit(1);
        }
    })
}

struct BoundStorageNodeProcess {
    server: StorageNodeServer,
    control_plane_node_incarnation: Option<u64>,
    static_storage_runtime_lock: Option<static_cluster_state::StaticStorageRuntimeLock>,
}

struct BuiltStorageNodeProcessConfig {
    prepared_server: PreparedStorageNodeServer,
    control_plane_node_incarnation: Option<u64>,
    static_storage_runtime_lock: Option<static_cluster_state::StaticStorageRuntimeLock>,
}

fn bind_storage_node_process(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> BoundStorageNodeProcess {
    let built = build_storage_node_process_config(config, ec_config).unwrap_or_else(|e| {
        eprintln!("storage-node configuration error: {e}");
        std::process::exit(1);
    });
    let prepared_server = built.prepared_server;
    let storage_config = prepared_server.config();
    let node_id = storage_config.node_id();
    let socket_path = storage_config.socket_path().to_path_buf();
    let server = prepared_server.bind().unwrap_or_else(|e| {
        eprintln!("failed to start storage-node server: {e}");
        std::process::exit(1);
    });
    process_info!(
        "argmin-s3 storage-node {} listening on {}",
        node_id.as_u32(),
        socket_path.display()
    );
    BoundStorageNodeProcess {
        server,
        control_plane_node_incarnation: built.control_plane_node_incarnation,
        static_storage_runtime_lock: built.static_storage_runtime_lock,
    }
}

fn maybe_spawn_storage_node_control_plane_refresh_loop(
    server: Arc<StorageNodeServer>,
    config: &ServerConfig,
    initial_node_incarnation: Option<u64>,
) -> Option<StorageNodeControlPlaneRefreshLoop> {
    let socket_path = config.control_plane_socket_path.as_deref()?;
    let node_incarnation = match initial_node_incarnation {
        Some(node_incarnation) => node_incarnation,
        None => server
            .advance_control_plane_node_incarnation()
            .unwrap_or_else(|error| {
                eprintln!("failed to advance storage-node control-plane incarnation: {error}");
                std::process::exit(1);
            }),
    };
    let lease_ms = u64::try_from(config.control_plane_heartbeat_lease_duration.as_millis())
        .unwrap_or_else(|_| {
            eprintln!("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS is too large");
            std::process::exit(1);
        });
    let node_id = NodeId::new(
        config
            .storage_node_id
            .expect("storage node id is required for storage roles"),
    );
    let control_plane_client =
        build_storage_node_control_plane_client(config, socket_path, node_id, node_incarnation)
            .unwrap_or_else(|error| {
                eprintln!("failed to configure storage-node control-plane auth client: {error}");
                std::process::exit(1);
            });
    let loop_handle = server
        .spawn_control_plane_refresh_loop(
            control_plane_client,
            node_incarnation,
            lease_ms,
            storage::clock::current_time_millis,
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to start storage-node control-plane refresh loop: {error}");
            std::process::exit(1);
        });
    process_info!(
        "argmin-s3 storage-node control-plane refresh using {} (incarnation {}, lease-derived jittered renewal capped at {} ms, lease {} ms)",
        socket_path,
        node_incarnation,
        storage::storage_node_server::STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MAX_INTERVAL_MS,
        config.control_plane_heartbeat_lease_duration.as_millis()
    );
    Some(loop_handle)
}

fn build_storage_node_process_config(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<BuiltStorageNodeProcessConfig, String> {
    if let Some(socket_path) = config.control_plane_socket_path.as_deref() {
        return build_control_plane_storage_node_process_config(config, ec_config, socket_path);
    }
    let storage_node_config = standalone_storage_node_process_config(config, ec_config)?;
    let static_storage_runtime_lock = config
        .static_cluster_identity
        .as_ref()
        .map(|identity| {
            static_cluster_state::lock_and_verify_standalone_storage_startup(
                identity,
                storage_node_config.node_id().as_u32(),
                storage_node_config.data_dir(),
                storage_node_config.pg_ids(),
            )
        })
        .transpose()?;
    let mut prepared = PreparedStorageNodeServer::new(storage_node_config);
    match config.storage_rpc_server_auth.clone() {
        Some(rpc_auth) => prepared = prepared.with_rpc_auth(rpc_auth),
        None if config.allow_unauthenticated_internal_rpc_for_tests => {}
        None => {
            return Err("storage-node listeners require authenticated storage RPC".to_string());
        }
    }
    if !config.storage_rpc_listeners.is_empty() {
        prepared = prepared.with_rpc_listeners(config.storage_rpc_listeners.clone());
    }
    Ok(BuiltStorageNodeProcessConfig {
        prepared_server: prepared,
        control_plane_node_incarnation: None,
        static_storage_runtime_lock,
    })
}

fn standalone_storage_node_process_config(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<StorageNodeProcessConfig, String> {
    let cluster_epoch = ClusterEpoch::new(config.storage_cluster_epoch)
        .ok_or_else(|| "ARGMIN_STORAGE_CLUSTER_EPOCH must be > 0".to_string())?;
    let pg_ids = config.storage_pg_ids.clone();
    let node_id = NodeId::new(
        config
            .storage_node_id
            .ok_or_else(|| "ARGMIN_STORAGE_NODE_ID is required for storage roles".to_string())?,
    );
    let node_data_dir = config
        .storage_node_data_dir
        .clone()
        .unwrap_or_else(|| format!("{}/node-{:04}", config.data_dir, node_id.as_u32()));
    let socket_path = config.storage_node_socket_path.clone().ok_or_else(|| {
        "ARGMIN_STORAGE_NODE_SOCKET_PATH is required for storage roles".to_string()
    })?;
    let acting_set: Vec<NodeId> = config
        .storage_node_ids
        .iter()
        .copied()
        .map(NodeId::new)
        .collect();
    let primary_node_id = acting_set
        .first()
        .copied()
        .ok_or_else(|| "storage configuration has no node ids".to_string())?;
    let pg_routes = pg_ids
        .iter()
        .map(|&pg_id| StorageNodePgRoute {
            pg_id,
            cluster_epoch,
            state: PgState::Active,
            primary_node_id,
            metadata_transfer_destination_epoch: None,
            metadata_read_route: None,
            acting_set: acting_set.clone(),
        })
        .collect();
    StorageNodeProcessConfig::new(StorageNodeProcessConfigParts {
        node_id,
        cluster_epoch,
        route_map_validity: RouteMapValidity::Forever,
        data_dir: Path::new(&node_data_dir).to_path_buf(),
        default_ec_shape: EcShape {
            k: ec_config.data_shards(),
            m: ec_config.parity_shards(),
        },
        pg_ids,
        socket_path: Path::new(&socket_path).to_path_buf(),
        pg_routes,
        historical_pg_routes: Vec::new(),
        pending_metadata_command_recoveries: Vec::new(),
    })
    .map_err(|error| error.to_string())
}

fn build_control_plane_storage_node_process_config(
    config: &ServerConfig,
    ec_config: &EcConfig,
    control_plane_socket_path: &str,
) -> Result<BuiltStorageNodeProcessConfig, String> {
    let node_id = NodeId::new(
        config
            .storage_node_id
            .ok_or_else(|| "ARGMIN_STORAGE_NODE_ID is required for storage roles".to_string())?,
    );
    let node_data_dir = config
        .storage_node_data_dir
        .clone()
        .unwrap_or_else(|| format!("{}/node-{:04}", config.data_dir, node_id.as_u32()));
    let configured_socket_path = config.storage_node_socket_path.as_deref().ok_or_else(|| {
        "ARGMIN_STORAGE_NODE_SOCKET_PATH is required for storage roles".to_string()
    })?;
    let node_data_dir_path = Path::new(&node_data_dir);
    let static_storage_runtime_lock = match config.static_cluster_identity.as_ref() {
        Some(identity) => {
            let (runtime_lock, inventory) =
                static_cluster_state::lock_and_inspect_replicated_storage_startup(
                    identity,
                    node_id.as_u32(),
                    node_data_dir_path,
                    &config.storage_pg_ids,
                )?;
            if !inventory.incomplete_payload_pg_ids().is_empty() {
                eprintln!(
                        "argmin-s3 storage node {} found incomplete local payload inventory in PGs {:?}; reads remain available through EC reconstruction and background repair",
                        node_id.as_u32(),
                        inventory.incomplete_payload_pg_ids()
                    );
            }
            Some(runtime_lock)
        }
        None => None,
    };
    let default_ec_shape = EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };
    let bootstrap = StorageNodeBootstrap::open_control_plane_managed(
        node_id,
        node_data_dir_path,
        &config.storage_pg_ids,
        default_ec_shape,
        configured_socket_path,
    )
    .map_err(|error| format!("failed to open storage node for startup heartbeat: {error}"))?;
    let node_incarnation = bootstrap.node_incarnation();
    let lease_ms = u64::try_from(config.control_plane_heartbeat_lease_duration.as_millis())
        .map_err(|_| "ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS is too large".to_string())?;
    let retry_deadline = storage_node_control_plane_startup_retry_deadline(config);
    let retry_delay = storage_node_control_plane_startup_retry_delay(config);
    let started_at = Instant::now();
    let mut attempts = 0_u32;
    let refresh = loop {
        attempts = attempts.saturating_add(1);
        let heartbeat = bootstrap
            .control_plane_heartbeat(lease_ms)
            .map_err(|error| format!("failed to build storage-node startup heartbeat: {error}"))?;
        let mut control_plane = build_storage_node_control_plane_client(
            config,
            control_plane_socket_path,
            node_id,
            node_incarnation,
        )?;
        match control_plane.refresh_node_heartbeat(heartbeat, storage::clock::current_time_millis())
        {
            Ok(refresh) => {
                if attempts > 1 {
                    process_info!(
                        "argmin-s3 storage-node control-plane runtime map became ready after {} attempts",
                        attempts
                    );
                }
                break refresh;
            }
            Err(error)
                if error.is_retryable_heartbeat_startup_error()
                    && started_at.elapsed() < retry_deadline =>
            {
                if attempts == 1 || attempts.is_multiple_of(10) {
                    eprintln!(
                        "argmin-s3 storage-node waiting for control-plane runtime map during startup: {error}"
                    );
                }
                thread::sleep(retry_delay);
            }
            Err(error) => {
                return Err(format!(
                    "failed to refresh control-plane runtime map from {control_plane_socket_path}: {error}"
                ));
            }
        }
    };
    let (_lease, runtime_map) = refresh.into_parts();
    let mut prepared_server = bootstrap
        .prepare(&runtime_map)
        .map_err(|error| error.to_string())?;
    match config.storage_rpc_server_auth.clone() {
        Some(rpc_auth) => prepared_server = prepared_server.with_rpc_auth(rpc_auth),
        None if config.allow_unauthenticated_internal_rpc_for_tests => {}
        None => {
            return Err("storage-node listeners require authenticated storage RPC".to_string());
        }
    }
    if !config.storage_rpc_listeners.is_empty() {
        prepared_server = prepared_server.with_rpc_listeners(config.storage_rpc_listeners.clone());
    }
    Ok(BuiltStorageNodeProcessConfig {
        prepared_server,
        control_plane_node_incarnation: Some(node_incarnation),
        static_storage_runtime_lock,
    })
}

async fn run_all_in_one_frontend(config: ServerConfig, host_id: String, ec_config: EcConfig) {
    let opened_storage_cluster = build_standalone_storage_cluster(&config, &ec_config)
        .unwrap_or_else(|e| {
            eprintln!("failed to open local storage cluster: {e}");
            std::process::exit(1);
        });
    let storage_cluster = opened_storage_cluster.cluster();

    run_frontend_server(
        config,
        host_id,
        FrontendStorageClusters::static_shared(storage_cluster),
        server_core::coordinator::BackgroundWorkerMode::all(),
    )
    .await;
    drop(opened_storage_cluster);
}

struct OpenedStandaloneStorageCluster {
    cluster: Arc<StorageCluster>,
    _static_storage_runtime_lock: Option<static_cluster_state::StaticStorageRuntimeLock>,
    _standalone_route_identity_lock: storage::StandaloneRouteIdentityLock,
}

impl OpenedStandaloneStorageCluster {
    fn cluster(&self) -> Arc<StorageCluster> {
        Arc::clone(&self.cluster)
    }
}

impl std::ops::Deref for OpenedStandaloneStorageCluster {
    type Target = StorageCluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

fn build_standalone_storage_cluster(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<OpenedStandaloneStorageCluster, String> {
    let pg_ids: Vec<u32> = (0..config.pg_count).collect();
    let data_dir = Path::new(&config.data_dir);
    let standalone_route_preparation =
        storage::StandaloneRouteIdentityPreparation::acquire(data_dir)
            .map_err(|error| error.to_string())?;
    let ec_shape = storage::EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };

    let node_ids: Vec<NodeId> = config
        .storage_node_ids
        .iter()
        .copied()
        .map(NodeId::new)
        .collect();
    let metadata_primary_node_id = if node_ids.len() == 1 {
        node_ids[0]
    } else {
        NodeId::new(0)
    };
    let node_configs = if node_ids.len() == 1 {
        let node_id = metadata_primary_node_id;
        let node_data_dir = config
            .storage_node_data_dir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join(format!("node-{:04}", node_id.as_u32())));
        vec![storage::LocalNodeStoreConfig::new(node_id, node_data_dir)]
    } else {
        node_ids
            .iter()
            .map(|node_id| {
                storage::LocalNodeStoreConfig::new(
                    *node_id,
                    data_dir.join(format!("node-{:04}", node_id.as_u32())),
                )
            })
            .collect::<Vec<_>>()
    };
    let cluster_epoch = ClusterEpoch::new(config.storage_cluster_epoch)
        .ok_or_else(|| "configured storage cluster epoch must be > 0".to_string())?;
    let mut static_storage_runtime_lock = None;
    if node_configs.len() == 1 {
        let node_id = metadata_primary_node_id;
        if let Some(identity) = &config.static_cluster_identity {
            static_storage_runtime_lock = Some(
                static_cluster_state::lock_and_verify_standalone_storage_startup(
                    identity,
                    node_id.as_u32(),
                    node_configs[0].data_dir(),
                    &pg_ids,
                )?,
            );
        }
    }
    let prepared_topology = StorageCluster::prepare_standalone_embedded_topology(
        metadata_primary_node_id,
        node_configs,
        &pg_ids,
        ec_shape,
        cluster_epoch,
    )
    .map_err(|error| error.to_string())?;
    let expected_route_identity = prepared_topology.route_identity();
    let standalone_route_identity_lock = standalone_route_preparation
        .bind(expected_route_identity)
        .map_err(|error| error.to_string())?;
    let storage_cluster = prepared_topology
        .open()
        .map_err(|error| error.to_string())?;
    Ok(OpenedStandaloneStorageCluster {
        cluster: storage_cluster,
        _static_storage_runtime_lock: static_storage_runtime_lock,
        _standalone_route_identity_lock: standalone_route_identity_lock,
    })
}

async fn run_remote_frontend(config: ServerConfig, host_id: String, ec_config: EcConfig) {
    let storage_clusters =
        build_remote_frontend_storage_cluster_retrying_startup(&config, &ec_config)
            .await
            .unwrap_or_else(|e| {
                eprintln!("failed to open remote frontend storage cluster: {e}");
                std::process::exit(1);
            });
    run_frontend_server(
        config,
        host_id,
        storage_clusters,
        server_core::coordinator::BackgroundWorkerMode::remote_frontend_phase_10_6(),
    )
    .await;
}

async fn build_remote_frontend_storage_cluster_retrying_startup(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<FrontendStorageClusters, String> {
    if config.control_plane_socket_path.is_none() {
        let foreground = build_remote_frontend_storage_cluster(config, ec_config)?;
        return Ok(FrontendStorageClusters::static_shared(foreground));
    }

    let retry_deadline = frontend_control_plane_startup_retry_deadline(config);
    let retry_delay = frontend_control_plane_startup_retry_delay(config);
    let started_at = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        match build_control_plane_frontend_storage_clusters(config, ec_config) {
            Ok(storage_clusters) => {
                if attempts > 1 {
                    process_info!(
                        "argmin-s3 frontend control-plane runtime map became ready after {} attempts",
                        attempts
                    );
                }
                return Ok(storage_clusters);
            }
            Err(error) if error.is_retryable() && started_at.elapsed() < retry_deadline => {
                if attempts == 1 || attempts.is_multiple_of(10) {
                    eprintln!(
                        "argmin-s3 frontend waiting for control-plane runtime map during startup: {error}"
                    );
                }
                tokio::time::sleep(retry_delay).await;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

struct FrontendStorageClusters {
    authority: FrontendStorageRouteAuthority,
    foreground: Arc<StorageCluster>,
    distinct_maintenance: Option<Arc<StorageCluster>>,
}

#[derive(Clone, Copy)]
enum FrontendStorageRouteAuthority {
    Static,
    Dynamic,
}

impl FrontendStorageClusters {
    fn static_shared(foreground: Arc<StorageCluster>) -> Self {
        Self {
            authority: FrontendStorageRouteAuthority::Static,
            foreground,
            distinct_maintenance: None,
        }
    }

    fn dynamic_shared(foreground: Arc<StorageCluster>) -> Self {
        Self {
            authority: FrontendStorageRouteAuthority::Dynamic,
            foreground,
            distinct_maintenance: None,
        }
    }

    fn dynamic_with_distinct_maintenance(
        foreground: Arc<StorageCluster>,
        maintenance: Arc<StorageCluster>,
    ) -> Self {
        Self {
            authority: FrontendStorageRouteAuthority::Dynamic,
            foreground,
            distinct_maintenance: Some(maintenance),
        }
    }
}

#[derive(Debug)]
enum FrontendControlPlaneStartupError {
    RuntimeMapFetch {
        socket_path: String,
        source: Box<ControlPlaneError>,
    },
    Permanent(String),
}

impl FrontendControlPlaneStartupError {
    fn runtime_map_fetch(socket_path: &str, source: ControlPlaneError) -> Self {
        Self::RuntimeMapFetch {
            socket_path: socket_path.to_owned(),
            source: Box::new(source),
        }
    }

    fn permanent(error: impl ToString) -> Self {
        Self::Permanent(error.to_string())
    }

    fn is_retryable(&self) -> bool {
        match self {
            Self::RuntimeMapFetch { source, .. } => {
                source.is_retryable_runtime_map_observation_error()
            }
            Self::Permanent(_) => false,
        }
    }
}

impl std::fmt::Display for FrontendControlPlaneStartupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RuntimeMapFetch {
                socket_path,
                source,
            } => write!(
                formatter,
                "failed to fetch control-plane runtime map from {socket_path}: {source}"
            ),
            Self::Permanent(message) => formatter.write_str(message),
        }
    }
}

fn frontend_control_plane_startup_retry_deadline(config: &ServerConfig) -> Duration {
    let refresh_budget = config
        .control_plane_frontend_refresh_interval
        .saturating_mul(20);
    let lease_budget = config
        .control_plane_heartbeat_lease_duration
        .saturating_mul(2);
    Duration::from_secs(30)
        .max(refresh_budget)
        .max(lease_budget)
}

fn storage_node_control_plane_startup_retry_deadline(config: &ServerConfig) -> Duration {
    let lease_budget = config
        .control_plane_heartbeat_lease_duration
        .saturating_mul(2);
    Duration::from_secs(30).max(lease_budget)
}

fn frontend_control_plane_startup_retry_delay(config: &ServerConfig) -> Duration {
    config
        .control_plane_frontend_refresh_interval
        .max(Duration::from_millis(50))
        .min(Duration::from_secs(1))
}

fn storage_node_control_plane_startup_retry_delay(config: &ServerConfig) -> Duration {
    let node_id = NodeId::new(config.storage_node_id.unwrap_or_default());
    let lease_ms = u64::try_from(config.control_plane_heartbeat_lease_duration.as_millis())
        .expect("validated control-plane heartbeat lease duration fits u64");
    storage::storage_node_server::storage_node_control_plane_heartbeat_interval(
        node_id, lease_ms, 0,
    )
    .expect("validated control-plane heartbeat lease has a renewal schedule")
    .max(Duration::from_millis(50))
    .min(Duration::from_secs(1))
}

fn build_remote_frontend_storage_cluster(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<Arc<StorageCluster>, String> {
    if config.control_plane_socket_path.is_some() {
        return build_control_plane_frontend_storage_clusters(config, ec_config)
            .map(|clusters| clusters.foreground)
            .map_err(|error| error.to_string());
    }
    let cluster_epoch = ClusterEpoch::new(config.storage_cluster_epoch)
        .ok_or_else(|| "ARGMIN_STORAGE_CLUSTER_EPOCH must be > 0".to_string())?;
    let ec_shape = storage::EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };
    let node_ids: Vec<NodeId> = config
        .storage_node_ids
        .iter()
        .copied()
        .map(NodeId::new)
        .collect();
    let metadata_primary_node_id = node_ids
        .first()
        .copied()
        .ok_or_else(|| "storage configuration has no node ids".to_string())?;
    let mut local_map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        metadata_primary_node_id,
        node_ids,
        &config.storage_pg_ids,
        ec_shape,
        cluster_epoch,
    )
    .map_err(|e| e.to_string())?;
    let frontend_auth = config.storage_rpc_frontend_client_auth.clone();
    if frontend_auth.is_none() && !config.allow_unauthenticated_internal_rpc_for_tests {
        return Err("remote frontend storage RPC requires authentication".to_string());
    }
    local_map
        .install_unix_storage_node_clients({
            let admission_settings = unix_storage_node_client_admission_settings(config);
            config.storage_node_sockets.iter().map(move |entry| {
                let client = LocalUnixStorageNodeClientConfig::with_rpc_admission_settings(
                    NodeId::new(entry.node_id),
                    entry.socket_path.clone(),
                    admission_settings,
                );
                match frontend_auth.clone() {
                    Some(auth) => client.with_frontend_rpc_auth(auth),
                    None => client,
                }
            })
        })
        .map_err(|e| e.to_string())?;
    StorageCluster::from_static_local_map(Arc::new(local_map)).map_err(|e| e.to_string())
}

fn build_control_plane_frontend_storage_clusters(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<FrontendStorageClusters, FrontendControlPlaneStartupError> {
    let socket_path = config.control_plane_socket_path.as_deref().ok_or_else(|| {
        FrontendControlPlaneStartupError::permanent(
            "control-plane frontend cluster requires a control-plane socket",
        )
    })?;
    let control_plane = build_frontend_control_plane_client(config, socket_path)
        .map_err(FrontendControlPlaneStartupError::permanent)?;
    let runtime_map = control_plane
        .runtime_map_snapshot(storage::clock::current_time_millis())
        .map_err(|source| {
            FrontendControlPlaneStartupError::runtime_map_fetch(socket_path, source)
        })?;
    let foreground =
        build_frontend_storage_cluster_from_runtime_map(config, ec_config, &runtime_map)
            .map_err(FrontendControlPlaneStartupError::permanent)?;
    let Some(maintenance) = build_maintenance_storage_cluster_from_runtime_map(
        config,
        ec_config,
        &runtime_map,
        &foreground,
    )
    .map_err(FrontendControlPlaneStartupError::permanent)?
    else {
        return Ok(FrontendStorageClusters::dynamic_shared(foreground));
    };
    Ok(FrontendStorageClusters::dynamic_with_distinct_maintenance(
        foreground,
        maintenance,
    ))
}

fn build_frontend_storage_cluster_from_runtime_map(
    config: &ServerConfig,
    ec_config: &EcConfig,
    runtime_map: &ClusterRuntimeMapSnapshot,
) -> Result<Arc<StorageCluster>, String> {
    let metadata_primary_node_id = runtime_map
        .nodes()
        .first()
        .map(|node| node.node_id())
        .ok_or_else(|| "control-plane runtime map has no routed nodes".to_string())?;
    let ec_shape = EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };
    if !config.storage_rpc_client_endpoints.is_empty() {
        let capability = config
            .storage_rpc_frontend_client_auth
            .clone()
            .ok_or_else(|| {
                "configured storage RPC endpoints require frontend authentication".to_string()
            })?;
        return StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_frontend_auth(
            metadata_primary_node_id,
            runtime_map,
            ec_shape,
            unix_storage_node_client_admission_settings(config),
            config
                .storage_rpc_client_endpoints
                .iter()
                .map(|(node_id, endpoint)| (NodeId::new(*node_id), endpoint.clone())),
            capability,
        )
        .map_err(|error| error.to_string());
    }
    match config.storage_rpc_frontend_client_auth.clone() {
        Some(capability) => StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_frontend_auth(
            metadata_primary_node_id,
            runtime_map,
            ec_shape,
            unix_storage_node_client_admission_settings(config),
            capability,
        ),
        None if config.allow_unauthenticated_internal_rpc_for_tests => {
            StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings(
                metadata_primary_node_id,
                runtime_map,
                ec_shape,
                unix_storage_node_client_admission_settings(config),
            )
        }
        None => return Err("frontend storage RPC requires authentication".to_string()),
    }
    .map_err(|error| error.to_string())
}

fn build_maintenance_storage_cluster_from_runtime_map(
    config: &ServerConfig,
    ec_config: &EcConfig,
    runtime_map: &ClusterRuntimeMapSnapshot,
    foreground: &StorageCluster,
) -> Result<Option<Arc<StorageCluster>>, String> {
    let Some(capability) = config.storage_rpc_maintenance_client_auth.clone() else {
        return Ok(None);
    };
    let metadata_primary_node_id = runtime_map
        .nodes()
        .first()
        .map(|node| node.node_id())
        .ok_or_else(|| "control-plane runtime map has no routed nodes".to_string())?;
    if !config.storage_rpc_client_endpoints.is_empty() {
        return StorageCluster::from_runtime_map_with_storage_rpc_endpoints_and_maintenance_auth_sharing_process_state(
                metadata_primary_node_id,
                runtime_map,
                EcShape {
                    k: ec_config.data_shards(),
                    m: ec_config.parity_shards(),
                },
                unix_storage_node_client_admission_settings(config),
                config
                    .storage_rpc_client_endpoints
                    .iter()
                    .map(|(node_id, endpoint)| (NodeId::new(*node_id), endpoint.clone())),
                capability,
                foreground,
            )
        .map(Some)
        .map_err(|error| error.to_string());
    }
    StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings_and_maintenance_auth_sharing_process_state(
            metadata_primary_node_id,
            runtime_map,
            EcShape {
                k: ec_config.data_shards(),
                m: ec_config.parity_shards(),
            },
            unix_storage_node_client_admission_settings(config),
            capability,
            foreground,
        )
    .map(Some)
    .map_err(|error| error.to_string())
}

fn unix_storage_node_client_admission_settings(
    config: &ServerConfig,
) -> LocalUnixStorageNodeClientAdmissionSettings {
    LocalUnixStorageNodeClientAdmissionSettings {
        rpc_admission_limit: config.storage_node_rpc_admission_limit,
        rpc_admission_wait_timeout: config.storage_node_rpc_admission_wait_timeout,
        rpc_control_admission_wait_timeout: config.storage_node_rpc_control_admission_wait_timeout,
    }
}

async fn run_frontend_server(
    config: ServerConfig,
    host_id: String,
    storage_clusters: FrontendStorageClusters,
    background_worker_mode: server_core::coordinator::BackgroundWorkerMode,
) {
    let sse_c_validator = config
        .sse_c_validator_key_b64
        .as_ref()
        .map(|key| SseCustomerValidatorConfig::from_base64(1, key.as_str()))
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("invalid SSE-C validator key: {e}");
            std::process::exit(1);
        });
    let managed_key_provider =
        ManagedWrappingKeyConfig::from_base64(1, config.sse_s3_wrapping_key_b64.as_str())
            .map(StaticManagedKeyProvider::single)
            .unwrap_or_else(|e| {
                eprintln!("invalid SSE-S3 wrapping key: {e}");
                std::process::exit(1);
            });

    let handles = frontend_route_handles(storage_clusters).unwrap_or_else(|error| {
        eprintln!("failed to establish frontend route authority: {error}");
        std::process::exit(1);
    });
    if handles.foreground_runtime.is_some() != config.control_plane_socket_path.is_some() {
        eprintln!(
            "frontend route authority does not match the configured control-plane startup mode"
        );
        std::process::exit(1);
    }
    let frontend_runtime_map_refresh_loop = maybe_spawn_frontend_control_plane_refresh_loop(
        handles.foreground_runtime.clone(),
        &config,
    );
    let _maintenance_runtime_map_refresh_loop = maybe_spawn_maintenance_control_plane_refresh_loop(
        handles.maintenance_runtime.clone(),
        &config,
    );
    let frontend_runtime_map_refresh_status = frontend_runtime_map_refresh_loop
        .as_ref()
        .map(storage::StorageClusterRuntimeMapRefreshLoop::status_handle);

    // Build frontend pool sharing the same storage cluster and identity-provider handles.
    let identity_provider = auth::IdentityProvider::in_memory(build_credential_store(&config))
        .unwrap_or_else(|error| {
            eprintln!("failed to initialize session-token key ring: {error}");
            std::process::exit(1);
        });
    let mut frontends = Vec::with_capacity(config.workers as usize);
    for _ in 0..config.workers {
        let coordinator =
            Coordinator::new_with_managed_key_provider_for_storage_cluster_route_handles_with_background_worker_mode(
                handles.foreground_route.clone(),
                handles.maintenance_route.clone(),
                config.region.clone(),
                sse_c_validator.clone(),
                managed_key_provider.clone(),
                background_worker_mode,
            );
        let coordinator = match coordinator {
            Ok(c) => c,
            Err(e) => {
                eprintln!("failed to create coordinator: {e}");
                std::process::exit(1);
            }
        };
        frontends.push(HttpFrontend {
            coordinator: Arc::new(coordinator),
            identity_provider: identity_provider.clone(),
            host_id: Arc::<str>::from(host_id.clone()),
        });
    }

    // Bind TCP listener
    let listener = match TcpListener::bind(&config.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind {}: {}", config.listen_addr, e);
            std::process::exit(1);
        }
    };
    let tls_acceptor = match build_tls_acceptor(&config) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("TLS configuration error: {e}");
            std::process::exit(1);
        }
    };
    let scheme = if tls_acceptor.is_some() {
        "https"
    } else {
        "http"
    };
    let effective_max_inflight_requests = if config.process_role.uses_remote_frontend_routing() {
        let storage_rpc_limit =
            u32::try_from(config.storage_node_rpc_admission_limit).unwrap_or(u32::MAX);
        config.max_inflight_requests.min(storage_rpc_limit)
    } else {
        config.max_inflight_requests
    };

    process_info!(
        "argmin-s3 listening on {}://{} (TLS provider {}, EC {},{}, {} PGs, {} workers, max {} conns, max {} in-flight effective {} in-flight, storage RPC admission {} bulk wait {} ms control wait {} ms, read chunk {} bytes, region {}, host id {})",
        scheme,
        config.listen_addr,
        tls_provider::provider_name(),
        config.ec_k,
        config.ec_m,
        config.pg_count,
        config.workers,
        config.max_connections,
        config.max_inflight_requests,
        effective_max_inflight_requests,
        config.storage_node_rpc_admission_limit,
        config.storage_node_rpc_admission_wait_timeout.as_millis(),
        config
            .storage_node_rpc_control_admission_wait_timeout
            .as_millis(),
        config.stream_read_chunk_size,
        config.region,
        host_id
    );

    let serve_config = server_http::http::serve::ServeConfig {
        stream_read_chunk_size: config.stream_read_chunk_size,
        #[cfg(debug_assertions)]
        panic_on_500: config.panic_on_500,
        #[cfg(debug_assertions)]
        abort_on_500: config.abort_on_500,
        #[cfg(feature = "local-debug-endpoints")]
        local_debug_endpoint: config.local_debug_endpoint,
        frontend_runtime_map_refresh_status,
        ..server_http::http::serve::ServeConfig::default()
    };
    match tls_acceptor {
        Some(tls_acceptor) => {
            server_http::http::serve::serve_tls(
                listener,
                tls_acceptor,
                frontends,
                config.max_connections,
                effective_max_inflight_requests,
                serve_config,
            )
            .await;
        }
        None => {
            server_http::http::serve::serve(
                listener,
                frontends,
                config.max_connections,
                effective_max_inflight_requests,
                serve_config,
            )
            .await;
        }
    }
}

struct FrontendRouteHandles {
    foreground_route: StorageClusterRouteHandle,
    maintenance_route: StorageClusterRouteHandle,
    foreground_runtime: Option<StorageClusterRuntimeMapHandle>,
    maintenance_runtime: Option<StorageClusterRuntimeMapHandle>,
}

fn frontend_route_handles(
    storage_clusters: FrontendStorageClusters,
) -> Result<FrontendRouteHandles, storage::StorageClusterRuntimeMapRefreshError> {
    match storage_clusters.authority {
        FrontendStorageRouteAuthority::Dynamic => {
            let foreground_runtime =
                StorageClusterRuntimeMapHandle::new(storage_clusters.foreground)?;
            let maintenance_runtime = storage_clusters
                .distinct_maintenance
                .map(StorageClusterRuntimeMapHandle::new)
                .transpose()?
                .unwrap_or_else(|| foreground_runtime.clone());
            Ok(FrontendRouteHandles {
                foreground_route: foreground_runtime.route_handle(),
                maintenance_route: maintenance_runtime.route_handle(),
                foreground_runtime: Some(foreground_runtime),
                maintenance_runtime: Some(maintenance_runtime),
            })
        }
        FrontendStorageRouteAuthority::Static => {
            let foreground_route =
                StorageClusterRouteHandle::from_static_cluster(storage_clusters.foreground)?;
            let maintenance_route = storage_clusters
                .distinct_maintenance
                .map(StorageClusterRouteHandle::from_static_cluster)
                .transpose()?
                .unwrap_or_else(|| foreground_route.clone());
            Ok(FrontendRouteHandles {
                foreground_route,
                maintenance_route,
                foreground_runtime: None,
                maintenance_runtime: None,
            })
        }
    }
}

fn maybe_spawn_frontend_control_plane_refresh_loop(
    storage_cluster_handle: Option<StorageClusterRuntimeMapHandle>,
    config: &ServerConfig,
) -> Option<storage::StorageClusterRuntimeMapRefreshLoop> {
    let storage_cluster_handle = storage_cluster_handle?;
    let socket_path = config.control_plane_socket_path.as_deref()?;
    let admission_settings = unix_storage_node_client_admission_settings(config);
    let loop_handle = storage_cluster_handle
        .spawn_control_plane_refresh_loop_with_unix_storage_node_clients(
            build_frontend_control_plane_client(config, socket_path).unwrap_or_else(|error| {
                eprintln!("failed to configure frontend control-plane auth client: {error}");
                std::process::exit(1);
            }),
            config.control_plane_frontend_refresh_interval,
            storage::clock::current_time_millis,
            admission_settings,
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to start frontend control-plane runtime-map refresh loop: {error}");
            std::process::exit(1);
        });
    process_info!(
        "argmin-s3 frontend control-plane runtime-map refresh using {} (refresh {} ms)",
        socket_path,
        config.control_plane_frontend_refresh_interval.as_millis(),
    );
    Some(loop_handle)
}

fn maybe_spawn_maintenance_control_plane_refresh_loop(
    storage_cluster_handle: Option<StorageClusterRuntimeMapHandle>,
    config: &ServerConfig,
) -> Option<storage::StorageClusterRuntimeMapRefreshLoop> {
    let storage_cluster_handle = storage_cluster_handle?;
    let socket_path = config.control_plane_socket_path.as_deref()?;
    config.storage_rpc_maintenance_client_auth.as_ref()?;
    let loop_handle = storage_cluster_handle
        .spawn_control_plane_refresh_only_loop_with_unix_storage_node_clients(
            build_frontend_control_plane_client(config, socket_path).unwrap_or_else(|error| {
                eprintln!("failed to configure maintenance control-plane auth client: {error}");
                std::process::exit(1);
            }),
            config.control_plane_frontend_refresh_interval,
            storage::clock::current_time_millis,
            unix_storage_node_client_admission_settings(config),
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to start maintenance runtime-map refresh loop: {error}");
            std::process::exit(1);
        });
    Some(loop_handle)
}

include!("main_tests.rs");
