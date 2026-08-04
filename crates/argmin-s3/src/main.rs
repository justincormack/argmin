mod config;
mod static_cluster_config;
mod static_cluster_state;

use std::ffi::OsString;
use std::fmt::Write as _;
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
#[cfg(test)]
use rustls::sign::CertifiedKey;
use server_core::coordinator::Coordinator;
use server_core::sse::{
    ManagedWrappingKeyConfig, SseCustomerValidatorConfig, StaticManagedKeyProvider,
};
#[cfg(test)]
use storage::control_plane::UnixControlPlaneClient;
use storage::control_plane::{
    ensure_control_plane_state_parent_directory, invalidate_authority_clock_restart_checkpoint,
    load_authority_clock_restart_checkpoint, store_authority_clock_restart_checkpoint,
    ClusterControlSnapshot, ClusterRuntimeMapSnapshot, ControlPlaneAdmin,
    ControlPlaneAdminAuthCredentialInput, ControlPlaneAuthorityClock,
    ControlPlaneAuthorityClockCheckpointBinding, ControlPlaneAuthorityClockCheckpointTarget,
    ControlPlaneAuthorityClockContext, ControlPlaneError, ControlPlaneFrontendAuthCredentialInput,
    ControlPlaneHeartbeatRefresh, ControlPlaneHeartbeatRuntimeMapSource,
    ControlPlaneRpcResponsePublication, ControlPlaneRuntimeMapSource,
    ControlPlaneStorageNodeAuthCredentialInput, FencedPgMetadataTransferSnapshot,
    FileControlPlaneStore, LeaseHorizonAuthorityBinding, PgMetadataTransferProof,
    SingleAuthorityControlPlane, CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
};
#[cfg(test)]
use storage::control_plane_auth::{ControlPlaneAuthOperation, ControlPlaneAuthRejectionReason};
use storage::control_plane_auth::{
    ControlPlaneAuthPrincipal, ControlPlaneScopedCredential, ControlPlaneScopedCredentialInput,
    ControlPlaneScopedCredentialStore,
};
use storage::control_plane_command::{ControlPlaneCommand, ControlPlaneCommandResponse};
use storage::control_plane_raft::{
    ControlPlaneRaftAuthority, ControlPlaneRaftAuthorityStatus, ControlPlaneRaftCommandOutcome,
    ControlPlaneRaftLogId, ControlPlaneRaftNodeId, ControlPlaneRaftPeerAuthPolicy,
    ControlPlaneRaftPeerNetworkConfig, ControlPlaneRaftPeerServerDurability,
    ControlPlaneRaftPeerServerListener, ControlPlaneRaftPeerServerPolicy,
    ControlPlaneRaftPeerTransportPolicy, SubmittedControlPlaneRaftCommand,
};
use storage::storage_node_server::{
    PreparedStorageNodeServer, StorageNodeBootstrap, StorageNodeControlPlaneRefreshLoop,
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts, StorageNodeServer,
};
#[cfg(test)]
use storage::ControlPlaneRpcOrdinaryTestServer;
use storage::{
    CanonicalUserId, ClusterEpoch, ControlPlaneRpcServerBootstrap,
    ControlPlaneRpcServerListenerInput, EcShape, LocalClusterMap,
    LocalUnixStorageNodeClientAdmissionSettings, LocalUnixStorageNodeClientConfig, NodeId, PgId,
    PgState, RouteMapValidity, StorageCluster, StorageClusterRouteHandle,
    StorageClusterRuntimeMapHandle,
};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio_rustls::TlsAcceptor;

use config::{
    ConfiguredControlPlaneAdminAuthCredential, ConfiguredControlPlaneAdminCommandAuth,
    ConfiguredControlPlaneFrontendAuthCredential, ConfiguredControlPlaneFrontendRuntimeMapAuth,
    ConfiguredControlPlaneRaftAuthCredential, ConfiguredControlPlaneRaftPeerListener,
    ConfiguredControlPlaneRpcListener, ConfiguredControlPlaneStorageAuthCredential,
    ConfiguredCredential, ConfiguredCredentialProfile, ConfiguredStaticClusterIdentity,
    ProcessRole, ServerConfig,
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
const CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_WAL_SUFFIX_BYTES: u64 = 64 * 1024 * 1024;
const CONTROL_PLANE_RAFT_PEER_PRE_AUTH_BYTE_BUDGET: usize = 64 * 1024 * 1024;
const CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_MUTATIONS: u64 = 4_096;
const CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_DELAY: Duration = Duration::from_secs(60);
const CONTROL_PLANE_RAFT_PEER_CHECKPOINT_POLL_INTERVAL: Duration = Duration::from_millis(100);
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
    let (Some(cert_path), Some(key_path)) = (&config.tls_cert_path, &config.tls_key_path) else {
        return Ok(None);
    };

    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("failed to build TLS config: {e}"))?;
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Some(TlsAcceptor::from(Arc::new(server_config))))
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
    let _ = rustls::crypto::ring::default_provider().install_default();
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
        return match transfer_control_plane_pg_metadata_live(Path::new(&path), pg_id, acting_set) {
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
                Some(0)
            }
            Err(error) => {
                eprintln!("{error}");
                Some(1)
            }
        };
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

fn configured_live_metadata_transfer_failpoint(
) -> Result<Option<storage::LivePgMetadataTransferFailpoint>, String> {
    let Ok(value) = std::env::var("ARGMIN_METADATA_TRANSFER_FAILPOINT") else {
        return Ok(None);
    };
    match value.as_str() {
        "" => Ok(None),
        "after-fence" => Ok(Some(storage::LivePgMetadataTransferFailpoint::AfterFence)),
        "after-transfer-install" => Ok(Some(
            storage::LivePgMetadataTransferFailpoint::AfterTransferInstall,
        )),
        "after-import" => Ok(Some(storage::LivePgMetadataTransferFailpoint::AfterImport)),
        _ => Err(format!(
            "ARGMIN_METADATA_TRANSFER_FAILPOINT must be after-fence, after-transfer-install, or after-import, got {value:?}"
        )),
    }
}

fn transfer_control_plane_pg_metadata_live(
    socket_path: &Path,
    pg_id: u32,
    acting_set: Vec<u32>,
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
    .with_failpoint(configured_live_metadata_transfer_failpoint()?);
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

// RPC workers clone this wrapper so quorum waits never hold a process-wide
// authority mutex. Every mutable correctness field remains explicitly shared.
#[derive(Clone)]
struct ExperimentalRaftControlPlane {
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    durable_artifact_path: Option<Arc<PathBuf>>,
    durable_checkpoint_lock: Option<Arc<Mutex<()>>>,
    durable_serving_checkpoint: Arc<Mutex<Option<ExperimentalRaftCheckpointMarker>>>,
    checkpoint_serving_reads: bool,
    resample_authority_time: bool,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    #[cfg(test)]
    after_heartbeat_commit_hook: Arc<Mutex<Option<ExperimentalRaftAfterHeartbeatCommitHook>>>,
}

#[cfg(test)]
type ExperimentalRaftAfterHeartbeatCommitHook = Box<
    dyn FnOnce(&ExperimentalRaftControlPlane) -> Result<Option<u64>, ControlPlaneError>
        + Send
        + 'static,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExperimentalRaftCheckpointMarker {
    current_leader: Option<ControlPlaneRaftNodeId>,
    persisted_vote: Option<(u64, ControlPlaneRaftNodeId, bool)>,
    current_term: Option<u64>,
    last_log: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    last_purged: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    committed: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    applied: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    current_snapshot: Option<(u64, ControlPlaneRaftNodeId, u64)>,
}

impl ExperimentalRaftCheckpointMarker {
    fn from_status(status: &ControlPlaneRaftAuthorityStatus) -> Self {
        Self {
            current_leader: status.current_leader(),
            persisted_vote: status
                .persisted_vote()
                .map(|vote| (vote.leader_id.term, vote.leader_id.node_id, vote.committed)),
            current_term: status.current_term(),
            last_log: status.last_log_id().map(|log_id| {
                (
                    log_id.leader_id.term,
                    log_id.leader_id.node_id,
                    log_id.index(),
                )
            }),
            last_purged: status.last_purged_log_id().map(|log_id| {
                (
                    log_id.leader_id.term,
                    log_id.leader_id.node_id,
                    log_id.index(),
                )
            }),
            committed: status.committed().map(|log_id| {
                (
                    log_id.leader_id.term,
                    log_id.leader_id.node_id,
                    log_id.index(),
                )
            }),
            applied: status.applied().map(|log_id| {
                (
                    log_id.leader_id.term,
                    log_id.leader_id.node_id,
                    log_id.index(),
                )
            }),
            current_snapshot: status.current_snapshot().map(|log_id| {
                (
                    log_id.leader_id.term,
                    log_id.leader_id.node_id,
                    log_id.index(),
                )
            }),
        }
    }
}

impl ExperimentalRaftControlPlane {
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        block_on_control_plane_raft(&self.runtime, future)
    }

    #[cfg(test)]
    fn run_after_heartbeat_commit_hook(&self) -> Result<Option<u64>, ControlPlaneError> {
        let Some(hook) = self
            .after_heartbeat_commit_hook
            .lock()
            .expect("experimental OpenRaft heartbeat hook mutex poisoned")
            .take()
        else {
            return Ok(None);
        };
        hook(self)
    }

    fn authority_now_ms(&self, supplied_now_ms: u64) -> Result<u64, ControlPlaneError> {
        Ok(self
            .authority_time_and_lease_horizon_binding(supplied_now_ms)?
            .0)
    }

    fn authority_time_and_lease_horizon_binding(
        &self,
        supplied_now_ms: u64,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        if !self.resample_authority_time {
            #[cfg(test)]
            {
                let status = self.block_on(self.authority.status())?;
                let term = status
                    .local_leader()
                    .then(|| status.current_term())
                    .flatten()
                    .ok_or(ControlPlaneError::AuthorityNotServing)?;
                return Ok((
                    supplied_now_ms,
                    LeaseHorizonAuthorityBinding::checked_new(1, Some(term)),
                ));
            }
            #[cfg(not(test))]
            return Ok((supplied_now_ms, None));
        }
        let status = self.block_on(self.authority.confirmed_linearized_authority_status())?;
        self.authority_time_and_lease_horizon_binding_for_status(status)
    }

    fn local_authority_time_and_lease_horizon_binding(
        &self,
        supplied_now_ms: u64,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        if !self.resample_authority_time {
            return self.authority_time_and_lease_horizon_binding(supplied_now_ms);
        }
        let status = self.block_on(self.authority.status())?;
        if !status.linearized_authority_serving() {
            return Err(ControlPlaneError::AuthorityNotServing);
        }
        self.authority_time_and_lease_horizon_binding_for_status(status)
    }

    fn authority_time_and_lease_horizon_binding_for_status(
        &self,
        status: ControlPlaneRaftAuthorityStatus,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        let current_term = status.current_term().ok_or_else(|| {
            ControlPlaneError::invariant_failure("local OpenRaft leader has no current term")
        })?;
        self.authority_time_and_lease_horizon_binding_for_term(current_term)
    }

    fn authority_time_and_lease_horizon_binding_for_term(
        &self,
        current_term: u64,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        let max_committed_timestamp_ms = self.current_snapshot()?.max_committed_timestamp_ms();
        let mut authority_clock = self
            .authority_clock
            .as_ref()
            .expect("resampled authority time requires a clock gate")
            .lock()
            .expect("control-plane authority clock mutex poisoned");
        authority_clock.observe_committed_timestamp_high_water(max_committed_timestamp_ms);
        authority_clock.validate_raft_leadership_term(current_term)?;
        let authority_now_ms = authority_clock.effective_process_now_ms()?;
        let authority = authority_clock.lease_horizon_authority_binding(Some(current_term))?;
        Ok((authority_now_ms, Some(authority)))
    }

    fn ensure_not_durably_poisoned(&self) -> Result<(), ControlPlaneError> {
        self.authority.durability_publication()?.ensure_available()
    }

    fn poison_durable_authority(&self, message: String) {
        self.authority
            .durability_publication()
            .expect("Raft durability publication should already be initialized")
            .poison(message);
    }

    fn store_durable_restart_artifact(&self) -> Result<(), ControlPlaneError> {
        let Some(path) = &self.durable_artifact_path else {
            return Ok(());
        };
        store_experimental_raft_durable_restart_artifact(
            &self.runtime,
            &self.authority,
            path,
            self.durable_checkpoint_lock.as_ref(),
        )
    }

    fn checkpoint_successful_linearized_read(&self) -> Result<(), ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        if !self.checkpoint_serving_reads {
            return Ok(());
        }
        if self.authority.durability_metric_snapshots().wal.is_some() {
            let wal_status = self.authority.durable_wal_monitor_snapshot()?;
            if let Some(reason) = wal_status.poisoned() {
                return Err(ControlPlaneError::durability_failure(format!(
                    "durable OpenRaft serving read observed poisoned WAL state: {reason}"
                )));
            }
            return Ok(());
        }

        let status = self.block_on(self.authority.status()).ok();
        let marker = status
            .as_ref()
            .filter(|status| status.linearized_authority_serving())
            .map(ExperimentalRaftCheckpointMarker::from_status);
        if let Some(marker) = marker {
            {
                let checkpointed = self
                    .durable_serving_checkpoint
                    .lock()
                    .expect("experimental OpenRaft serving checkpoint mutex poisoned");
                if *checkpointed == Some(marker) {
                    return Ok(());
                }
            }
            self.store_durable_restart_artifact()?;
            *self
                .durable_serving_checkpoint
                .lock()
                .expect("experimental OpenRaft serving checkpoint mutex poisoned") = Some(marker);
            return Ok(());
        }

        self.store_durable_restart_artifact()?;
        *self
            .durable_serving_checkpoint
            .lock()
            .expect("experimental OpenRaft serving checkpoint mutex poisoned") = None;
        Ok(())
    }

    fn submit_raft_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        self.submit_raft_command_with_checkpoint_policy(command, true)
    }

    fn submit_raft_liveness_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        let wal_backed = self.authority.durability_metric_snapshots().wal.is_some();
        self.submit_raft_command_with_checkpoint_policy(command, !wal_backed)
    }

    fn submit_raft_command_with_checkpoint_policy(
        &mut self,
        command: ControlPlaneCommand,
        checkpoint_after_commit: bool,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let submitted = self.block_on(self.authority.submit_control_plane_command(command))?;
        self.finish_submitted_raft_command(submitted, checkpoint_after_commit)
    }

    fn finish_submitted_raft_command(
        &mut self,
        submitted: SubmittedControlPlaneRaftCommand,
        checkpoint_after_commit: bool,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        let outcome = submitted.into_outcome();
        if checkpoint_after_commit {
            self.checkpoint_committed_raft_command()?;
        }
        match outcome {
            ControlPlaneRaftCommandOutcome::Applied(response) => Ok(response),
            ControlPlaneRaftCommandOutcome::Rejected(error) => Err(error),
        }
    }

    fn checkpoint_committed_raft_command(&self) -> Result<(), ControlPlaneError> {
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "experimental OpenRaft control-plane durability checkpoint failed after a \
                 committed command; refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        Ok(())
    }

    fn current_snapshot(&self) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(self.authority.current_control_plane_snapshot())
    }

    fn expire_heartbeat_leases(
        &mut self,
        now_ms: u64,
    ) -> Result<(ClusterEpoch, usize, usize), ControlPlaneError> {
        let (preflight_now_ms, preflight_authority) =
            self.local_authority_time_and_lease_horizon_binding(now_ms)?;
        let preflight_snapshot = self.current_snapshot()?;
        let preflight_expired = preflight_snapshot.expired_node_heartbeat_leases(
            preflight_snapshot.heartbeat_lease_expiry_timestamp(preflight_now_ms),
        );
        if preflight_expired.is_empty() {
            return Ok((preflight_snapshot.cluster_epoch(), 0, 0));
        }
        let preflight_authority = preflight_authority.ok_or_else(|| {
            ControlPlaneError::invariant_failure(
                "OpenRaft heartbeat expiry has no serving lease-horizon authority",
            )
        })?;
        preflight_snapshot
            .validate_lease_grant_horizon_rebinding(preflight_authority, preflight_now_ms)?;

        let (now_ms, lease_horizon_authority) =
            self.authority_time_and_lease_horizon_binding(now_ms)?;
        let snapshot = self.current_snapshot()?;
        let expire_at_ms = snapshot.heartbeat_lease_expiry_timestamp(now_ms);
        let expired = snapshot.expired_node_heartbeat_leases(expire_at_ms);
        if expired.is_empty() {
            return Ok((snapshot.cluster_epoch(), 0, 0));
        }
        let authority = lease_horizon_authority.ok_or_else(|| {
            ControlPlaneError::invariant_failure(
                "OpenRaft heartbeat expiry has no serving lease-horizon authority",
            )
        })?;
        snapshot.validate_lease_grant_horizon_rebinding(authority, now_ms)?;
        let response =
            self.submit_raft_liveness_command(ControlPlaneCommand::ExpireNodeHeartbeatLeases {
                authority,
                expire_at_ms,
                expired,
            })?;
        let ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes,
            peering_pgs,
        } = response
        else {
            unreachable!("heartbeat lease expiry command returned the wrong response");
        };
        Ok((
            self.current_snapshot()?.cluster_epoch(),
            expired_nodes.len(),
            peering_pgs.len(),
        ))
    }
}

impl ControlPlaneRuntimeMapSource for ExperimentalRaftControlPlane {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let snapshot = self.block_on(
            self.authority
                .linearized_runtime_map_snapshot(authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(snapshot)
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<storage::control_plane::ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let status = self.block_on(
            self.authority
                .linearized_runtime_map_status(authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(status)
    }

    fn runtime_map_diagnostics_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<storage::control_plane::ControlPlaneRuntimeMapDiagnosticSnapshot, ControlPlaneError>
    {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let diagnostics = self.block_on(
            self.authority
                .linearized_runtime_map_diagnostics_snapshot(authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(diagnostics)
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let snapshot = self.block_on(
            self.authority
                .linearized_serving_pg_runtime_map_snapshot(pg_id, authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(snapshot)
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for ExperimentalRaftControlPlane {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: storage::control_plane::NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        let (authority_now_ms, lease_horizon_authority) =
            self.authority_time_and_lease_horizon_binding(authority_now_ms)?;
        let node_id = heartbeat.node_id;
        let requested_observed_epoch = heartbeat.observed_epoch;
        let requested_lease_duration_ms = heartbeat.requested_lease_duration_ms;
        let pre_record_snapshot = self.current_snapshot()?;
        let previous_observed_epoch = pre_record_snapshot
            .node(node_id)
            .and_then(|node| node.last_observed_epoch());
        let carries_peering_evidence = heartbeat
            .pg_observations
            .iter()
            .any(|observation| observation.state == PgState::Peering);
        let lease_deadline_ms = pre_record_snapshot.heartbeat_lease_deadline(
            node_id,
            authority_now_ms,
            requested_lease_duration_ms,
        )?;
        let pre_record_epoch = pre_record_snapshot.cluster_epoch();
        let command = ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: authority_now_ms,
            lease_deadline_ms,
            lease_horizon_authority,
        };
        let mut volatile_snapshot = if carries_peering_evidence {
            None
        } else {
            self.block_on(self.authority.try_apply_volatile_heartbeat(command.clone()))?
        };
        if volatile_snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .ready_pg_peering_completions(authority_now_ms)
                .is_ok_and(|ready| !ready.is_empty())
        }) {
            volatile_snapshot = None;
        }
        if volatile_snapshot.is_none() {
            self.submit_raft_liveness_command(command)?;
        }
        #[cfg(test)]
        let post_commit_term_override = if volatile_snapshot.is_none() {
            self.run_after_heartbeat_commit_hook()?
        } else {
            None
        };
        let snapshot = match volatile_snapshot {
            Some(snapshot) => snapshot,
            None => self.current_snapshot()?,
        };
        if let Some(expected_authority) = lease_horizon_authority {
            #[cfg(test)]
            let current_authority = match post_commit_term_override {
                Some(current_term) => {
                    self.authority_time_and_lease_horizon_binding_for_term(current_term)?
                        .1
                }
                None => {
                    self.authority_time_and_lease_horizon_binding(authority_now_ms)?
                        .1
                }
            };
            #[cfg(not(test))]
            let current_authority = self
                .authority_time_and_lease_horizon_binding(authority_now_ms)?
                .1;
            if current_authority != Some(expected_authority) {
                return Err(ControlPlaneError::LeaseGrantHorizonAuthorityTermMismatch {
                    authority_term: expected_authority.raft_term(),
                    committed_term: current_authority.and_then(|authority| authority.raft_term()),
                });
            }
            if !snapshot.lease_grant_horizon_covers(expected_authority, lease_deadline_ms) {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "committed Raft heartbeat horizon does not cover its lease",
                    message: format!(
                        "lease deadline {lease_deadline_ms} is outside the committed horizon"
                    ),
                });
            }
        }
        let mut lease = snapshot.heartbeat_lease_after_record(
            node_id,
            requested_observed_epoch,
            pre_record_epoch,
            lease_deadline_ms,
            authority_now_ms,
        )?;
        let ready = snapshot.ready_pg_peering_completions(authority_now_ms)?;
        if !ready.is_empty() {
            self.submit_raft_liveness_command(ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: authority_now_ms,
                ready,
            })?;
            lease = self
                .current_snapshot()?
                .current_heartbeat_lease_for_node(node_id, authority_now_ms)?;
        }
        let current_snapshot = self.current_snapshot()?;
        let current_epoch = current_snapshot.cluster_epoch();
        let observed_epoch = [Some(requested_observed_epoch), previous_observed_epoch]
            .into_iter()
            .flatten()
            .filter(|observed_epoch| *observed_epoch <= current_epoch)
            .max()
            .unwrap_or(requested_observed_epoch);
        let runtime_map = self
            .current_snapshot()?
            .runtime_map_for_storage_node_refresh(authority_now_ms, node_id, observed_epoch)?;
        Ok(ControlPlaneHeartbeatRefresh::new(
            lease,
            runtime_map,
            pre_record_epoch,
        ))
    }
}

impl ControlPlaneAdmin for ExperimentalRaftControlPlane {
    fn authority_clock_context(
        &self,
    ) -> Result<ControlPlaneAuthorityClockContext, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(self.authority.authority_clock_context())
    }

    fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command(ControlPlaneCommand::SetPgActingSet { pg_id, acting_set })?;
        self.current_snapshot()
    }

    fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_with_source_lease(pg_id)?
            .into_parts()
            .0)
    }

    fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        let response =
            self.submit_raft_command(ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id,
                source_primary_lease_deadline_ms: None,
                lease_horizon_authority: None,
            })?;
        let ControlPlaneCommandResponse::FencePgForMetadataTransfer {
            source_primary_lease_deadline_ms,
        } = response
        else {
            unreachable!("metadata transfer fence command returned the wrong response");
        };
        Ok(FencedPgMetadataTransferSnapshot::new(
            self.current_snapshot()?,
            source_primary_lease_deadline_ms,
        ))
    }

    fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command(ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
        })?;
        self.current_snapshot()
    }

    fn transfer_raft_leadership_to(
        &mut self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(self.authority.transfer_leadership_to(node_id))
    }

    fn trigger_raft_snapshot_and_purge(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let snapshot_log_id = self.block_on(self.authority.trigger_snapshot_applied())?;
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "experimental OpenRaft control-plane durability checkpoint failed before a \
                 snapshot purge; refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        let Some(snapshot_log_id) = snapshot_log_id else {
            return Ok(None);
        };
        let purge_result =
            self.block_on(self.authority.purge_log_through_snapshot(snapshot_log_id));
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "experimental OpenRaft control-plane durability checkpoint failed after a \
                 snapshot purge attempt; refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        purge_result?;
        Ok(Some(snapshot_log_id.index()))
    }

    fn trigger_raft_election(&mut self) -> Result<(), ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(
            self.authority
                .trigger_pre_vote_election_until_serving(Duration::from_secs(10)),
        )?;
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "experimental OpenRaft control-plane durability checkpoint failed after an \
                 election trigger; refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        Ok(())
    }
}

fn block_on_control_plane_raft<F: Future>(runtime: &Handle, future: F) -> F::Output {
    if Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(future))
    } else {
        runtime.block_on(future)
    }
}

async fn wait_for_experimental_raft_startup_catch_up(
    authority: &ControlPlaneRaftAuthority,
    timeout: Duration,
    message: &'static str,
) -> Result<(), ControlPlaneError> {
    wait_for_experimental_raft_startup_catch_up_from(authority, timeout, message).await
}

trait ExperimentalRaftStartupCatchUpSource {
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

impl ExperimentalRaftStartupCatchUpSource for ControlPlaneRaftAuthority {
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

async fn wait_for_experimental_raft_startup_catch_up_from<S>(
    source: &S,
    timeout: Duration,
    message: &'static str,
) -> Result<(), ControlPlaneError>
where
    S: ExperimentalRaftStartupCatchUpSource,
{
    let deadline = Instant::now() + timeout;
    loop {
        let (committed, applied) = source.committed_and_applied().await?;
        let Some(committed) = committed else {
            return Ok(());
        };
        if applied == Some(committed) {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(ControlPlaneError::startup_timeout(format!(
                "OpenRaft startup did not apply through committed state within {timeout:?}: \
                 {message}"
            )));
        }
        source
            .wait_for_applied(committed, deadline.saturating_duration_since(now), message)
            .await?;
    }
}

fn build_experimental_raft_peer_transport_policy(
    config: &ServerConfig,
    cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<Option<ControlPlaneRaftPeerTransportPolicy>, String> {
    if config.control_plane_raft_peer_socket_path.is_none()
        && config.control_plane_raft_peer_listeners.is_empty()
    {
        return Ok(None);
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
    let mut policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
        cluster_name.to_string(),
        peer_endpoints,
        config.control_plane_raft_peer_transport_limits,
    )
    .with_timeouts(
        config.control_plane_raft_peer_connect_timeout,
        config.control_plane_raft_peer_io_timeout,
    );
    if let Some(initial) = &config.static_initial_cluster_map {
        policy = policy.with_static_initial_topology(initial);
    } else if let Some(identity) = &config.static_cluster_identity {
        policy = policy.with_topology_identity(
            identity.topology_generation,
            identity.topology_digest.clone(),
        );
    }
    if let Some(auth_policy) =
        build_experimental_raft_peer_auth_policy(config, cluster_name, local_node_id)?
    {
        policy = policy.with_auth_policy(auth_policy);
    }
    Ok(Some(policy))
}

fn build_experimental_raft_peer_auth_policy(
    config: &ServerConfig,
    cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<Option<ControlPlaneRaftPeerAuthPolicy>, String> {
    if config.control_plane_raft_auth_credentials.is_empty() {
        return Ok(None);
    }

    let mut local_candidates = config
        .control_plane_raft_auth_credentials
        .iter()
        .filter(|credential| credential.node_id == local_node_id);
    let local_configured = match &config.control_plane_raft_auth_signing_credential {
        Some((credential_id, credential_version)) => local_candidates.find(|credential| {
                credential.credential_id == *credential_id
                    && credential.credential_version == *credential_version
            }),
        None => latest_auth_credential_by_version_then_id(
            local_candidates,
            |credential| credential.credential_id.as_str(),
            |credential| credential.credential_version,
        ),
    }
    .ok_or_else(|| {
        format!(
            "ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS must include local Raft node id {local_node_id}"
        )
    })?;
    let local_credential = configured_raft_peer_auth_credential(local_configured, cluster_name)?;

    let mut credentials = Vec::new();
    for configured in &config.control_plane_raft_auth_credentials {
        credentials.push(configured_raft_peer_auth_credential(
            configured,
            cluster_name,
        )?);
    }

    let verifier = ControlPlaneScopedCredentialStore::new(credentials).map_err(|error| {
        format!("invalid ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS credential set: {error}")
    })?;
    ControlPlaneRaftPeerAuthPolicy::new(local_credential, verifier)
        .map(Some)
        .map_err(|error| {
            format!("invalid ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS auth policy: {error}")
        })
}

fn configured_raft_peer_auth_credential(
    configured: &ConfiguredControlPlaneRaftAuthCredential,
    cluster_name: &str,
) -> Result<ControlPlaneScopedCredential, String> {
    ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
        cluster_id: cluster_name.to_string(),
        credential_id: configured.credential_id.clone(),
        credential_version: configured.credential_version,
        principal: ControlPlaneAuthPrincipal::RaftPeer {
            node_id: configured.node_id,
        },
        secret: configured.secret.as_bytes().to_vec(),
    })
    .map_err(|error| {
        format!(
            "invalid ARGMIN_CONTROL_PLANE_RAFT_AUTH_CREDENTIALS scoped credential for node {}: {error}",
            configured.node_id
        )
    })
}

fn format_experimental_raft_peer_auth_diagnostics(
    policy: &ControlPlaneRaftPeerTransportPolicy,
) -> String {
    let status = policy.auth_status_snapshot();
    let metrics = status.metrics();
    let local_principal = status
        .local_principal()
        .map_or_else(|| "-".to_string(), |principal| format!("{principal:?}"));
    let credential_id = status.credential_id().unwrap_or("-");
    let credential_version = status
        .credential_version()
        .map_or_else(|| "-".to_string(), |version| version.to_string());
    let mut diagnostics = format!(
        "raft_peer_auth required={} local_principal={} credential_id={} credential_version={} accepted_total={} rejected_total={} rejected_without_operation_total={}",
        status.required(),
        local_principal,
        credential_id,
        credential_version,
        metrics.accepted_total(),
        metrics.rejected_total(),
        metrics.rejected_without_operation_total()
    );
    for (operation, count) in metrics.accepted_by_operation() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "raft_peer_auth accepted_by_operation{{operation=\"{operation:?}\"}} {count}"
        )
        .expect("write to String should not fail");
    }
    for (operation, count) in metrics.rejected_by_operation() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "raft_peer_auth rejected_by_operation{{operation=\"{operation:?}\"}} {count}"
        )
        .expect("write to String should not fail");
    }
    for (reason, count) in metrics.rejected_by_reason() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "raft_peer_auth rejected_by_reason{{reason=\"{reason:?}\"}} {count}"
        )
        .expect("write to String should not fail");
    }
    diagnostics
}

fn experimental_raft_startup_requires_local_leader(
    peer_policy: Option<&ControlPlaneRaftPeerTransportPolicy>,
) -> bool {
    match peer_policy {
        Some(policy) => policy.peers().len() == 1,
        None => true,
    }
}

fn experimental_raft_startup_initializes_membership(
    peer_policy: Option<&ControlPlaneRaftPeerTransportPolicy>,
    local_node_id: ControlPlaneRaftNodeId,
) -> bool {
    match peer_policy {
        Some(policy) if policy.peers().len() > 1 => {
            policy.peers().keys().next().copied() == Some(local_node_id)
        }
        _ => true,
    }
}

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
) -> Result<Vec<ControlPlaneRaftPeerServerListener>, String> {
    let policy =
        build_experimental_raft_peer_transport_policy(config, cluster_name, local_node_id)?;
    bind_experimental_raft_peer_listener_with_policy(config, policy)
}

fn bind_experimental_raft_peer_listener_with_policy(
    config: &ServerConfig,
    policy: Option<ControlPlaneRaftPeerTransportPolicy>,
) -> Result<Vec<ControlPlaneRaftPeerServerListener>, String> {
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
    let Some(_policy) = policy else {
        return Err(
            "configured OpenRaft peer listeners are missing peer transport policy".to_string(),
        );
    };
    configured_listeners
        .into_iter()
        .map(|listener| match listener {
            ConfiguredControlPlaneRaftPeerListener::Unix {
                endpoint_id,
                socket_path,
                max_connections,
                io_timeout,
            } => ControlPlaneRaftPeerServerListener::unix(
                endpoint_id,
                bind_control_plane_raft_peer_socket(Path::new(&socket_path))?,
                max_connections,
                io_timeout,
            )
            .map_err(|error| error.to_string()),
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
                ControlPlaneRaftPeerServerListener::tls_tcp(
                    endpoint_id,
                    listener,
                    certified_key,
                    max_connections,
                    io_timeout,
                )
                .map_err(|error| error.to_string())
            }
        })
        .collect()
}

#[derive(Clone)]
struct ExperimentalRaftPeerDurabilityContext {
    artifact_path: Option<Arc<PathBuf>>,
    checkpoint_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct ExperimentalRaftPeerServerDurability {
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    context: ExperimentalRaftPeerDurabilityContext,
}

impl ControlPlaneRaftPeerServerDurability for ExperimentalRaftPeerServerDurability {
    fn is_poisoned(&self) -> bool {
        self.authority
            .durability_publication()
            .expect("Raft durability publication should already be initialized")
            .is_poisoned()
    }

    fn checkpoint_before_snapshot_response(&self) -> Result<(), ControlPlaneError> {
        let path = self
            .context
            .artifact_path
            .as_deref()
            .ok_or_else(|| {
                ControlPlaneError::durability_failure(
                    "experimental OpenRaft snapshot peer RPC requires durable checkpoint path before response",
                )
            })?;
        let result = store_experimental_raft_durable_restart_artifact(
            &self.runtime,
            &self.authority,
            path,
            Some(&self.context.checkpoint_lock),
        );
        if result.is_err() {
            self.authority
                .durability_publication()
                .expect("Raft durability publication should already be initialized")
                .poison("control-plane Raft snapshot response checkpoint publication failed");
        }
        result
    }

    fn publish_response(
        &self,
        publish: &mut dyn FnMut() -> Result<(), ControlPlaneError>,
    ) -> Result<(), ControlPlaneError> {
        let publication = self.authority.durability_publication()?;
        ControlPlaneRpcResponsePublication::publish(&publication, publish)
    }
}

#[derive(Clone, Copy, Debug)]
struct ExperimentalRaftPeerCheckpointPolicy {
    max_wal_suffix_bytes: u64,
    max_mutations: u64,
    max_delay: Duration,
    poll_interval: Duration,
}

impl Default for ExperimentalRaftPeerCheckpointPolicy {
    fn default() -> Self {
        Self {
            max_wal_suffix_bytes: CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_WAL_SUFFIX_BYTES,
            max_mutations: CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_MUTATIONS,
            max_delay: CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_DELAY,
            poll_interval: CONTROL_PLANE_RAFT_PEER_CHECKPOINT_POLL_INTERVAL,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ExperimentalRaftPeerCheckpointObservation {
    wal_suffix_bytes: u64,
    successful_append_total: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExperimentalRaftPeerCheckpointWork {
    pending_mutations: u64,
    elapsed: Duration,
    wal_suffix_bytes: u64,
}

#[derive(Debug)]
struct ExperimentalRaftPeerCheckpointTracker {
    policy: ExperimentalRaftPeerCheckpointPolicy,
    checkpoint_append_total: u64,
    first_pending_observed_at: Option<Instant>,
}

impl ExperimentalRaftPeerCheckpointTracker {
    fn new(policy: ExperimentalRaftPeerCheckpointPolicy) -> Self {
        assert!(policy.max_wal_suffix_bytes > 0);
        assert!(policy.max_mutations > 0);
        assert!(!policy.max_delay.is_zero());
        assert!(!policy.poll_interval.is_zero());
        assert!(policy.poll_interval < policy.max_delay);
        Self {
            policy,
            checkpoint_append_total: 0,
            first_pending_observed_at: None,
        }
    }

    fn observe(
        &mut self,
        now: Instant,
        observation: ExperimentalRaftPeerCheckpointObservation,
    ) -> Option<ExperimentalRaftPeerCheckpointWork> {
        if observation.wal_suffix_bytes == 0 {
            self.checkpoint_append_total = observation.successful_append_total;
            self.first_pending_observed_at = None;
            return None;
        }
        let first_pending_at = self.first_pending_observed_at.get_or_insert(now);
        let work = ExperimentalRaftPeerCheckpointWork {
            pending_mutations: observation
                .successful_append_total
                .saturating_sub(self.checkpoint_append_total),
            elapsed: now.saturating_duration_since(*first_pending_at),
            wal_suffix_bytes: observation.wal_suffix_bytes,
        };
        (work.wal_suffix_bytes >= self.policy.max_wal_suffix_bytes
            || work.pending_mutations >= self.policy.max_mutations
            || work.elapsed
                >= self
                    .policy
                    .max_delay
                    .saturating_sub(self.policy.poll_interval))
        .then_some(work)
    }

    fn complete_checkpoint(&mut self, successful_append_total: u64) {
        self.checkpoint_append_total = successful_append_total;
        self.first_pending_observed_at = None;
    }
}

impl Default for ExperimentalRaftPeerCheckpointTracker {
    fn default() -> Self {
        Self::new(ExperimentalRaftPeerCheckpointPolicy::default())
    }
}

fn checkpoint_experimental_raft_peer_wal_if_due(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    durability: &ExperimentalRaftPeerDurabilityContext,
    tracker: &mut ExperimentalRaftPeerCheckpointTracker,
    now: Instant,
) -> Result<bool, ControlPlaneError> {
    let wal_status = authority.durable_wal_monitor_snapshot()?;
    if let Some(reason) = wal_status.poisoned() {
        return Err(ControlPlaneError::durability_failure(format!(
            "durable OpenRaft peer checkpoint scheduler observed poisoned WAL state: {reason}"
        )));
    }
    let wal_metrics = wal_status.metrics();
    let successful_append_total = wal_metrics
        .append_total
        .saturating_sub(wal_metrics.append_error_total);
    let offsets = wal_status.offsets();
    let wal_suffix_bytes = offsets
        .clean_len()
        .checked_sub(offsets.base_offset())
        .ok_or_else(|| {
            ControlPlaneError::invariant_failure(format!(
                "durable OpenRaft WAL clean offset {} precedes base offset {}",
                offsets.clean_len(),
                offsets.base_offset()
            ))
        })?;
    let observation = ExperimentalRaftPeerCheckpointObservation {
        wal_suffix_bytes,
        successful_append_total,
    };
    if tracker.observe(now, observation).is_none() {
        return Ok(false);
    }
    let path = durability.artifact_path.as_deref().ok_or_else(|| {
        ControlPlaneError::durability_failure(
            "durable OpenRaft peer checkpoint scheduler requires an artifact path",
        )
    })?;
    snapshot_purge_and_checkpoint_experimental_raft_peer_wal(
        runtime,
        authority,
        path,
        &durability.checkpoint_lock,
    )?;
    let completed_wal_metrics = authority.durable_wal_monitor_snapshot()?.metrics();
    tracker.complete_checkpoint(
        completed_wal_metrics
            .append_total
            .saturating_sub(completed_wal_metrics.append_error_total),
    );
    Ok(true)
}

fn spawn_experimental_raft_peer_checkpoint_loop(
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    durability: ExperimentalRaftPeerDurabilityContext,
    policy: ExperimentalRaftPeerCheckpointPolicy,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut tracker = ExperimentalRaftPeerCheckpointTracker::new(policy);
        loop {
            if authority
                .durability_publication()
                .expect("Raft durability publication should already be initialized")
                .is_poisoned()
            {
                return;
            }
            if let Err(error) = checkpoint_experimental_raft_peer_wal_if_due(
                &runtime,
                &authority,
                &durability,
                &mut tracker,
                Instant::now(),
            ) {
                publish_experimental_raft_checkpoint_monitor_poison(&authority);
                eprintln!(
                    "experimental OpenRaft control-plane bounded peer WAL checkpoint failed; exiting to avoid serving after durability failure: {error}"
                );
                std::process::exit(1);
            }
            thread::sleep(tracker.policy.poll_interval);
        }
    })
}

fn publish_experimental_raft_checkpoint_monitor_poison(authority: &ControlPlaneRaftAuthority) {
    authority
        .durability_publication()
        .expect("Raft durability publication should already be initialized")
        .poison("control-plane Raft checkpoint monitor observed a durability failure");
}

fn spawn_experimental_raft_peer_listener_loop(
    listener: ControlPlaneRaftPeerServerListener,
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    policy: ControlPlaneRaftPeerServerPolicy,
) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        listener
            .accept_one(&runtime, Arc::clone(&authority), &policy)
            .unwrap_or_else(|error| {
                eprintln!("control-plane OpenRaft peer listener stopped: {error}");
                std::process::exit(1);
            });
    })
}

fn store_experimental_raft_durable_restart_artifact(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    path: &Path,
    durable_checkpoint_lock: Option<&Arc<Mutex<()>>>,
) -> Result<(), ControlPlaneError> {
    let _guard = durable_checkpoint_lock.map(|lock| {
        lock.lock()
            .expect("experimental OpenRaft durable checkpoint mutex poisoned")
    });
    store_experimental_raft_durable_restart_artifact_while_locked(runtime, authority, path)
}

fn store_experimental_raft_durable_restart_artifact_while_locked(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    path: &Path,
) -> Result<(), ControlPlaneError> {
    (|| {
        let artifact_existed = path.exists();
        let checkpoint =
            block_on_control_plane_raft(runtime, authority.capture_durable_restart_checkpoint())?;
        let committed_timestamp_high_water_ms =
            authority.persist_durable_restart_checkpoint(checkpoint)?;
        let binding = authority.authority_clock_checkpoint_binding();
        if !artifact_existed && load_authority_clock_restart_checkpoint(path, binding)?.is_none() {
            store_authority_clock_restart_checkpoint(
                path,
                binding,
                1,
                committed_timestamp_high_water_ms,
            )?;
        }
        Ok(())
    })()
    .map_err(|error: ControlPlaneError| {
        error.into_durability_failure("experimental OpenRaft durable restart checkpoint failed")
    })
}

fn establish_static_raft_control_plane_identity(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    identity: &ConfiguredStaticClusterIdentity,
    node_id: ControlPlaneRaftNodeId,
    path: &Path,
    durable_checkpoint_lock: &Arc<Mutex<()>>,
) -> Result<(), ControlPlaneError> {
    loop {
        let guard = durable_checkpoint_lock
            .lock()
            .expect("experimental OpenRaft durable checkpoint mutex poisoned");
        let publication = block_on_control_plane_raft(
            runtime,
            authority.publish_static_identity_restart_checkpoint(),
        )?;
        if let Some(publication) = publication {
            store_authority_clock_restart_checkpoint(
                path,
                publication.authority_clock_binding(),
                1,
                publication.committed_timestamp_high_water_ms(),
            )?;
            static_cluster_state::mark_static_control_plane_identity_established(
                identity, node_id, path,
            )
            .map_err(classify_static_control_plane_identity_establishment_error)?;
            return Ok(());
        }
        drop(guard);
        process_info!(
            "static control-plane identity checkpoint is not converged; waiting for Raft apply"
        );
        std::thread::sleep(Duration::from_millis(100));
        block_on_control_plane_raft(runtime, authority.wait_for_static_initial_membership())?;
    }
}

fn classify_static_control_plane_identity_establishment_error(
    error: static_cluster_state::StaticControlPlaneIdentityEstablishmentError,
) -> ControlPlaneError {
    use static_cluster_state::StaticControlPlaneIdentityEstablishmentError;

    match error {
        StaticControlPlaneIdentityEstablishmentError::Validation(message) => {
            ControlPlaneError::static_topology_failure(format!(
                "failed to establish static control-plane identity: {message}"
            ))
        }
        StaticControlPlaneIdentityEstablishmentError::Persistence(message) => {
            ControlPlaneError::durability_failure(format!(
                "failed to persist established static control-plane identity: {message}"
            ))
        }
    }
}

fn snapshot_purge_and_checkpoint_experimental_raft_peer_wal(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    path: &Path,
    durable_checkpoint_lock: &Arc<Mutex<()>>,
) -> Result<(), ControlPlaneError> {
    let _guard = durable_checkpoint_lock
        .lock()
        .expect("experimental OpenRaft durable checkpoint mutex poisoned");
    let snapshot_log_id =
        block_on_control_plane_raft(runtime, authority.trigger_local_snapshot_applied())?;
    store_experimental_raft_durable_restart_artifact_while_locked(runtime, authority, path)?;
    let Some(snapshot_log_id) = snapshot_log_id else {
        return Ok(());
    };
    let purge_result = block_on_control_plane_raft(
        runtime,
        authority.purge_log_through_snapshot(snapshot_log_id),
    );
    store_experimental_raft_durable_restart_artifact_while_locked(runtime, authority, path)?;
    purge_result
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
    let raft_peer_policy =
        build_experimental_raft_peer_transport_policy(config, &cluster_name, node_id)
            .unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(1);
            });
    let raft_peer_network = raft_peer_policy.as_ref().map(|_| {
        if config.control_plane_raft_peer_client_endpoints.is_empty() {
            Ok(ControlPlaneRaftPeerNetworkConfig::unix(
                config.control_plane_raft_peer_io_timeout,
            ))
        } else {
            ControlPlaneRaftPeerNetworkConfig::with_peer_endpoints(
                config.control_plane_raft_peer_io_timeout,
                config.control_plane_raft_peer_client_endpoints.clone(),
            )
            .map_err(|error| format!("invalid control-plane Raft peer endpoints: {error}"))
        }
    });
    let raft_peer_network = raft_peer_network.transpose().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let durable_checkpoint_lock = Arc::new(Mutex::new(()));
    let durable_artifact_path = Arc::new(PathBuf::from(state_path));
    let authority_clock_checkpoint_binding =
        ControlPlaneAuthorityClockCheckpointBinding::for_raft(&cluster_name, node_id);
    let authority = block_on_control_plane_raft(&runtime, async {
        let authority = if let Some(policy) = raft_peer_policy.clone() {
            let network = raft_peer_network
                .clone()
                .expect("Raft peer policy has a validated peer network");
            if config.static_cluster_identity.is_some() && !static_cluster_identity_established {
                ControlPlaneRaftAuthority::new_experimental_peer_durable_pending_static_initialization_network(
                        cluster_name.clone(),
                        node_id,
                        Path::new(state_path),
                        policy,
                        config.static_initial_cluster_map.as_ref().expect(
                            "static initialization requires configured initial topology",
                        ),
                        network,
                    )
                    .await?
            } else {
                ControlPlaneRaftAuthority::new_experimental_peer_durable_network(
                        cluster_name.clone(),
                        node_id,
                        Path::new(state_path),
                        policy,
                        network,
                    )
                    .await?
            }
        } else {
            ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name.clone(),
                node_id,
                Path::new(state_path),
            )
            .await?
        };
        Ok::<_, ControlPlaneError>(Arc::new(authority))
    })
    .unwrap_or_else(|error| {
        eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
        std::process::exit(1);
    });
    let raft_peer_listeners =
        bind_experimental_raft_peer_listener_with_policy(config, raft_peer_policy.clone())
            .unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(1);
            });
    let durable_publication = authority.durability_publication().unwrap_or_else(|error| {
        eprintln!("failed to initialize control-plane Raft durability publication: {error}");
        std::process::exit(1);
    });
    let multi_node_raft_peer_mode = raft_peer_policy
        .as_ref()
        .is_some_and(|policy| policy.peers().len() > 1);
    let raft_peer_durability = ExperimentalRaftPeerDurabilityContext {
        artifact_path: Some(Arc::clone(&durable_artifact_path)),
        checkpoint_lock: Arc::clone(&durable_checkpoint_lock),
    };
    let raft_peer_server_durability: Arc<dyn ControlPlaneRaftPeerServerDurability> =
        Arc::new(ExperimentalRaftPeerServerDurability {
            runtime: runtime.clone(),
            authority: Arc::clone(&authority),
            context: raft_peer_durability.clone(),
        });
    let raft_peer_server_policy = raft_peer_policy.clone().map(|peer_policy| {
        ControlPlaneRaftPeerServerPolicy::new(
            node_id,
            peer_policy,
            CONTROL_PLANE_RAFT_PEER_PRE_AUTH_BYTE_BUDGET,
        )
        .map(|policy| {
            policy
                .with_durability(Arc::clone(&raft_peer_server_durability))
                .with_fatal_error_handler(Arc::new(|| std::process::exit(1)))
        })
    });
    let raft_peer_server_policy = match raft_peer_server_policy.transpose() {
        Ok(policy) => policy,
        Err(error) => {
            eprintln!("failed to configure control-plane OpenRaft peer server: {error}");
            std::process::exit(1);
        }
    };
    let _raft_checkpoint_loop = spawn_experimental_raft_peer_checkpoint_loop(
        runtime.clone(),
        Arc::clone(&authority),
        raft_peer_durability.clone(),
        ExperimentalRaftPeerCheckpointPolicy::default(),
    );
    let _raft_peer_listener_loops = raft_peer_listeners
        .into_iter()
        .map(|listener| {
            spawn_experimental_raft_peer_listener_loop(
                listener,
                runtime.clone(),
                Arc::clone(&authority),
                raft_peer_server_policy
                    .clone()
                    .expect("bound Raft peer listeners require a server policy"),
            )
        })
        .collect::<Vec<_>>();
    let initialized_membership = block_on_control_plane_raft(&runtime, async {
        let mut initialized_membership = false;
        if !authority.is_initialized().await?
            && experimental_raft_startup_initializes_membership(raft_peer_policy.as_ref(), node_id)
        {
            if let Some(policy) = &raft_peer_policy {
                authority.initialize_membership(policy.peers()).await?;
            } else {
                authority.initialize_single_node_membership(node_id).await?;
            }
            initialized_membership = true;
        }
        Ok::<_, ControlPlaneError>(initialized_membership)
    })
    .unwrap_or_else(|error| {
        eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
        std::process::exit(1);
    });
    if initialized_membership {
        store_experimental_raft_durable_restart_artifact(
            &runtime,
            &authority,
            Path::new(state_path),
            Some(&durable_checkpoint_lock),
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
            std::process::exit(1);
        });
    }
    block_on_control_plane_raft(&runtime, async {
        if experimental_raft_startup_requires_local_leader(raft_peer_policy.as_ref()) {
            authority
                .wait_for_current_leader(
                    node_id,
                    Duration::from_secs(1),
                    "experimental single-node control-plane startup leadership",
                )
                .await?;
            wait_for_experimental_raft_local_authority_serving(
                &authority,
                Duration::from_secs(1),
                "experimental single-node control-plane startup",
            )
            .await?;
        } else if config.static_cluster_identity.is_none() {
            wait_for_experimental_raft_startup_catch_up(
                &authority,
                Duration::from_secs(1),
                "experimental control-plane startup committed replay",
            )
            .await?;
        }
        Ok::<_, ControlPlaneError>(())
    })
    .unwrap_or_else(|error| {
        eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
        std::process::exit(1);
    });
    if config.static_cluster_identity.is_some() {
        let expected = config
            .static_initial_cluster_map
            .as_ref()
            .unwrap_or_else(|| {
                eprintln!("static initial cluster map is not configured");
                std::process::exit(1);
            });
        block_on_control_plane_raft(
            &runtime,
            authority
                .establish_static_initial_topology(expected, !static_cluster_identity_established),
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to establish static control-plane topology: {error}");
            std::process::exit(1);
        });
    }
    if let Some(identity) = &config.static_cluster_identity {
        if static_cluster_identity_established {
            store_experimental_raft_durable_restart_artifact(
                &runtime,
                &authority,
                Path::new(state_path),
                Some(&durable_checkpoint_lock),
            )
            .unwrap_or_else(|error| {
                eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
                std::process::exit(1);
            });
        } else {
            establish_static_raft_control_plane_identity(
                &runtime,
                &authority,
                identity,
                node_id,
                Path::new(state_path),
                &durable_checkpoint_lock,
            )
            .unwrap_or_else(|error| {
                eprintln!("failed to establish static control-plane state: {error}");
                std::process::exit(1);
            });
        }
    } else {
        store_experimental_raft_durable_restart_artifact(
            &runtime,
            &authority,
            Path::new(state_path),
            Some(&durable_checkpoint_lock),
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
            std::process::exit(1);
        });
    }
    let restart_clock_checkpoint = load_process_authority_clock_restart_checkpoint(
        &durable_artifact_path,
        authority_clock_checkpoint_binding,
    )
    .unwrap_or_else(|error| {
        eprintln!("failed to load experimental OpenRaft authority clock checkpoint: {error}");
        std::process::exit(1);
    });
    let initial_clock_snapshot =
        block_on_control_plane_raft(&runtime, authority.current_control_plane_snapshot())
            .unwrap_or_else(|error| {
                eprintln!(
                    "failed to read experimental OpenRaft control-plane clock state: {error}"
                );
                std::process::exit(1);
            });
    let initial_clock_status = block_on_control_plane_raft(&runtime, authority.status())
        .unwrap_or_else(|error| {
            eprintln!("failed to read experimental OpenRaft leadership state: {error}");
            std::process::exit(1);
        });
    let mut initial_authority_clock =
        ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
            initial_clock_snapshot.max_committed_timestamp_ms(),
            restart_clock_checkpoint,
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to initialize experimental OpenRaft authority clock: {error}");
            std::process::exit(1);
        });
    if !initial_authority_clock.is_established() {
        invalidate_authority_clock_restart_checkpoint(Path::new(state_path)).unwrap_or_else(
            |error| {
                eprintln!(
                    "failed to invalidate blocked experimental OpenRaft authority clock checkpoint: {error}"
                );
                std::process::exit(1);
            },
        );
    }
    if let Some(previous_authority) = initial_clock_snapshot.lease_grant_horizon_authority() {
        initial_authority_clock
            .advance_generation_past_lease_horizon(previous_authority)
            .unwrap_or_else(|error| {
                eprintln!(
                    "failed to advance restarted experimental OpenRaft clock generation: {error}"
                );
                std::process::exit(1);
            });
    }
    if initial_clock_status.local_leader() {
        initial_authority_clock
            .bind_initial_raft_leadership_term(initial_clock_status.current_term());
    } else if !initialized_membership {
        // A restored or joining follower must not use the fresh-cluster first
        // term exception when it later becomes leader. Only the process that
        // initialized new membership may bind that initial term lazily.
        initial_authority_clock.bind_initial_raft_leadership_term(None);
    }
    let authority_clock = Arc::new(Mutex::new(initial_authority_clock));
    let mut control_plane = ExperimentalRaftControlPlane {
        runtime: runtime.clone(),
        authority: Arc::clone(&authority),
        durable_artifact_path: Some(Arc::clone(&durable_artifact_path)),
        durable_checkpoint_lock: Some(Arc::clone(&durable_checkpoint_lock)),
        durable_serving_checkpoint: Arc::new(Mutex::new(None)),
        checkpoint_serving_reads: multi_node_raft_peer_mode,
        resample_authority_time: true,
        authority_clock: Some(Arc::clone(&authority_clock)),
        #[cfg(test)]
        after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
    };
    if config.static_initial_cluster_map.is_none() {
        bootstrap_empty_experimental_raft_control_plane(&mut control_plane, config).unwrap_or_else(
            |error| {
                eprintln!("failed to bootstrap experimental OpenRaft control-plane state: {error}");
                std::process::exit(1);
            },
        );
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
    let raft_authority = Arc::clone(&authority);
    let mut authority = control_plane;
    let authority_clock_checkpoint_target =
        Arc::new(ControlPlaneAuthorityClockCheckpointTarget::new(
            durable_artifact_path.as_ref().clone(),
            authority_clock_checkpoint_binding,
        ));
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
    if let Some(policy) = &raft_peer_policy {
        process_info!("{}", format_experimental_raft_peer_auth_diagnostics(policy));
    }
    if let Some(diagnostics) = server_auth.diagnostics() {
        process_info!("{}", diagnostics);
    }

    let rpc_runtime = runtime.clone();
    let rpc_raft_authority = Arc::clone(&raft_authority);
    let raft_authority_confirmation: Arc<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync> =
        Arc::new(move || {
            block_on_control_plane_raft(
                &rpc_runtime,
                rpc_raft_authority.confirmed_linearized_authority_status(),
            )
            .map(|_| ())
        });
    let fatal_error_handler: Arc<dyn Fn() + Send + Sync> = Arc::new(|| std::process::exit(1));
    let response_publication: Arc<dyn ControlPlaneRpcResponsePublication> =
        Arc::new(durable_publication.clone());
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
    let _rpc_listener_loops = rpc_server.serve_cloned_raft_authority(
        authority.clone(),
        Arc::clone(&authority_clock),
        Arc::clone(&authority_clock_checkpoint_target),
        raft_authority_confirmation,
        response_publication,
        fatal_error_handler,
    );

    let mut lease_expiry_not_before_ms = None;
    loop {
        let raft_status = if multi_node_raft_peer_mode {
            block_on_control_plane_raft(&runtime, async { raft_authority.status().await })
                .map(Some)
                .unwrap_or_else(|error| {
                    eprintln!("experimental OpenRaft control-plane status check failed: {error}");
                    std::process::exit(1);
                })
        } else {
            None
        };
        let local_raft_authority_serving = raft_status
            .as_ref()
            .is_none_or(|status| status.linearized_authority_serving());
        let expiry_now_ms = storage::clock::current_time_millis();
        let expiry = if local_raft_authority_serving {
            if multi_node_raft_peer_mode && config.static_initial_cluster_map.is_none() {
                bootstrap_empty_experimental_raft_control_plane(&mut authority, config)
                    .unwrap_or_else(|error| {
                        eprintln!(
                            "failed to bootstrap experimental OpenRaft control-plane state: {error}"
                        );
                        std::process::exit(1);
                    });
            }
            if lease_expiry_not_before_ms.is_some_and(|not_before_ms| expiry_now_ms < not_before_ms)
            {
                Ok((ClusterEpoch::INITIAL, 0, 0))
            } else {
                authority.expire_heartbeat_leases(expiry_now_ms)
            }
        } else {
            Ok((ClusterEpoch::INITIAL, 0, 0))
        };
        {
            let authority_clock = authority_clock
                .lock()
                .expect("control-plane authority clock mutex poisoned");
            authority_clock_checkpoint_target
                .invalidate_if_blocked(&authority_clock)
            .unwrap_or_else(|error| {
                eprintln!(
                    "failed to invalidate blocked experimental OpenRaft authority clock checkpoint: {error}"
                );
                std::process::exit(1);
            });
        }
        match expiry {
            Ok((cluster_epoch, expired_nodes, peering_pgs)) if expired_nodes > 0 => {
                process_info!(
                    "experimental OpenRaft control-plane expired {} node leases at epoch {} and moved {} PGs to peering",
                    expired_nodes,
                    cluster_epoch,
                    peering_pgs
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
    authority: &mut ExperimentalRaftControlPlane,
    config: &ServerConfig,
) -> Result<(), ControlPlaneError> {
    if config.static_initial_cluster_map.is_some() {
        return Ok(());
    }
    let topology = uncertified_initial_control_plane_topology(config)
        .map_err(storage::StaticStorageTopologyError::into_control_plane_error)?;
    let Some(epoch) = authority.block_on(
        authority
            .authority
            .establish_uncertified_initial_control_plane_topology(&topology),
    )?
    else {
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
    storage::ControlPlaneRpcServerAuth::new(
        config.control_plane_auth_cluster_id.as_deref(),
        config.control_plane_admin_auth_instance_id.as_deref(),
        storage_credentials,
        frontend_credentials,
        admin_credentials,
    )
    .map_err(|error| error.to_string())
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
        None => build_admin_credential_binding(None, None, &[]),
    }
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

fn latest_auth_credential_by_version_then_id<'a, T>(
    credentials: impl Iterator<Item = &'a T>,
    credential_id: impl Fn(&T) -> &str,
    credential_version: impl Fn(&T) -> u64,
) -> Option<&'a T> {
    credentials.max_by(|left, right| {
        credential_version(left)
            .cmp(&credential_version(right))
            .then_with(|| credential_id(left).cmp(credential_id(right)))
    })
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
    if let Some(rpc_auth) = config.storage_rpc_server_auth.clone() {
        prepared = prepared.with_rpc_auth(rpc_auth);
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
    if let Some(rpc_auth) = config.storage_rpc_server_auth.clone() {
        prepared_server = prepared_server.with_rpc_auth(rpc_auth);
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
    local_map
        .install_unix_storage_node_clients({
            let admission_settings = unix_storage_node_client_admission_settings(config);
            config.storage_node_sockets.iter().map(move |entry| {
                LocalUnixStorageNodeClientConfig::with_rpc_admission_settings(
                    NodeId::new(entry.node_id),
                    entry.socket_path.clone(),
                    admission_settings,
                )
                .with_optional_frontend_rpc_auth(config.storage_rpc_frontend_client_auth.clone())
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
        None => StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings(
            metadata_primary_node_id,
            runtime_map,
            ec_shape,
            unix_storage_node_client_admission_settings(config),
        ),
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
        "argmin-s3 listening on {}://{} (EC {},{}, {} PGs, {} workers, max {} conns, max {} in-flight effective {} in-flight, storage RPC admission {} bulk wait {} ms control wait {} ms, read chunk {} bytes, panic-on-500 {}, abort-on-500 {}, local-debug {}, region {}, host id {})",
        scheme,
        config.listen_addr,
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
        config.panic_on_500,
        config.abort_on_500,
        config.local_debug_endpoint,
        config.region,
        host_id
    );

    let serve_config = server_http::http::serve::ServeConfig {
        stream_read_chunk_size: config.stream_read_chunk_size,
        panic_on_500: config.panic_on_500,
        abort_on_500: config.abort_on_500,
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

#[cfg(test)]
mod tests {
    use super::*;
    use auth::SecretKey;
    use config::{
        BinarySecretConfigValue, ConfiguredControlPlaneRaftAuthCredential,
        ConfiguredControlPlaneRaftPeerSocket, SecretConfigValue,
    };
    use openraft::impls::{BasicNode, Vote};
    use storage::control_plane::{
        ControlPlaneHeartbeatSink, ControlPlaneRpcClientEndpoint, NodeAvailabilityState,
        NodeHeartbeat, NodeMembershipState, NodePgHeartbeatObservation, PgMetadataProof,
    };
    use storage::control_plane_raft::{
        ControlPlaneRaftLeaderId, ControlPlaneRaftPeerTestClient,
        ControlPlaneRaftPeerTransportLimits,
    };

    fn spawn_control_plane_test_rpc_server<T>(
        listener: UnixListener,
        authority: Arc<Mutex<T>>,
        authority_times_ms: impl IntoIterator<Item = u64> + Send + 'static,
        server_auth: Option<storage::ControlPlaneRpcServerAuth>,
    ) -> std::thread::JoinHandle<()>
    where
        T: ControlPlaneAdmin
            + ControlPlaneHeartbeatRuntimeMapSource
            + ControlPlaneRuntimeMapSource
            + Send
            + 'static,
    {
        let server = ControlPlaneRpcOrdinaryTestServer::unix(
            listener,
            CONTROL_PLANE_RPC_WORKER_LIMIT,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            CONTROL_PLANE_RPC_PRE_AUTH_BYTE_BUDGET,
            server_auth.as_ref(),
        )
        .unwrap();
        std::thread::spawn(move || {
            server
                .serve_shared_requests(authority, authority_times_ms, |_| {})
                .unwrap();
        })
    }

    #[test]
    fn standalone_control_plane_binds_rpc_endpoints_only_after_replay() {
        use storage::control_plane::ControlPlaneStore;

        #[derive(Clone)]
        struct ReplayBlockedStore {
            inner: FileControlPlaneStore,
            replay_gate: Arc<(Mutex<(bool, bool)>, Condvar)>,
        }

        impl ControlPlaneStore for ReplayBlockedStore {
            fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
                let (state, wake) = &*self.replay_gate;
                let mut state = state.lock().expect("replay gate should not be poisoned");
                state.0 = true;
                wake.notify_all();
                while !state.1 {
                    state = wake
                        .wait(state)
                        .expect("replay gate should not be poisoned while blocked");
                }
                drop(state);
                self.inner.load()
            }

            fn checkpoint(
                &self,
                previous_snapshot: Option<&ClusterControlSnapshot>,
                next_snapshot: &ClusterControlSnapshot,
            ) -> Result<(), ControlPlaneError> {
                self.inner.checkpoint(previous_snapshot, next_snapshot)
            }
        }

        let tmp = test_util::tempdir();
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let ordinary_path = tmp.path().join("control-plane.sock");
        let recovery_path = tmp.path().join("clock-recovery.sock");
        let inner_store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut seeded = SingleAuthorityControlPlane::open(inner_store.clone()).unwrap();
        seeded
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        drop(seeded);

        let replay_gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let replay_store = ReplayBlockedStore {
            inner: inner_store,
            replay_gate: Arc::clone(&replay_gate),
        };
        let ordinary_path_for_startup = ordinary_path.clone();
        let recovery_path_for_startup = recovery_path.clone();
        let startup = thread::spawn(move || {
            initialize_standalone_control_plane_before_binding(
                || {
                    Arc::new(Mutex::new(
                        SingleAuthorityControlPlane::open(replay_store).unwrap(),
                    ))
                },
                || {
                    let ordinary = bind_configured_control_plane_rpc_listeners(
                        &[],
                        &ordinary_path_for_startup,
                        "test ordinary control-plane socket",
                        CONTROL_PLANE_RPC_WORKER_LIMIT,
                    )
                    .unwrap();
                    let recovery = bind_configured_control_plane_rpc_listeners(
                        &[],
                        &recovery_path_for_startup,
                        "test recovery control-plane socket",
                        CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
                    )
                    .unwrap();
                    (ordinary, recovery)
                },
            )
        });

        let (state, wake) = &*replay_gate;
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut state = state.lock().expect("replay gate should not be poisoned");
        while !state.0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "control-plane replay did not block");
            let (next_state, timeout) = wake
                .wait_timeout(state, remaining)
                .expect("replay gate should not be poisoned while waiting");
            state = next_state;
            assert!(
                !timeout.timed_out() || state.0,
                "control-plane replay did not block"
            );
        }
        for path in [&ordinary_path, &recovery_path] {
            let error = UnixStream::connect(path)
                .expect_err("control-plane endpoint must not accept before replay completes");
            assert!(matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ));
        }
        state.1 = true;
        wake.notify_all();
        drop(state);

        let (authority, (mut ordinary_listeners, recovery_listeners)) = startup.join().unwrap();
        assert_eq!(ordinary_listeners.len(), 1);
        assert_eq!(recovery_listeners.len(), 1);
        UnixStream::connect(&recovery_path)
            .expect("recovery endpoint should bind after replay completes");
        drop(recovery_listeners);

        let frontend_credential = ControlPlaneFrontendAuthCredentialInput {
            instance_id: "frontend-1".to_owned(),
            credential_id: "frontend-1".to_owned(),
            credential_version: 1,
            secret: b"frontend-secret".to_vec(),
        };
        let server_auth = storage::ControlPlaneRpcServerAuth::new(
            Some("startup-order-cluster"),
            Some("admin-1"),
            Vec::new(),
            vec![frontend_credential.clone()],
            vec![ControlPlaneAdminAuthCredentialInput {
                instance_id: "admin-1".to_owned(),
                credential_id: "admin-1".to_owned(),
                credential_version: 1,
                secret: b"admin-secret".to_vec(),
            }],
        )
        .unwrap();
        let ControlPlaneRpcServerListenerInput::Unix {
            listener,
            max_connections,
            max_frame_bytes,
            io_timeout,
        } = ordinary_listeners.pop().unwrap()
        else {
            panic!("fallback control-plane listener must use a Unix socket");
        };
        let server = ControlPlaneRpcOrdinaryTestServer::unix(
            listener,
            max_connections,
            max_frame_bytes,
            io_timeout,
            CONTROL_PLANE_RPC_PRE_AUTH_BYTE_BUDGET,
            Some(&server_auth),
        )
        .unwrap();
        let _server = std::thread::spawn(move || {
            server
                .serve_shared_requests(authority, [storage::clock::current_time_millis()], |_| {})
                .unwrap();
        });
        let now_ms = storage::clock::current_time_millis();
        let status = storage::ControlPlaneFrontendClient::with_socket_paths(
            [ordinary_path],
            Some("startup-order-cluster"),
            Some("frontend-1"),
            vec![frontend_credential],
            None,
        )
        .unwrap()
        .runtime_map_status(now_ms)
        .expect("freshly signed request should succeed after replay and binding");
        assert_eq!(status.pg_routes(), 0);
    }

    #[test]
    fn clock_recovery_socket_is_distinct_and_shorter_than_standard_process_socket() {
        let socket = Path::new("/tmp/private/control-plane-101.sock");
        let recovery = storage::control_plane_clock_recovery_socket_path(socket);

        assert_eq!(recovery.parent(), socket.parent());
        assert_ne!(recovery, socket);
        assert!(recovery.as_os_str().as_bytes().len() <= socket.as_os_str().as_bytes().len());
    }

    fn durable_raft_checkpoint_vote(path: &Path) -> Option<(u64, u64, bool)> {
        let state =
            storage::control_plane_raft::inspect_control_plane_raft_checkpoint_state_for_test(path)
                .ok()?;
        state
            .persisted_vote()
            .map(|vote| (vote.term(), vote.node_id(), vote.committed()))
    }

    #[test]
    fn established_authority_clock_admin_state_refreshes_restart_checkpoint() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-authority-clock-checkpoint-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let state_path = tmp.join("control-plane.state");
        let store = FileControlPlaneStore::new(&state_path);
        let mut authority = SingleAuthorityControlPlane::open(store)
            .expect("test control-plane authority should open");
        storage::control_plane::ControlPlaneLinearizedCommandSink::submit_control_plane_command(
            &mut authority,
            ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority: LeaseHorizonAuthorityBinding::checked_new(1, None).unwrap(),
                authority_now_ms: 1_000,
                horizon_duration_ms: 100,
            },
        )
        .expect("durable horizon should establish the timestamp high-water");
        let context = authority
            .authority_clock_context()
            .expect("clock context should read");
        assert_eq!(context.committed_timestamp_high_water_ms(), Some(1_000));
        let binding = FileControlPlaneStore::new(&state_path)
            .load_or_create_authority_clock_checkpoint_binding()
            .unwrap();
        let checkpoint_target =
            ControlPlaneAuthorityClockCheckpointTarget::new(state_path.clone(), binding);

        let mut authority_clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
            Some(1_000),
            5_000,
            Some(5_000),
            None,
        )
        .expect("blocked clock should construct");
        let blocked = authority_clock.status(context);
        assert!(!blocked.established());
        authority_clock
            .reestablish(
                blocked.generation(),
                Some(1_000),
                None,
                context,
                5_000,
                Some(5_000),
            )
            .expect("clock should re-establish against current durable state");

        storage::clock::with_time_override(5_000, || {
            checkpoint_target
                .persist_established(context, &mut authority_clock)
                .expect("established clock checkpoint should persist");
        });
        let checkpoint = load_authority_clock_restart_checkpoint(&state_path, binding)
            .expect("checkpoint should load")
            .expect("checkpoint should exist");
        assert_eq!(checkpoint.authority_generation(), 2);
        assert_eq!(checkpoint.committed_timestamp_high_water_ms(), Some(1_000));
        assert_eq!(checkpoint.wall_time_ms(), 5_000);
        assert_eq!(checkpoint.health_time_ms(), 5_000);
        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn corrupt_authority_clock_checkpoint_starts_recoverably_blocked() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-corrupt-authority-clock-checkpoint-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let state_path = tmp.join("control-plane.state");
        let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
        storage::clock::with_time_override(1_000, || {
            store_authority_clock_restart_checkpoint(&state_path, binding, 1, Some(999)).unwrap();
        });
        let mut checkpoint_path = state_path.as_os_str().to_os_string();
        checkpoint_path.push(".clock");
        let checkpoint_path = PathBuf::from(checkpoint_path);
        let mut bytes = std::fs::read(&checkpoint_path).unwrap();
        let last = bytes
            .last_mut()
            .expect("checkpoint should contain a checksum");
        *last ^= 1;
        std::fs::write(checkpoint_path, bytes).unwrap();

        assert_eq!(
            load_process_authority_clock_restart_checkpoint(&state_path, binding).unwrap(),
            None
        );
        let clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
            Some(999),
            1_000,
            Some(1_000),
            None,
        )
        .unwrap();
        assert!(!clock
            .status(ControlPlaneAuthorityClockContext::new(
                Some(999),
                None,
                true,
                true,
            ))
            .established());
        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn authority_clock_checkpoint_failure_latches_non_serving() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-authority-clock-checkpoint-failure-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
            tmp.join("authority.state"),
        ))
        .unwrap();
        let context = authority.authority_clock_context().unwrap();
        let mut authority_clock =
            ControlPlaneAuthorityClock::new(None, 1_000, Some(1_000)).unwrap();
        let blocked_parent = tmp.join("not-a-directory");
        std::fs::write(&blocked_parent, b"file").unwrap();
        let target = ControlPlaneAuthorityClockCheckpointTarget::new(
            blocked_parent.join("authority.state"),
            ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1),
        );

        storage::clock::with_time_override(1_000, || {
            assert!(target
                .persist_established(context, &mut authority_clock)
                .is_err());
        });
        let status = authority_clock.status(context);
        assert!(!status.established());
        assert_eq!(
            status.blocked_reason(),
            Some(
                storage::control_plane::ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure
            )
        );
        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn authority_clock_fault_durably_invalidates_restart_continuation() {
        let tmp = short_unix_socket_test_dir("clock-fault-checkpoint-invalidation");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let state_path = tmp.join("authority.state");
        let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
        storage::clock::with_time_override(1_000, || {
            store_authority_clock_restart_checkpoint(&state_path, binding, 7, None).unwrap();
        });
        let checkpoint = load_authority_clock_restart_checkpoint(&state_path, binding)
            .unwrap()
            .unwrap();
        let mut authority_clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
            None,
            1_000,
            Some(1_000),
            Some(checkpoint),
        )
        .unwrap();
        assert!(
            authority_clock.resume_single_authority_lease_horizon_generation(
                LeaseHorizonAuthorityBinding::checked_new(7, None).unwrap()
            )
        );
        assert!(authority_clock.effective_now_ms(0, Some(1_001)).is_err());
        let target = ControlPlaneAuthorityClockCheckpointTarget::new(state_path.clone(), binding);

        target.invalidate_if_blocked(&authority_clock).unwrap();
        assert_eq!(
            load_authority_clock_restart_checkpoint(&state_path, binding).unwrap(),
            None
        );
        std::fs::remove_dir_all(tmp).unwrap();
    }

    #[test]
    fn failed_clock_recovery_replacement_cannot_reuse_old_checkpoint() {
        let tmp = short_unix_socket_test_dir("clock-recovery-checkpoint-invalidation");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let state_path = tmp.join("authority.state");
        let store = FileControlPlaneStore::new(&state_path);
        let binding = store
            .load_or_create_authority_clock_checkpoint_binding()
            .unwrap();
        let authority = SingleAuthorityControlPlane::open(store).unwrap();
        storage::clock::with_time_override(1_000, || {
            store_authority_clock_restart_checkpoint(&state_path, binding, 7, None).unwrap();
        });
        let checkpoint = load_authority_clock_restart_checkpoint(&state_path, binding)
            .unwrap()
            .unwrap();
        let context = authority.authority_clock_context().unwrap();
        let mut authority_clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
            None,
            1_000,
            Some(1_000),
            Some(checkpoint),
        )
        .unwrap();
        assert!(authority_clock.effective_now_ms(0, Some(1_001)).is_err());
        let blocked = authority_clock.status(context);
        authority_clock
            .reestablish(
                blocked.generation(),
                None,
                None,
                context,
                1_000,
                Some(1_000),
            )
            .unwrap();

        let mut tmp_checkpoint_path = state_path.as_os_str().to_os_string();
        tmp_checkpoint_path.push(".clock.tmp");
        std::fs::create_dir(PathBuf::from(tmp_checkpoint_path)).unwrap();
        let target = ControlPlaneAuthorityClockCheckpointTarget::new(state_path.clone(), binding);
        storage::clock::with_time_override(1_000, || {
            assert!(target
                .persist_established(context, &mut authority_clock)
                .is_err());
        });
        assert!(!authority_clock.is_established());
        assert_eq!(
            load_authority_clock_restart_checkpoint(&state_path, binding).unwrap(),
            None
        );

        let mut restarted =
            ControlPlaneAuthorityClock::new_with_restart_checkpoint(None, 1_001, Some(1_001), None)
                .unwrap();
        assert!(!restarted.resume_single_authority_lease_horizon_generation(
            LeaseHorizonAuthorityBinding::checked_new(7, None).unwrap()
        ));
        std::fs::remove_dir_all(tmp).unwrap();
    }

    struct UnixSocketTestDir(test_util::TempDir);

    impl std::ops::Deref for UnixSocketTestDir {
        type Target = Path;

        fn deref(&self) -> &Self::Target {
            self.0.path()
        }
    }

    impl AsRef<Path> for UnixSocketTestDir {
        fn as_ref(&self) -> &Path {
            self.0.path()
        }
    }

    fn short_unix_socket_test_dir(_name: &str) -> UnixSocketTestDir {
        UnixSocketTestDir(test_util::tempdir())
    }

    fn test_server_config() -> ServerConfig {
        ServerConfig {
            process_role: ProcessRole::StorageNode,
            listen_addr: "127.0.0.1:9000".to_string(),
            tls_cert_path: None,
            tls_key_path: None,
            data_dir: "/tmp/argmin-test".to_string(),
            pg_count: 8,
            storage_node_ids: (0..6).collect(),
            storage_node_id: Some(2),
            storage_node_data_dir: Some("/tmp/argmin-test/node-0002".to_string()),
            static_cluster_identity: None,
            static_initial_cluster_map: None,
            storage_node_socket_path: Some("/tmp/argmin-test/node-0002.sock".to_string()),
            storage_node_sockets: Vec::new(),
            storage_rpc_client_endpoints: Vec::new(),
            storage_rpc_listeners: Vec::new(),
            storage_node_rpc_admission_limit:
                LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT,
            storage_node_rpc_admission_wait_timeout:
                LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            storage_node_rpc_control_admission_wait_timeout:
                LocalUnixStorageNodeClientConfig::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
            storage_rpc_frontend_client_auth: None,
            storage_rpc_maintenance_client_auth: None,
            storage_rpc_storage_node_client_auth: None,
            storage_rpc_server_auth: None,
            control_plane_state_path: None,
            control_plane_socket_path: None,
            control_plane_clock_recovery_socket_path: None,
            control_plane_client_socket_paths: Vec::new(),
            control_plane_rpc_listeners: Vec::new(),
            control_plane_clock_recovery_rpc_listeners: Vec::new(),
            control_plane_rpc_client_endpoints: Vec::new(),
            control_plane_clock_recovery_rpc_client_endpoints: Vec::new(),
            control_plane_auth_cluster_id: None,
            control_plane_storage_auth_credentials: Vec::new(),
            control_plane_storage_auth_signing_credential: None,
            control_plane_frontend_auth_instance_id: None,
            control_plane_frontend_auth_credentials: Vec::new(),
            control_plane_frontend_auth_signing_credential: None,
            control_plane_admin_auth_instance_id: None,
            control_plane_admin_auth_credentials: Vec::new(),
            control_plane_experimental_raft: false,
            control_plane_raft_cluster_name: None,
            control_plane_raft_node_id: None,
            control_plane_raft_peer_socket_path: None,
            control_plane_raft_peer_sockets: Vec::new(),
            control_plane_raft_peer_listeners: Vec::new(),
            control_plane_raft_peer_client_endpoints: Vec::new(),
            control_plane_raft_peer_transport_limits: ControlPlaneRaftPeerTransportLimits::default(
            ),
            control_plane_raft_peer_max_connections: CONTROL_PLANE_RAFT_PEER_RPC_WORKER_LIMIT,
            control_plane_raft_peer_connect_timeout: Duration::from_secs(1),
            control_plane_raft_peer_io_timeout: Duration::from_secs(1),
            control_plane_raft_auth_credentials: Vec::new(),
            control_plane_raft_auth_signing_credential: None,
            control_plane_lease_scan_interval: std::time::Duration::from_millis(250),
            control_plane_frontend_refresh_interval: std::time::Duration::from_millis(250),
            control_plane_heartbeat_lease_duration: std::time::Duration::from_millis(2000),
            storage_cluster_epoch: 9,
            storage_pg_ids: vec![1, 3, 5],
            ec_k: 4,
            ec_m: 2,
            account_id: String::new(),
            access_key_id: String::new(),
            secret_access_key: SecretKey::new(String::new()),
            uat_credentials: Vec::new(),
            host_id: None,
            sse_c_validator_key_b64: None,
            sse_s3_wrapping_key_b64: SecretConfigValue::new(String::new()),
            region: "us-east-1".to_string(),
            workers: 4,
            max_connections: 512,
            max_inflight_requests: 32,
            stream_read_chunk_size: server_core::coordinator::INTERNAL_SEGMENT_SIZE,
            panic_on_500: false,
            abort_on_500: false,
            local_debug_endpoint: false,
        }
    }

    #[test]
    fn static_raft_peer_policy_binds_manifest_topology_identity() {
        let mut config = test_server_config();
        config.control_plane_raft_peer_socket_path = Some("/tmp/raft-1.sock".to_string());
        config.control_plane_raft_peer_sockets = vec![
            ConfiguredControlPlaneRaftPeerSocket {
                node_id: 1,
                socket_path: "/tmp/raft-1.sock".to_string(),
            },
            ConfiguredControlPlaneRaftPeerSocket {
                node_id: 2,
                socket_path: "/tmp/raft-2.sock".to_string(),
            },
        ];
        config.static_cluster_identity = Some(ConfiguredStaticClusterIdentity {
            cluster_id: "cluster-a".to_string(),
            topology_generation: 7,
            topology_digest: "a".repeat(64),
            process_id: "control-1".to_string(),
            process_identity_digest: "b".repeat(64),
        });

        let policy = build_experimental_raft_peer_transport_policy(&config, "cluster-a", 1)
            .unwrap()
            .unwrap();
        assert_eq!(
            policy.topology_identity(),
            Some(
                &storage::control_plane_raft::ControlPlaneRaftTopologyIdentity {
                    generation: 7,
                    digest: "a".repeat(64),
                }
            )
        );
    }

    #[test]
    fn static_identity_establishment_error_mapping_preserves_failure_class() {
        use static_cluster_state::StaticControlPlaneIdentityEstablishmentError;

        let validation = classify_static_control_plane_identity_establishment_error(
            StaticControlPlaneIdentityEstablishmentError::Validation(
                "synthetic identity mismatch".to_owned(),
            ),
        );
        assert!(matches!(
            validation,
            ControlPlaneError::StaticTopologyFailure { .. }
        ));

        let persistence = classify_static_control_plane_identity_establishment_error(
            StaticControlPlaneIdentityEstablishmentError::Persistence(
                "synthetic directory sync failure".to_owned(),
            ),
        );
        assert!(matches!(
            persistence,
            ControlPlaneError::DurabilityFailure { .. }
        ));
    }

    #[test]
    fn static_raft_identity_establishment_publishes_after_background_checkpoint() {
        let tmp = test_util::tempdir();
        let state_path = tmp.path().join("control-plane.state");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-static-membership-establishment-{}",
            std::process::id()
        );
        let static_identity = ConfiguredStaticClusterIdentity {
            cluster_id: cluster_name.clone(),
            topology_generation: 1,
            topology_digest: "a".repeat(64),
            process_id: "control-1".to_string(),
            process_identity_digest: "b".repeat(64),
        };
        static_cluster_state::initialize_static_control_plane_identity(
            &static_identity,
            1,
            &state_path,
        )
        .expect("static identity should initialize before durable state exists");
        let placement = storage::derive_static_initial_pg_placement(
            1,
            1,
            0,
            storage::StaticStorageFailureDomain::None,
            &["host-1".to_owned()],
            &["disk-1".to_owned()],
            &[storage::StaticStoragePlacementNode::new(
                11, "host-1", "disk-1",
            )],
        )
        .expect("test static placement should derive");
        let topology = storage::derive_static_initial_control_plane_topology(
            static_identity.topology_generation,
            &static_identity.topology_digest,
            &[1],
            &[storage::StaticStorageNodeEndpoint::new(11, "node-11")],
            placement,
        )
        .expect("test static topology should derive");
        let policy = ControlPlaneRaftPeerTransportPolicy::new(
            cluster_name.clone(),
            std::collections::BTreeMap::from([(1, BasicNode::new("localhost"))]),
            ControlPlaneRaftPeerTransportLimits::default(),
        )
        .with_static_initial_topology(&topology);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let authority = runtime
            .block_on(async {
                let authority = ControlPlaneRaftAuthority::new_experimental_peer_durable_pending_static_initialization_network(
                    cluster_name,
                    1,
                    &state_path,
                    policy,
                    &topology,
                    ControlPlaneRaftPeerNetworkConfig::unix(Duration::from_millis(50)),
                )
                .await?;
                authority.initialize_single_node_membership(1).await?;
                authority
                    .wait_for_current_leader(
                        1,
                        Duration::from_secs(1),
                        "static identity publication test leadership",
                    )
                    .await?;
                authority
                    .establish_static_initial_topology(&topology, true)
                    .await?;
                Ok::<_, ControlPlaneError>(Arc::new(authority))
            })
            .unwrap();

        let background_checkpoint = runtime
            .block_on(authority.capture_durable_restart_checkpoint())
            .expect("background checkpoint should capture");
        authority
            .persist_durable_restart_checkpoint(background_checkpoint)
            .expect("background checkpoint should publish an artifact without a clock sidecar");
        let checkpoint_binding = authority.authority_clock_checkpoint_binding();
        assert_eq!(
            load_authority_clock_restart_checkpoint(&state_path, checkpoint_binding)
                .expect("absent authority-clock checkpoint should inspect"),
            None
        );
        let checkpoint_lock = Arc::new(Mutex::new(()));
        establish_static_raft_control_plane_identity(
            runtime.handle(),
            &authority,
            &static_identity,
            1,
            &state_path,
            &checkpoint_lock,
        )
        .expect("static establishment should publish artifact, clock checkpoint, and identity");
        assert!(
            load_authority_clock_restart_checkpoint(&state_path, checkpoint_binding)
                .expect("authority-clock checkpoint should load after establishment")
                .is_some(),
            "static establishment must not depend on being the first artifact writer"
        );

        runtime
            .block_on(authority.shutdown())
            .expect("test authority should shut down");
    }

    #[test]
    fn storage_node_control_plane_client_uses_authenticated_client_when_configured() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_storage_auth_credentials = vec![
            ConfiguredControlPlaneStorageAuthCredential {
                node_id: 2,
                credential_id: "storage-node".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("storage-node-2-secret".to_string()),
            },
            ConfiguredControlPlaneStorageAuthCredential {
                node_id: 2,
                credential_id: "storage-node".to_string(),
                credential_version: 8,
                secret: BinarySecretConfigValue::from_utf8("storage-node-2-new-secret".to_string()),
            },
        ];

        let client = build_storage_node_control_plane_client(
            &config,
            "/tmp/argmin-control-plane.sock",
            NodeId::new(2),
            55,
        )
        .expect("storage-node auth client should build");

        assert!(client.is_authenticated());
    }

    #[test]
    fn storage_node_control_plane_client_sends_authenticated_heartbeat_from_config() {
        let tmp = short_unix_socket_test_dir("storage-node-auth-refresh");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let state_path = tmp.join("control-plane.state");
        let node_id = NodeId::new(2);
        let node_incarnation = 55;
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_storage_auth_credentials =
            vec![ConfiguredControlPlaneStorageAuthCredential {
                node_id: node_id.as_u32(),
                credential_id: "storage-node".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("storage-node-2-secret".to_string()),
            }];
        config.control_plane_admin_auth_instance_id = Some("admin-1".to_string());
        config.control_plane_admin_auth_credentials =
            vec![ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 1,
                secret: BinarySecretConfigValue::from_utf8("admin-secret".to_string()),
            }];
        let server_auth = build_control_plane_rpc_server_auth(&config)
            .expect("test storage-node server auth should build");
        let server_auth_for_assert = server_auth.clone();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(node_id, storage::control_plane::NodeMembershipState::Active)
            .unwrap();
        let server = spawn_control_plane_test_rpc_server(
            listener,
            Arc::new(Mutex::new(authority)),
            [2_000],
            Some(server_auth),
        );

        let mut client = build_storage_node_control_plane_client(
            &config,
            socket_path.to_str().unwrap(),
            node_id,
            node_incarnation,
        )
        .expect("storage-node auth client should build");
        let heartbeat = storage::control_plane::NodeHeartbeat {
            node_id,
            node_incarnation,
            endpoint: "/tmp/argmin-node-2.sock".to_string(),
            observed_epoch: ClusterEpoch::INITIAL,
            requested_lease_duration_ms: 100,
            cluster_map_history_route_references: Default::default(),
            pg_observations: Vec::new(),
        };
        let refresh = storage::clock::with_time_override(2_000, || {
            client.refresh_node_heartbeat(heartbeat, 2_000)
        })
        .unwrap();

        server.join().unwrap();
        assert_eq!(refresh.lease().node_id(), node_id);
        assert_eq!(refresh.lease().lease_deadline_ms(), 2_100);
        assert_eq!(
            refresh.runtime_map().nodes()[0].endpoint(),
            "/tmp/argmin-node-2.sock"
        );
        let diagnostics = server_auth_for_assert.diagnostics().unwrap();
        assert!(diagnostics.contains("accepted_total=1"), "{diagnostics}");
        assert!(
            diagnostics.contains("accepted_by_operation{operation=\"StorageRuntimeMapRefresh\"} 1"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("rejected_total=0"), "{diagnostics}");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn storage_node_control_plane_client_requires_local_auth_credential() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_storage_auth_credentials =
            vec![ConfiguredControlPlaneStorageAuthCredential {
                node_id: 1,
                credential_id: "storage-node".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("storage-node-1-secret".to_string()),
            }];

        let result = build_storage_node_control_plane_client(
            &config,
            "/tmp/argmin-control-plane.sock",
            NodeId::new(2),
            55,
        );
        let Err(err) = result else {
            panic!("storage-node control-plane client should reject missing local credential");
        };

        assert!(err.contains("local storage node id 2"));
    }

    #[test]
    fn frontend_control_plane_client_uses_authenticated_client_when_configured() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_frontend_auth_instance_id = Some("frontend-1".to_string());
        config.control_plane_frontend_auth_credentials = vec![
            ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("frontend-1-secret".to_string()),
            },
            ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 8,
                secret: BinarySecretConfigValue::from_utf8("frontend-1-new-secret".to_string()),
            },
        ];

        let client = build_frontend_control_plane_client(&config, "/tmp/argmin-control-plane.sock")
            .expect("frontend auth client should build");

        assert!(client.is_authenticated());
    }

    #[test]
    fn frontend_control_plane_client_sends_authenticated_runtime_map_read_from_config() {
        let tmp = short_unix_socket_test_dir("frontend-auth-runtime-map");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let state_path = tmp.join("control-plane.state");
        let node_id = NodeId::new(1);
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_frontend_auth_instance_id = Some("frontend-1".to_string());
        config.control_plane_frontend_auth_credentials =
            vec![ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("frontend-1-secret".to_string()),
            }];
        config.control_plane_admin_auth_instance_id = Some("admin-1".to_string());
        config.control_plane_admin_auth_credentials =
            vec![ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 1,
                secret: BinarySecretConfigValue::from_utf8("admin-secret".to_string()),
            }];
        let server_auth = build_control_plane_rpc_server_auth(&config)
            .expect("test frontend server auth should build");
        let server_auth_for_assert = server_auth.clone();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(node_id, storage::control_plane::NodeMembershipState::Active)
            .unwrap();
        let server = spawn_control_plane_test_rpc_server(
            listener,
            Arc::new(Mutex::new(authority)),
            [2_000],
            Some(server_auth),
        );

        let client = build_frontend_control_plane_client(&config, socket_path.to_str().unwrap())
            .expect("frontend auth client should build");
        let _runtime_map =
            storage::clock::with_time_override(2_000, || client.runtime_map_snapshot(2_000))
                .expect("authenticated frontend runtime-map read should succeed");

        server.join().unwrap();
        let diagnostics = server_auth_for_assert.diagnostics().unwrap();
        assert!(diagnostics.contains("accepted_total=1"), "{diagnostics}");
        assert!(
            diagnostics.contains("accepted_by_operation{operation=\"FrontendRuntimeMapRead\"} 1"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("rejected_total=0"), "{diagnostics}");
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn configured_authority_clock_command_client_uses_recovery_endpoints() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("static-topology-cluster".to_string());
        config.control_plane_admin_auth_instance_id = Some("admin-1".to_string());
        config.control_plane_admin_auth_credentials =
            vec![ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 9,
                secret: BinarySecretConfigValue::from_utf8("admin-secret".to_string()),
            }];
        config.control_plane_clock_recovery_rpc_client_endpoints =
            [("control-1.internal", 7601), ("control-2.internal", 7602)]
                .into_iter()
                .map(|(host, port)| {
                    ControlPlaneRpcClientEndpoint::tls_tcp(
                        format!("tcp://{host}:{port}"),
                        "127.0.0.1",
                        port,
                        host,
                        Duration::from_secs(1),
                        rustls::RootCertStore::empty(),
                    )
                    .unwrap()
                })
                .collect();

        let client = build_admin_clock_recovery_client_from_config(
            &config,
            Path::new("/tmp/unused-control.sock"),
        )
        .unwrap();
        assert!(client.is_authenticated());
        let debug = format!("{client:?}");
        assert!(debug.contains("authenticated: true"));
        assert!(debug.contains("transport: \"<opaque>\""));
        assert!(!debug.contains("control-1.internal"));
        assert!(!debug.contains("admin-secret"));
        assert!(!debug.contains("unused-control.sock"));
    }

    #[test]
    fn frontend_control_plane_client_requires_local_auth_credential() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_frontend_auth_instance_id = Some("frontend-2".to_string());
        config.control_plane_frontend_auth_credentials =
            vec![ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("frontend-1-secret".to_string()),
            }];

        let result = build_frontend_control_plane_client(&config, "/tmp/argmin-control-plane.sock");
        let Err(err) = result else {
            panic!("frontend control-plane client should reject missing local credential");
        };

        assert!(err.contains("local frontend instance id frontend-2"));
    }

    #[test]
    fn control_plane_rpc_server_auth_includes_frontend_credentials() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_frontend_auth_credentials =
            vec![ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("frontend-1-secret".to_string()),
            }];
        config.control_plane_admin_auth_credentials =
            vec![ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 8,
                secret: BinarySecretConfigValue::from_utf8("admin-1-secret".to_string()),
            }];

        let auth = build_control_plane_rpc_server_auth(&config)
            .expect("frontend server auth should build");
        let diagnostics = auth.diagnostics().expect("server auth should be enabled");

        assert!(diagnostics.contains("required=true"), "{diagnostics}");
        assert!(
            diagnostics.contains("storage_node_heartbeat_required=false"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("frontend_runtime_map_required=true"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("admin_control_plane_required=true"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("storage_node_credentials=0"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("frontend_credentials=1"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("admin_credentials=1"), "{diagnostics}");
        assert!(
            diagnostics.contains("instance_id=\"frontend-1\""),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("instance_id=\"admin-1\""),
            "{diagnostics}"
        );
    }

    #[test]
    fn control_plane_rpc_server_auth_rejects_frontend_without_admin_credentials() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_frontend_auth_credentials =
            vec![ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("frontend-1-secret".to_string()),
            }];

        let error = build_control_plane_rpc_server_auth(&config)
            .expect_err("frontend server auth should require admin credentials");

        assert!(error.contains("admin control-plane credentials are required"));
    }

    #[test]
    fn control_plane_rpc_server_auth_includes_admin_credentials() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_admin_auth_credentials =
            vec![ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 7,
                secret: BinarySecretConfigValue::from_utf8("admin-1-secret".to_string()),
            }];

        let auth =
            build_control_plane_rpc_server_auth(&config).expect("admin server auth should build");
        let diagnostics = auth.diagnostics().expect("server auth should be enabled");

        assert!(diagnostics.contains("required=true"), "{diagnostics}");
        assert!(
            diagnostics.contains("storage_node_heartbeat_required=false"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("frontend_runtime_map_required=false"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("admin_control_plane_required=true"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("storage_node_credentials=0"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("frontend_credentials=0"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("admin_credentials=1"), "{diagnostics}");
        assert!(
            diagnostics.contains("instance_id=\"admin-1\""),
            "{diagnostics}"
        );
    }

    #[test]
    fn lease_expiry_clock_wait_classifier_covers_committed_timestamp_guards() {
        assert!(control_plane_lease_expiry_error_is_clock_wait(
            &ControlPlaneError::CommittedTimestampRegression {
                timestamp_ms: 999,
                max_committed_timestamp_ms: 1_001,
            }
        ));
        assert!(control_plane_lease_expiry_error_is_clock_wait(
            &ControlPlaneError::CommittedTimestampTooFarAhead {
                timestamp_ms: 3_601_002,
                max_committed_timestamp_ms: 1_001,
                max_forward_jump_ms: 3_600_000,
            }
        ));
        assert!(control_plane_lease_expiry_error_is_clock_wait(
            &ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: Some(
                    storage::control_plane::ControlPlaneAuthorityClockBlockedReason::WallClockRegression,
                ),
            }
        ));
        assert!(control_plane_lease_expiry_error_is_clock_wait(
            &ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
                authority_now_ms: 1_999,
                fenced_until_ms: 2_000,
            }
        ));
        assert!(!control_plane_lease_expiry_error_is_clock_wait(
            &ControlPlaneError::InvalidLeaseDuration
        ));
    }

    #[test]
    fn raft_lease_expiry_classifier_covers_term_change() {
        assert!(experimental_raft_lease_expiry_error_is_transient(
            &ControlPlaneError::LeaseGrantHorizonAuthorityTermMismatch {
                authority_term: Some(2),
                committed_term: Some(3),
            }
        ));
    }

    #[test]
    fn successor_expiry_waits_for_one_heartbeat_attempt_after_predecessor_fence() {
        assert_eq!(
            successor_heartbeat_renewal_not_before_ms(10_000).unwrap(),
            12_000
        );
        assert!(matches!(
            successor_heartbeat_renewal_not_before_ms(u64::MAX),
            Err(ControlPlaneError::LeaseDeadlineOverflow)
        ));
    }

    #[test]
    fn experimental_raft_startup_catch_up_rechecks_advanced_committed_watermark() {
        struct ScriptedCatchUpSource {
            statuses: Mutex<std::collections::VecDeque<(Option<u64>, Option<u64>)>>,
            waited_for: Mutex<Vec<u64>>,
        }

        impl ExperimentalRaftStartupCatchUpSource for ScriptedCatchUpSource {
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
            statuses: Mutex::new(std::collections::VecDeque::from([
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
            .block_on(wait_for_experimental_raft_startup_catch_up_from(
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

    struct ExperimentalRaftTestHarness {
        runtime: tokio::runtime::Runtime,
        authority: Arc<ControlPlaneRaftAuthority>,
        control_plane: ExperimentalRaftControlPlane,
        _owned_state_dir: Option<test_util::TempDir>,
    }

    impl ExperimentalRaftTestHarness {
        fn shutdown(self) {
            self.runtime
                .block_on(self.authority.shutdown())
                .expect("experimental raft authority should shut down");
        }
    }

    fn enable_resampled_authority_time(
        control_plane: &mut ExperimentalRaftControlPlane,
        now_ms: u64,
    ) {
        let max_committed_timestamp_ms = control_plane
            .current_snapshot()
            .expect("experimental snapshot should read before enabling clock resampling")
            .max_committed_timestamp_ms();
        let status = control_plane
            .block_on(control_plane.authority.status())
            .expect("experimental status should read before enabling clock resampling");
        let mut authority_clock =
            ControlPlaneAuthorityClock::new(max_committed_timestamp_ms, now_ms, Some(now_ms))
                .expect("test authority clock should initialize");
        authority_clock.bind_initial_raft_leadership_term(
            status
                .local_leader()
                .then(|| status.current_term())
                .flatten(),
        );
        control_plane.authority_clock = Some(Arc::new(Mutex::new(authority_clock)));
        control_plane.resample_authority_time = true;
    }

    fn experimental_raft_test_harness(name: &str) -> ExperimentalRaftTestHarness {
        let state_dir = test_util::tempdir();
        let artifact_path = state_dir.path().join("control-plane-raft.state");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let handle = runtime.handle().clone();
        let authority = runtime.block_on(async {
            let cluster_name = format!("argmin-s3-experimental-raft-{name}-{}", std::process::id());
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory_with_checkpoint_for_test(
                cluster_name,
                1,
                &artifact_path,
            )
            .await
            .expect("experimental raft authority should initialize");
            authority
                .initialize_single_node_membership(1)
                .await
                .expect("single-node raft membership should initialize");
            authority
                .wait_for_current_leader(
                    1,
                    Duration::from_secs(1),
                    "experimental process test leadership",
                )
                .await
                .expect("single-node raft should become leader");
            wait_for_experimental_raft_local_authority_serving(
                &authority,
                Duration::from_secs(1),
                "experimental process test committed membership",
            )
            .await
            .expect("single-node raft should apply committed membership and become serving");
            Arc::new(authority)
        });
        let control_plane = ExperimentalRaftControlPlane {
            runtime: handle,
            authority: Arc::clone(&authority),
            durable_artifact_path: None,
            durable_checkpoint_lock: None,
            durable_serving_checkpoint: Arc::new(Mutex::new(None)),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        ExperimentalRaftTestHarness {
            runtime,
            authority,
            control_plane,
            _owned_state_dir: Some(state_dir),
        }
    }

    #[test]
    fn cloned_raft_wrapper_suppresses_response_publication_after_concurrent_poison() {
        let harness = experimental_raft_test_harness("cloned-response-poison");
        let in_flight = harness.control_plane.clone();
        let poisoner = harness.control_plane.clone();
        let publication = in_flight
            .authority
            .durability_publication()
            .expect("test authority durability publication should initialize");
        let published = Arc::new(AtomicBool::new(false));
        let worker_published = Arc::clone(&published);
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            in_flight
                .ensure_not_durably_poisoned()
                .expect("in-flight clone should pass its initial poison check");
            admitted_tx
                .send(())
                .expect("in-flight clone should report admission");
            resume_rx
                .recv()
                .expect("in-flight clone should be released after poison");
            let mut publish = || {
                worker_published.store(true, Ordering::Release);
                Ok(())
            };
            ControlPlaneRpcResponsePublication::publish(&publication, &mut publish)
        });
        admitted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("in-flight clone should pass its initial poison check");

        poisoner.poison_durable_authority(
            "test durability failure after another clone passed admission".to_owned(),
        );
        resume_tx
            .send(())
            .expect("in-flight clone should resume after poison publication");
        let error = worker
            .join()
            .expect("in-flight clone worker should exit")
            .expect_err("response publication after poison must fail closed");
        assert!(matches!(error, ControlPlaneError::DurabilityFailure { .. }));
        assert!(!published.load(Ordering::Acquire));
        assert!(harness.control_plane.ensure_not_durably_poisoned().is_err());
        harness.shutdown();
    }

    fn experimental_raft_peer_auth_credential(
        cluster_name: &str,
        node_id: ControlPlaneRaftNodeId,
        credential_id: &str,
        credential_version: u64,
        secret: &str,
    ) -> ControlPlaneScopedCredential {
        ControlPlaneScopedCredential::new(ControlPlaneScopedCredentialInput {
            cluster_id: cluster_name.to_string(),
            credential_id: credential_id.to_string(),
            credential_version,
            principal: ControlPlaneAuthPrincipal::RaftPeer { node_id },
            secret: secret.as_bytes().to_vec(),
        })
        .expect("test auth credential should build")
    }

    fn experimental_raft_peer_auth_policy(
        cluster_name: &str,
        local_node_id: ControlPlaneRaftNodeId,
        node_1_version: u64,
    ) -> ControlPlaneRaftPeerAuthPolicy {
        let node_1 = experimental_raft_peer_auth_credential(
            cluster_name,
            1,
            "raft-node-1",
            node_1_version,
            "node-1-test-secret",
        );
        let node_2 = experimental_raft_peer_auth_credential(
            cluster_name,
            2,
            "raft-node-2",
            1,
            "node-2-test-secret",
        );
        let local_credential = match local_node_id {
            1 => node_1.clone(),
            2 => node_2.clone(),
            other => panic!("unexpected test local node id {other}"),
        };
        ControlPlaneRaftPeerAuthPolicy::new(
            local_credential,
            ControlPlaneScopedCredentialStore::new(vec![node_1, node_2])
                .expect("test auth verifier store should build"),
        )
        .expect("test peer auth policy should build")
    }

    fn experimental_raft_durable_test_harness(
        name: &str,
        state_path: &Path,
    ) -> ExperimentalRaftTestHarness {
        experimental_raft_durable_test_harness_inner(name, state_path)
    }

    fn experimental_raft_durable_wal_test_harness(
        name: &str,
        state_path: &Path,
    ) -> ExperimentalRaftTestHarness {
        experimental_raft_durable_test_harness_inner(name, state_path)
    }

    fn experimental_raft_durable_test_harness_inner(
        name: &str,
        state_path: &Path,
    ) -> ExperimentalRaftTestHarness {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let handle = runtime.handle().clone();
        let authority = runtime.block_on(async {
            let cluster_name = format!(
                "argmin-s3-experimental-durable-raft-{name}-{}",
                std::process::id()
            );
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name,
                1,
                state_path,
            )
            .await
            .expect("durable experimental raft authority should initialize");
            if !authority
                .is_initialized()
                .await
                .expect("durable raft initialization status should read")
            {
                authority
                    .initialize_single_node_membership(1)
                    .await
                    .expect("single-node durable raft membership should initialize");
                authority
                    .store_durable_restart_artifact()
                    .await
                    .expect("single-node durable raft membership should checkpoint");
            }
            authority
                .wait_for_current_leader(
                    1,
                    Duration::from_secs(1),
                    "durable experimental process test leadership",
                )
                .await
                .expect("single-node durable raft should become leader");
            wait_for_experimental_raft_local_authority_serving(
                &authority,
                Duration::from_secs(1),
                "durable experimental process test committed replay",
            )
            .await
            .expect("single-node durable raft should apply committed prefix");
            authority
                .store_durable_restart_artifact()
                .await
                .expect("single-node durable raft startup should checkpoint");
            Arc::new(authority)
        });
        let control_plane = ExperimentalRaftControlPlane {
            runtime: handle,
            authority: Arc::clone(&authority),
            durable_artifact_path: Some(Arc::new(state_path.to_path_buf())),
            durable_checkpoint_lock: Some(Arc::new(Mutex::new(()))),
            durable_serving_checkpoint: Arc::new(Mutex::new(None)),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        ExperimentalRaftTestHarness {
            runtime,
            authority,
            control_plane,
            _owned_state_dir: None,
        }
    }

    fn spawn_experimental_raft_unix_rpc_server(
        harness: &ExperimentalRaftTestHarness,
        socket_path: &Path,
        authority_now_ms: u64,
    ) -> std::thread::JoinHandle<()> {
        spawn_experimental_raft_unix_rpc_server_requests(harness, socket_path, authority_now_ms, 1)
    }

    fn spawn_experimental_raft_unix_rpc_server_requests(
        harness: &ExperimentalRaftTestHarness,
        socket_path: &Path,
        authority_now_ms: u64,
        request_count: usize,
    ) -> std::thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(socket_path).unwrap();
        let control_plane = ExperimentalRaftControlPlane {
            runtime: harness.runtime.handle().clone(),
            authority: Arc::clone(&harness.authority),
            durable_artifact_path: None,
            durable_checkpoint_lock: None,
            durable_serving_checkpoint: Arc::new(Mutex::new(None)),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        spawn_control_plane_test_rpc_server(
            listener,
            Arc::new(Mutex::new(control_plane)),
            (0..request_count)
                .map(move |request_index| authority_now_ms + u64::try_from(request_index).unwrap()),
            None,
        )
    }

    #[test]
    fn root_process_check_rejects_effective_uid_zero() {
        assert_eq!(reject_root_process(0), Err(ROOT_PROCESS_ERROR));
    }

    #[test]
    fn experimental_raft_control_plane_bootstraps_runtime_map() {
        let mut harness = experimental_raft_test_harness("process-bootstrap-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![0];

        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let runtime_map = harness
            .control_plane
            .runtime_map_snapshot(10_000)
            .expect("experimental raft runtime map should serve");
        assert_eq!(runtime_map.nodes().len(), 1);
        assert_eq!(runtime_map.nodes()[0].node_id(), NodeId::new(1));
        assert_eq!(
            runtime_map.nodes()[0].endpoint(),
            "/tmp/argmin-experimental-raft-node-1.sock"
        );
        assert_eq!(runtime_map.pg_routes().len(), 1);
        assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(0));

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_binds_configured_peer_listener() {
        let test_dir = short_unix_socket_test_dir("raft-peer-listener");
        let _ = fs::remove_dir_all(&test_dir);
        let peer_socket_path = test_dir.join("control-plane-raft-peer.sock");
        let peer_socket = peer_socket_path.display().to_string();
        let mut config = test_server_config();
        config.control_plane_experimental_raft = true;
        config.control_plane_raft_cluster_name = Some("process-peer-listener-test".to_string());
        config.control_plane_raft_node_id = Some(1);
        config.control_plane_raft_peer_socket_path = Some(peer_socket.clone());
        config.control_plane_raft_peer_sockets =
            vec![config::ConfiguredControlPlaneRaftPeerSocket {
                node_id: 1,
                socket_path: peer_socket,
            }];

        let listeners =
            bind_experimental_raft_peer_listener(&config, "process-peer-listener-test", 1)
                .expect("peer listener should bind");

        assert_eq!(listeners.len(), 1);
        assert!(peer_socket_path.exists());
        drop(listeners);
        fs::remove_file(&peer_socket_path).unwrap();
        fs::remove_dir(&test_dir).unwrap();
    }

    #[test]
    fn experimental_raft_control_plane_binds_configured_tcp_peer_listener() {
        let mut config = test_server_config();
        config.control_plane_experimental_raft = true;
        config.control_plane_raft_cluster_name = Some("process-tcp-peer-listener-test".to_string());
        config.control_plane_raft_node_id = Some(1);
        config.control_plane_raft_peer_sockets =
            vec![config::ConfiguredControlPlaneRaftPeerSocket {
                node_id: 1,
                socket_path: "tcp://localhost:7401".to_string(),
            }];
        let certificates = CertificateDer::pem_slice_iter(include_bytes!(
            "../../s3-tests/testdata/localhost-cert.pem"
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let private_key = PrivateKeyDer::from_pem_slice(include_bytes!(
            "../../s3-tests/testdata/localhost-key.pem"
        ))
        .unwrap();
        let signing_key = rustls::crypto::ring::default_provider()
            .key_provider
            .load_private_key(private_key)
            .unwrap();
        let certified_key = Arc::new(CertifiedKey::new(certificates, signing_key));
        config.control_plane_raft_peer_listeners =
            vec![ConfiguredControlPlaneRaftPeerListener::Tcp {
                endpoint_id: "raft-tcp-1".to_string(),
                bind_addr: "127.0.0.1:0".to_string(),
                certified_key,
                max_connections: 7,
                io_timeout: Duration::from_secs(3),
            }];

        let listeners =
            bind_experimental_raft_peer_listener(&config, "process-tcp-peer-listener-test", 1)
                .expect("TCP peer listener should bind");

        assert_eq!(listeners.len(), 1);
        let debug = format!("{:?}", listeners[0]);
        assert!(debug.contains("raft-tcp-1"));
        assert!(debug.contains("tls-tcp"));
        assert!(debug.contains("max_connections: 7"));
        assert!(debug.contains("3s"));
        assert!(!debug.contains("PRIVATE"));
    }

    #[test]
    fn experimental_raft_startup_leader_wait_tracks_peer_policy_size() {
        assert!(experimental_raft_startup_requires_local_leader(None));
        assert!(experimental_raft_startup_initializes_membership(None, 1));

        let single_node_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            "process-peer-startup-leader-wait-test",
            [(1, "/tmp/argmin-raft-node-1.sock".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        assert!(experimental_raft_startup_requires_local_leader(Some(
            &single_node_policy
        )));
        assert!(experimental_raft_startup_initializes_membership(
            Some(&single_node_policy),
            1
        ));

        let multi_node_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            "process-peer-startup-leader-wait-test",
            [
                (1, "/tmp/argmin-raft-node-1.sock".to_string()),
                (2, "/tmp/argmin-raft-node-2.sock".to_string()),
            ],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        assert!(!experimental_raft_startup_requires_local_leader(Some(
            &multi_node_policy
        )));
        assert!(experimental_raft_startup_initializes_membership(
            Some(&multi_node_policy),
            1
        ));
        assert!(!experimental_raft_startup_initializes_membership(
            Some(&multi_node_policy),
            2
        ));
    }

    #[test]
    fn experimental_raft_control_plane_bootstrap_does_not_rewrite_existing_state() {
        let mut harness = experimental_raft_test_harness("process-bootstrap-idempotence-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![0];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let initial_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after bootstrap");

        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 2,
            socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
        }];
        config.storage_pg_ids = vec![1];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap retry should succeed");
        let retried_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after bootstrap retry");

        assert_eq!(
            retried_snapshot.cluster_epoch(),
            initial_snapshot.cluster_epoch()
        );
        assert!(retried_snapshot.node(NodeId::new(1)).is_some());
        assert!(retried_snapshot.node(NodeId::new(2)).is_none());
        assert!(retried_snapshot.pg(PgId::new(0)).is_some());
        assert!(retried_snapshot.pg(PgId::new(1)).is_none());

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_heartbeat_completes_ready_peering() {
        let mut harness = experimental_raft_test_harness("heartbeat-peering-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![7];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                20_000,
            )
            .expect("experimental raft startup heartbeat should refresh");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let proof = PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        let peering_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_100,
            )
            .expect("experimental raft heartbeat should refresh");
        assert_eq!(peering_refresh.lease().lease_deadline_ms(), 20_600);
        let peering_route = &peering_refresh.runtime_map().pg_routes()[0];
        assert_eq!(peering_route.pg_id(), PgId::new(7));
        assert_eq!(peering_route.state(), PgState::Active);
        assert_eq!(peering_route.primary_node_id(), NodeId::new(1));
        assert_eq!(peering_route.primary_lease_deadline_ms(), Some(20_600));

        let active_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after peering completion");
        let pg = active_snapshot.pg(PgId::new(7)).expect("PG should exist");
        assert_eq!(pg.state(), PgState::Active);
        assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
        assert_eq!(pg.active_metadata_proof(), Some(proof));
        assert_eq!(pg.active_metadata_proof_epoch(), Some(peering_epoch));
        let active_epoch = active_snapshot.cluster_epoch();
        let applied_before_active_observation = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read before active observation")
            .applied();

        let active_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_200,
            )
            .expect("experimental raft active heartbeat should refresh");
        assert!(active_refresh.lease().serving());
        assert_eq!(active_refresh.lease().lease_deadline_ms(), 20_800);
        let active_route = &active_refresh.runtime_map().pg_routes()[0];
        assert_eq!(active_route.state(), PgState::Active);
        assert_eq!(active_route.primary_lease_deadline_ms(), Some(20_800));
        let applied_after_active_observation = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read after active observation")
            .applied();
        assert_ne!(
            applied_after_active_observation, applied_before_active_observation,
            "a changed Active proof must be durable for metadata-transfer authorization"
        );

        let steady_active_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_300,
            )
            .expect("unchanged experimental raft active heartbeat should refresh");
        assert!(steady_active_refresh.lease().serving());
        assert_eq!(steady_active_refresh.lease().lease_deadline_ms(), 20_900);
        let frontend_runtime_map = harness
            .control_plane
            .runtime_map_snapshot(20_300)
            .expect("linearized frontend runtime map should include volatile heartbeat state");
        assert_eq!(
            frontend_runtime_map.pg_routes()[0].primary_lease_deadline_ms(),
            Some(20_900)
        );
        assert_eq!(
            harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("experimental Raft status should read after active renewal")
                .applied(),
            applied_after_active_observation,
            "an unchanged active heartbeat must update the runtime map without log progress"
        );

        let response = harness
            .control_plane
            .submit_raft_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(1),
                availability: NodeAvailabilityState::Unavailable,
            })
            .expect("availability fence should commit after volatile renewal");
        assert_eq!(response, ControlPlaneCommandResponse::MarkNodeAvailability);
        let fenced = harness
            .control_plane
            .current_snapshot()
            .expect("availability fence snapshot should read");
        assert_eq!(
            fenced
                .pg(PgId::new(7))
                .expect("fenced PG should remain present")
                .previous_primary_lease_deadline_ms(),
            Some(20_900),
            "the durable availability transition must preserve the acknowledged volatile lease"
        );
        let durable_previous_deadline = harness
            .control_plane
            .block_on(harness.authority.durable_state_machine_snapshot_for_test())
            .expect("durable availability fence state should read")
            .pg(PgId::new(7))
            .and_then(|pg| pg.previous_primary_lease_deadline_ms());
        assert_eq!(durable_previous_deadline, Some(20_900));

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_production_shaped_heartbeat_write_amplification_gate() {
        const STORAGE_NODE_COUNT: u32 = 3;
        const PG_COUNT: u32 = 116;
        const RETAINED_HISTORY_EPOCHS: usize = 256;
        const LARGE_RESTART_ARTIFACT_MIN_BYTES: u64 = 24 * 1024 * 1024;
        const LARGE_RESTART_ARTIFACT_PADDED_ENTRIES: u64 = 256;
        const SUSTAINED_PEERING_INTERVALS: u64 = 2;
        const SUSTAINED_PEERING_ROUNDS: u64 = 600;
        const SUSTAINED_PEERING_ROUND_MS: u64 = 100;
        const POST_PURGE_ARTIFACT_MAX_BYTES: u64 = 1024 * 1024;
        const STEADY_HEARTBEAT_ROUNDS: u64 = 64;
        const CONCURRENT_CHECKPOINT_COUNT: u64 = 8;
        const CONCURRENT_HEARTBEAT_ROUNDS: u64 = 16;
        const HEARTBEAT_LEASE_MS: u64 = 10_000;
        const MAX_AMORTIZED_DURABLE_BYTES_PER_SECOND: u64 = 1024 * 1024;

        fn heartbeat(
            node_id: u32,
            endpoint: &str,
            observed_epoch: ClusterEpoch,
            pg_observations: Vec<NodePgHeartbeatObservation>,
        ) -> NodeHeartbeat {
            NodeHeartbeat {
                node_id: NodeId::new(node_id),
                node_incarnation: 1,
                endpoint: endpoint.to_owned(),
                observed_epoch,
                requested_lease_duration_ms: HEARTBEAT_LEASE_MS,
                cluster_map_history_route_references: Default::default(),
                pg_observations,
            }
        }

        fn durable_wal_offsets(
            harness: &ExperimentalRaftTestHarness,
        ) -> storage::control_plane_raft::ControlPlaneRaftWalOffsets {
            harness
                .authority
                .durable_wal_monitor_snapshot()
                .expect("production-shaped WAL monitor snapshot should read")
                .offsets()
        }

        fn durable_snapshot(harness: &ExperimentalRaftTestHarness) -> ClusterControlSnapshot {
            harness
                .control_plane
                .block_on(harness.authority.durable_state_machine_snapshot_for_test())
                .expect("durable state-machine snapshot should read")
        }

        fn active_primary_observations(
            snapshot: &ClusterControlSnapshot,
            node_id: u32,
            metadata_proof: PgMetadataProof,
        ) -> Vec<NodePgHeartbeatObservation> {
            snapshot
                .pgs()
                .filter(|pg| pg.active_primary() == Some(NodeId::new(node_id)))
                .map(|pg| NodePgHeartbeatObservation {
                    pg_id: pg.pg_id(),
                    state: PgState::Active,
                    metadata_proof,
                    pending_metadata_command: None,
                })
                .collect()
        }

        let state_dir = short_unix_socket_test_dir("raft-write-amplification-gate");
        let state_path = state_dir.0.path().join("control-plane.state");
        let endpoints = (0..STORAGE_NODE_COUNT)
            .map(|node_id| {
                state_dir
                    .0
                    .path()
                    .join(format!("storage-node-{node_id}.sock"))
                    .display()
                    .to_string()
            })
            .collect::<Vec<_>>();
        let mut config = test_server_config();
        config.storage_node_sockets = endpoints
            .iter()
            .enumerate()
            .map(
                |(node_id, socket_path)| config::ConfiguredStorageNodeSocket {
                    node_id: u32::try_from(node_id).expect("test node id should fit u32"),
                    socket_path: socket_path.clone(),
                },
            )
            .collect();
        config.storage_pg_ids = (0..PG_COUNT).collect();

        let mut harness = experimental_raft_durable_wal_test_harness(
            "production-shaped-write-amplification",
            &state_path,
        );
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("production-shaped Raft bootstrap should succeed");

        let acting_set_a = vec![NodeId::new(0), NodeId::new(1)];
        let acting_set_b = vec![NodeId::new(1), NodeId::new(2)];
        let mut use_acting_set_b = vec![false; usize::try_from(PG_COUNT).unwrap()];
        for change_index in 0..(RETAINED_HISTORY_EPOCHS - 1) {
            let pg_index = change_index % usize::try_from(PG_COUNT).unwrap();
            let acting_set = if use_acting_set_b[pg_index] {
                acting_set_b.clone()
            } else {
                acting_set_a.clone()
            };
            use_acting_set_b[pg_index] = !use_acting_set_b[pg_index];
            harness
                .control_plane
                .submit_raft_command(ControlPlaneCommand::SetPgActingSet {
                    pg_id: PgId::new(u32::try_from(pg_index).unwrap()),
                    acting_set,
                })
                .expect("production-shaped route change should commit");
        }
        let shaped_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("production-shaped snapshot should read");
        assert_eq!(shaped_snapshot.pgs().count(), PG_COUNT as usize);
        assert_eq!(
            shaped_snapshot.cluster_map_history().len(),
            RETAINED_HISTORY_EPOCHS,
            "release workload must exercise the full ordinary retained-history window"
        );

        let mut now_ms = 1_000_000_u64;
        let mut heartbeat_requests = 0_u64;
        let mut stable = false;
        for _ in 0..8 {
            let applied_before = harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("pre-activation Raft status should read")
                .applied();
            for node_id in 0..STORAGE_NODE_COUNT {
                let observed_epoch = harness
                    .control_plane
                    .current_snapshot()
                    .expect("heartbeat epoch should read")
                    .cluster_epoch();
                harness
                    .control_plane
                    .refresh_node_heartbeat(
                        heartbeat(
                            node_id,
                            &endpoints[usize::try_from(node_id).unwrap()],
                            observed_epoch,
                            Vec::new(),
                        ),
                        now_ms,
                    )
                    .expect("pre-activation heartbeat should refresh");
                heartbeat_requests += 1;
                now_ms += 1;
            }
            let applied_after = harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("post-heartbeat Raft status should read")
                .applied();
            if applied_after == applied_before {
                stable = true;
                break;
            }
        }
        assert!(stable, "storage-node heartbeat state should converge");

        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("peering snapshot should read")
            .cluster_epoch();
        harness.control_plane.checkpoint_serving_reads = true;

        // Reproduce the retained-log artifact size from the soak failure
        // without changing control-plane state. These deterministic
        // rejections still advance the applied Raft cursor and are
        // recoverable from the WAL.
        let padded_endpoint = "x".repeat(96 * 1024);
        for _ in 0..LARGE_RESTART_ARTIFACT_PADDED_ENTRIES {
            let error = harness
                .control_plane
                .submit_raft_liveness_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(0), padded_endpoint.clone())],
                    pg_ids: vec![PgId::new(0)],
                })
                .expect_err("repeated bootstrap padding command should be rejected");
            assert!(matches!(
                error,
                ControlPlaneError::BootstrapRequiresEmptyState
            ));
        }
        let pre_restart_status = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("pre-restart retained-log status should read");
        assert_eq!(
            pre_restart_status.last_purged_log_id(),
            None,
            "automatic OpenRaft snapshot-driven purge must remain disabled"
        );
        assert_eq!(
            pre_restart_status.current_snapshot(),
            None,
            "automatic OpenRaft snapshot construction must remain disabled"
        );
        let expected_before_restart = durable_snapshot(&harness);
        harness.shutdown();
        harness = experimental_raft_durable_wal_test_harness(
            "production-shaped-write-amplification",
            &state_path,
        );
        assert_eq!(
            durable_snapshot(&harness),
            expected_before_restart,
            "restart must recover the large retained WAL suffix before monitor checkpoint"
        );
        let large_artifact_bytes = harness
            .authority
            .durability_metric_snapshots()
            .checkpoint
            .bytes_last;
        assert!(
            large_artifact_bytes >= LARGE_RESTART_ARTIFACT_MIN_BYTES,
            "release workload restart artifact {large_artifact_bytes} bytes does not reproduce the production-sized checkpoint"
        );

        let monitor_durability = ExperimentalRaftPeerDurabilityContext {
            artifact_path: harness.control_plane.durable_artifact_path.clone(),
            checkpoint_lock: harness
                .control_plane
                .durable_checkpoint_lock
                .clone()
                .expect("durable test authority should retain a checkpoint lock"),
        };
        let mut monitor_tracker = ExperimentalRaftPeerCheckpointTracker::default();
        let peering_monitor_started_at = Instant::now();
        assert!(
            !checkpoint_experimental_raft_peer_wal_if_due(
                &harness.control_plane.runtime,
                &harness.authority,
                &monitor_durability,
                &mut monitor_tracker,
                peering_monitor_started_at,
            )
            .expect("clean large-artifact WAL observation should succeed"),
            "clean WAL must only initialize the checkpoint tracker"
        );
        let sustained_metrics_before = harness.authority.durability_metric_snapshots();
        let sustained_offsets_before = harness
            .authority
            .durable_wal_monitor_snapshot()
            .expect("pre-peering WAL monitor snapshot should read")
            .offsets();
        let mut peering_monitor_checkpoint_total = 0_u64;
        let mut post_purge_artifact_bytes = Vec::new();
        for interval in 0..SUSTAINED_PEERING_INTERVALS {
            for interval_round in 0..SUSTAINED_PEERING_ROUNDS {
                let round = interval
                    .saturating_mul(SUSTAINED_PEERING_ROUNDS)
                    .saturating_add(interval_round);
                for node_id in 0..STORAGE_NODE_COUNT {
                    let node_proof = PgMetadataProof {
                        applied_log_index: round + 2,
                        applied_log_hash: round
                            .saturating_mul(u64::from(STORAGE_NODE_COUNT))
                            .saturating_add(u64::from(node_id))
                            .saturating_add(3),
                        state_digest: round
                            .saturating_mul(u64::from(STORAGE_NODE_COUNT))
                            .saturating_add(u64::from(node_id))
                            .saturating_add(4),
                    };
                    let snapshot = harness
                        .control_plane
                        .current_snapshot()
                        .expect("sustained Peering observation snapshot should read");
                    let pg_observations = snapshot
                        .pgs()
                        .filter(|pg| pg.acting_set().contains(&NodeId::new(node_id)))
                        .map(|pg| NodePgHeartbeatObservation {
                            pg_id: pg.pg_id(),
                            state: PgState::Peering,
                            metadata_proof: node_proof,
                            pending_metadata_command: None,
                        })
                        .collect();
                    harness
                        .control_plane
                        .refresh_node_heartbeat(
                            heartbeat(
                                node_id,
                                &endpoints[usize::try_from(node_id).unwrap()],
                                peering_epoch,
                                pg_observations,
                            ),
                            now_ms,
                        )
                        .expect("mismatched-proof Peering heartbeat should refresh");
                    heartbeat_requests += 1;
                    now_ms += SUSTAINED_PEERING_ROUND_MS / u64::from(STORAGE_NODE_COUNT);
                }
                let monitor_now = peering_monitor_started_at
                    + Duration::from_millis(round.saturating_mul(SUSTAINED_PEERING_ROUND_MS));
                if checkpoint_experimental_raft_peer_wal_if_due(
                    &harness.control_plane.runtime,
                    &harness.authority,
                    &monitor_durability,
                    &mut monitor_tracker,
                    monitor_now,
                )
                .expect("sustained Peering WAL monitor poll should succeed")
                {
                    peering_monitor_checkpoint_total += 1;
                }
            }
            assert_eq!(
                peering_monitor_checkpoint_total,
                interval + 1,
                "each sustained Peering interval should produce one coordinated snapshot/purge"
            );
            let artifact_bytes = harness
                .authority
                .durability_metric_snapshots()
                .checkpoint
                .bytes_last;
            assert!(
                artifact_bytes <= POST_PURGE_ARTIFACT_MAX_BYTES,
                "coordinated interval {} retained a {}-byte artifact after purge",
                interval + 1,
                artifact_bytes
            );
            post_purge_artifact_bytes.push(artifact_bytes);
        }
        assert_eq!(
            peering_monitor_checkpoint_total, SUSTAINED_PEERING_INTERVALS,
            "each minute of sustained Peering traffic should produce one coordinated snapshot/purge"
        );
        assert!(
            post_purge_artifact_bytes[1] <= post_purge_artifact_bytes[0].saturating_add(64 * 1024),
            "post-purge artifacts must reach a bounded steady state: {post_purge_artifact_bytes:?}"
        );
        let sustained_metrics_after = harness.authority.durability_metric_snapshots();
        let sustained_checkpoint_bytes = sustained_metrics_after
            .checkpoint
            .bytes_total
            .checked_sub(sustained_metrics_before.checkpoint.bytes_total)
            .expect("sustained checkpoint bytes must advance monotonically");
        let sustained_wal_bytes = sustained_metrics_after
            .wal
            .expect("WAL-backed release authority should expose metrics")
            .frame_bytes_total
            .checked_sub(
                sustained_metrics_before
                    .wal
                    .expect("baseline WAL metrics should exist")
                    .frame_bytes_total,
            )
            .expect("sustained WAL bytes must advance monotonically");
        let sustained_duration_ms = SUSTAINED_PEERING_INTERVALS
            .saturating_mul(SUSTAINED_PEERING_ROUNDS)
            .saturating_mul(SUSTAINED_PEERING_ROUND_MS);
        let sustained_durable_bytes_per_second = sustained_checkpoint_bytes
            .checked_add(sustained_wal_bytes)
            .and_then(|bytes| bytes.checked_mul(1_000))
            .expect("sustained durable byte accounting should not overflow")
            .div_ceil(sustained_duration_ms);
        assert!(
            sustained_durable_bytes_per_second < MAX_AMORTIZED_DURABLE_BYTES_PER_SECOND,
            "sustained Peering durability rate {sustained_durable_bytes_per_second} B/s exceeds the release limit {MAX_AMORTIZED_DURABLE_BYTES_PER_SECOND} B/s"
        );
        let sustained_offsets_after = harness
            .authority
            .durable_wal_monitor_snapshot()
            .expect("post-sustained Peering WAL monitor snapshot should read")
            .offsets();
        assert_eq!(
            sustained_offsets_after.base_offset(),
            sustained_offsets_after.clean_len(),
            "the amortized checkpoint should compact sustained Peering WAL state"
        );
        assert!(
            sustained_offsets_after.base_offset() > sustained_offsets_before.base_offset(),
            "sustained Peering checkpoint must advance the WAL base"
        );

        let metadata_proof = PgMetadataProof {
            applied_log_index: SUSTAINED_PEERING_INTERVALS
                .saturating_mul(SUSTAINED_PEERING_ROUNDS)
                .saturating_add(2),
            applied_log_hash: 0xfeed,
            state_digest: 0xbeef,
        };
        for node_id in [0_u32, 2, 1] {
            let snapshot = harness
                .control_plane
                .current_snapshot()
                .expect("peering observation snapshot should read");
            assert_eq!(snapshot.cluster_epoch(), peering_epoch);
            let pg_observations = snapshot
                .pgs()
                .filter(|pg| pg.acting_set().contains(&NodeId::new(node_id)))
                .map(|pg| NodePgHeartbeatObservation {
                    pg_id: pg.pg_id(),
                    state: PgState::Peering,
                    metadata_proof,
                    pending_metadata_command: None,
                })
                .collect();
            harness
                .control_plane
                .refresh_node_heartbeat(
                    heartbeat(
                        node_id,
                        &endpoints[usize::try_from(node_id).unwrap()],
                        peering_epoch,
                        pg_observations,
                    ),
                    now_ms,
                )
                .expect("production-shaped peering heartbeat should refresh");
            heartbeat_requests += 1;
            now_ms += 1;
        }
        assert!(
            harness
                .control_plane
                .current_snapshot()
                .expect("active production-shaped snapshot should read")
                .pgs()
                .all(|pg| pg.state() == PgState::Active),
            "production-shaped workload should serve every PG before measurement"
        );

        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("active observation snapshot should read")
            .cluster_epoch();
        for node_id in 0..STORAGE_NODE_COUNT {
            let snapshot = harness
                .control_plane
                .current_snapshot()
                .expect("active observation routes should read");
            assert_eq!(snapshot.cluster_epoch(), active_epoch);
            let pg_observations = active_primary_observations(&snapshot, node_id, metadata_proof);
            harness
                .control_plane
                .refresh_node_heartbeat(
                    heartbeat(
                        node_id,
                        &endpoints[usize::try_from(node_id).unwrap()],
                        active_epoch,
                        pg_observations,
                    ),
                    now_ms,
                )
                .expect("current primary Active observation should refresh");
            heartbeat_requests += 1;
            now_ms += 1;
        }

        stable = false;
        for _ in 0..8 {
            let applied_before = harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("post-activation Raft status should read")
                .applied();
            for node_id in 0..STORAGE_NODE_COUNT {
                let snapshot = harness
                    .control_plane
                    .current_snapshot()
                    .expect("post-activation heartbeat state should read");
                let observed_epoch = snapshot.cluster_epoch();
                let pg_observations =
                    active_primary_observations(&snapshot, node_id, metadata_proof);
                harness
                    .control_plane
                    .refresh_node_heartbeat(
                        heartbeat(
                            node_id,
                            &endpoints[usize::try_from(node_id).unwrap()],
                            observed_epoch,
                            pg_observations,
                        ),
                        now_ms,
                    )
                    .expect("post-activation heartbeat should refresh");
                heartbeat_requests += 1;
                now_ms += 1;
            }
            let applied_after = harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("post-activation heartbeat status should read")
                .applied();
            if applied_after == applied_before {
                stable = true;
                break;
            }
        }
        assert!(stable, "active heartbeat state should converge");

        let read_checkpoint_before = harness.authority.durability_metric_snapshots().checkpoint;
        let pre_measurement_status = harness
            .control_plane
            .runtime_map_status(now_ms)
            .expect("active compact status should read without checkpointing");
        assert_eq!(
            pre_measurement_status.active_serving_pg_routes(),
            PG_COUNT as usize
        );
        let read_checkpoint_after = harness.authority.durability_metric_snapshots().checkpoint;
        assert_eq!(
            read_checkpoint_after.store_total, read_checkpoint_before.store_total,
            "a serving read must not convert the Peering WAL suffix into an artifact rewrite"
        );
        assert_eq!(
            read_checkpoint_after.file_sync_total, read_checkpoint_before.file_sync_total,
            "a serving read must not synchronously sync a liveness checkpoint"
        );

        let warm_status = harness
            .control_plane
            .runtime_map_status(now_ms)
            .expect("production-shaped compact status should warm its certificate");
        assert_eq!(warm_status.pg_routes(), PG_COUNT as usize);
        assert_eq!(warm_status.active_serving_pg_routes(), PG_COUNT as usize);
        let content_digest = warm_status
            .lease_renewal()
            .expect("active production-shaped status should carry a lease renewal")
            .content_digest();
        harness
            .control_plane
            .store_durable_restart_artifact()
            .expect("measurement baseline should compact setup WAL state");

        let durable_timestamp_before = harness
            .control_plane
            .current_snapshot()
            .expect("pre-measurement snapshot should read")
            .max_committed_timestamp_ms()
            .expect("heartbeat setup should establish a committed timestamp");
        let applied_before = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("pre-measurement Raft status should read")
            .applied();
        let wal_offsets_before = durable_wal_offsets(&harness);
        let durability_metrics_before = harness.authority.durability_metric_snapshots();
        assert_eq!(
            wal_offsets_before.base_offset(),
            wal_offsets_before.clean_len(),
            "warm checkpoint should leave no uncompacted WAL suffix"
        );

        let measured_heartbeats_before = heartbeat_requests;
        let mut monitor_poll_total = 0_u64;
        let mut monitor_poll_us_total = 0_u64;
        let mut monitor_poll_us_max = 0_u64;
        for _ in 0..STEADY_HEARTBEAT_ROUNDS {
            let snapshot = harness
                .control_plane
                .current_snapshot()
                .expect("steady heartbeat state should read");
            let observed_epoch = snapshot.cluster_epoch();
            for node_id in 0..STORAGE_NODE_COUNT {
                let pg_observations =
                    active_primary_observations(&snapshot, node_id, metadata_proof);
                harness
                    .control_plane
                    .refresh_node_heartbeat(
                        heartbeat(
                            node_id,
                            &endpoints[usize::try_from(node_id).unwrap()],
                            observed_epoch,
                            pg_observations,
                        ),
                        now_ms,
                    )
                    .expect("covered steady heartbeat should refresh");
                heartbeat_requests += 1;
                now_ms += 1;
            }
            let status = harness
                .control_plane
                .runtime_map_status(now_ms)
                .expect("compact runtime-map status should refresh");
            assert_eq!(status.pg_routes(), PG_COUNT as usize);
            assert_eq!(status.active_serving_pg_routes(), PG_COUNT as usize);
            assert_eq!(
                status
                    .lease_renewal()
                    .expect("active compact status should renew its lease")
                    .content_digest(),
                content_digest,
                "lease-only heartbeats must not invalidate route content"
            );
            now_ms += 1;
            let monitor_started = Instant::now();
            assert!(
                !checkpoint_experimental_raft_peer_wal_if_due(
                    &harness.control_plane.runtime,
                    &harness.authority,
                    &monitor_durability,
                    &mut monitor_tracker,
                    monitor_started,
                )
                .expect("production-shaped WAL monitor poll should succeed"),
                "steady covered traffic must not require a WAL checkpoint"
            );
            let monitor_poll_us =
                u64::try_from(monitor_started.elapsed().as_micros()).unwrap_or(u64::MAX);
            monitor_poll_total += 1;
            monitor_poll_us_total = monitor_poll_us_total.saturating_add(monitor_poll_us);
            monitor_poll_us_max = monitor_poll_us_max.max(monitor_poll_us);
        }
        let steady_heartbeat_requests = heartbeat_requests - measured_heartbeats_before;

        let applied_after_steady = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("post-measurement Raft status should read")
            .applied();
        let durability_metrics_after_steady = harness.authority.durability_metric_snapshots();
        assert_eq!(applied_after_steady, applied_before);
        assert_eq!(durable_wal_offsets(&harness), wal_offsets_before);
        assert_eq!(
            durability_metrics_after_steady, durability_metrics_before,
            "steady covered heartbeats and compact status reads must not encode, store, sync, compact, or append durable state"
        );
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("post-measurement snapshot should read")
                .max_committed_timestamp_ms(),
            Some(durable_timestamp_before),
            "covered renewals must not ratchet committed timestamp state"
        );

        let horizon_probe_heartbeats_before = heartbeat_requests;
        let mut horizon_extension_at_ms = None;
        for _ in 0..128 {
            now_ms += 250;
            let snapshot = harness
                .control_plane
                .current_snapshot()
                .expect("horizon-extension state should read");
            let observed_epoch = snapshot.cluster_epoch();
            let pg_observations = active_primary_observations(&snapshot, 0, metadata_proof);
            harness
                .control_plane
                .refresh_node_heartbeat(
                    heartbeat(0, &endpoints[0], observed_epoch, pg_observations),
                    now_ms,
                )
                .expect("horizon-extension heartbeat should refresh");
            heartbeat_requests += 1;
            let applied = harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("horizon-extension status should read")
                .applied();
            if applied != applied_before {
                horizon_extension_at_ms = Some(now_ms);
                break;
            }
        }
        let horizon_extension_at_ms = horizon_extension_at_ms
            .expect("bounded heartbeat runway should eventually require one durable extension");
        let horizon_probe_heartbeats = heartbeat_requests - horizon_probe_heartbeats_before;
        let extension_checkpoint_observed_at = Instant::now();
        assert!(
            !checkpoint_experimental_raft_peer_wal_if_due(
                &harness.control_plane.runtime,
                &harness.authority,
                &monitor_durability,
                &mut monitor_tracker,
                extension_checkpoint_observed_at,
            )
            .expect("horizon-extension WAL observation should succeed"),
            "a new WAL suffix should start the bounded checkpoint delay"
        );
        assert!(
            checkpoint_experimental_raft_peer_wal_if_due(
                &harness.control_plane.runtime,
                &harness.authority,
                &monitor_durability,
                &mut monitor_tracker,
                extension_checkpoint_observed_at + CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_DELAY,
            )
            .expect("horizon-extension WAL checkpoint should succeed"),
            "the WAL monitor must checkpoint a liveness suffix within its delay bound"
        );
        let wal_offsets_after_extension = durable_wal_offsets(&harness);
        let durability_metrics_after_extension = harness.authority.durability_metric_snapshots();
        assert_eq!(
            wal_offsets_after_extension.base_offset(),
            wal_offsets_after_extension.clean_len(),
            "horizon-extension checkpoint should compact its WAL suffix"
        );
        let wal_offset_advance = wal_offsets_after_extension
            .base_offset()
            .checked_sub(wal_offsets_before.base_offset())
            .expect("WAL base offset must advance monotonically");
        assert!(wal_offset_advance > 0);
        let checkpoint_metrics_before = durability_metrics_after_steady.checkpoint;
        let checkpoint_metrics_after = durability_metrics_after_extension.checkpoint;
        let checkpoint_store_delta = checkpoint_metrics_after
            .store_total
            .checked_sub(checkpoint_metrics_before.store_total)
            .expect("checkpoint store count must advance monotonically");
        let checkpoint_store_us_total_delta = checkpoint_metrics_after
            .store_us_total
            .checked_sub(checkpoint_metrics_before.store_us_total)
            .expect("checkpoint store duration must advance monotonically");
        let checkpoint_file_sync_delta = checkpoint_metrics_after
            .file_sync_total
            .checked_sub(checkpoint_metrics_before.file_sync_total)
            .expect("checkpoint file-sync count must advance monotonically");
        let checkpoint_file_sync_us_total_delta = checkpoint_metrics_after
            .file_sync_us_total
            .checked_sub(checkpoint_metrics_before.file_sync_us_total)
            .expect("checkpoint file-sync duration must advance monotonically");
        let checkpoint_directory_sync_delta = checkpoint_metrics_after
            .directory_sync_total
            .checked_sub(checkpoint_metrics_before.directory_sync_total)
            .expect("checkpoint directory-sync count must advance monotonically");
        let checkpoint_directory_sync_us_total_delta = checkpoint_metrics_after
            .directory_sync_us_total
            .checked_sub(checkpoint_metrics_before.directory_sync_us_total)
            .expect("checkpoint directory-sync duration must advance monotonically");
        let checkpoint_bytes = checkpoint_metrics_after
            .bytes_total
            .checked_sub(checkpoint_metrics_before.bytes_total)
            .expect("checkpoint byte count must advance monotonically");
        assert!(checkpoint_store_delta > 0);
        assert!(checkpoint_file_sync_delta > 0);
        assert!(checkpoint_directory_sync_delta > 0);
        assert!(checkpoint_bytes > 0);
        let wal_metrics_before = durability_metrics_after_steady
            .wal
            .expect("production-shaped durable authority should expose WAL metrics");
        let wal_metrics_after = durability_metrics_after_extension
            .wal
            .expect("production-shaped durable authority should retain WAL metrics");
        let wal_append_delta = wal_metrics_after
            .append_total
            .checked_sub(wal_metrics_before.append_total)
            .expect("WAL append count must advance monotonically");
        let wal_append_us_total_delta = wal_metrics_after
            .append_us_total
            .checked_sub(wal_metrics_before.append_us_total)
            .expect("WAL append duration must advance monotonically");
        let wal_append_accept_us_total_delta = wal_metrics_after
            .append_accept_us_total
            .checked_sub(wal_metrics_before.append_accept_us_total)
            .expect("WAL append acceptance duration must advance monotonically");
        let wal_durability_queue_wait_us_total_delta = wal_metrics_after
            .durability_queue_wait_us_total
            .checked_sub(wal_metrics_before.durability_queue_wait_us_total)
            .expect("WAL durability queue wait must advance monotonically");
        let wal_durability_operation_us_total_delta = wal_metrics_after
            .durability_operation_us_total
            .checked_sub(wal_metrics_before.durability_operation_us_total)
            .expect("WAL durability operation duration must advance monotonically");
        let wal_file_sync_delta = wal_metrics_after
            .file_sync_total
            .checked_sub(wal_metrics_before.file_sync_total)
            .expect("WAL file-sync count must advance monotonically");
        let wal_file_sync_us_total_delta = wal_metrics_after
            .file_sync_us_total
            .checked_sub(wal_metrics_before.file_sync_us_total)
            .expect("WAL file-sync duration must advance monotonically");
        let wal_directory_sync_delta = wal_metrics_after
            .directory_sync_total
            .checked_sub(wal_metrics_before.directory_sync_total)
            .expect("WAL directory-sync count must advance monotonically");
        let wal_directory_sync_us_total_delta = wal_metrics_after
            .directory_sync_us_total
            .checked_sub(wal_metrics_before.directory_sync_us_total)
            .expect("WAL directory-sync duration must advance monotonically");
        let wal_bytes_appended = wal_metrics_after
            .frame_bytes_total
            .checked_sub(wal_metrics_before.frame_bytes_total)
            .expect("WAL frame byte count must advance monotonically");
        assert!(wal_append_delta > 0);
        assert!(wal_file_sync_delta > 0);
        assert!(wal_directory_sync_delta > 0);
        assert!(wal_bytes_appended > 0);
        assert_eq!(wal_metrics_after.durability_queue_depth, 0);
        let extension_interval_ms = horizon_extension_at_ms
            .checked_sub(durable_timestamp_before)
            .expect("horizon extension must follow the previous durable timestamp");
        assert!(extension_interval_ms > 0);
        let checkpoint_amortization_interval_ms = extension_interval_ms.max(
            u64::try_from(CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_DELAY.as_millis())
                .expect("checkpoint delay should fit u64 milliseconds"),
        );
        let logical_durable_bytes = checkpoint_bytes
            .checked_add(wal_bytes_appended)
            .expect("logical durable byte accounting should not overflow");
        let amortized_durable_bytes_per_second = logical_durable_bytes
            .checked_mul(1000)
            .expect("amortized byte accounting should not overflow")
            .div_ceil(checkpoint_amortization_interval_ms);
        assert!(
            amortized_durable_bytes_per_second < MAX_AMORTIZED_DURABLE_BYTES_PER_SECOND,
            "measured horizon-extension durability rate {amortized_durable_bytes_per_second} B/s exceeds the release limit {MAX_AMORTIZED_DURABLE_BYTES_PER_SECOND} B/s"
        );
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("post-extension snapshot should read")
                .cluster_map_history()
                .len(),
            RETAINED_HISTORY_EPOCHS
        );

        let post_extension_status = harness
            .control_plane
            .runtime_map_status(now_ms)
            .expect("post-extension compact status should warm its checkpoint marker");
        assert_eq!(
            post_extension_status
                .lease_renewal()
                .expect("post-extension status should renew its lease")
                .content_digest(),
            content_digest
        );
        now_ms += 1;
        let concurrent_applied_before = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("pre-concurrent-checkpoint status should read")
            .applied();
        let concurrent_offsets_before = durable_wal_offsets(&harness);
        let concurrent_metrics_before = harness.authority.durability_metric_snapshots();
        let concurrent_start = Arc::new(std::sync::Barrier::new(2));
        let checkpoint_start = Arc::clone(&concurrent_start);
        let checkpoint_runtime = harness.runtime.handle().clone();
        let checkpoint_authority = Arc::clone(&harness.authority);
        let checkpoint_path = harness
            .control_plane
            .durable_artifact_path
            .clone()
            .expect("durable release harness should retain an artifact path");
        let checkpoint_lock = harness
            .control_plane
            .durable_checkpoint_lock
            .clone()
            .expect("durable release harness should retain a checkpoint lock");
        let checkpoint_thread = thread::spawn(move || {
            checkpoint_start.wait();
            let started = Instant::now();
            let mut checkpoint_call_us_total = 0_u64;
            let mut checkpoint_us_max = 0_u64;
            for _ in 0..CONCURRENT_CHECKPOINT_COUNT {
                let checkpoint_started = Instant::now();
                store_experimental_raft_durable_restart_artifact(
                    &checkpoint_runtime,
                    &checkpoint_authority,
                    &checkpoint_path,
                    Some(&checkpoint_lock),
                )
                .expect("concurrent production-shaped checkpoint should persist");
                let checkpoint_us =
                    u64::try_from(checkpoint_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                checkpoint_call_us_total = checkpoint_call_us_total.saturating_add(checkpoint_us);
                checkpoint_us_max = checkpoint_us_max.max(checkpoint_us);
            }
            (
                started.elapsed(),
                checkpoint_call_us_total,
                checkpoint_us_max,
            )
        });

        concurrent_start.wait();
        let mut concurrent_heartbeat_us_max = 0_u64;
        let mut concurrent_status_us_max = 0_u64;
        for _ in 0..CONCURRENT_HEARTBEAT_ROUNDS {
            let snapshot = harness
                .control_plane
                .current_snapshot()
                .expect("concurrent heartbeat state should read");
            let observed_epoch = snapshot.cluster_epoch();
            for node_id in 0..STORAGE_NODE_COUNT {
                let pg_observations =
                    active_primary_observations(&snapshot, node_id, metadata_proof);
                let heartbeat_started = Instant::now();
                harness
                    .control_plane
                    .refresh_node_heartbeat(
                        heartbeat(
                            node_id,
                            &endpoints[usize::try_from(node_id).unwrap()],
                            observed_epoch,
                            pg_observations,
                        ),
                        now_ms,
                    )
                    .expect("heartbeat should remain available during checkpoint persistence");
                concurrent_heartbeat_us_max = concurrent_heartbeat_us_max.max(
                    u64::try_from(heartbeat_started.elapsed().as_micros()).unwrap_or(u64::MAX),
                );
                now_ms += 1;
            }
            let status_started = Instant::now();
            let status = harness
                .control_plane
                .runtime_map_status(now_ms)
                .expect("compact status should remain available during checkpoint persistence");
            concurrent_status_us_max = concurrent_status_us_max
                .max(u64::try_from(status_started.elapsed().as_micros()).unwrap_or(u64::MAX));
            assert_eq!(status.pg_routes(), PG_COUNT as usize);
            assert_eq!(status.active_serving_pg_routes(), PG_COUNT as usize);
            assert_eq!(
                status
                    .lease_renewal()
                    .expect("concurrent compact status should renew its lease")
                    .content_digest(),
                content_digest
            );
            now_ms += 1;
        }
        let (
            concurrent_checkpoint_batch_elapsed,
            concurrent_checkpoint_call_us_total,
            concurrent_checkpoint_us_max,
        ) = checkpoint_thread
            .join()
            .expect("concurrent checkpoint worker should finish");
        let concurrent_metrics_after = harness.authority.durability_metric_snapshots();
        assert_eq!(
            harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("post-concurrent-checkpoint status should read")
                .applied(),
            concurrent_applied_before,
            "checkpoint persistence and covered traffic must not append Raft commands"
        );
        assert_eq!(durable_wal_offsets(&harness), concurrent_offsets_before);
        assert_eq!(
            concurrent_metrics_after
                .checkpoint
                .store_total
                .checked_sub(concurrent_metrics_before.checkpoint.store_total)
                .expect("concurrent checkpoint count must advance monotonically"),
            CONCURRENT_CHECKPOINT_COUNT
        );
        assert_eq!(
            concurrent_metrics_after.wal, concurrent_metrics_before.wal,
            "checkpoint persistence and covered traffic must not append or sync WAL records"
        );
        let concurrent_checkpoint_batch_us =
            u64::try_from(concurrent_checkpoint_batch_elapsed.as_micros()).unwrap_or(u64::MAX);
        let final_checkpoint_metrics = concurrent_metrics_after.checkpoint;

        eprintln!(
            "control_plane_write_amplification_release pgs={PG_COUNT} retained_epochs={RETAINED_HISTORY_EPOCHS} retained_artifact_bytes={large_artifact_bytes} storage_nodes={STORAGE_NODE_COUNT} sustained_peering_intervals={SUSTAINED_PEERING_INTERVALS} sustained_peering_rounds_per_interval={SUSTAINED_PEERING_ROUNDS} post_purge_artifact_bytes={post_purge_artifact_bytes:?} sustained_peering_checkpoint_bytes={sustained_checkpoint_bytes} sustained_peering_wal_bytes={sustained_wal_bytes} sustained_peering_durable_bytes_per_second={sustained_durable_bytes_per_second} steady_heartbeats={steady_heartbeat_requests} compact_status_reads={STEADY_HEARTBEAT_ROUNDS} wal_monitor_polls={monitor_poll_total} wal_monitor_poll_us_total={monitor_poll_us_total} wal_monitor_poll_us_max={monitor_poll_us_max} steady_checkpoint_stores=0 steady_checkpoint_syncs=0 steady_wal_appends=0 steady_wal_syncs=0 horizon_probe_heartbeats={horizon_probe_heartbeats} checkpoint_stores={checkpoint_store_delta} checkpoint_store_us_total={checkpoint_store_us_total_delta} checkpoint_store_us_lifetime_max={} checkpoint_file_syncs={checkpoint_file_sync_delta} checkpoint_file_sync_us_total={checkpoint_file_sync_us_total_delta} checkpoint_file_sync_us_lifetime_max={} checkpoint_directory_syncs={checkpoint_directory_sync_delta} checkpoint_directory_sync_us_total={checkpoint_directory_sync_us_total_delta} checkpoint_directory_sync_us_lifetime_max={} checkpoint_bytes={checkpoint_bytes} horizon_wal_appends={wal_append_delta} horizon_wal_append_accept_us_total={wal_append_accept_us_total_delta} horizon_wal_append_accept_us_lifetime_max={} horizon_wal_append_us_total={wal_append_us_total_delta} horizon_wal_append_us_lifetime_max={} horizon_wal_durability_queue_depth_max={} horizon_wal_durability_queue_wait_us_total={wal_durability_queue_wait_us_total_delta} horizon_wal_durability_queue_wait_us_lifetime_max={} horizon_wal_durability_operation_us_total={wal_durability_operation_us_total_delta} horizon_wal_durability_operation_us_lifetime_max={} horizon_wal_file_syncs={wal_file_sync_delta} horizon_wal_file_sync_us_total={wal_file_sync_us_total_delta} horizon_wal_file_sync_us_lifetime_max={} horizon_wal_directory_syncs={wal_directory_sync_delta} horizon_wal_directory_sync_us_total={wal_directory_sync_us_total_delta} horizon_wal_directory_sync_us_lifetime_max={} horizon_wal_bytes_appended={wal_bytes_appended} horizon_wal_offset_advance={wal_offset_advance} horizon_extension_interval_ms={extension_interval_ms} checkpoint_amortization_interval_ms={checkpoint_amortization_interval_ms} amortized_durable_bytes_per_second={amortized_durable_bytes_per_second} concurrent_checkpoints={CONCURRENT_CHECKPOINT_COUNT} concurrent_checkpoint_call_us_total={concurrent_checkpoint_call_us_total} concurrent_checkpoint_batch_us={concurrent_checkpoint_batch_us} concurrent_checkpoint_us_max={concurrent_checkpoint_us_max} concurrent_heartbeats={} concurrent_heartbeat_us_max={concurrent_heartbeat_us_max} concurrent_status_reads={CONCURRENT_HEARTBEAT_ROUNDS} concurrent_status_us_max={concurrent_status_us_max}",
            final_checkpoint_metrics.store_us_max,
            final_checkpoint_metrics.file_sync_us_max,
            final_checkpoint_metrics.directory_sync_us_max,
            wal_metrics_after.append_accept_us_max,
            wal_metrics_after.append_us_max,
            wal_metrics_after.durability_queue_depth_max,
            wal_metrics_after.durability_queue_wait_us_max,
            wal_metrics_after.durability_operation_us_max,
            wal_metrics_after.file_sync_us_max,
            wal_metrics_after.directory_sync_us_max,
            CONCURRENT_HEARTBEAT_ROUNDS * u64::from(STORAGE_NODE_COUNT),
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_durably_accumulates_multi_node_peering_evidence() {
        let mut harness = experimental_raft_test_harness("multi-node-heartbeat-peering-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids = vec![7];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        for (node_id, now_ms) in [(1, 20_000), (2, 20_010)] {
            let observed_epoch = harness
                .control_plane
                .current_snapshot()
                .expect("experimental snapshot should read before startup heartbeat")
                .cluster_epoch();
            harness
                .control_plane
                .refresh_node_heartbeat(
                    NodeHeartbeat {
                        node_id: NodeId::new(node_id),
                        node_incarnation: 1,
                        endpoint: format!("/tmp/argmin-experimental-raft-node-{node_id}.sock"),
                        observed_epoch,
                        requested_lease_duration_ms: 500,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    now_ms,
                )
                .expect("experimental raft startup heartbeat should refresh");
        }

        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read before peering heartbeats")
            .cluster_epoch();
        let proof = PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        let applied_before_peering = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read before peering heartbeats")
            .applied();

        let first = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_020,
            )
            .expect("first peering observation should refresh durably");
        assert_eq!(first.runtime_map().pg_routes()[0].state(), PgState::Peering);
        assert_ne!(
            harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("experimental Raft status should read after first peering heartbeat")
                .applied(),
            applied_before_peering,
            "Peering evidence must advance the durable applied cursor"
        );

        let second = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(2),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_030,
            )
            .expect("second peering observation should complete from durable evidence");
        let route = &second.runtime_map().pg_routes()[0];
        assert_eq!(route.state(), PgState::Active);
        assert_eq!(route.primary_node_id(), NodeId::new(1));

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_resamples_heartbeat_time_when_enabled() {
        let mut harness = experimental_raft_test_harness("heartbeat-resample-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![7];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        let refresh = storage::clock::with_time_override(30_000, || {
            enable_resampled_authority_time(&mut harness.control_plane, 30_000);
            harness.control_plane.refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                20_000,
            )
        })
        .expect("experimental raft heartbeat should refresh");

        assert_eq!(refresh.lease().lease_deadline_ms(), 30_500);
        let status = harness
            .control_plane
            .block_on(harness.control_plane.authority.status())
            .expect("experimental Raft status should read");
        let lease_horizon_authority = harness
            .control_plane
            .authority_clock
            .as_ref()
            .expect("resampled authority uses a clock gate")
            .lock()
            .expect("authority clock mutex should not be poisoned")
            .lease_horizon_authority_binding(status.current_term())
            .expect("heartbeat authority binding should remain established");
        assert!(harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after horizon establishment")
            .lease_grant_horizon_covers(
                lease_horizon_authority,
                refresh.lease().lease_deadline_ms(),
            ));

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_heartbeat_rechecks_leadership_after_commit() {
        let mut harness = experimental_raft_test_harness("heartbeat-post-commit-term-change-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![7];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        enable_resampled_authority_time(&mut harness.control_plane, 31_000);
        let initial_term = harness
            .control_plane
            .block_on(harness.control_plane.authority.status())
            .expect("experimental Raft status should read")
            .current_term()
            .expect("single-node leader should have a term");
        *harness
            .control_plane
            .after_heartbeat_commit_hook
            .lock()
            .expect("heartbeat hook mutex should not be poisoned") =
            Some(Box::new(move |_| Ok(Some(initial_term + 1))));

        let error = storage::clock::with_time_override(31_000, || {
            harness.control_plane.refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                30_000,
            )
        })
        .expect_err("a heartbeat committed under the previous term must not return a lease");
        assert!(
            matches!(
                error,
                ControlPlaneError::AuthorityClockLeadershipChanged {
                    established_term: Some(term),
                    current_term,
                } if term == initial_term && current_term > initial_term
            ),
            "unexpected post-election heartbeat error: {error:?}"
        );

        let committed = harness
            .control_plane
            .current_snapshot()
            .expect("committed heartbeat snapshot should remain readable");
        assert_eq!(
            committed
                .node(NodeId::new(1))
                .expect("heartbeat should commit before the injected election")
                .lease_deadline_ms(),
            Some(31_500)
        );
        assert_eq!(
            committed
                .lease_grant_horizon_authority()
                .and_then(LeaseHorizonAuthorityBinding::raft_term),
            Some(initial_term)
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_heartbeat_clamps_shorter_requested_lease() {
        let mut harness = experimental_raft_test_harness("heartbeat-lease-clamp-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![7];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        let first = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                40_000,
            )
            .expect("experimental raft initial heartbeat should refresh");
        assert_eq!(first.lease().lease_deadline_ms(), 41_000);

        let refreshed_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after initial heartbeat")
            .cluster_epoch();
        let applied_before_epoch_acknowledgement = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read before epoch acknowledgement")
            .applied();
        let shortened = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: refreshed_epoch,
                    requested_lease_duration_ms: 100,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                40_100,
            )
            .expect("experimental raft heartbeat should preserve longer existing lease");
        assert_eq!(shortened.lease().lease_deadline_ms(), 41_000);
        let applied_after_epoch_acknowledgement = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read after epoch acknowledgement")
            .applied();
        assert_ne!(
            applied_after_epoch_acknowledgement, applied_before_epoch_acknowledgement,
            "a node's first acknowledgement of a new epoch must be durable"
        );
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("experimental snapshot should read after shortened heartbeat")
                .node(NodeId::new(1))
                .expect("node should exist")
                .lease_deadline_ms(),
            Some(41_000)
        );

        let renewed = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: refreshed_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                40_200,
            )
            .expect("covered experimental raft heartbeat should renew volatile lease");
        assert_eq!(renewed.lease().lease_deadline_ms(), 41_200);
        assert_eq!(
            harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("experimental Raft status should read after volatile renewal")
                .applied(),
            applied_after_epoch_acknowledgement,
            "repeated covered renewal must retain the same applied cursor"
        );

        let rejected = harness
            .control_plane
            .submit_raft_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(99),
                availability: NodeAvailabilityState::Unavailable,
            })
            .expect_err("unknown-node command should reject after committing");
        assert!(matches!(rejected, ControlPlaneError::UnknownNode { .. }));
        let durable_promoted_deadline = harness
            .control_plane
            .block_on(harness.authority.durable_state_machine_snapshot_for_test())
            .expect("durable state should retain promotion before rejected command")
            .node(NodeId::new(1))
            .and_then(|node| node.lease_deadline_ms());
        assert_eq!(
            durable_promoted_deadline,
            Some(41_200),
            "a rejected follow-up must not discard the acknowledged lease promotion"
        );
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("snapshot should read after committed log progress")
                .node(NodeId::new(1))
                .expect("node should exist")
                .lease_deadline_ms(),
            Some(41_200),
            "same-term rejected log progress must rebase the acknowledged volatile lease"
        );
        harness
            .control_plane
            .submit_raft_command(ControlPlaneCommand::SetNodeMembership {
                node_id: NodeId::new(1),
                membership: NodeMembershipState::Active,
            })
            .expect("unrelated applied no-op should commit");
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("snapshot should read after applied log progress")
                .node(NodeId::new(1))
                .expect("node should exist")
                .lease_deadline_ms(),
            Some(41_200),
            "same-term applied log progress must rebase the acknowledged volatile lease"
        );
        harness
            .control_plane
            .block_on(harness.authority.add_learner(
                2,
                BasicNode::new("unused-test-learner"),
                false,
            ))
            .expect("unrelated learner membership entry should commit");
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("snapshot should read after membership log progress")
                .node(NodeId::new(1))
                .expect("node should exist")
                .lease_deadline_ms(),
            Some(41_200),
            "same-term membership log progress must rebase the acknowledged volatile lease"
        );
        let expiry = harness
            .control_plane
            .expire_heartbeat_leases(41_000)
            .expect("the old durable deadline must not expire a rebased volatile lease");
        assert_eq!(expiry.1, 0);
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("snapshot should retain the rebased lease after the old deadline")
                .node(NodeId::new(1))
                .expect("node should exist")
                .lease_deadline_ms(),
            Some(41_200)
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_restart_refresh_uses_control_plane_last_observed_epoch() {
        let mut harness = experimental_raft_test_harness("restart-observed-epoch-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![30];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                60_000,
            )
            .expect("experimental raft startup heartbeat should refresh");
        let protected_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup")
            .cluster_epoch();
        for node_id in 10..18 {
            harness
                .control_plane
                .submit_raft_command(ControlPlaneCommand::SetNodeMembership {
                    node_id: NodeId::new(node_id),
                    membership: NodeMembershipState::Active,
                })
                .expect("experimental raft node membership should update");
        }
        let observed_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after churn")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                61_000,
            )
            .expect("experimental raft observed heartbeat should refresh");

        let restart_heartbeat = NodeHeartbeat {
            node_id: NodeId::new(1),
            node_incarnation: 2,
            endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            observed_epoch: ClusterEpoch::INITIAL,
            requested_lease_duration_ms: 1_000,
            cluster_map_history_route_references: Default::default(),
            pg_observations: Vec::new(),
        };
        let lease_horizon_authority = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should expose its lease horizon")
            .lease_grant_horizon_authority();
        harness
            .control_plane
            .submit_raft_command(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: restart_heartbeat.clone(),
                heartbeat_at_ms: 62_000,
                lease_deadline_ms: 63_000,
                lease_horizon_authority,
            })
            .expect("lost experimental raft restart heartbeat response should still apply");
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("experimental snapshot should read after lost heartbeat")
                .node(NodeId::new(1))
                .and_then(|node| node.last_observed_epoch()),
            Some(observed_epoch),
            "stale restart heartbeat must not regress stored observed epoch"
        );

        let restart_refresh = harness
            .control_plane
            .refresh_node_heartbeat(restart_heartbeat, 63_000)
            .expect("experimental raft restart heartbeat should refresh");

        assert!(restart_refresh
            .runtime_map()
            .historical_pg_routes()
            .iter()
            .all(|route| route.cluster_epoch() >= observed_epoch));
        assert!(!restart_refresh
            .runtime_map()
            .historical_pg_routes()
            .iter()
            .any(|route| route.cluster_epoch() == protected_epoch));

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_peer_auth_diagnostics_are_redacted() {
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-auth-diagnostics-{}",
            std::process::id()
        );
        let unauthenticated_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string()), (2, "node-2".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let unauthenticated =
            format_experimental_raft_peer_auth_diagnostics(&unauthenticated_policy);
        assert!(
            unauthenticated.contains("required=false"),
            "{unauthenticated}"
        );
        assert!(
            unauthenticated.contains("credential_id=-"),
            "{unauthenticated}"
        );

        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string()), (2, "node-2".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        )
        .with_auth_policy(experimental_raft_peer_auth_policy(&cluster_name, 2, 1));
        let auth_policy = policy
            .auth_policy()
            .expect("test policy should have auth policy");
        auth_policy.record_peer_frame_rejection(
            ControlPlaneAuthOperation::RaftVote,
            ControlPlaneAuthRejectionReason::WrongCluster,
        );
        auth_policy.record_peer_frame_rejection_without_operation(
            ControlPlaneAuthRejectionReason::Malformed,
        );

        let diagnostics = format_experimental_raft_peer_auth_diagnostics(&policy);
        assert!(diagnostics.contains("required=true"), "{diagnostics}");
        assert!(
            diagnostics.contains("local_principal=RaftPeer { node_id: 2 }"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("credential_id=raft-node-2"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("credential_version=1"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("accepted_total=0"), "{diagnostics}");
        assert!(diagnostics.contains("rejected_total=2"), "{diagnostics}");
        assert!(
            diagnostics.contains("rejected_without_operation_total=1"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("rejected_by_operation{operation=\"RaftVote\"} 1"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("rejected_by_reason{reason=\"WrongCluster\"} 1"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("rejected_by_reason{reason=\"Malformed\"} 1"),
            "{diagnostics}"
        );
        assert!(!diagnostics.contains("node-1-test-secret"));
        assert!(!diagnostics.contains("node-2-test-secret"));
        assert!(!diagnostics.contains("payload"));
        assert!(!diagnostics.contains("authenticator"));
    }

    #[test]
    fn experimental_raft_peer_auth_policy_selects_latest_local_credential() {
        let mut config = test_server_config();
        config.control_plane_raft_auth_credentials = vec![
            ConfiguredControlPlaneRaftAuthCredential {
                node_id: 2,
                credential_id: "raft-node-2".to_string(),
                credential_version: 1,
                secret: BinarySecretConfigValue::from_utf8("node-2-old-test-secret".to_string()),
            },
            ConfiguredControlPlaneRaftAuthCredential {
                node_id: 2,
                credential_id: "raft-node-2".to_string(),
                credential_version: 2,
                secret: BinarySecretConfigValue::from_utf8("node-2-new-test-secret".to_string()),
            },
            ConfiguredControlPlaneRaftAuthCredential {
                node_id: 1,
                credential_id: "raft-node-1".to_string(),
                credential_version: 1,
                secret: BinarySecretConfigValue::from_utf8("node-1-test-secret".to_string()),
            },
        ];

        let policy = build_experimental_raft_peer_auth_policy(&config, "auth-cluster", 2)
            .expect("test Raft peer auth policy should build")
            .expect("test Raft peer auth policy should be enabled");
        let status = policy.status_snapshot();

        assert_eq!(status.credential_id(), Some("raft-node-2"));
        assert_eq!(status.credential_version(), Some(2));
    }

    #[test]
    fn experimental_raft_wal_checkpoint_observer_enforces_each_bound() {
        let mut tracker =
            ExperimentalRaftPeerCheckpointTracker::new(ExperimentalRaftPeerCheckpointPolicy {
                max_wal_suffix_bytes: 100,
                max_mutations: 2,
                max_delay: Duration::from_secs(1),
                poll_interval: Duration::from_millis(100),
            });
        let started = Instant::now();
        assert!(tracker
            .observe(
                started,
                ExperimentalRaftPeerCheckpointObservation {
                    wal_suffix_bytes: 0,
                    successful_append_total: 10,
                },
            )
            .is_none());
        assert!(tracker
            .observe(
                started,
                ExperimentalRaftPeerCheckpointObservation {
                    wal_suffix_bytes: 99,
                    successful_append_total: 11,
                },
            )
            .is_none());
        assert_eq!(
            tracker.observe(
                started,
                ExperimentalRaftPeerCheckpointObservation {
                    wal_suffix_bytes: 100,
                    successful_append_total: 11,
                },
            ),
            Some(ExperimentalRaftPeerCheckpointWork {
                pending_mutations: 1,
                elapsed: Duration::ZERO,
                wal_suffix_bytes: 100,
            })
        );

        tracker.complete_checkpoint(11);
        assert!(tracker
            .observe(
                started,
                ExperimentalRaftPeerCheckpointObservation {
                    wal_suffix_bytes: 1,
                    successful_append_total: 13,
                },
            )
            .is_some_and(|work| work.pending_mutations == 2));

        tracker.complete_checkpoint(13);
        assert!(tracker
            .observe(
                started,
                ExperimentalRaftPeerCheckpointObservation {
                    wal_suffix_bytes: 1,
                    successful_append_total: 14,
                },
            )
            .is_none());
        assert!(tracker
            .observe(
                started + Duration::from_millis(900),
                ExperimentalRaftPeerCheckpointObservation {
                    wal_suffix_bytes: 1,
                    successful_append_total: 14,
                },
            )
            .is_some_and(|work| work.elapsed == Duration::from_millis(900)));
    }

    #[test]
    fn experimental_raft_peer_vote_acks_from_wal_then_bounded_checkpoint_compacts() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let state_dir = short_unix_socket_test_dir("experimental-raft-peer-default-checkpoint");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).expect("durable test directory should exist");
        let state_path = state_dir.join("control-plane.state");
        let peer_socket_path = state_dir.join("peer.sock");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-wal-ack-{}",
            std::process::id()
        );
        let authority = runtime.block_on(async {
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name.clone(),
                1,
                &state_path,
            )
            .await
            .expect("WAL-backed durable authority should initialize");
            authority
                .store_durable_restart_artifact()
                .await
                .expect("uninitialized WAL-backed raft should checkpoint initial artifact");
            Arc::new(authority)
        });
        let initial_status = runtime
            .block_on(authority.status())
            .expect("initial WAL-backed authority status should read");
        let initial_wal_offsets = initial_status
            .durable_wal_offsets()
            .expect("initial WAL-backed authority should report WAL offsets");
        assert_eq!(
            initial_wal_offsets.base_offset(),
            initial_wal_offsets.clean_len(),
            "initial checkpoint should compact the WAL suffix"
        );

        let peer_policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let checkpoint_lock = Arc::new(Mutex::new(()));
        let durability_context = ExperimentalRaftPeerDurabilityContext {
            artifact_path: Some(Arc::new(state_path.clone())),
            checkpoint_lock,
        };
        let server_policy = ControlPlaneRaftPeerServerPolicy::new(1, peer_policy, 4096)
            .expect("test peer server policy should build")
            .with_durability(Arc::new(ExperimentalRaftPeerServerDurability {
                runtime: runtime.handle().clone(),
                authority: Arc::clone(&authority),
                context: durability_context.clone(),
            }));
        let listener = Arc::new(
            ControlPlaneRaftPeerServerListener::unix(
                "test-peer",
                UnixListener::bind(&peer_socket_path).expect("test peer socket should bind"),
                1,
                Duration::from_secs(5),
            )
            .expect("test peer listener should build"),
        );
        let client = ControlPlaneRaftPeerTestClient::unix(
            peer_socket_path,
            cluster_name,
            1,
            1,
            ControlPlaneRaftPeerTransportLimits::default(),
            Duration::from_secs(5),
        );
        let send_vote = |term| {
            let accept_listener = Arc::clone(&listener);
            let accept_authority = Arc::clone(&authority);
            let accept_policy = server_policy.clone();
            let runtime_handle = runtime.handle().clone();
            let accept = thread::spawn(move || {
                accept_listener
                    .accept_one(&runtime_handle, accept_authority, &accept_policy)
                    .expect("test peer listener should accept");
            });
            let granted = client
                .send_vote(term, None, false)
                .expect("test peer vote should receive a response");
            accept.join().expect("test peer accept should finish");
            let worker_deadline = Instant::now() + Duration::from_secs(5);
            while listener.active_workers() != 0 {
                assert!(
                    Instant::now() < worker_deadline,
                    "test peer worker did not release its listener slot"
                );
                thread::sleep(Duration::from_millis(1));
            }
            granted
        };

        let mut checkpoint_tracker =
            ExperimentalRaftPeerCheckpointTracker::new(ExperimentalRaftPeerCheckpointPolicy {
                max_wal_suffix_bytes: u64::MAX,
                max_mutations: 1,
                max_delay: Duration::from_secs(60),
                poll_interval: Duration::from_secs(1),
            });
        assert!(!checkpoint_experimental_raft_peer_wal_if_due(
            runtime.handle(),
            &authority,
            &durability_context,
            &mut checkpoint_tracker,
            Instant::now(),
        )
        .expect("clean WAL checkpoint observation should succeed"));
        let checkpoint_guard = durability_context
            .checkpoint_lock
            .lock()
            .expect("checkpoint lock should acquire");

        let expected_vote = Vote::<ControlPlaneRaftLeaderId>::new(3, 1);
        assert!(
            send_vote(expected_vote.leader_id.term),
            "peer RPC should acknowledge after its WAL mutation is durable"
        );
        assert_eq!(
            durable_raft_checkpoint_vote(&state_path),
            None,
            "ordinary peer acknowledgement must not rewrite the checkpoint artifact"
        );
        drop(checkpoint_guard);
        let pre_checkpoint_status = runtime
            .block_on(authority.status())
            .expect("post-peer RPC WAL-backed authority status should read");
        let pre_checkpoint_offsets = pre_checkpoint_status
            .durable_wal_offsets()
            .expect("post-peer RPC WAL-backed authority should report WAL offsets");
        assert!(
            pre_checkpoint_offsets.clean_len() > pre_checkpoint_offsets.base_offset(),
            "acknowledged vote should remain in the fsynced WAL suffix before compaction"
        );
        assert!(
            checkpoint_experimental_raft_peer_wal_if_due(
                runtime.handle(),
                &authority,
                &durability_context,
                &mut checkpoint_tracker,
                Instant::now(),
            )
            .expect("bounded peer checkpoint should succeed"),
            "WAL append count should reach the test checkpoint bound"
        );
        assert_eq!(
            durable_raft_checkpoint_vote(&state_path),
            Some((
                expected_vote.leader_id.term,
                expected_vote.leader_id.node_id,
                expected_vote.committed,
            )),
            "bounded checkpoint should capture the acknowledged WAL vote"
        );
        let post_checkpoint_status = runtime
            .block_on(authority.status())
            .expect("post-checkpoint WAL-backed authority status should read");
        let post_checkpoint_offsets = post_checkpoint_status
            .durable_wal_offsets()
            .expect("post-checkpoint WAL-backed authority should report WAL offsets");
        assert_eq!(
            post_checkpoint_offsets.base_offset(),
            post_checkpoint_offsets.clean_len(),
            "bounded checkpoint should compact the acknowledged WAL suffix"
        );

        let no_op_wal_metrics_before = authority
            .durability_metric_snapshots()
            .wal
            .expect("WAL-backed authority should report WAL metrics");
        let no_op_offsets_before = runtime
            .block_on(authority.status())
            .expect("status before no-op vote should read")
            .durable_wal_offsets();
        assert!(
            send_vote(expected_vote.leader_id.term),
            "unchanged durable Raft state should receive a response"
        );
        let no_op_wal_metrics_after = authority
            .durability_metric_snapshots()
            .wal
            .expect("WAL-backed authority should report WAL metrics");
        assert_eq!(
            (
                no_op_wal_metrics_after.append_total,
                no_op_wal_metrics_after.append_error_total,
                no_op_wal_metrics_after.frame_bytes_total,
                no_op_wal_metrics_after.file_sync_total,
                no_op_wal_metrics_after.directory_sync_total,
            ),
            (
                no_op_wal_metrics_before.append_total,
                no_op_wal_metrics_before.append_error_total,
                no_op_wal_metrics_before.frame_bytes_total,
                no_op_wal_metrics_before.file_sync_total,
                no_op_wal_metrics_before.directory_sync_total,
            ),
            "no-op peer vote must not perform physical WAL I/O"
        );
        assert_eq!(
            runtime
                .block_on(authority.status())
                .expect("status after no-op vote should read")
                .durable_wal_offsets(),
            no_op_offsets_before,
            "no-op peer vote must not extend the WAL suffix"
        );
        assert!(
            !checkpoint_experimental_raft_peer_wal_if_due(
                runtime.handle(),
                &authority,
                &durability_context,
                &mut checkpoint_tracker,
                Instant::now(),
            )
            .expect("no-op checkpoint observation should succeed"),
            "no-op peer vote must not cause another checkpoint"
        );

        runtime
            .block_on(authority.shutdown())
            .expect("WAL-backed authority should shut down");
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn experimental_raft_wal_checkpoint_observer_captures_local_election_without_peer_rpc() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let state_dir = short_unix_socket_test_dir("experimental-raft-local-election-checkpoint");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).expect("durable test directory should exist");
        let state_path = state_dir.join("control-plane.state");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-local-election-checkpoint-{}",
            std::process::id()
        );
        let (authority, initial_vote) = runtime.block_on(async {
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name,
                1,
                &state_path,
            )
            .await
            .expect("WAL-backed durable authority should initialize");
            authority
                .initialize_single_node_membership(1)
                .await
                .expect("single-node membership should initialize");
            authority
                .wait_for_current_leader(
                    1,
                    Duration::from_secs(1),
                    "local-election checkpoint baseline leadership",
                )
                .await
                .expect("single-node authority should become leader");
            wait_for_experimental_raft_local_authority_serving(
                &authority,
                Duration::from_secs(1),
                "local-election checkpoint baseline",
            )
            .await
            .expect("single-node authority should apply committed membership");
            authority
                .store_durable_restart_artifact()
                .await
                .expect("leader baseline authority state should checkpoint");
            let vote_granted = authority
                .force_step_down_for_test(2)
                .await
                .expect("higher peer vote should step down the local leader");
            assert!(vote_granted);
            let stepped_down = authority
                .status()
                .await
                .expect("stepped-down authority status should read");
            assert!(!stepped_down.local_leader());
            let initial_vote = stepped_down
                .persisted_vote()
                .expect("stepped-down authority should have a persisted vote");
            (Arc::new(authority), initial_vote)
        });
        let checkpoint_loop = spawn_experimental_raft_peer_checkpoint_loop(
            runtime.handle().clone(),
            Arc::clone(&authority),
            ExperimentalRaftPeerDurabilityContext {
                artifact_path: Some(Arc::new(state_path.clone())),
                checkpoint_lock: Arc::new(Mutex::new(())),
            },
            ExperimentalRaftPeerCheckpointPolicy {
                max_wal_suffix_bytes: u64::MAX,
                max_mutations: u64::MAX,
                max_delay: Duration::from_millis(100),
                poll_interval: Duration::from_millis(10),
            },
        );

        let elected_vote = runtime.block_on(async {
            authority
                .trigger_local_election_for_test()
                .await
                .expect("local election should trigger");
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let vote = authority
                        .status()
                        .await
                        .expect("authority status should read during local election")
                        .persisted_vote()
                        .expect("local election should persist a vote");
                    if vote.committed
                        && vote.leader_id.node_id == 1
                        && vote.leader_id.term > initial_vote.leader_id.term
                    {
                        break vote;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("local election should commit the local persisted vote")
        });
        let checkpoint_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let status = runtime
                .block_on(authority.status())
                .expect("authority status should read while awaiting checkpoint");
            let offsets = status
                .durable_wal_offsets()
                .expect("WAL-backed authority should report offsets");
            let checkpointed_vote = durable_raft_checkpoint_vote(&state_path);
            if checkpointed_vote.is_some_and(|(term, node_id, committed)| {
                committed && node_id == 1 && term >= elected_vote.leader_id.term
            }) && offsets.base_offset() == offsets.clean_len()
            {
                break;
            }
            assert!(
                Instant::now() < checkpoint_deadline,
                "WAL observer did not checkpoint locally initiated election; expected at least \
                 {elected_vote:?}, checkpointed={checkpointed_vote:?}, offsets={offsets:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }

        authority
            .durability_publication()
            .expect("test authority durability publication should initialize")
            .poison("test checkpoint observer shutdown");
        checkpoint_loop
            .join()
            .expect("checkpoint observer should stop after poison gate closes");
        runtime
            .block_on(authority.shutdown())
            .expect("WAL-backed authority should shut down");
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn experimental_raft_control_plane_durable_restart_restores_heartbeat_refresh() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-durable-heartbeat");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).unwrap();
        let state_path = state_dir.join("control-plane.state");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![7];

        let mut harness = experimental_raft_durable_test_harness("heartbeat-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("durable experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                20_000,
            )
            .expect("durable experimental startup heartbeat should checkpoint");

        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let proof = PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        let peering_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_100,
            )
            .expect("durable experimental peering heartbeat should checkpoint");
        assert_eq!(peering_refresh.lease().lease_deadline_ms(), 20_600);
        let peering_route = &peering_refresh.runtime_map().pg_routes()[0];
        assert_eq!(peering_route.state(), PgState::Active);
        assert_eq!(peering_route.primary_lease_deadline_ms(), Some(20_600));

        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after peering completion")
            .cluster_epoch();
        let active_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_200,
            )
            .expect("durable experimental active heartbeat should checkpoint");
        assert!(active_refresh.lease().serving());
        assert_eq!(active_refresh.lease().lease_deadline_ms(), 20_800);
        let steady_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_300,
            )
            .expect("unchanged durable experimental active heartbeat should stay live");
        assert_eq!(steady_refresh.lease().lease_deadline_ms(), 20_900);
        let live_before_restart = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read before restart");
        assert_eq!(
            live_before_restart
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms(),
            Some(20_900)
        );
        assert!(state_path.exists());
        harness.shutdown();

        let mut restarted =
            experimental_raft_durable_test_harness("heartbeat-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut restarted.control_plane, &config)
            .expect("durable experimental raft control-plane restart bootstrap should be a no-op");
        let restored = restarted
            .control_plane
            .current_snapshot()
            .expect("durable experimental raft snapshot should read after restart");
        assert_eq!(
            restored.node(NodeId::new(1)).unwrap().lease_deadline_ms(),
            Some(20_800),
            "leader-local covered renewal must not enter the durable restart artifact"
        );
        assert_ne!(restored, live_before_restart);

        let durable_runtime_map =
            ControlPlaneRuntimeMapSource::runtime_map_snapshot(&restarted.control_plane, 20_400)
                .expect("restart should serve from the last durable Active observation");
        assert_eq!(
            durable_runtime_map.pg_routes()[0].primary_lease_deadline_ms(),
            Some(20_800)
        );
        let applied_before_refresh = restarted
            .control_plane
            .block_on(restarted.authority.status())
            .expect("restarted experimental Raft status should read")
            .applied();
        let refreshed = restarted
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: restored.cluster_epoch(),
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(7),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                20_400,
            )
            .expect("fresh post-restart heartbeat should renew live serving state");
        assert!(refreshed.lease().serving());
        assert_eq!(refreshed.lease().lease_deadline_ms(), 21_000);
        assert_eq!(
            restarted
                .control_plane
                .block_on(restarted.authority.status())
                .expect("restarted experimental Raft status should read after refresh")
                .applied(),
            applied_before_refresh,
            "post-restart heartbeat covered by the restored horizon should remain volatile"
        );
        let runtime_map =
            ControlPlaneRuntimeMapSource::runtime_map_snapshot(&restarted.control_plane, 20_400)
                .expect("durable experimental raft runtime map should read after fresh heartbeat");
        let restored_route = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(7))
            .expect("restored runtime map should include PG route");
        assert_eq!(restored_route.state(), PgState::Active);
        assert_eq!(restored_route.primary_node_id(), NodeId::new(1));
        assert_eq!(restored_route.primary_lease_deadline_ms(), Some(21_000));

        restarted.shutdown();
        fs::remove_dir_all(&state_dir).unwrap();
    }

    #[test]
    fn experimental_raft_wal_recovers_heartbeat_acknowledged_before_checkpoint() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-wal-heartbeat-restart");
        let state_path = state_dir.0.path().join("control-plane.state");
        let endpoint = state_dir.0.path().join("node-1.sock");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: endpoint.display().to_string(),
        }];
        config.storage_pg_ids = vec![7];

        let mut harness =
            experimental_raft_durable_wal_test_harness("wal-heartbeat-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("WAL-backed control-plane bootstrap should succeed");
        let checkpoint_before = harness.authority.durability_metric_snapshots().checkpoint;
        let wal_offsets_before = harness
            .authority
            .durable_wal_monitor_snapshot()
            .expect("baseline WAL offsets should read")
            .offsets();
        let observed_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("baseline snapshot should read")
            .cluster_epoch();

        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: endpoint.display().to_string(),
                    observed_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                20_000,
            )
            .expect("WAL-backed heartbeat should be acknowledged");
        harness.control_plane.checkpoint_serving_reads = true;
        harness
            .control_plane
            .runtime_map_status(20_000)
            .expect("WAL-backed serving read should use the synced WAL boundary");
        let expected = harness
            .control_plane
            .current_snapshot()
            .expect("acknowledged heartbeat snapshot should read");
        assert_eq!(
            harness.authority.durability_metric_snapshots().checkpoint,
            checkpoint_before,
            "WAL-backed heartbeat acknowledgement must not synchronously rewrite the artifact"
        );
        let wal_offsets_after = harness
            .authority
            .durable_wal_monitor_snapshot()
            .expect("post-heartbeat WAL offsets should read")
            .offsets();
        assert_eq!(
            wal_offsets_after.base_offset(),
            wal_offsets_before.base_offset()
        );
        assert!(
            wal_offsets_after.clean_len() > wal_offsets_before.clean_len(),
            "acknowledged heartbeat must advance the durable WAL suffix"
        );
        harness.shutdown();

        let restarted =
            experimental_raft_durable_wal_test_harness("wal-heartbeat-restart", &state_path);
        let restored = restarted
            .control_plane
            .current_snapshot()
            .expect("artifact plus WAL heartbeat state should restore");
        assert_eq!(
            restored.node(NodeId::new(1)).map(|node| (
                node.node_incarnation(),
                node.last_observed_epoch(),
                node.lease_deadline_ms(),
            )),
            expected.node(NodeId::new(1)).map(|node| (
                node.node_incarnation(),
                node.last_observed_epoch(),
                node.lease_deadline_ms(),
            )),
            "restart must replay the acknowledged heartbeat from the WAL suffix"
        );
        restarted.shutdown();
    }

    #[test]
    fn experimental_raft_wal_recovers_purge_after_snapshot_checkpoint() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-wal-snapshot-purge-restart");
        let state_path = state_dir.0.path().join("control-plane.state");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: state_dir.0.path().join("node-1.sock").display().to_string(),
        }];
        config.storage_pg_ids = vec![7];

        let mut harness =
            experimental_raft_durable_wal_test_harness("wal-snapshot-purge-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("WAL-backed control-plane bootstrap should succeed");
        let snapshot_log_id = harness
            .control_plane
            .block_on(harness.authority.trigger_snapshot_applied())
            .expect("coordinated snapshot should build")
            .expect("bootstrapped state should have an applied log id");
        harness
            .control_plane
            .store_durable_restart_artifact()
            .expect("snapshot payload must be durable before purge");
        let checkpoint_before_purge = harness.authority.durability_metric_snapshots().checkpoint;
        harness
            .control_plane
            .block_on(
                harness
                    .authority
                    .purge_log_through_snapshot(snapshot_log_id),
            )
            .expect("snapshot-covered log prefix should purge");
        assert_eq!(
            harness.authority.durability_metric_snapshots().checkpoint,
            checkpoint_before_purge,
            "purge must be recoverable before its post-purge artifact checkpoint"
        );
        let purge_wal = harness
            .authority
            .durable_wal_monitor_snapshot()
            .expect("post-purge WAL state should read")
            .offsets();
        assert!(
            purge_wal.clean_len() > purge_wal.base_offset(),
            "purge should remain as a replayable WAL suffix before post-purge checkpoint"
        );
        let expected = harness
            .control_plane
            .block_on(harness.authority.durable_state_machine_snapshot_for_test())
            .expect("pre-crash state-machine snapshot should read");
        harness.shutdown();

        let restarted =
            experimental_raft_durable_wal_test_harness("wal-snapshot-purge-restart", &state_path);
        let restored = restarted
            .control_plane
            .block_on(
                restarted
                    .authority
                    .durable_state_machine_snapshot_for_test(),
            )
            .expect("artifact plus purge WAL restart should restore state");
        assert_eq!(restored, expected);
        assert_eq!(
            restarted
                .control_plane
                .block_on(restarted.authority.status())
                .expect("restarted purge status should read")
                .last_purged_log_id(),
            Some(snapshot_log_id)
        );
        restarted.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_expires_heartbeat_leases() {
        let mut harness = experimental_raft_test_harness("lease-expiry-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![17];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                50_000,
            )
            .expect("experimental raft startup heartbeat should refresh");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let proof = PgMetadataProof {
            applied_log_index: 92,
            applied_log_hash: 0x1234,
            state_digest: 0x5678,
        };
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                50_100,
            )
            .expect("experimental raft peering heartbeat should refresh");
        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after peering completion")
            .cluster_epoch();
        let active_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                50_200,
            )
            .expect("experimental raft active heartbeat should refresh");
        assert_eq!(active_refresh.lease().lease_deadline_ms(), 50_900);
        let active_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental active snapshot should read");
        let active_cluster_epoch = active_snapshot.cluster_epoch();
        assert_eq!(
            active_snapshot.pg(PgId::new(17)).unwrap().state(),
            PgState::Active
        );
        let renewed = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_cluster_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                50_400,
            )
            .expect("unchanged active heartbeat should renew the live overlay");
        assert_eq!(renewed.lease().lease_deadline_ms(), 51_100);
        let rejected = harness
            .control_plane
            .submit_raft_command(ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(99),
                availability: NodeAvailabilityState::Unavailable,
            })
            .expect_err("unrelated unknown-node command should reject after committing");
        assert!(matches!(rejected, ControlPlaneError::UnknownNode { .. }));
        assert_eq!(
            harness
                .control_plane
                .current_snapshot()
                .expect("rejected command should rebase the live overlay")
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms(),
            Some(51_100)
        );
        let applied_before_empty_scan = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read before empty expiry scan")
            .applied();

        let no_expiry = harness
            .control_plane
            .expire_heartbeat_leases(50_900)
            .expect("the old durable deadline must not expire the acknowledged lease");
        assert_eq!(no_expiry, (active_cluster_epoch, 0, 0));
        assert_eq!(
            harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("experimental Raft status should read after empty expiry scan")
                .applied(),
            applied_before_empty_scan,
            "an empty expiry scan must not append a timestamp-only Raft command"
        );

        let expiry = harness
            .control_plane
            .expire_heartbeat_leases(51_100)
            .expect("acknowledged deadline expiry should apply through raft");
        assert!(expiry.0 > active_cluster_epoch);
        assert_eq!(expiry.1, 1);
        assert_eq!(expiry.2, 1);
        let expired_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental expired snapshot should read");
        let node = expired_snapshot
            .node(NodeId::new(1))
            .expect("expired node should remain recorded");
        assert_eq!(node.availability(), NodeAvailabilityState::Unavailable);
        assert_eq!(node.lease_deadline_ms(), None);
        let pg = expired_snapshot
            .pg(PgId::new(17))
            .expect("expired PG should remain recorded");
        assert_eq!(pg.state(), PgState::Peering);
        assert_eq!(pg.active_primary(), None);
        assert_eq!(pg.peering_metadata_proof_floor(), Some(proof));
        assert_eq!(pg.previous_primary_lease_deadline_ms(), Some(51_100));

        let successor_endpoint = "/tmp/argmin-experimental-raft-node-1.sock".to_string();
        let first_successor = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 2,
                    endpoint: successor_endpoint.clone(),
                    observed_epoch: expired_snapshot.cluster_epoch(),
                    requested_lease_duration_ms: 3_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                51_101,
            )
            .expect("successor heartbeat should be recorded while activation remains fenced");
        let successor_epoch = first_successor.runtime_map().cluster_epoch();
        let fenced = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 2,
                    endpoint: successor_endpoint.clone(),
                    observed_epoch: successor_epoch,
                    requested_lease_duration_ms: 3_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                52_099,
            )
            .expect("successor heartbeat at the skew fence should remain non-serving");
        assert_eq!(
            fenced.runtime_map().pg_routes()[0].state(),
            PgState::Peering
        );

        let activated = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 2,
                    endpoint: successor_endpoint,
                    observed_epoch: fenced.runtime_map().cluster_epoch(),
                    requested_lease_duration_ms: 3_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                52_100,
            )
            .expect("successor should activate after the acknowledged lease and skew fence");
        assert_eq!(
            activated.runtime_map().pg_routes()[0].state(),
            PgState::Active
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_targeted_expiry_preserves_unlisted_volatile_renewal() {
        let mut harness = experimental_raft_test_harness("targeted-lease-expiry-overlay-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids.clear();
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        for (node_id, now_ms, lease_ms) in [(1, 50_000, 900), (2, 50_010, 1_000)] {
            let observed_epoch = harness
                .control_plane
                .current_snapshot()
                .expect("experimental snapshot should read before startup heartbeat")
                .cluster_epoch();
            harness
                .control_plane
                .refresh_node_heartbeat(
                    NodeHeartbeat {
                        node_id: NodeId::new(node_id),
                        node_incarnation: 1,
                        endpoint: format!("/tmp/argmin-experimental-raft-node-{node_id}.sock"),
                        observed_epoch,
                        requested_lease_duration_ms: lease_ms,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    now_ms,
                )
                .expect("experimental raft startup heartbeat should refresh");
        }

        let observed_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read before volatile renewals")
            .cluster_epoch();
        for (node_id, lease_ms) in [(1, 800), (2, 1_000)] {
            harness
                .control_plane
                .refresh_node_heartbeat(
                    NodeHeartbeat {
                        node_id: NodeId::new(node_id),
                        node_incarnation: 1,
                        endpoint: format!("/tmp/argmin-experimental-raft-node-{node_id}.sock"),
                        observed_epoch,
                        requested_lease_duration_ms: lease_ms,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    50_100,
                )
                .expect("experimental raft heartbeat should acknowledge the current epoch");
        }
        let applied_before_volatile_renewals = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read before volatile renewals")
            .applied();
        for (node_id, lease_ms) in [(1, 700), (2, 1_000)] {
            harness
                .control_plane
                .refresh_node_heartbeat(
                    NodeHeartbeat {
                        node_id: NodeId::new(node_id),
                        node_incarnation: 1,
                        endpoint: format!("/tmp/argmin-experimental-raft-node-{node_id}.sock"),
                        observed_epoch,
                        requested_lease_duration_ms: lease_ms,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    50_200,
                )
                .expect("covered experimental raft heartbeat should renew volatile lease");
        }
        assert_eq!(
            harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("experimental Raft status should read after volatile renewals")
                .applied(),
            applied_before_volatile_renewals,
            "steady covered renewals must not append OpenRaft commands"
        );
        let before_expiry = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should include volatile renewals");
        assert_eq!(
            before_expiry
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms(),
            Some(50_900)
        );
        assert_eq!(
            before_expiry
                .node(NodeId::new(2))
                .unwrap()
                .lease_deadline_ms(),
            Some(51_200)
        );

        let expiry = harness
            .control_plane
            .expire_heartbeat_leases(50_900)
            .expect("targeted expiry should commit through Raft");
        assert_eq!(expiry.1, 1);
        let after_expiry = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should retain unexpired overlay lease");
        assert_eq!(
            after_expiry
                .node(NodeId::new(1))
                .unwrap()
                .observed_availability(),
            NodeAvailabilityState::Unavailable
        );
        let unlisted = after_expiry.node(NodeId::new(2)).unwrap();
        assert_eq!(
            unlisted.observed_availability(),
            NodeAvailabilityState::Healthy
        );
        assert_eq!(unlisted.lease_deadline_ms(), Some(51_200));

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_elapsed_expiry_advances_timestamp() {
        let mut harness = experimental_raft_test_harness("lease-expiry-far-forward-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![17];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                50_000,
            )
            .expect("experimental raft startup heartbeat should refresh");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let proof = PgMetadataProof {
            applied_log_index: 92,
            applied_log_hash: 0x1234,
            state_digest: 0x5678,
        };
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                50_100,
            )
            .expect("experimental raft peering heartbeat should refresh");
        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after peering completion")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                50_200,
            )
            .expect("experimental raft active heartbeat should refresh");

        let far_future_now_ms =
            50_201 + storage::control_plane::CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 123;
        let expiry = harness
            .control_plane
            .expire_heartbeat_leases(far_future_now_ms)
            .expect("elapsed expiry should commit through raft");
        assert_eq!(expiry.1, 1);
        assert_eq!(expiry.2, 1);
        let expired_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental far-forward expired snapshot should read");
        assert_eq!(
            expired_snapshot.max_committed_timestamp_ms(),
            Some(far_future_now_ms)
        );
        let expired_node = expired_snapshot
            .node(NodeId::new(1))
            .expect("expired node should remain recorded");
        assert_eq!(
            expired_node.availability(),
            NodeAvailabilityState::Unavailable
        );
        assert_eq!(expired_node.lease_deadline_ms(), None);

        let recovery_now_ms = far_future_now_ms + 1;
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: expired_snapshot.cluster_epoch(),
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                recovery_now_ms,
            )
            .expect("post-downtime raft heartbeat should not remain wedged");
        let recovered_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental recovered snapshot should read");
        assert_eq!(
            recovered_snapshot.max_committed_timestamp_ms(),
            Some(recovery_now_ms)
        );
        let recovered_node = recovered_snapshot
            .node(NodeId::new(1))
            .expect("recovered node should remain recorded");
        assert_eq!(
            recovered_node.availability(),
            NodeAvailabilityState::Healthy
        );
        assert_eq!(
            recovered_node.lease_deadline_ms(),
            Some(recovery_now_ms + 500)
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_resamples_expiry_time_when_enabled() {
        let mut harness = experimental_raft_test_harness("lease-expiry-resample-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![17];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                50_000,
            )
            .expect("experimental raft startup heartbeat should refresh");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let proof = PgMetadataProof {
            applied_log_index: 92,
            applied_log_hash: 0x1234,
            state_digest: 0x5678,
        };
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                50_100,
            )
            .expect("experimental raft peering heartbeat should refresh");
        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after peering completion")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(17),
                        state: PgState::Active,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                50_200,
            )
            .expect("experimental raft active heartbeat should refresh");
        let expiry = storage::clock::with_time_override(50_900, || {
            enable_resampled_authority_time(&mut harness.control_plane, 50_900);
            harness.control_plane.expire_heartbeat_leases(50_000)
        })
        .expect("deadline expiry should use resampled time");
        assert_eq!(expiry.1, 1);
        assert_eq!(expiry.2, 1);
        let expired_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental expired snapshot should read");
        assert_eq!(
            expired_snapshot
                .node(NodeId::new(1))
                .expect("expired node should remain recorded")
                .availability(),
            NodeAvailabilityState::Unavailable
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_serves_unix_heartbeat_refresh() {
        let mut harness = experimental_raft_test_harness("unix-heartbeat-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![9];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        let tmp = short_unix_socket_test_dir("experimental-raft-unix-heartbeat");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let startup_server =
            spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 30_000);
        let mut client = UnixControlPlaneClient::new(&socket_path);
        let startup_refresh = client
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                0,
            )
            .expect("Unix heartbeat refresh should succeed");
        startup_server.join().unwrap();
        assert_eq!(startup_refresh.lease().lease_deadline_ms(), 30_500);

        std::fs::remove_file(&socket_path).unwrap();
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let proof = PgMetadataProof {
            applied_log_index: 77,
            applied_log_hash: 0x123,
            state_digest: 0x456,
        };
        let peering_server =
            spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 30_100);
        let mut client = UnixControlPlaneClient::new(&socket_path);
        let peering_refresh = client
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(9),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: None,
                    }],
                },
                0,
            )
            .expect("Unix Peering heartbeat refresh should succeed");
        peering_server.join().unwrap();
        assert_eq!(peering_refresh.lease().lease_deadline_ms(), 30_700);
        let route = &peering_refresh.runtime_map().pg_routes()[0];
        assert_eq!(route.pg_id(), PgId::new(9));
        assert_eq!(route.state(), PgState::Active);
        assert_eq!(route.primary_node_id(), NodeId::new(1));
        assert_eq!(route.primary_lease_deadline_ms(), Some(30_700));

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_unix_heartbeat_rejects_unknown_node() {
        let mut harness = experimental_raft_test_harness("unix-heartbeat-reject-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![9];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let before = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read before rejected heartbeat");

        let tmp = short_unix_socket_test_dir("experimental-raft-unix-heartbeat-reject");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 30_050);
        let mut client = UnixControlPlaneClient::new(&socket_path);
        let error = client
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(99),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-99.sock".to_string(),
                    observed_epoch: before.cluster_epoch(),
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                0,
            )
            .expect_err("Unix heartbeat rejection should cross the RPC boundary");
        server.join().unwrap();

        assert!(matches!(
            error,
            ControlPlaneError::UnknownNode { node_id: 99 }
        ));
        let after = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after rejected heartbeat");
        assert_eq!(after, before);

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_serves_unix_runtime_map_read_index() {
        let mut harness = experimental_raft_test_harness("unix-runtime-map-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![11];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        let tmp = short_unix_socket_test_dir("experimental-raft-unix-runtime-map");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 31_000);
        let client = UnixControlPlaneClient::new(&socket_path);
        let runtime_map = client
            .runtime_map_snapshot(0)
            .expect("Unix runtime-map read should succeed");
        server.join().unwrap();

        assert_eq!(runtime_map.nodes().len(), 1);
        assert_eq!(runtime_map.nodes()[0].node_id(), NodeId::new(1));
        assert_eq!(
            runtime_map.nodes()[0].endpoint(),
            "/tmp/argmin-experimental-raft-node-1.sock"
        );
        assert_eq!(runtime_map.pg_routes().len(), 1);
        assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(11));
        assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(31_000));
        let read_index = runtime_map
            .freshness_proof()
            .read_index()
            .expect("experimental raft runtime-map proof should carry a read index");
        assert_ne!(read_index.term(), 0);
        assert_ne!(read_index.index(), 0);
        assert!(runtime_map.freshness_proof().is_serving_authority_read());

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_serves_runtime_map_admin_helpers() {
        let mut harness = experimental_raft_test_harness("runtime-map-admin-helpers-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![12];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let before = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read before diagnostic helpers");

        let tmp = short_unix_socket_test_dir("experimental-raft-runtime-map-admin");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let ready_server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 31_100);
        let (ready_epoch, pg_routes, active_serving_pg_routes) =
            control_plane_runtime_map_ready(&socket_path)
                .expect("runtime-map ready helper should read experimental raft map");
        ready_server.join().unwrap();

        assert_eq!(ready_epoch, before.cluster_epoch());
        assert_eq!(pg_routes, 1);
        assert_eq!(active_serving_pg_routes, 0);

        std::fs::remove_file(&socket_path).unwrap();
        let diagnostics_server =
            spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 31_200);
        let diagnostics = control_plane_runtime_map_diagnostics(&socket_path)
            .expect("runtime-map diagnostics helper should read experimental raft map");
        diagnostics_server.join().unwrap();

        assert!(
            diagnostics.contains(&format!("epoch={}", before.cluster_epoch().get())),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("nodes=1"), "{diagnostics}");
        assert!(diagnostics.contains("pg_routes=1"), "{diagnostics}");
        assert!(
            diagnostics.contains("active_serving_pg_routes=0"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "node_id=1 incarnation=0 endpoint=/tmp/argmin-experimental-raft-node-1.sock"
            ),
            "{diagnostics}"
        );
        let after = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after diagnostic helpers");
        assert_eq!(after, before);

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_serves_unix_acting_set_admin() {
        let mut harness = experimental_raft_test_harness("unix-acting-set-admin-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids = vec![19];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();

        let tmp = short_unix_socket_test_dir("experimental-raft-unix-acting-set");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 32_000);
        let client = UnixControlPlaneClient::new(&socket_path);
        let changed_epoch = client
            .set_pg_acting_set(PgId::new(19), vec![NodeId::new(2)])
            .expect("Unix acting-set admin request should succeed");
        server.join().unwrap();

        assert!(changed_epoch > bootstrap_epoch);
        let snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after acting-set change");
        assert_eq!(snapshot.cluster_epoch(), changed_epoch);
        let pg = snapshot
            .pg(PgId::new(19))
            .expect("changed PG should remain present");
        assert_eq!(pg.state(), PgState::Peering);
        assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
        assert_eq!(pg.active_primary(), None);

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_serves_live_acting_set_helper() {
        let mut harness = experimental_raft_test_harness("live-acting-set-helper-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids = vec![19];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();

        let tmp = short_unix_socket_test_dir("experimental-raft-live-acting-set");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let server =
            spawn_experimental_raft_unix_rpc_server_requests(&harness, &socket_path, 32_050, 2);
        let changed_epoch = set_control_plane_pg_acting_set_live(&socket_path, 19, vec![2])
            .expect("live acting-set helper should succeed");
        server.join().unwrap();

        assert!(changed_epoch > bootstrap_epoch.get());
        let snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after acting-set helper");
        assert_eq!(snapshot.cluster_epoch().get(), changed_epoch);
        let pg = snapshot
            .pg(PgId::new(19))
            .expect("changed PG should remain present");
        assert_eq!(pg.state(), PgState::Peering);
        assert_eq!(pg.acting_set(), &[NodeId::new(2)]);

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_unix_acting_set_admin_rejects_unknown_node() {
        let mut harness = experimental_raft_test_harness("unix-acting-set-admin-reject-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![19];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        let before = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read before rejected admin command");

        let tmp = short_unix_socket_test_dir("experimental-raft-unix-acting-set-reject");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 32_100);
        let client = UnixControlPlaneClient::new(&socket_path);
        let error = client
            .set_pg_acting_set(PgId::new(19), vec![NodeId::new(99)])
            .expect_err("Unix acting-set admin rejection should cross the RPC boundary");
        server.join().unwrap();

        assert!(matches!(
            error,
            ControlPlaneError::UnknownActingSetNode {
                pg_id: 19,
                node_id: 99,
            }
        ));
        let after = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after rejected admin command");
        assert_eq!(after, before);

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_durable_restart_restores_acting_set_admin() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-durable-acting-set");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).unwrap();
        let state_path = state_dir.join("control-plane.state");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids = vec![19];

        let mut harness = experimental_raft_durable_test_harness("acting-set-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("durable experimental raft control-plane bootstrap should succeed");
        let changed = harness
            .control_plane
            .set_pg_acting_set(PgId::new(19), vec![NodeId::new(2)])
            .expect("durable acting-set admin command should checkpoint");
        let changed_epoch = changed.cluster_epoch();
        let changed_pg = changed
            .pg(PgId::new(19))
            .expect("changed PG should remain present");
        assert_eq!(changed_pg.state(), PgState::Peering);
        assert_eq!(changed_pg.acting_set(), &[NodeId::new(2)]);
        assert!(state_path.exists());
        harness.shutdown();

        let mut restarted =
            experimental_raft_durable_test_harness("acting-set-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut restarted.control_plane, &config)
            .expect("durable experimental raft control-plane restart bootstrap should be a no-op");
        let restored = restarted
            .control_plane
            .current_snapshot()
            .expect("durable experimental raft snapshot should read after restart");
        assert_eq!(restored.cluster_epoch(), changed_epoch);
        let restored_pg = restored
            .pg(PgId::new(19))
            .expect("restored PG should remain present");
        assert_eq!(restored_pg.state(), PgState::Peering);
        assert_eq!(restored_pg.acting_set(), &[NodeId::new(2)]);
        assert_eq!(restored_pg.active_primary(), None);

        restarted.shutdown();
        fs::remove_dir_all(&state_dir).unwrap();
    }

    #[test]
    fn experimental_raft_control_plane_durable_restart_preserves_rejected_admin_entry() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-durable-reject");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).unwrap();
        let state_path = state_dir.join("control-plane.state");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![19];

        let mut harness = experimental_raft_durable_test_harness("reject-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("durable experimental raft control-plane bootstrap should succeed");
        let before = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read before rejected admin command");
        let before_status = harness
            .runtime
            .block_on(harness.authority.status())
            .expect("durable experimental status should read before rejection");
        let before_applied_index = before_status
            .applied_index()
            .expect("bootstrapped durable authority should have an applied index");

        let error = harness
            .control_plane
            .set_pg_acting_set(PgId::new(19), vec![NodeId::new(99)])
            .expect_err("durable rejected admin command should return the semantic error");
        assert!(error
            .to_string()
            .contains("PG 19 acting set references unknown node 99"));
        let after = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after rejected admin command");
        assert_eq!(after, before);
        let after_status = harness
            .runtime
            .block_on(harness.authority.status())
            .expect("durable experimental status should read after rejection");
        let after_applied_index = after_status
            .applied_index()
            .expect("rejected command should still advance the applied index");
        assert!(after_applied_index > before_applied_index);
        assert_eq!(after_status.committed_index(), Some(after_applied_index));
        assert!(state_path.exists());
        harness.shutdown();

        let mut restarted = experimental_raft_durable_test_harness("reject-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut restarted.control_plane, &config)
            .expect("durable experimental raft control-plane restart bootstrap should be a no-op");
        let restored = restarted
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after rejected-entry restart");
        assert_eq!(restored, before);
        let restored_status = restarted
            .runtime
            .block_on(restarted.authority.status())
            .expect("restarted durable experimental status should read");
        assert_eq!(restored_status.applied_index(), Some(after_applied_index));
        assert_eq!(restored_status.committed_index(), Some(after_applied_index));

        restarted.shutdown();
        fs::remove_dir_all(&state_dir).unwrap();
    }

    #[test]
    fn experimental_raft_control_plane_serves_unix_metadata_transfer_admin() {
        let mut harness = experimental_raft_test_harness("unix-transfer-admin-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids = vec![13];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        harness
            .control_plane
            .set_pg_acting_set(PgId::new(13), vec![NodeId::new(1)])
            .expect("source acting set should install");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                40_000,
            )
            .expect("experimental raft startup heartbeat should refresh");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let active_proof = PgMetadataProof {
            applied_log_index: 91,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(13),
                        state: PgState::Peering,
                        metadata_proof: active_proof,
                        pending_metadata_command: None,
                    }],
                },
                40_100,
            )
            .expect("experimental raft peering heartbeat should refresh");
        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after peering completion")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(13),
                        state: PgState::Active,
                        metadata_proof: active_proof,
                        pending_metadata_command: None,
                    }],
                },
                40_200,
            )
            .expect("experimental raft active heartbeat should refresh");
        let live_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental active snapshot should read before volatile renewal")
            .cluster_epoch();
        let applied_before_volatile_renewal = harness
            .control_plane
            .block_on(harness.authority.status())
            .expect("experimental Raft status should read before volatile renewal")
            .applied();
        let renewed = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: live_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(13),
                        state: PgState::Active,
                        metadata_proof: active_proof,
                        pending_metadata_command: None,
                    }],
                },
                40_400,
            )
            .expect("covered heartbeat should renew the source lease in the live overlay");
        assert_eq!(renewed.lease().lease_deadline_ms(), 41_100);
        assert_eq!(
            harness
                .control_plane
                .block_on(harness.authority.status())
                .expect("experimental Raft status should read after volatile renewal")
                .applied(),
            applied_before_volatile_renewal,
            "covered source renewal must remain leader-local before fencing"
        );

        let tmp = short_unix_socket_test_dir("experimental-raft-unix-transfer-admin");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let fence_server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 40_500);
        let client = UnixControlPlaneClient::new(&socket_path);
        let fenced = client
            .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(PgId::new(13))
            .expect("Unix metadata-transfer fence should succeed");
        fence_server.join().unwrap();
        assert_eq!(fenced.source_primary_lease_deadline_ms(), Some(41_100));
        let fenced_epoch = fenced.runtime_map().cluster_epoch();
        assert!(fenced_epoch > active_epoch);
        let fenced_route = fenced
            .runtime_map()
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(13))
            .expect("fenced runtime map should include source PG");
        assert_eq!(fenced_route.state(), PgState::Peering);
        assert_eq!(fenced_route.acting_set(), &[NodeId::new(1)]);
        assert_eq!(fenced_route.primary_lease_deadline_ms(), None);
        let durable_fence_deadline = harness
            .control_plane
            .block_on(harness.authority.durable_state_machine_snapshot_for_test())
            .expect("durable metadata-transfer fence state should read")
            .pg(PgId::new(13))
            .and_then(|pg| pg.metadata_transfer_fence_source_lease_deadline_ms());
        assert_eq!(
            durable_fence_deadline,
            Some(41_100),
            "the committed fence must promote the acknowledged live lease deadline"
        );

        std::fs::remove_file(&socket_path).unwrap();
        let transfer = PgMetadataTransferProof::new(active_epoch, active_proof);
        let install_server =
            spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 40_600);
        let client = UnixControlPlaneClient::new(&socket_path);
        let expected_destination_epoch = ClusterEpoch::new(
            fenced
                .runtime_map()
                .cluster_epoch()
                .get()
                .checked_add(1)
                .unwrap(),
        )
        .unwrap();
        let transfer_runtime_map = client
            .set_pg_acting_set_with_metadata_transfer_runtime_map(
                PgId::new(13),
                vec![NodeId::new(2)],
                transfer,
                expected_destination_epoch,
            )
            .expect("Unix metadata-transfer acting set install should succeed");
        install_server.join().unwrap();
        let transfer_route = transfer_runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(13))
            .expect("transfer runtime map should include destination PG");
        assert_eq!(transfer_route.state(), PgState::Peering);
        assert_eq!(transfer_route.acting_set(), &[NodeId::new(2)]);
        assert_eq!(transfer_route.peering_metadata_transfer(), Some(transfer));
        assert_eq!(
            transfer_route.peering_metadata_transfer_source_route_epoch(),
            Some(fenced_epoch)
        );
        assert_eq!(
            transfer_route.peering_metadata_transfer_source_node_id(),
            Some(NodeId::new(1))
        );

        let snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after transfer install");
        let pg = snapshot
            .pg(PgId::new(13))
            .expect("transferred PG should remain present");
        assert_eq!(pg.state(), PgState::Peering);
        assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
        assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
        assert!(!pg.metadata_transfer_fenced());

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn experimental_raft_control_plane_durable_restart_restores_metadata_transfer_admin() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-durable-transfer-admin");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).unwrap();
        let state_path = state_dir.join("control-plane.state");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids = vec![13];

        let mut harness = experimental_raft_durable_test_harness("transfer-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("durable experimental raft control-plane bootstrap should succeed");
        harness
            .control_plane
            .set_pg_acting_set(PgId::new(13), vec![NodeId::new(1)])
            .expect("durable source acting set should checkpoint");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                40_000,
            )
            .expect("durable experimental startup heartbeat should checkpoint");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let active_proof = PgMetadataProof {
            applied_log_index: 91,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(13),
                        state: PgState::Peering,
                        metadata_proof: active_proof,
                        pending_metadata_command: None,
                    }],
                },
                40_100,
            )
            .expect("durable experimental peering heartbeat should checkpoint");
        let active_snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after peering completion");
        let active_epoch = active_snapshot.cluster_epoch();
        assert_eq!(
            active_snapshot
                .pg(PgId::new(13))
                .expect("source PG should exist")
                .active_primary(),
            Some(NodeId::new(1))
        );
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(13),
                        state: PgState::Active,
                        metadata_proof: active_proof,
                        pending_metadata_command: None,
                    }],
                },
                40_200,
            )
            .expect("durable experimental active heartbeat should checkpoint");

        let fenced = harness
            .control_plane
            .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(13))
            .expect("durable metadata-transfer fence should checkpoint");
        assert_eq!(fenced.source_primary_lease_deadline_ms(), Some(40_900));
        let (fenced_snapshot, _) = fenced.into_parts();
        let fenced_epoch = fenced_snapshot.cluster_epoch();
        assert!(fenced_epoch > active_epoch);
        let fenced_pg = fenced_snapshot
            .pg(PgId::new(13))
            .expect("fenced PG should remain present");
        assert_eq!(fenced_pg.state(), PgState::Peering);
        assert_eq!(fenced_pg.acting_set(), &[NodeId::new(1)]);
        assert!(fenced_pg.metadata_transfer_fenced());
        assert_eq!(
            fenced_pg.metadata_transfer_fence_source_lease_deadline_ms(),
            Some(40_900)
        );

        let transfer = PgMetadataTransferProof::new(active_epoch, active_proof);
        let expected_destination_epoch =
            ClusterEpoch::new(fenced_epoch.get().checked_add(1).unwrap()).unwrap();
        let transfer_snapshot = harness
            .control_plane
            .set_pg_acting_set_with_metadata_transfer(
                PgId::new(13),
                vec![NodeId::new(2)],
                transfer,
                expected_destination_epoch,
            )
            .expect("durable metadata-transfer acting-set install should checkpoint");
        let transfer_pg = transfer_snapshot
            .pg(PgId::new(13))
            .expect("transferred PG should remain present");
        assert_eq!(transfer_pg.state(), PgState::Peering);
        assert_eq!(transfer_pg.acting_set(), &[NodeId::new(2)]);
        assert_eq!(transfer_pg.peering_metadata_transfer(), Some(transfer));
        assert_eq!(
            transfer_pg.peering_metadata_transfer_source_route_epoch(),
            Some(fenced_epoch)
        );
        assert_eq!(
            transfer_pg.peering_metadata_transfer_source_node_id(),
            Some(NodeId::new(1))
        );
        assert!(!transfer_pg.metadata_transfer_fenced());
        assert!(state_path.exists());
        harness.shutdown();

        let mut restarted = experimental_raft_durable_test_harness("transfer-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(&mut restarted.control_plane, &config)
            .expect("durable experimental raft control-plane restart bootstrap should be a no-op");
        let restored = restarted
            .control_plane
            .current_snapshot()
            .expect("durable experimental raft snapshot should read after restart");
        assert_eq!(restored, transfer_snapshot);

        let runtime_map =
            ControlPlaneRuntimeMapSource::runtime_map_snapshot(&restarted.control_plane, 40_500)
                .expect("durable experimental raft runtime map should read after restart");
        let restored_route = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(13))
            .expect("restored runtime map should include transferred PG route");
        assert_eq!(restored_route.state(), PgState::Peering);
        assert_eq!(restored_route.acting_set(), &[NodeId::new(2)]);
        assert_eq!(restored_route.peering_metadata_transfer(), Some(transfer));
        assert_eq!(
            restored_route.peering_metadata_transfer_source_route_epoch(),
            Some(fenced_epoch)
        );
        assert_eq!(
            restored_route.peering_metadata_transfer_source_node_id(),
            Some(NodeId::new(1))
        );

        restarted.shutdown();
        fs::remove_dir_all(&state_dir).unwrap();
    }

    #[test]
    fn experimental_raft_control_plane_serves_metadata_transfer_live_helpers() {
        let mut harness = experimental_raft_test_harness("transfer-live-helper-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 1,
                socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: "/tmp/argmin-experimental-raft-node-2.sock".to_string(),
            },
        ];
        config.storage_pg_ids = vec![14];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");
        harness
            .control_plane
            .set_pg_acting_set(PgId::new(14), vec![NodeId::new(1)])
            .expect("source acting set should install");

        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 500,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                41_000,
            )
            .expect("experimental raft startup heartbeat should refresh");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        let active_proof = PgMetadataProof {
            applied_log_index: 101,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(14),
                        state: PgState::Peering,
                        metadata_proof: active_proof,
                        pending_metadata_command: None,
                    }],
                },
                41_100,
            )
            .expect("experimental raft peering heartbeat should refresh");
        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after peering completion")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 700,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(14),
                        state: PgState::Active,
                        metadata_proof: active_proof,
                        pending_metadata_command: None,
                    }],
                },
                41_150,
            )
            .expect("experimental raft active heartbeat should refresh");

        let tmp = short_unix_socket_test_dir("experimental-raft-transfer-live-helper");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let fence_server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 41_200);
        let fenced_epoch = fence_control_plane_pg_for_metadata_transfer_live(&socket_path, 14)
            .expect("live metadata-transfer fence helper should succeed");
        fence_server.join().unwrap();
        assert!(fenced_epoch > active_epoch.get());

        std::fs::remove_file(&socket_path).unwrap();
        let transfer = PgMetadataTransferProof::new(active_epoch, active_proof);
        let install = storage::ControlPlanePgMetadataTransferInstall::new(
            14,
            vec![2],
            active_epoch.get(),
            fenced_epoch.checked_add(1).unwrap(),
            active_proof.applied_log_index,
            active_proof.applied_log_hash,
            active_proof.state_digest,
            active_proof.applied_log_index,
            active_proof.applied_log_hash,
            active_proof.state_digest,
        )
        .unwrap();
        let install_server =
            spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 41_300);
        let installed_epoch =
            set_control_plane_pg_acting_set_with_metadata_transfer_live(&socket_path, install)
                .expect("live metadata-transfer acting-set helper should succeed");
        install_server.join().unwrap();
        assert!(installed_epoch > fenced_epoch);

        let snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after transfer helper install");
        let pg = snapshot
            .pg(PgId::new(14))
            .expect("transferred PG should remain present");
        assert_eq!(snapshot.cluster_epoch().get(), installed_epoch);
        assert_eq!(pg.state(), PgState::Peering);
        assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
        assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
        assert!(!pg.metadata_transfer_fenced());

        std::fs::remove_file(&socket_path).unwrap();
        std::fs::remove_dir_all(&tmp).unwrap();
        harness.shutdown();
    }

    #[test]
    fn root_process_check_accepts_non_root_effective_uid() {
        assert_eq!(reject_root_process(1_000), Ok(()));
    }

    #[test]
    fn metadata_transfer_admin_args_parse_source_and_imported_proofs() {
        let args = [
            "7", "12", "13", "20", "30", "40", "20", "31", "40", "2", "3",
        ]
        .into_iter()
        .map(OsString::from);

        let (pg_id, install) =
            parse_control_plane_pg_acting_set_with_metadata_transfer_args(args).unwrap();

        assert_eq!(pg_id, 7);
        assert_eq!(
            format!("{install:?}"),
            "ControlPlanePgMetadataTransferInstall(<opaque>)"
        );
    }

    #[test]
    fn pg_runtime_map_ready_uses_serving_pg_scope_when_unrelated_pg_is_unserved() {
        let tmp = short_unix_socket_test_dir("pg-ready");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_control_plane_with_target_pg_and_unserved_pg(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
            false,
            2,
        );

        let (runtime_epoch, route_epoch) = storage::clock::with_time_override(2_000, || {
            control_plane_pg_runtime_map_ready(&socket_path, 0, &[0])
        })
        .unwrap();
        assert!(runtime_epoch >= route_epoch);

        let mismatch = storage::clock::with_time_override(2_000, || {
            control_plane_pg_runtime_map_ready(&socket_path, 0, &[1])
        })
        .unwrap_err();
        assert_eq!(
            mismatch,
            "control-plane PG runtime map is not ready: control-plane PG read serving status failed: PG is not serving on the expected acting set"
        );

        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn frontend_startup_retry_classification_is_typed() {
        let cluster_epoch = ClusterEpoch::new(35).unwrap();
        assert!(FrontendControlPlaneStartupError::runtime_map_fetch(
            "/tmp/control-plane.sock",
            ControlPlaneError::PgHasNoServingPrimary {
                pg_id: 1,
                cluster_epoch,
            },
        )
        .is_retryable());
        assert!(!FrontendControlPlaneStartupError::runtime_map_fetch(
            "/tmp/control-plane.sock",
            ControlPlaneError::UnknownPg { pg_id: 1 },
        )
        .is_retryable());
        assert!(!FrontendControlPlaneStartupError::permanent(
            "ARGMIN_STORAGE_CLUSTER_EPOCH must be > 0"
        )
        .is_retryable());
    }

    #[test]
    fn runtime_map_diagnostics_include_storage_history_floors() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-runtime-map-diagnostics-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = FileControlPlaneStore::new(tmp.join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(3), vec![NodeId::new(2)])
            .unwrap();
        let floor_epoch = authority.snapshot().cluster_epoch();
        let history_references = storage::PgClusterMapHistoryRouteReferences::try_from_iter([
            storage::PgClusterMapHistoryRouteReference::new(
                storage::PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                floor_epoch,
                PgId::new(3),
            ),
        ])
        .unwrap();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(2),
                    node_incarnation: 7,
                    endpoint: "node-2.sock".to_owned(),
                    observed_epoch: floor_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: history_references.clone(),
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let peering_epoch = authority.snapshot().cluster_epoch();
        let metadata_proof = PgMetadataProof {
            applied_log_index: 1,
            applied_log_hash: 2,
            state_digest: 3,
        };
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(2),
                    node_incarnation: 7,
                    endpoint: "node-2.sock".to_owned(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: history_references.clone(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(3),
                        state: PgState::Peering,
                        metadata_proof,
                        pending_metadata_command: None,
                    }],
                },
                1_001,
            )
            .unwrap();
        authority.complete_ready_pg_peerings(1_001).unwrap();
        let active_epoch = authority.snapshot().cluster_epoch();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(2),
                    node_incarnation: 7,
                    endpoint: "node-2.sock".to_owned(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: history_references,
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(3),
                        state: PgState::Active,
                        metadata_proof,
                        pending_metadata_command: None,
                    }],
                },
                1_002,
            )
            .unwrap();
        let diagnostic_snapshot = authority.runtime_map_diagnostics_snapshot(1_002).unwrap();

        let rpc_metrics = [observability::ControlPlaneRpcMetricSample {
            kind: observability::ControlPlaneRpcMetricKind::RefreshNodeHeartbeat,
            total: 9,
            lock_wait_us_total: 10,
            lock_wait_us_max: 11,
            operation_us_total: 12,
            operation_us_max: 13,
            response_write_us_total: 14,
            response_write_us_max: 15,
            response_write_error_total: 5,
            response_write_broken_pipe_total: 2,
            response_write_connection_reset_total: 1,
            response_write_timeout_total: 1,
            response_write_other_error_total: 1,
        }];
        let diagnostics = format_control_plane_runtime_map_diagnostics_parts(
            (
                diagnostic_snapshot.runtime_map(),
                diagnostic_snapshot.node_leases(),
            ),
            &rpc_metrics,
            (
                observability::ControlPlaneSnapshotMetricSnapshot {
                    save_total: 7,
                    bytes_last: 1234,
                    ..observability::ControlPlaneSnapshotMetricSnapshot::default()
                },
                observability::ControlPlaneJournalMetricSnapshot {
                    append_total: 7,
                    frame_bytes_last: 234,
                    file_sync_total: 8,
                    directory_sync_total: 9,
                    compaction_total: 10,
                    compaction_bytes_last: 345,
                    compaction_file_sync_total: 11,
                    compaction_directory_sync_total: 12,
                    ..observability::ControlPlaneJournalMetricSnapshot::default()
                },
                observability::ControlPlaneRaftCheckpointMetricSnapshot {
                    store_total: 8,
                    file_sync_total: 9,
                    directory_sync_total: 10,
                    bytes_last: 5678,
                    compaction_total: 11,
                    ..observability::ControlPlaneRaftCheckpointMetricSnapshot::default()
                },
                observability::ControlPlaneRaftWalMetricSnapshot {
                    append_total: 12,
                    frame_bytes_last: 345,
                    file_sync_total: 13,
                    directory_sync_total: 14,
                    durability_queue_depth: 20,
                    durability_queue_depth_max: 21,
                    durability_queue_wait_us_total: 22,
                    durability_queue_wait_us_max: 23,
                    append_accept_us_total: 24,
                    append_accept_us_max: 25,
                    durability_operation_us_total: 26,
                    durability_operation_us_max: 27,
                    ..observability::ControlPlaneRaftWalMetricSnapshot::default()
                },
                observability::ControlPlaneRaftCommandMetricSnapshot {
                    submit_total: 15,
                    submit_error_total: 1,
                    queue_wait_us_total: 16,
                    queue_wait_us_max: 17,
                    operation_us_total: 18,
                    operation_us_max: 19,
                },
            ),
            &[observability::ControlPlaneHistoryReferenceSample {
                node_id: 2,
                observed_epoch: floor_epoch.get(),
                validation_epoch: floor_epoch.get(),
                observed_at_ms: 1_000,
                oldest_live_placement_epoch: Some(floor_epoch.get()),
                oldest_durable_backfill_epoch: Some(floor_epoch.get() + 1),
                oldest_pending_metadata_command_epoch: None,
                oldest_object_payload_reclaim_claim_epoch: Some(floor_epoch.get()),
            }],
        );

        assert!(diagnostics.contains("nodes=1"), "{diagnostics}");
        assert!(
            diagnostics.contains(&format!(
                "oldest_storage_history_floor_epoch={}",
                floor_epoch.get()
            )),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "control_plane_rpc kind=refresh_node_heartbeat total=9 lock_wait_us_total=10 lock_wait_us_max=11 operation_us_total=12 operation_us_max=13 response_write_us_total=14 response_write_us_max=15 response_write_error_total=5 response_write_broken_pipe_total=2 response_write_connection_reset_total=1 response_write_timeout_total=1 response_write_other_error_total=1"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "control_plane_journal append_total=7 append_error_total=0 append_us_total=0 append_us_max=0 lock_wait_us_total=0 lock_wait_us_max=0 frame_bytes_total=0 frame_bytes_last=234 frame_bytes_max=0 file_sync_total=8 file_sync_us_total=0 file_sync_us_max=0 directory_sync_total=9 directory_sync_us_total=0 directory_sync_us_max=0 compaction_total=10 compaction_error_total=0 compaction_us_total=0 compaction_us_max=0 compaction_lock_wait_us_total=0 compaction_lock_wait_us_max=0 compaction_bytes_total=0 compaction_bytes_last=345 compaction_bytes_max=0 compaction_file_sync_total=11 compaction_file_sync_us_total=0 compaction_file_sync_us_max=0 compaction_directory_sync_total=12"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "control_plane_raft_wal append_total=12 append_error_total=0 append_us_total=0 append_us_max=0 lock_wait_us_total=0 lock_wait_us_max=0 frame_bytes_total=0 frame_bytes_last=345 frame_bytes_max=0 file_sync_total=13 file_sync_us_total=0 file_sync_us_max=0 directory_sync_total=14 directory_sync_us_total=0 directory_sync_us_max=0 durability_queue_depth=20 durability_queue_depth_max=21 durability_queue_wait_us_total=22 durability_queue_wait_us_max=23 append_accept_us_total=24 append_accept_us_max=25 durability_operation_us_total=26 durability_operation_us_max=27"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "control_plane_raft_command submit_total=15 submit_error_total=1 queue_wait_us_total=16 queue_wait_us_max=17 operation_us_total=18 operation_us_max=19"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("control_plane_snapshot serialize_total=0 serialize_us_total=0 serialize_us_max=0 save_total=7"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("bytes_last=1234"), "{diagnostics}");
        assert!(
            diagnostics.contains(
                "control_plane_raft_checkpoint encode_total=0 encode_us_total=0 encode_us_max=0 store_total=8 store_error_total=0 store_us_total=0 store_us_max=0 file_sync_total=9 file_sync_us_total=0 file_sync_us_max=0 directory_sync_total=10 directory_sync_us_total=0 directory_sync_us_max=0 bytes_total=0 bytes_last=5678 bytes_max=0 compaction_total=11"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(&format!(
                "history_report_observed_epoch={} history_report_validation_epoch={} history_report_accepted_at_ms=1000 history_live_payload_epoch={} history_durable_backfill_epoch={} history_pending_metadata_command_epoch=-",
                floor_epoch.get(),
                floor_epoch.get(),
                floor_epoch.get(),
                floor_epoch.get() + 1
            )),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(&format!(
                "node_id=2 incarnation=7 endpoint=node-2.sock lease_deadline_ms=2002 storage_history_floor_epoch={}",
                floor_epoch.get()
            )),
            "{diagnostics}"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn control_plane_state_lock_uses_sibling_lock_file() {
        let state_path = Path::new("/tmp/argmin/control-plane.state");

        assert_eq!(
            control_plane_state_lock_path(state_path).unwrap(),
            Path::new("/tmp/argmin/control-plane.state.lock")
        );
    }

    #[test]
    fn control_plane_state_lock_rejects_second_manager_for_same_state() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-control-plane-lock-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let state_path = tmp.join("control-plane.state");
        let first = acquire_control_plane_state_lock(&state_path).unwrap();

        let error = acquire_control_plane_state_lock(&state_path).unwrap_err();

        assert!(error.contains("already locked"), "{error}");
        drop(first);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn control_plane_socket_bind_creates_private_missing_directory() {
        let tmp = short_unix_socket_test_dir("cppriv");
        let _ = std::fs::remove_dir_all(&tmp);
        let socket_path = tmp.join("n").join("cp.sock");

        let listener = bind_control_plane_socket(&socket_path).unwrap();

        drop(listener);
        let mode = std::fs::metadata(socket_path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn control_plane_socket_bind_rejects_public_directory() {
        let tmp = short_unix_socket_test_dir("cppub");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).unwrap();
        let socket_path = tmp.join("cp.sock");

        let error = bind_control_plane_socket(&socket_path).unwrap_err();

        assert!(error.contains("must be private"), "{error}");
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o700));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn control_plane_bootstrap_initializes_empty_state_from_storage_node_sockets() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-control-plane-bootstrap-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let store = FileControlPlaneStore::new(tmp.join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        let mut config = test_server_config();
        config.storage_pg_ids = vec![0, 3];
        config.storage_node_sockets = vec![
            config::ConfiguredStorageNodeSocket {
                node_id: 2,
                socket_path: tmp.join("node-2.sock").display().to_string(),
            },
            config::ConfiguredStorageNodeSocket {
                node_id: 4,
                socket_path: tmp.join("node-4.sock").display().to_string(),
            },
        ];

        bootstrap_empty_control_plane(&mut authority, &config).unwrap();

        assert_eq!(
            authority
                .snapshot()
                .nodes()
                .map(|node| node.node_id())
                .collect::<Vec<_>>(),
            vec![NodeId::new(2), NodeId::new(4)]
        );
        assert_eq!(
            authority
                .snapshot()
                .runtime_map(1_000)
                .unwrap()
                .nodes()
                .iter()
                .map(|node| node.endpoint().to_owned())
                .collect::<Vec<_>>(),
            vec![
                tmp.join("node-2.sock").display().to_string(),
                tmp.join("node-4.sock").display().to_string(),
            ]
        );
        for pg_id in [0, 3] {
            let pg = authority.snapshot().pg(storage::PgId::new(pg_id)).unwrap();
            assert_eq!(pg.acting_set(), &[NodeId::new(2), NodeId::new(4)]);
            assert_eq!(pg.state(), PgState::Peering);
        }
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn control_plane_bootstrap_does_not_rewrite_existing_state() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-control-plane-bootstrap-existing-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let store = FileControlPlaneStore::new(tmp.join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(7), NodeMembershipState::Active)
            .unwrap();
        let before = authority.snapshot().clone();
        let mut config = test_server_config();
        config.storage_pg_ids = vec![0];
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: tmp.join("node-1.sock").display().to_string(),
        }];

        bootstrap_empty_control_plane(&mut authority, &config).unwrap();

        assert_eq!(authority.snapshot(), &before);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn storage_node_process_config_uses_configured_epoch_and_pg_ids() {
        let ec_config = EcConfig::new(4, 2).unwrap();
        let config = test_server_config();

        let built = build_storage_node_process_config(&config, &ec_config).unwrap();
        let prepared_server = built.prepared_server;
        let control_plane_node_incarnation = built.control_plane_node_incarnation;
        let storage_config = prepared_server.config();

        assert_eq!(control_plane_node_incarnation, None);
        assert_eq!(storage_config.node_id(), NodeId::new(2));
        assert_eq!(
            storage_config.cluster_epoch(),
            ClusterEpoch::new(9).unwrap()
        );
        assert_eq!(storage_config.pg_ids(), &[1, 3, 5]);
        assert_eq!(
            storage_config
                .pg_routes()
                .iter()
                .map(|route| (route.pg_id, route.cluster_epoch))
                .collect::<Vec<_>>(),
            vec![
                (1, ClusterEpoch::new(9).unwrap()),
                (3, ClusterEpoch::new(9).unwrap()),
                (5, ClusterEpoch::new(9).unwrap()),
            ]
        );
    }

    #[test]
    fn standalone_storage_cluster_preserves_explicit_node_id_and_data_dir() {
        let tmp = test_util::tempdir();
        let node_data_dir = tmp.path().join("manifest-node-data");
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.data_dir = tmp.path().join("unused-dense-layout").display().to_string();
        config.pg_count = 2;
        config.storage_node_ids = vec![17];
        config.storage_node_id = Some(17);
        config.storage_node_data_dir = Some(node_data_dir.display().to_string());

        let cluster = build_standalone_storage_cluster(&config, &ec_config).unwrap();

        assert_eq!(
            cluster.local_node_ids().collect::<Vec<_>>(),
            [NodeId::new(17)]
        );
        assert_eq!(cluster.local_node_count(), 1);
        for pg_id in [0, 1] {
            let route = cluster.local_pg_route(PgId::new(pg_id)).unwrap();
            assert_eq!(route.primary_node_id(), NodeId::new(17));
            assert_eq!(route.acting_set(), &[NodeId::new(17)]);
        }
        assert!(node_data_dir.exists());
        assert!(
            Path::new(&config.data_dir).is_dir(),
            "standalone startup must durably bind its storage-owned route identity"
        );
    }

    #[test]
    fn standalone_multi_node_cluster_uses_configured_epoch() {
        let tmp = test_util::tempdir();
        let ec_config = EcConfig::new(1, 1).unwrap();
        let mut config = test_server_config();
        config.data_dir = tmp.path().join("standalone").display().to_string();
        config.pg_count = 2;
        config.storage_node_ids = vec![0, 4];
        config.storage_cluster_epoch = 17;

        let cluster = build_standalone_storage_cluster(&config, &ec_config).unwrap();

        assert_eq!(cluster.cluster_epoch(), ClusterEpoch::new(17).unwrap());
        for pg_id in [0, 1] {
            assert_eq!(
                cluster
                    .local_pg_route(PgId::new(pg_id))
                    .unwrap()
                    .cluster_epoch(),
                ClusterEpoch::new(17).unwrap()
            );
        }
    }

    #[test]
    fn standalone_storage_cluster_rejects_topology_change_on_restart() {
        let tmp = test_util::tempdir();
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::AllInOne;
        config.data_dir = tmp.path().join("standalone").display().to_string();
        config.pg_count = 1;
        config.storage_node_ids = vec![17];
        config.storage_node_id = Some(17);
        config.storage_node_data_dir = Some(tmp.path().join("node-a").display().to_string());
        drop(build_standalone_storage_cluster(&config, &ec_config).unwrap());

        config.storage_node_data_dir = Some(tmp.path().join("node-b").display().to_string());
        let error = match build_standalone_storage_cluster(&config, &ec_config) {
            Ok(_) => panic!("changed standalone route topology must fail closed"),
            Err(error) => error,
        };
        assert!(
            error.contains("does not match the configured static topology"),
            "{error}"
        );
        assert!(
            !tmp.path().join("node-b/pg-0000").exists(),
            "route mismatch must be rejected before opening PG storage"
        );
    }

    #[test]
    fn no_control_plane_process_roles_share_durable_static_route_binding() {
        let temp = test_util::tempdir();
        let ec_config = EcConfig::new(1, 0).unwrap();
        for (index, role) in [
            ProcessRole::Frontend,
            ProcessRole::StorageNode,
            ProcessRole::Combined,
        ]
        .into_iter()
        .enumerate()
        {
            let process_dir = temp.path().join(format!("process-{index}"));
            let mut config = test_server_config();
            config.process_role = role;
            config.data_dir = process_dir.display().to_string();
            config.storage_node_ids = vec![4];
            config.storage_node_id = Some(4);
            config.storage_node_data_dir = Some(process_dir.join("node").display().to_string());
            config.storage_pg_ids = vec![0];
            let first_socket_path = temp
                .path()
                .join(format!("node-{index}-a.sock"))
                .display()
                .to_string();
            config.storage_node_socket_path = Some(first_socket_path.clone());
            config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
                node_id: 4,
                socket_path: first_socket_path,
            }];

            drop(
                bind_no_control_plane_remote_route_identity(&config, &ec_config)
                    .unwrap()
                    .expect("no-control-plane process must retain a route binding"),
            );
            drop(
                bind_no_control_plane_remote_route_identity(&config, &ec_config)
                    .unwrap()
                    .expect("unchanged route topology must reopen"),
            );

            let changed_socket_path = temp
                .path()
                .join(format!("node-{index}-b.sock"))
                .display()
                .to_string();
            config.storage_node_socket_path = Some(changed_socket_path.clone());
            config.storage_node_sockets[0].socket_path = changed_socket_path;
            let error =
                bind_no_control_plane_remote_route_identity(&config, &ec_config).unwrap_err();
            assert!(
                error.contains("does not match the configured static topology"),
                "role {role:?}: {error}"
            );

            if matches!(role, ProcessRole::StorageNode | ProcessRole::Combined) {
                let original_socket_path = temp
                    .path()
                    .join(format!("node-{index}-a.sock"))
                    .display()
                    .to_string();
                config.storage_node_socket_path = Some(original_socket_path.clone());
                config.storage_node_sockets[0].socket_path = original_socket_path;
                config.storage_node_data_dir =
                    Some(process_dir.join("other-node").display().to_string());
                let error =
                    bind_no_control_plane_remote_route_identity(&config, &ec_config).unwrap_err();
                assert!(
                    error.contains("does not match the configured static topology"),
                    "role {role:?} did not bind its storage path: {error}"
                );
            }
        }
    }

    #[test]
    fn remote_frontend_storage_cluster_uses_configured_epoch_and_socket_clients() {
        let tmp = std::env::temp_dir().join(format!(
            "argmin-remote-frontend-cluster-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::Frontend;
        config.data_dir = tmp.join("frontend").display().to_string();
        config.storage_node_ids = vec![0];
        config.pg_count = 2;
        config.storage_pg_ids = vec![0, 1];
        config.storage_cluster_epoch = 9;
        config.storage_node_id = None;
        config.storage_node_data_dir = None;
        config.storage_node_socket_path = None;
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 0,
            socket_path: tmp
                .join("sockets")
                .join("node-0.sock")
                .display()
                .to_string(),
        }];

        let cluster = build_remote_frontend_storage_cluster(&config, &ec_config).unwrap();

        assert_eq!(cluster.cluster_epoch(), ClusterEpoch::new(9).unwrap());
        assert_eq!(cluster.local_node_count(), 1);
        for pg_id in [0, 1] {
            let route = cluster.local_pg_route(storage::PgId::new(pg_id)).unwrap();
            assert_eq!(route.cluster_epoch(), ClusterEpoch::new(9).unwrap());
            assert_eq!(route.primary_node_id(), NodeId::new(0));
        }
        assert!(
            !tmp.join("frontend").exists(),
            "frontend-only cluster construction must not open placeholder PG directories"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn frontend_without_maintenance_auth_shares_refreshed_runtime_map_handle() {
        storage::clock::with_time_and_monotonic_override(10_000_000, 5_000_000, || {
            frontend_without_maintenance_auth_shares_refreshed_runtime_map_handle_at_fixed_time();
        });
    }

    fn frontend_without_maintenance_auth_shares_refreshed_runtime_map_handle_at_fixed_time() {
        let tmp = short_unix_socket_test_dir("shared-maintenance-map");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_one_active_control_plane_runtime_map(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::Frontend;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = None;
        config.storage_node_socket_path = None;
        config.storage_node_sockets.clear();
        config.control_plane_socket_path = Some(socket_path.display().to_string());
        config.storage_rpc_maintenance_client_auth = None;

        let storage_clusters =
            build_control_plane_frontend_storage_clusters(&config, &ec_config).unwrap();
        assert!(storage_clusters.distinct_maintenance.is_none());
        server.join().unwrap();

        let handles = frontend_route_handles(storage_clusters).unwrap();
        assert!(handles
            .foreground_route
            .shares_route_admission_with(&handles.maintenance_route));

        let mut replacement_config = config.clone();
        let replacement_socket_path = tmp.join("replacement-cp.sock");
        replacement_config.control_plane_socket_path =
            Some(replacement_socket_path.display().to_string());
        let replacement_server = serve_one_active_control_plane_runtime_map(
            replacement_socket_path,
            NodeId::new(0),
            endpoint.display().to_string(),
        );
        let replacement_clusters =
            build_control_plane_frontend_storage_clusters(&replacement_config, &ec_config).unwrap();
        assert!(replacement_clusters.distinct_maintenance.is_none());
        let replacement = replacement_clusters.foreground;
        replacement_server.join().unwrap();
        handles
            .foreground_runtime
            .as_ref()
            .unwrap()
            .install(Arc::clone(&replacement))
            .unwrap();

        assert!(Arc::ptr_eq(
            &handles.foreground_route.current(),
            &replacement
        ));
        assert!(Arc::ptr_eq(
            &handles.maintenance_route.current(),
            &replacement
        ));
        let _ = std::fs::remove_dir_all(&tmp);
    }

    fn serve_one_control_plane_runtime_map(
        socket_path: PathBuf,
        node_id: NodeId,
        endpoint: String,
    ) -> std::thread::JoinHandle<()> {
        serve_one_control_plane_runtime_map_with_serving_pg_routes(
            socket_path,
            node_id,
            endpoint,
            false,
        )
    }

    fn serve_one_active_control_plane_runtime_map(
        socket_path: PathBuf,
        node_id: NodeId,
        endpoint: String,
    ) -> std::thread::JoinHandle<()> {
        serve_one_control_plane_runtime_map_with_serving_pg_routes(
            socket_path,
            node_id,
            endpoint,
            true,
        )
    }

    fn serve_one_control_plane_runtime_map_with_serving_pg_routes(
        socket_path: PathBuf,
        node_id: NodeId,
        endpoint: String,
        serving_pg_routes: bool,
    ) -> std::thread::JoinHandle<()> {
        const TEST_LEASE_DURATION_MS: u64 = 10_000;

        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let state_path = socket_path.with_extension("state");
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(0), vec![node_id])
            .unwrap();
        let heartbeat_started_at_ms = storage::clock::current_time_millis();
        let pg_observations = if serving_pg_routes {
            vec![NodePgHeartbeatObservation {
                pg_id: PgId::new(0),
                state: PgState::Peering,
                metadata_proof: PgMetadataProof::empty(),
                pending_metadata_command: None,
            }]
        } else {
            Vec::new()
        };
        for heartbeat_offset_ms in 0..4 {
            let now_ms = heartbeat_started_at_ms.saturating_add(heartbeat_offset_ms);
            let observed_epoch = authority.snapshot().cluster_epoch();
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
                        endpoint: endpoint.clone(),
                        observed_epoch,
                        requested_lease_duration_ms: TEST_LEASE_DURATION_MS,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: pg_observations.clone(),
                    },
                    now_ms,
                )
                .unwrap();
            if lease.serving() {
                break;
            }
            assert!(
                heartbeat_offset_ms < 3,
                "authority did not grant serving lease"
            );
        }
        if serving_pg_routes {
            authority
                .complete_pg_peering(
                    PgId::new(0),
                    node_id,
                    1,
                    heartbeat_started_at_ms.saturating_add(4),
                )
                .unwrap();
            authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
                        endpoint: endpoint.clone(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: TEST_LEASE_DURATION_MS,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: vec![NodePgHeartbeatObservation {
                            pg_id: PgId::new(0),
                            state: PgState::Active,
                            metadata_proof: PgMetadataProof::empty(),
                            pending_metadata_command: None,
                        }],
                    },
                    heartbeat_started_at_ms.saturating_add(5),
                )
                .unwrap();
        }
        spawn_control_plane_test_rpc_server(
            listener,
            Arc::new(Mutex::new(authority)),
            [heartbeat_started_at_ms.saturating_add(6)],
            None,
        )
    }

    fn serve_control_plane_with_target_pg_and_unserved_pg(
        socket_path: PathBuf,
        node_id: NodeId,
        endpoint: String,
        transfer_target: bool,
        request_count: usize,
    ) -> std::thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let state_path = socket_path.with_extension("state");
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        let unserved_node_id = NodeId::new(node_id.as_u32() + 1);
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_node_membership(unserved_node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(0), vec![node_id])
            .unwrap();
        for now_ms in 1_000..1_004 {
            let observed_epoch = authority.snapshot().cluster_epoch();
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
                        endpoint: endpoint.clone(),
                        observed_epoch,
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: vec![NodePgHeartbeatObservation {
                            pg_id: PgId::new(0),
                            state: PgState::Peering,
                            metadata_proof: PgMetadataProof::empty(),
                            pending_metadata_command: None,
                        }],
                    },
                    now_ms,
                )
                .unwrap();
            if lease.serving() {
                break;
            }
            assert!(now_ms < 1_003, "authority did not grant serving lease");
        }
        authority
            .complete_pg_peering(PgId::new(0), node_id, 1, 1_001)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(1), vec![unserved_node_id])
            .unwrap();
        for now_ms in 1_002..1_006 {
            let observed_epoch = authority.snapshot().cluster_epoch();
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id: unserved_node_id,
                        node_incarnation: 1,
                        endpoint: format!("{endpoint}.unserved"),
                        observed_epoch,
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: vec![NodePgHeartbeatObservation {
                            pg_id: PgId::new(1),
                            state: PgState::Peering,
                            metadata_proof: PgMetadataProof::empty(),
                            pending_metadata_command: None,
                        }],
                    },
                    now_ms,
                )
                .unwrap();
            if lease.serving() {
                break;
            }
            assert!(
                now_ms < 1_005,
                "authority did not grant unrelated PG serving lease"
            );
        }
        authority
            .complete_pg_peering(PgId::new(1), unserved_node_id, 1, 1_006)
            .unwrap();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 1,
                    endpoint,
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(0),
                        state: PgState::Active,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                1_007,
            )
            .unwrap();
        if transfer_target {
            authority
                .fence_pg_for_metadata_transfer(PgId::new(0))
                .unwrap();
            let source_epoch = authority.snapshot().cluster_epoch();
            authority
                .set_pg_acting_set_with_metadata_transfer(
                    PgId::new(0),
                    vec![unserved_node_id],
                    PgMetadataTransferProof::new(source_epoch, PgMetadataProof::empty()),
                )
                .unwrap();
        }

        spawn_control_plane_test_rpc_server(
            listener,
            Arc::new(Mutex::new(authority)),
            (0..request_count).map(|request_index| 1_008 + u64::try_from(request_index).unwrap()),
            None,
        )
    }

    fn serve_control_plane_storage_node_startup_refreshes(
        socket_path: PathBuf,
        node_id: NodeId,
        request_count: usize,
    ) -> std::thread::JoinHandle<Vec<u64>> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let server = ControlPlaneRpcOrdinaryTestServer::unix(
            listener,
            CONTROL_PLANE_RPC_WORKER_LIMIT,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            CONTROL_PLANE_RPC_PRE_AUTH_BYTE_BUDGET,
            None,
        )
        .unwrap();
        let state_path = socket_path.with_extension("state");
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(0), vec![node_id])
            .unwrap();
        let authority = Arc::new(Mutex::new(authority));
        std::thread::spawn(move || {
            let mut observed_incarnations = Vec::with_capacity(request_count);
            server
                .serve_shared_requests(
                    authority,
                    (0..request_count)
                        .map(|request_index| 1_000 + u64::try_from(request_index).unwrap()),
                    |authority| {
                        observed_incarnations.push(
                            authority
                                .snapshot()
                                .node(node_id)
                                .unwrap()
                                .node_incarnation(),
                        );
                    },
                )
                .unwrap();
            observed_incarnations
        })
    }

    fn serve_control_plane_storage_node_startup_refresh_after_dropped_connection(
        socket_path: PathBuf,
        node_id: NodeId,
    ) -> std::thread::JoinHandle<Vec<u64>> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let server = ControlPlaneRpcOrdinaryTestServer::unix(
            listener,
            CONTROL_PLANE_RPC_WORKER_LIMIT,
            CONTROL_PLANE_RPC_MAX_FRAME_BYTES,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            CONTROL_PLANE_RPC_PRE_AUTH_BYTE_BUDGET,
            None,
        )
        .unwrap();
        let state_path = socket_path.with_extension("state");
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(0), vec![node_id])
            .unwrap();
        std::thread::spawn(move || {
            let mut observed_incarnations = Vec::new();
            server
                .serve_shared_requests_after_dropped_connection(
                    Arc::new(Mutex::new(authority)),
                    [1_000],
                    |authority| {
                        observed_incarnations.push(
                            authority
                                .snapshot()
                                .node(node_id)
                                .unwrap()
                                .node_incarnation(),
                        );
                    },
                )
                .unwrap();
            observed_incarnations
        })
    }

    fn serve_frontend_control_plane_runtime_map_refresh(
        socket_path: PathBuf,
        node_id: NodeId,
        endpoint: String,
    ) -> std::thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let state_path = socket_path.with_extension("state");
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(node_id, NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(0), vec![node_id])
            .unwrap();

        let peering_observation = NodePgHeartbeatObservation {
            pg_id: PgId::new(0),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: None,
        };
        for now_ms in 1_000..1_004 {
            let observed_epoch = authority.snapshot().cluster_epoch();
            let lease = authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
                        endpoint: endpoint.clone(),
                        observed_epoch,
                        requested_lease_duration_ms: 1_000,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: vec![peering_observation],
                    },
                    now_ms,
                )
                .unwrap();
            if lease.serving() {
                break;
            }
            assert!(now_ms < 1_003, "authority did not grant serving lease");
        }

        authority
            .complete_pg_peering(PgId::new(0), node_id, 1, 1_002)
            .unwrap();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id,
                    node_incarnation: 1,
                    endpoint,
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(0),
                        state: PgState::Active,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                1_003,
            )
            .unwrap();

        spawn_control_plane_test_rpc_server(
            listener,
            Arc::new(Mutex::new(authority)),
            [1_004, 1_005, 1_006],
            None,
        )
    }

    #[test]
    fn remote_frontend_storage_cluster_can_bootstrap_from_control_plane_socket() {
        let tmp = short_unix_socket_test_dir("fb");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_one_active_control_plane_runtime_map(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::Frontend;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = None;
        config.storage_node_socket_path = None;
        config.storage_node_sockets.clear();
        config.control_plane_socket_path = Some(socket_path.display().to_string());

        let cluster = build_remote_frontend_storage_cluster(&config, &ec_config).unwrap();

        server.join().unwrap();
        assert_eq!(cluster.local_node_count(), 1);
        assert_eq!(
            cluster
                .local_pg_route(storage::PgId::new(0))
                .unwrap()
                .state(),
            PgState::Active
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn dynamic_frontend_bootstraps_nonserving_map_so_recovery_can_start() {
        let tmp = short_unix_socket_test_dir("fbr");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_one_control_plane_runtime_map(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::Frontend;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = None;
        config.storage_node_socket_path = None;
        config.storage_node_sockets.clear();
        config.control_plane_socket_path = Some(socket_path.display().to_string());

        let storage_clusters =
            build_control_plane_frontend_storage_clusters(&config, &ec_config).unwrap();

        server.join().unwrap();
        assert!(matches!(
            storage_clusters.authority,
            FrontendStorageRouteAuthority::Dynamic
        ));
        let route = storage_clusters
            .foreground
            .local_pg_route(PgId::new(0))
            .unwrap();
        assert_eq!(route.state(), PgState::Peering);
        let handles = frontend_route_handles(storage_clusters).unwrap();
        assert_eq!(
            handles
                .foreground_route
                .current()
                .local_pg_route(PgId::new(0))
                .unwrap()
                .state(),
            PgState::Peering,
            "frontend startup route handles must remain available while one PG is Peering"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn remote_frontend_control_plane_bootstrap_uses_routed_metadata_primary() {
        let tmp = short_unix_socket_test_dir("fnz");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n3.sock");
        let server = serve_one_active_control_plane_runtime_map(
            socket_path.clone(),
            NodeId::new(3),
            endpoint.display().to_string(),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::Frontend;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = None;
        config.storage_node_socket_path = None;
        config.storage_node_sockets.clear();
        config.control_plane_socket_path = Some(socket_path.display().to_string());

        let cluster = build_remote_frontend_storage_cluster(&config, &ec_config).unwrap();

        server.join().unwrap();
        assert_eq!(cluster.local_node_count(), 1);
        assert_eq!(
            cluster
                .local_pg_route(storage::PgId::new(0))
                .unwrap()
                .primary_node_id(),
            NodeId::new(3)
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn remote_frontend_control_plane_refresh_loop_updates_bootstrap_map() {
        let tmp = short_unix_socket_test_dir("fr");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_frontend_control_plane_runtime_map_refresh(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::Frontend;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = None;
        config.storage_node_socket_path = None;
        config.storage_node_sockets.clear();
        config.control_plane_socket_path = Some(socket_path.display().to_string());
        config.control_plane_frontend_refresh_interval = std::time::Duration::from_millis(5);

        let cluster = build_remote_frontend_storage_cluster(&config, &ec_config).unwrap();
        assert_eq!(
            cluster
                .local_pg_route(storage::PgId::new(0))
                .unwrap()
                .state(),
            PgState::Active
        );

        let handle = StorageClusterRuntimeMapHandle::new(cluster).unwrap();
        let mut refresh_loop =
            maybe_spawn_frontend_control_plane_refresh_loop(Some(handle.clone()), &config).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let status = refresh_loop.status();
            if status.successes > 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "frontend refresh loop did not install active runtime map: {:?}",
                status
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        assert_eq!(
            handle
                .current()
                .local_pg_route(storage::PgId::new(0))
                .unwrap()
                .state(),
            PgState::Active
        );
        refresh_loop.stop();
        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn experimental_raft_durable_restart_serves_frontend_runtime_map_refresh_loop() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-durable-runtime-refresh");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).unwrap();
        let state_path = state_dir.join("control-plane.state");
        let socket_path = state_dir.join("control-plane.sock");
        let endpoint = state_dir.join("node-1.sock");
        let mut control_plane_config = test_server_config();
        control_plane_config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: endpoint.display().to_string(),
        }];
        control_plane_config.storage_pg_ids = vec![0];

        let mut harness =
            experimental_raft_durable_test_harness("runtime-refresh-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(
            &mut harness.control_plane,
            &control_plane_config,
        )
        .expect("durable experimental raft control-plane bootstrap should succeed");
        let bootstrap_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: endpoint.display().to_string(),
                    observed_epoch: bootstrap_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                61_000,
            )
            .expect("durable experimental startup heartbeat should checkpoint");
        let peering_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after startup heartbeat")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: endpoint.display().to_string(),
                    observed_epoch: peering_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(0),
                        state: PgState::Peering,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                61_100,
            )
            .expect("durable experimental Peering heartbeat should checkpoint");
        let active_epoch = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after peering completion")
            .cluster_epoch();
        harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: endpoint.display().to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(0),
                        state: PgState::Active,
                        metadata_proof: PgMetadataProof::empty(),
                        pending_metadata_command: None,
                    }],
                },
                61_200,
            )
            .expect("durable experimental Active heartbeat should checkpoint");
        harness.shutdown();

        let mut restarted =
            experimental_raft_durable_test_harness("runtime-refresh-restart", &state_path);
        bootstrap_empty_experimental_raft_control_plane(
            &mut restarted.control_plane,
            &control_plane_config,
        )
        .expect("durable experimental raft control-plane restart bootstrap should be a no-op");
        let server =
            spawn_experimental_raft_unix_rpc_server_requests(&restarted, &socket_path, 61_300, 3);

        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut frontend_config = test_server_config();
        frontend_config.process_role = ProcessRole::Frontend;
        frontend_config.pg_count = 1;
        frontend_config.storage_pg_ids = vec![0];
        frontend_config.storage_node_id = None;
        frontend_config.storage_node_socket_path = None;
        frontend_config.storage_node_sockets.clear();
        frontend_config.control_plane_socket_path = Some(socket_path.display().to_string());
        frontend_config.control_plane_frontend_refresh_interval =
            std::time::Duration::from_millis(5);

        let cluster = build_remote_frontend_storage_cluster(&frontend_config, &ec_config)
            .expect("frontend should bootstrap from restarted durable raft runtime map");
        assert_eq!(
            cluster
                .local_pg_route(storage::PgId::new(0))
                .expect("bootstrapped cluster should have PG route")
                .primary_node_id(),
            NodeId::new(1)
        );

        let handle = StorageClusterRuntimeMapHandle::new(cluster).unwrap();
        let mut refresh_loop =
            maybe_spawn_frontend_control_plane_refresh_loop(Some(handle.clone()), &frontend_config)
                .expect("frontend runtime-map refresh loop should start");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let status = refresh_loop.status();
            if status.successes > 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "durable raft frontend refresh loop did not install runtime map: {:?}",
                status
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        let refreshed = handle.current();
        let refreshed_route = refreshed
            .local_pg_route(storage::PgId::new(0))
            .expect("refreshed cluster should retain PG route");
        assert_eq!(refreshed_route.state(), PgState::Active);
        assert_eq!(refreshed_route.primary_node_id(), NodeId::new(1));
        assert_eq!(refreshed.route_map_valid_until_ms(), Some(62_200));

        refresh_loop.stop();
        server.join().unwrap();
        restarted.shutdown();
        fs::remove_dir_all(&state_dir).unwrap();
    }

    #[test]
    fn storage_node_process_config_can_bootstrap_from_control_plane_socket() {
        let tmp = short_unix_socket_test_dir("sb");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_one_control_plane_runtime_map(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::StorageNode;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(tmp.join("node-0-data").display().to_string());
        config.storage_node_socket_path = Some(endpoint.display().to_string());
        config.control_plane_socket_path = Some(socket_path.display().to_string());

        let built = build_storage_node_process_config(&config, &ec_config).unwrap();
        let prepared_server = built.prepared_server;
        let control_plane_node_incarnation = built.control_plane_node_incarnation;

        server.join().unwrap();
        assert_eq!(control_plane_node_incarnation, Some(1));
        let node_config = prepared_server.config();
        assert_eq!(node_config.node_id(), NodeId::new(0));
        assert_eq!(node_config.socket_path(), endpoint);
        assert_eq!(node_config.pg_ids(), &[0]);
        assert_eq!(node_config.pg_routes()[0].state, PgState::Peering);
        let bound = prepared_server.bind().unwrap();
        let heartbeat = bound.control_plane_heartbeat(1, 10_000).unwrap();
        assert_eq!(heartbeat.endpoint, endpoint.to_str().unwrap());
        drop(bound);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn replicated_storage_node_verifies_static_identity_before_control_plane_bootstrap() {
        let tmp = short_unix_socket_test_dir("static-storage-verify");
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::StorageNode;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(tmp.join("missing-node-state").display().to_string());
        config.storage_node_socket_path = Some(tmp.join("node.sock").display().to_string());
        config.control_plane_socket_path =
            Some(tmp.join("missing-control.sock").display().to_string());
        config.static_cluster_identity = Some(ConfiguredStaticClusterIdentity {
            cluster_id: "cluster-a".to_string(),
            topology_generation: 7,
            topology_digest: "a".repeat(64),
            process_id: "storage-1".to_string(),
            process_identity_digest: "b".repeat(64),
        });

        let error = build_storage_node_process_config(&config, &ec_config)
            .err()
            .expect("missing static storage identity must fail before control-plane access");

        assert!(error.contains("static storage directory"), "{error}");
        assert!(error.contains("initialize-cluster-state"), "{error}");
        assert!(!error.contains("control-plane runtime map"), "{error}");
    }

    #[test]
    fn replicated_storage_node_retains_static_runtime_lock_after_bootstrap() {
        let tmp = short_unix_socket_test_dir("static-storage-lock");
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("node.sock");
        let data_dir = tmp.join("node-state");
        let identity = ConfiguredStaticClusterIdentity {
            cluster_id: "cluster-a".to_string(),
            topology_generation: 7,
            topology_digest: "a".repeat(64),
            process_id: "storage-1".to_string(),
            process_identity_digest: "b".repeat(64),
        };
        static_cluster_state::initialize_static_storage(
            &identity,
            0,
            &data_dir,
            &[0],
            storage::EcShape { k: 1, m: 0 },
            ClusterEpoch::INITIAL,
        )
        .unwrap();
        let server = serve_one_control_plane_runtime_map(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::StorageNode;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(data_dir.display().to_string());
        config.storage_node_socket_path = Some(endpoint.display().to_string());
        config.control_plane_socket_path = Some(socket_path.display().to_string());
        config.static_cluster_identity = Some(identity.clone());

        let built = build_storage_node_process_config(&config, &ec_config).unwrap();
        server.join().unwrap();

        assert!(built.static_storage_runtime_lock.is_some());
        let error = static_cluster_state::lock_and_verify_standalone_storage_startup(
            &identity,
            0,
            &data_dir,
            &[0],
        )
        .unwrap_err();
        assert!(error.contains("runtime is already active"), "{error}");
        drop(built);
        static_cluster_state::lock_and_verify_standalone_storage_startup(
            &identity,
            0,
            &data_dir,
            &[0],
        )
        .unwrap();
    }

    #[test]
    fn storage_node_control_plane_startup_advances_incarnation_once_per_start() {
        let tmp = short_unix_socket_test_dir("sbi");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_control_plane_storage_node_startup_refreshes(
            socket_path.clone(),
            NodeId::new(0),
            2,
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::StorageNode;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(tmp.join("node-0-data").display().to_string());
        config.storage_node_socket_path = Some(endpoint.display().to_string());
        config.control_plane_socket_path = Some(socket_path.display().to_string());

        let first_built = build_storage_node_process_config(&config, &ec_config).unwrap();
        let first_prepared = first_built.prepared_server;
        let first_incarnation = first_built.control_plane_node_incarnation;
        let first_config = first_prepared.config().clone();
        drop(first_prepared);
        let second_built = build_storage_node_process_config(&config, &ec_config).unwrap();
        let second_prepared = second_built.prepared_server;
        let second_incarnation = second_built.control_plane_node_incarnation;
        let second_config = second_prepared.config().clone();

        let observed_incarnations = server.join().unwrap();
        assert_eq!(first_incarnation, Some(1));
        assert_eq!(second_incarnation, Some(2));
        assert_eq!(observed_incarnations, vec![1, 2]);
        for node_config in [first_config, second_config] {
            assert_eq!(node_config.node_id(), NodeId::new(0));
            assert_eq!(node_config.socket_path(), endpoint);
            assert_eq!(node_config.pg_ids(), &[0]);
            assert_eq!(node_config.pg_routes()[0].state, PgState::Peering);
        }
        drop(second_prepared);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn storage_node_control_plane_startup_retries_transient_refresh_failure() {
        let tmp = short_unix_socket_test_dir("sbr");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_control_plane_storage_node_startup_refresh_after_dropped_connection(
            socket_path.clone(),
            NodeId::new(0),
        );
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::StorageNode;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(tmp.join("node-0-data").display().to_string());
        config.storage_node_socket_path = Some(endpoint.display().to_string());
        config.control_plane_socket_path = Some(socket_path.display().to_string());
        config.control_plane_frontend_refresh_interval = std::time::Duration::from_millis(1);

        let built = build_storage_node_process_config(&config, &ec_config).unwrap();
        let prepared_server = built.prepared_server;
        let control_plane_node_incarnation = built.control_plane_node_incarnation;

        let observed_incarnations = server.join().unwrap();
        assert_eq!(control_plane_node_incarnation, Some(1));
        assert_eq!(observed_incarnations, vec![1]);
        let node_config = prepared_server.config();
        assert_eq!(node_config.node_id(), NodeId::new(0));
        assert_eq!(node_config.socket_path(), endpoint);
        assert_eq!(node_config.pg_ids(), &[0]);
        drop(prepared_server);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
