mod config;
mod static_cluster_config;
mod static_cluster_state;

use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use auth::{AccountIdentity, ConfiguredPrincipalIdentity, CredentialStore, StoredCredential};
use ec::EcConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use server_core::coordinator::Coordinator;
use server_core::sse::{
    ManagedWrappingKeyConfig, SseCustomerValidatorConfig, StaticManagedKeyProvider,
};
use storage::control_plane::{
    build_control_plane_authority_clock_admin_response_from_verified_with_context,
    build_control_plane_unix_admission_error_response,
    build_control_plane_unix_response_from_verified, ensure_control_plane_state_parent_directory,
    finish_control_plane_heartbeat_response, invalidate_authority_clock_restart_checkpoint,
    load_authority_clock_restart_checkpoint,
    prepare_control_plane_heartbeat_response_from_verified,
    prepare_control_plane_heartbeat_response_with_lease_horizon_authority_from_verified,
    read_control_plane_unix_request, store_authority_clock_restart_checkpoint,
    store_validated_authority_clock_restart_checkpoint, verify_control_plane_unix_request,
    write_control_plane_unix_response, AuthenticatedUnixControlPlaneClient, ClusterControlSnapshot,
    ClusterRuntimeMapSnapshot, ControlPlaneAdmin, ControlPlaneAdminAuthCredential,
    ControlPlaneAdminAuthCredentialInput, ControlPlaneAuthorityClock,
    ControlPlaneAuthorityClockAdminSample, ControlPlaneAuthorityClockCheckpointBinding,
    ControlPlaneAuthorityClockContext, ControlPlaneAuthorityClockStatus, ControlPlaneError,
    ControlPlaneFrontendAuthCredential, ControlPlaneFrontendAuthCredentialInput,
    ControlPlaneHeartbeatRefresh, ControlPlaneHeartbeatRuntimeMapSource,
    ControlPlaneRuntimeMapSource, ControlPlaneStorageNodeAuthCredential,
    ControlPlaneStorageNodeAuthCredentialInput, ControlPlaneUnixAuthVerifier,
    FencedPgMetadataTransferSnapshot, FileControlPlaneStore, LeaseHorizonAuthorityBinding,
    PgMetadataProof, PgMetadataTransferProof, PgRouteSnapshot, SingleAuthorityControlPlane,
    UnixControlPlaneClient,
};
use storage::control_plane_auth::{
    ControlPlaneAuthEnvelope, ControlPlaneAuthOperation, ControlPlaneAuthPrincipal,
    ControlPlaneAuthRejectionReason, ControlPlaneAuthTarget, ControlPlaneScopedCredential,
    ControlPlaneScopedCredentialInput, ControlPlaneScopedCredentialStore,
};
use storage::control_plane_command::{ControlPlaneCommand, ControlPlaneCommandResponse};
use storage::control_plane_raft::{
    decode_control_plane_raft_peer_request_auth_operation,
    decode_control_plane_raft_peer_request_frame_identity,
    decode_control_plane_raft_peer_request_frame_kind, durable_artifact_wal_path,
    handle_control_plane_raft_peer_rpc_frame, handle_control_plane_raft_peer_snapshot_frame,
    read_control_plane_raft_peer_transport_frame, write_control_plane_raft_peer_transport_frame,
    ControlPlaneRaftAuthority, ControlPlaneRaftAuthorityStatus, ControlPlaneRaftCommandOutcome,
    ControlPlaneRaftLogId, ControlPlaneRaftNodeId, ControlPlaneRaftPeerAuthPolicy,
    ControlPlaneRaftPeerFrameIdentity, ControlPlaneRaftPeerFrameKind,
    ControlPlaneRaftPeerTransportLimits, ControlPlaneRaftPeerTransportPolicy,
};
use storage::storage_node_server::{
    PreparedStorageNodeServer, StorageNodeBootstrap, StorageNodeControlPlaneRefreshLoop,
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts, StorageNodeServer,
};
use storage::{
    CanonicalUserId, ClusterEpoch, EcShape, LocalClusterMap,
    LocalUnixStorageNodeClientAdmissionSettings, LocalUnixStorageNodeClientConfig, NodeId, PgId,
    PgMetadataTransferArtifact, PgMetadataTransferError, PgState, RouteMapValidity, StorageCluster,
    StorageClusterRuntimeMapHandle, StorageRpcErrorCode, StoreError,
};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio_rustls::TlsAcceptor;

use config::{
    ConfiguredControlPlaneAdminAuthCredential, ConfiguredControlPlaneAdminCommandAuth,
    ConfiguredControlPlaneFrontendAuthCredential, ConfiguredControlPlaneFrontendRuntimeMapAuth,
    ConfiguredControlPlaneRaftAuthCredential, ConfiguredControlPlaneStorageAuthCredential,
    ConfiguredCredential, ConfiguredCredentialProfile, ProcessRole, ServerConfig,
};
use server_http::http::HttpFrontend;

const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;
const CONTROL_PLANE_RPC_WORKER_LIMIT: usize = 64;
const CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT: usize = 8;
const CONTROL_PLANE_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);
const CONTROL_PLANE_RAFT_PEER_RPC_WORKER_LIMIT: usize = 64;
const CONTROL_PLANE_RAFT_PEER_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);
const CONTROL_PLANE_RAFT_PEER_CHECKPOINT_MAX_WAL_SUFFIX_BYTES: u64 = 64 * 1024 * 1024;
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
        ProcessRole::LegacyLocal => {
            run_legacy_local_frontend(config, host_id, ec_config).await;
        }
    }
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
        .and_then(|manifest| manifest.initialize_standalone_storage())
        {
            Ok(()) => {
                println!("initialized static standalone cluster state");
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
                println!("{} {}", runtime_epoch.get(), route_epoch.get());
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
                println!("{}", format_authority_clock_status(status));
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
                println!("{}", format_authority_clock_status(status));
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
                .parse::<ControlPlaneRaftNodeId>()
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
                "usage: argmin-s3 {} <socket-path> <pg-id> <source-epoch> <source-applied-log-index> <source-applied-log-hash> <source-state-digest> <imported-applied-log-index> <imported-applied-log-hash> <imported-state-digest> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        let Some((pg_id, transfer, acting_set)) =
            parse_control_plane_pg_acting_set_with_metadata_transfer_args(args)
        else {
            eprintln!(
                "usage: argmin-s3 {} <socket-path> <pg-id> <source-epoch> <source-applied-log-index> <source-applied-log-hash> <source-state-digest> <imported-applied-log-index> <imported-applied-log-hash> <imported-state-digest> <node-id>...",
                command.to_string_lossy()
            );
            return Some(2);
        };
        return match set_control_plane_pg_acting_set_with_metadata_transfer_live(
            Path::new(&path),
            pg_id,
            acting_set,
            transfer,
        ) {
            Ok(epoch) => {
                eprintln!(
                    "control-plane set PG {} acting set with metadata transfer at epoch {}",
                    pg_id.get(),
                    epoch.get()
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
                    pg_id.get(),
                    epoch.get()
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
                if summary.already_completed {
                    eprintln!(
                        "control-plane PG {} metadata transfer already completed at epoch {} with acting primary node {}",
                        pg_id.get(),
                        summary.destination_epoch.get(),
                        summary.source_node_id.as_u32()
                    );
                } else {
                    eprintln!(
                        "control-plane transferred PG {} metadata from node {} epoch {} to epoch {} with imported proof {}:{}:{}",
                        pg_id.get(),
                        summary.source_node_id.as_u32(),
                        summary.source_epoch.get(),
                        summary.destination_epoch.get(),
                        summary.imported_proof.applied_log_index,
                        summary.imported_proof.applied_log_hash,
                        summary.imported_proof.state_digest
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
                pg_id.get(),
                epoch.get()
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
) -> Option<(PgId, Vec<NodeId>)> {
    let pg_id = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| value.parse::<u32>().ok())
        .map(PgId::new)?;
    let mut acting_set = Vec::new();
    for node_id in args {
        let node_id = node_id
            .into_string()
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .map(NodeId::new)?;
        acting_set.push(node_id);
    }
    if acting_set.is_empty() {
        return None;
    }
    Some((pg_id, acting_set))
}

fn parse_pg_id_arg(value: OsString) -> Option<PgId> {
    value
        .into_string()
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .map(PgId::new)
}

fn parse_control_plane_pg_acting_set_with_metadata_transfer_args(
    mut args: impl Iterator<Item = OsString>,
) -> Option<(PgId, PgMetadataTransferProof, Vec<NodeId>)> {
    let pg_id = parse_next_u32(&mut args).map(PgId::new)?;
    let source_epoch = parse_next_u64(&mut args).and_then(ClusterEpoch::new)?;
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
            .and_then(|value| value.parse::<u32>().ok())
            .map(NodeId::new)?;
        acting_set.push(node_id);
    }
    if acting_set.is_empty() {
        return None;
    }
    Some((
        pg_id,
        PgMetadataTransferProof::new_with_imported_metadata_proof(
            source_epoch,
            PgMetadataProof {
                applied_log_index: source_applied_log_index,
                applied_log_hash: source_applied_log_hash,
                state_digest: source_state_digest,
            },
            PgMetadataProof {
                applied_log_index: imported_applied_log_index,
                applied_log_hash: imported_applied_log_hash,
                state_digest: imported_state_digest,
            },
        ),
        acting_set,
    ))
}

fn parse_next_u32(args: &mut impl Iterator<Item = OsString>) -> Option<u32> {
    args.next()?.into_string().ok()?.parse().ok()
}

fn parse_next_u64(args: &mut impl Iterator<Item = OsString>) -> Option<u64> {
    args.next()?.into_string().ok()?.parse().ok()
}

fn set_control_plane_pg_acting_set(
    state_path: &Path,
    pg_id: PgId,
    acting_set: Vec<NodeId>,
) -> Result<ClusterEpoch, String> {
    let _state_lock = acquire_control_plane_state_lock(state_path)?;
    let store = FileControlPlaneStore::new(state_path);
    let mut authority = SingleAuthorityControlPlane::open(store)
        .map_err(|error| format!("failed to open control-plane state: {error}"))?;
    let snapshot = authority
        .set_pg_acting_set(pg_id, acting_set)
        .map_err(|error| format!("failed to set PG acting set: {error}"))?;
    Ok(snapshot.cluster_epoch())
}

fn set_control_plane_pg_acting_set_live(
    socket_path: &Path,
    pg_id: PgId,
    acting_set: Vec<NodeId>,
) -> Result<ClusterEpoch, String> {
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .set_pg_acting_set_checked(pg_id, acting_set)
        .map_err(|error| format!("failed to set live PG acting set: {error}"))
}

fn transfer_control_plane_raft_leadership(
    socket_path: &Path,
    node_id: ControlPlaneRaftNodeId,
) -> Result<(), String> {
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .transfer_raft_leadership_to(node_id)
        .map_err(|error| format!("failed to transfer control-plane Raft leadership: {error}"))
}

fn trigger_control_plane_raft_snapshot_and_purge(
    socket_path: &Path,
) -> Result<Option<u64>, String> {
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .trigger_raft_snapshot_and_purge()
        .map_err(|error| format!("failed to trigger control-plane Raft snapshot purge: {error}"))
}

fn trigger_control_plane_raft_election(socket_path: &Path) -> Result<(), String> {
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .trigger_raft_election()
        .map_err(|error| format!("failed to trigger control-plane Raft election: {error}"))
}

fn control_plane_authority_clock_status(
    socket_path: &Path,
) -> Result<ControlPlaneAuthorityClockStatus, String> {
    build_admin_clock_recovery_client_from_command_auth_env(socket_path)?
        .authority_clock_status()
        .map_err(|error| format!("failed to read control-plane authority-clock status: {error}"))
}

fn reestablish_control_plane_authority_clock(
    socket_path: &Path,
) -> Result<ControlPlaneAuthorityClockStatus, String> {
    build_admin_clock_recovery_client_from_command_auth_env(socket_path)?
        .reestablish_authority_clock()
        .map_err(|error| format!("failed to re-establish control-plane authority clock: {error}"))
}

fn control_plane_clock_recovery_socket_path(control_plane_socket_path: &Path) -> PathBuf {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in control_plane_socket_path.as_os_str().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    control_plane_socket_path.with_file_name(format!(".c-{hash:016x}"))
}

fn format_authority_clock_status(status: ControlPlaneAuthorityClockStatus) -> String {
    format!(
        "generation={} established={} blocked_reason={:?} committed_timestamp_high_water_ms={} bound_raft_term={} current_raft_term={} local_raft_authority_leader={} local_raft_authority_serving={}",
        status.generation(),
        status.established(),
        status.blocked_reason(),
        status
            .committed_timestamp_high_water_ms()
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
        status
            .bound_raft_leadership_term()
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
        status
            .current_raft_leadership_term()
            .map_or_else(|| "-".to_owned(), |value| value.to_string()),
        status.local_raft_authority_leader(),
        status.local_raft_authority_serving(),
    )
}

fn fence_control_plane_pg_for_metadata_transfer_live(
    socket_path: &Path,
    pg_id: PgId,
) -> Result<ClusterEpoch, String> {
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .fence_pg_for_metadata_transfer_runtime_map_checked(pg_id)
        .map(|runtime_map| runtime_map.cluster_epoch())
        .map_err(|error| format!("failed to fence live PG for metadata transfer: {error}"))
}

fn set_control_plane_pg_acting_set_with_metadata_transfer_live(
    socket_path: &Path,
    pg_id: PgId,
    acting_set: Vec<NodeId>,
    transfer: PgMetadataTransferProof,
) -> Result<ClusterEpoch, String> {
    let min_cluster_epoch = transfer
        .source_epoch()
        .get()
        .checked_add(1)
        .and_then(ClusterEpoch::new)
        .ok_or_else(|| "metadata-transfer destination cluster epoch overflowed".to_owned())?;
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .set_pg_acting_set_with_metadata_transfer_checked(
            pg_id,
            acting_set,
            transfer,
            min_cluster_epoch,
        )
        .map_err(|error| {
            format!("failed to set live PG acting set with metadata transfer: {error}")
        })
}

struct MetadataTransferLiveSummary {
    source_node_id: NodeId,
    source_epoch: ClusterEpoch,
    destination_epoch: ClusterEpoch,
    imported_proof: PgMetadataProof,
    already_completed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataTransferLiveFailpoint {
    Fence,
    TransferInstall,
    Import,
}

impl MetadataTransferLiveFailpoint {
    fn from_env() -> Result<Option<Self>, String> {
        let Ok(value) = std::env::var("ARGMIN_METADATA_TRANSFER_FAILPOINT") else {
            return Ok(None);
        };
        match value.as_str() {
            "" => Ok(None),
            "after-fence" => Ok(Some(Self::Fence)),
            "after-transfer-install" => Ok(Some(Self::TransferInstall)),
            "after-import" => Ok(Some(Self::Import)),
            _ => Err(format!(
                "ARGMIN_METADATA_TRANSFER_FAILPOINT must be after-fence, after-transfer-install, or after-import, got {value:?}"
            )),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Fence => "after-fence",
            Self::TransferInstall => "after-transfer-install",
            Self::Import => "after-import",
        }
    }
}

fn maybe_fail_metadata_transfer_live(
    configured: Option<MetadataTransferLiveFailpoint>,
    point: MetadataTransferLiveFailpoint,
) -> Result<(), String> {
    if configured == Some(point) {
        return Err(format!(
            "injected metadata transfer live failure at {}",
            point.label()
        ));
    }
    Ok(())
}

fn metadata_transfer_peering_route(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
) -> Result<&PgRouteSnapshot, String> {
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .ok_or_else(|| format!("control-plane runtime map has no PG {}", pg_id.get()))?;
    if route.state() != PgState::Peering {
        return Err(format!(
            "PG {} must be Peering after live metadata transfer fence, got {:?}",
            pg_id.get(),
            route.state()
        ));
    }
    Ok(route)
}

fn metadata_transfer_peering_source_node_id(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
) -> Result<NodeId, String> {
    Ok(metadata_transfer_peering_route(runtime_map, pg_id)?.primary_node_id())
}

fn metadata_transfer_peering_source_route_matches(
    runtime_map: &ClusterRuntimeMapSnapshot,
    pg_id: PgId,
    source_node_id: NodeId,
) -> Result<(), String> {
    let actual_source_node_id = metadata_transfer_peering_source_node_id(runtime_map, pg_id)?;
    if actual_source_node_id == source_node_id {
        return Ok(());
    }
    Err(format!(
        "PG {} fenced source node changed from {} to {}",
        pg_id.get(),
        source_node_id.as_u32(),
        actual_source_node_id.as_u32()
    ))
}

fn metadata_transfer_source_lease_wait_duration(
    now_ms: u64,
    lease_deadline_ms: u64,
) -> Option<Duration> {
    if now_ms >= lease_deadline_ms {
        return None;
    }
    let remaining_ms = lease_deadline_ms - now_ms;
    Some(Duration::from_millis(remaining_ms.clamp(1, 100)))
}

fn wait_for_metadata_transfer_source_lease_to_expire(lease_deadline_ms: u64) {
    while let Some(duration) = metadata_transfer_source_lease_wait_duration(
        storage::clock::current_time_millis(),
        lease_deadline_ms,
    ) {
        thread::sleep(duration);
    }
}

fn transfer_control_plane_pg_metadata_live(
    socket_path: &Path,
    pg_id: PgId,
    acting_set: Vec<NodeId>,
) -> Result<MetadataTransferLiveSummary, String> {
    let config = static_cluster_config::load_server_config_from_environment()
        .map_err(|error| format!("configuration error: {error}"))?;
    let ec_config = EcConfig::new(config.ec_k, config.ec_m)
        .map_err(|error| format!("invalid EC config: {error}"))?;
    let read_control_plane =
        build_frontend_control_plane_client_from_runtime_map_auth_env(socket_path)?;
    let admin_control_plane = build_admin_control_plane_client_from_command_auth_env(socket_path)?;
    let failpoint = MetadataTransferLiveFailpoint::from_env()?;
    if let Some(summary) =
        completed_metadata_transfer_live_summary(&read_control_plane, pg_id, &acting_set)?
    {
        return Ok(summary);
    }
    let fenced = admin_control_plane
        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(pg_id)
        .map_err(|error| format!("failed to fence live PG for metadata transfer: {error}"))?;
    let (fenced_runtime, source_lease_deadline_ms) = fenced.into_parts();
    let source_node_id = metadata_transfer_peering_source_node_id(&fenced_runtime, pg_id)?;
    if let Some(source_lease_deadline_ms) = source_lease_deadline_ms {
        wait_for_metadata_transfer_source_lease_to_expire(source_lease_deadline_ms);
    }
    let source_runtime = admin_control_plane
        .fence_pg_for_metadata_transfer_runtime_map_checked(pg_id)
        .map_err(|error| format!("failed to refresh fenced PG metadata transfer map: {error}"))?;
    metadata_transfer_peering_source_route_matches(&source_runtime, pg_id, source_node_id)?;
    maybe_fail_metadata_transfer_live(failpoint, MetadataTransferLiveFailpoint::Fence)?;
    let source_route = metadata_transfer_peering_route(&source_runtime, pg_id)?;
    let (artifact, destination_runtime, import_epoch, imported_proof, source_node_id) =
        if let Some(existing_transfer) = source_route.peering_metadata_transfer() {
            if source_route.acting_set() != acting_set.as_slice() {
                return Err(format!(
                    "PG {} already has transfer marker for acting set {:?}, not requested {:?}",
                    pg_id.get(),
                    source_route.acting_set(),
                    acting_set
                ));
            }
            let source_route_epoch = source_route
                .peering_metadata_transfer_source_route_epoch()
                .ok_or_else(|| {
                    format!(
                        "PG {} transfer marker is missing source route epoch",
                        pg_id.get()
                    )
                })?;
            let existing_source_node_id = source_route
                .peering_metadata_transfer_source_node_id()
                .ok_or_else(|| {
                format!(
                    "PG {} transfer marker is missing source node id",
                    pg_id.get()
                )
            })?;
            let export_runtime = source_runtime
                .runtime_map_at_epoch(source_route_epoch)
                .map_err(|error| {
                    format!(
                        "failed to reconstruct PG {} metadata transfer source route at epoch {}: {error}",
                        pg_id.get(),
                        source_route_epoch.get()
                    )
                })?;
            let export_route = metadata_transfer_peering_route(&export_runtime, pg_id)?;
            if export_route.primary_node_id() != existing_source_node_id {
                return Err(format!(
                    "PG {} transfer marker source node {} does not match retained source route primary {}",
                    pg_id.get(),
                    existing_source_node_id.as_u32(),
                    export_route.primary_node_id().as_u32()
                ));
            }
            let source_cluster = build_frontend_storage_cluster_from_runtime_map(
                &config,
                &ec_config,
                &export_runtime,
            )?;
            let artifact = export_pg_metadata_transfer_artifact_retrying_stale_route(
                &source_cluster,
                pg_id,
                existing_source_node_id,
            )?;
            if artifact.cluster_epoch() != existing_transfer.source_epoch()
                || artifact.source_metadata_proof() != existing_transfer.source_metadata_proof()
            {
                return Err(format!(
                    "PG {} resumed transfer artifact {:?} at epoch {} does not match installed marker {:?}",
                    pg_id.get(),
                    artifact.source_metadata_proof(),
                    artifact.cluster_epoch().get(),
                    existing_transfer
                ));
            }
            let recomputed_imported_proof =
                StorageCluster::metadata_transfer_imported_proof_at_epoch(
                    &artifact,
                    source_runtime.cluster_epoch(),
                )
                .map_err(|error| {
                    format!("failed to compute imported PG metadata proof: {error}")
                })?;
            if recomputed_imported_proof != existing_transfer.metadata_proof() {
                return Err(format!(
                    "PG {} resumed transfer imported proof {:?} does not match installed marker {:?}",
                    pg_id.get(),
                    recomputed_imported_proof,
                    existing_transfer.metadata_proof()
                ));
            }
            (
                artifact,
                source_runtime.clone(),
                source_runtime.cluster_epoch(),
                existing_transfer.metadata_proof(),
                existing_source_node_id,
            )
        } else {
            let source_cluster = build_frontend_storage_cluster_from_runtime_map(
                &config,
                &ec_config,
                &source_runtime,
            )?;
            let artifact = export_pg_metadata_transfer_artifact_retrying_stale_route(
                &source_cluster,
                pg_id,
                source_node_id,
            )?;
            let planned_destination_epoch = source_runtime
                .cluster_epoch()
                .get()
                .checked_add(1)
                .and_then(ClusterEpoch::new)
                .ok_or_else(|| "destination cluster epoch overflowed".to_string())?;
            let planned_imported_proof = StorageCluster::metadata_transfer_imported_proof_at_epoch(
                &artifact,
                planned_destination_epoch,
            )
            .map_err(|error| format!("failed to compute imported PG metadata proof: {error}"))?;
            let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
                artifact.cluster_epoch(),
                artifact.source_metadata_proof(),
                planned_imported_proof,
            );
            let destination_runtime = admin_control_plane
                .set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                    pg_id,
                    acting_set.clone(),
                    transfer,
                    planned_destination_epoch,
                )
                .map_err(|error| {
                    format!("failed to install transfer-backed live PG acting set: {error}")
                })?;
            let actual_destination_epoch = destination_runtime.cluster_epoch();
            if actual_destination_epoch != planned_destination_epoch {
                return Err(format!(
                    "control-plane installed transfer at epoch {}, expected {}",
                    actual_destination_epoch.get(),
                    planned_destination_epoch.get()
                ));
            }
            (
                artifact,
                destination_runtime,
                planned_destination_epoch,
                planned_imported_proof,
                source_node_id,
            )
        };
    maybe_fail_metadata_transfer_live(failpoint, MetadataTransferLiveFailpoint::TransferInstall)?;
    let destination_cluster =
        build_frontend_storage_cluster_from_runtime_map(&config, &ec_config, &destination_runtime)?;
    let actual_imported_proof = import_pg_metadata_transfer_artifact_retrying_stale_route(
        &read_control_plane,
        &destination_cluster,
        MetadataTransferImportContext {
            config: &config,
            ec_config: &ec_config,
            pg_id,
            acting_set: &acting_set,
            destination_epoch: import_epoch,
            expected_transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
                artifact.cluster_epoch(),
                artifact.source_metadata_proof(),
                imported_proof,
            ),
            imported_proof,
        },
        &artifact,
    )?;
    if actual_imported_proof != imported_proof {
        return Err(format!(
            "imported PG metadata proof {:?} did not match expected {:?}",
            actual_imported_proof, imported_proof
        ));
    }
    maybe_fail_metadata_transfer_live(failpoint, MetadataTransferLiveFailpoint::Import)?;
    Ok(MetadataTransferLiveSummary {
        source_node_id,
        source_epoch: artifact.cluster_epoch(),
        destination_epoch: import_epoch,
        imported_proof,
        already_completed: false,
    })
}

fn completed_metadata_transfer_live_summary(
    control_plane: &impl ControlPlaneRuntimeMapSource,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<Option<MetadataTransferLiveSummary>, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let runtime_map = match control_plane
            .pg_runtime_map_snapshot(pg_id, storage::clock::current_time_millis())
        {
            Ok(runtime_map) => runtime_map,
            Err(error) => {
                if control_plane_metadata_transfer_observation_error_is_retryable(&error) {
                    if Instant::now() >= deadline {
                        return Ok(None);
                    }
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
                return Err(format!(
                    "failed to fetch control-plane PG {} runtime map before metadata transfer: {error}",
                    pg_id.get()
                ));
            }
        };
        let Some(route) = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
        else {
            return Ok(None);
        };
        if route.state() != PgState::Active || route.acting_set() != acting_set {
            return Ok(None);
        }
        return Ok(Some(MetadataTransferLiveSummary {
            source_node_id: route.primary_node_id(),
            source_epoch: route.cluster_epoch(),
            destination_epoch: route.cluster_epoch(),
            imported_proof: PgMetadataProof {
                applied_log_index: 0,
                applied_log_hash: 0,
                state_digest: 0,
            },
            already_completed: true,
        }));
    }
}

fn export_pg_metadata_transfer_artifact_retrying_stale_route(
    source_cluster: &StorageCluster,
    pg_id: PgId,
    source_node_id: NodeId,
) -> Result<PgMetadataTransferArtifact, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match source_cluster
            .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, source_node_id)
        {
            Ok(artifact) => return Ok(artifact),
            Err(error) if metadata_transfer_error_is_transient_route_refresh(&error) => {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "timed out exporting PG metadata transfer artifact: {error}"
                    ));
                }
            }
            Err(error) => {
                return Err(format!(
                    "failed to export PG metadata transfer artifact: {error}"
                ));
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
}

struct MetadataTransferImportContext<'a> {
    config: &'a ServerConfig,
    ec_config: &'a EcConfig,
    pg_id: PgId,
    acting_set: &'a [NodeId],
    destination_epoch: ClusterEpoch,
    expected_transfer: PgMetadataTransferProof,
    imported_proof: PgMetadataProof,
}

fn import_pg_metadata_transfer_artifact_retrying_stale_route(
    control_plane: &impl ControlPlaneRuntimeMapSource,
    destination_cluster: &Arc<StorageCluster>,
    context: MetadataTransferImportContext<'_>,
    artifact: &PgMetadataTransferArtifact,
) -> Result<PgMetadataProof, String> {
    retry_pg_metadata_transfer_import(
        control_plane,
        Arc::clone(destination_cluster),
        &context,
        |cluster| cluster.import_pg_metadata_transfer_artifact_from_retained_log(artifact),
    )
}

fn retry_pg_metadata_transfer_import(
    control_plane: &impl ControlPlaneRuntimeMapSource,
    mut destination_cluster: Arc<StorageCluster>,
    context: &MetadataTransferImportContext<'_>,
    mut import: impl FnMut(&StorageCluster) -> Result<PgMetadataProof, PgMetadataTransferError>,
) -> Result<PgMetadataProof, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match import(&destination_cluster) {
            Ok(proof) => return Ok(proof),
            Err(error) if metadata_transfer_error_is_transient_route_refresh(&error) => {
                match refresh_pg_metadata_transfer_import_route(control_plane, context)? {
                    MetadataTransferImportRouteRefresh::Completed => {
                        return Ok(context.imported_proof);
                    }
                    MetadataTransferImportRouteRefresh::Retry(cluster) => {
                        destination_cluster = cluster;
                    }
                    MetadataTransferImportRouteRefresh::NotReady => {}
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "timed out importing PG metadata transfer artifact: {error}"
                    ));
                }
            }
            Err(error) => {
                return Err(format!(
                    "failed to import PG metadata transfer artifact: {error}"
                ));
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
}

enum MetadataTransferImportRouteRefresh {
    Completed,
    Retry(Arc<StorageCluster>),
    NotReady,
}

fn refresh_pg_metadata_transfer_import_route(
    control_plane: &impl ControlPlaneRuntimeMapSource,
    context: &MetadataTransferImportContext<'_>,
) -> Result<MetadataTransferImportRouteRefresh, String> {
    let runtime_map = match control_plane
        .pg_runtime_map_snapshot(context.pg_id, storage::clock::current_time_millis())
    {
        Ok(runtime_map) => runtime_map,
        Err(error) => {
            if control_plane_metadata_transfer_observation_error_is_retryable(&error) {
                return Ok(MetadataTransferImportRouteRefresh::NotReady);
            }
            return Err(format!(
                "failed to refresh live PG {} metadata transfer state: {error}",
                context.pg_id.get()
            ));
        }
    };
    if runtime_map.cluster_epoch() < context.destination_epoch {
        return Ok(MetadataTransferImportRouteRefresh::NotReady);
    }
    let Some(route) = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == context.pg_id)
    else {
        return Err(format!(
            "refreshed metadata transfer destination for PG {} is missing at authoritative epoch {}; expected epoch {}, Peering acting set {:?}, transfer {:?}",
            context.pg_id.get(),
            runtime_map.cluster_epoch().get(),
            context.destination_epoch.get(),
            context.acting_set,
            context.expected_transfer,
        ));
    };
    if route.state() == PgState::Active && route.acting_set() == context.acting_set {
        return Ok(MetadataTransferImportRouteRefresh::Completed);
    }
    if runtime_map.cluster_epoch() != context.destination_epoch
        || route.state() != PgState::Peering
        || route.acting_set() != context.acting_set
        || route.peering_metadata_transfer() != Some(context.expected_transfer)
    {
        return Err(format!(
            "refreshed metadata transfer destination for PG {} does not match: expected epoch {}, state {:?}, acting set {:?}, transfer {:?}; actual epoch {}, state {:?}, acting set {:?}, transfer {:?}",
            context.pg_id.get(),
            context.destination_epoch.get(),
            PgState::Peering,
            context.acting_set,
            context.expected_transfer,
            runtime_map.cluster_epoch().get(),
            route.state(),
            route.acting_set(),
            route.peering_metadata_transfer(),
        ));
    }
    build_frontend_storage_cluster_from_runtime_map(context.config, context.ec_config, &runtime_map)
        .map(MetadataTransferImportRouteRefresh::Retry)
        .map_err(|error| {
            format!(
                "failed to rebuild live PG {} metadata transfer destination route: {error}",
                context.pg_id.get()
            )
        })
}

fn control_plane_runtime_map_not_ready_for_serving(message: &str) -> bool {
    message.contains("control-plane runtime map has no routed PGs")
        || message.contains(" has no serving primary in cluster epoch ")
        || (message.contains(" primary node ")
            && message.contains(" has not reported active state in cluster epoch "))
        || (message.contains(" reported unresolved pending metadata command for PG ")
            && message.contains(" in cluster epoch "))
}

fn control_plane_metadata_transfer_observation_error_is_retryable(
    error: &ControlPlaneError,
) -> bool {
    error.is_retryable_read_only_rpc_transport_error()
        || control_plane_runtime_map_not_ready_for_serving(&error.to_string())
}

fn metadata_transfer_error_is_transient_route_refresh(error: &PgMetadataTransferError) -> bool {
    match error {
        PgMetadataTransferError::Store(store_error) => {
            store_error_is_transient_route_refresh_for_metadata_transfer(store_error)
        }
        PgMetadataTransferError::Apply(storage::BucketSnapshotLoadError::Store(store_error)) => {
            store_error_is_transient_route_refresh_for_metadata_transfer(store_error)
        }
        PgMetadataTransferError::Reconstruction { message } => {
            message.starts_with("PG peering node ")
                && message.contains(" reported epoch ")
                && message.contains(", expected ")
        }
        _ => false,
    }
}

fn store_error_is_transient_route_refresh_for_metadata_transfer(error: &StoreError) -> bool {
    match error {
        StoreError::RouteMapExpired { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::StaleShardLocation { .. } => true,
        StoreError::StorageRpc { code, .. } => {
            *code == StorageRpcErrorCode::StaleShardLocation
                || *code == StorageRpcErrorCode::MetadataTransferHistoricalRouteActive
                || *code == StorageRpcErrorCode::MetadataCommandContention
                || *code == StorageRpcErrorCode::TransportTimeout
                || *code == StorageRpcErrorCode::TransportClosed
        }
        StoreError::ShardStore { source, .. } => {
            store_error_is_transient_route_refresh_for_metadata_transfer(source)
        }
        _ => false,
    }
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
    pg_id: PgId,
    expected_acting_set: &[NodeId],
) -> Result<(ClusterEpoch, ClusterEpoch), String> {
    let control_plane = build_frontend_control_plane_client_from_runtime_map_auth_env(socket_path)?;
    let runtime_map = control_plane
        .runtime_map_snapshot(storage::clock::current_time_millis())
        .map_err(|error| format!("control-plane PG runtime map is not ready: {error}"))?;
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
        .ok_or_else(|| {
            format!(
                "control-plane runtime map has no route for PG {}",
                pg_id.get()
            )
        })?;
    if route.state() != PgState::Active
        || route.primary_lease_deadline_ms().is_none()
        || route.acting_set() != expected_acting_set
    {
        return Err(format!(
            "control-plane PG {} is not serving on expected acting set {:?}: state {:?}, acting set {:?}, primary lease deadline {:?}",
            pg_id.get(),
            expected_acting_set,
            route.state(),
            route.acting_set(),
            route.primary_lease_deadline_ms(),
        ));
    }
    Ok((runtime_map.cluster_epoch(), route.cluster_epoch()))
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
            "node_id={} incarnation={} endpoint={} lease_deadline_ms={} storage_history_floor_epoch={} history_report_observed_epoch={} history_report_validation_epoch={} history_report_accepted_at_ms={} history_live_payload_epoch={} history_durable_backfill_epoch={} history_pending_metadata_command_epoch={}",
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
        "control_plane_raft_wal append_total={} append_error_total={} append_us_total={} append_us_max={} lock_wait_us_total={} lock_wait_us_max={} frame_bytes_total={} frame_bytes_last={} frame_bytes_max={} file_sync_total={} file_sync_us_total={} file_sync_us_max={} directory_sync_total={} directory_sync_us_total={} directory_sync_us_max={}",
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
    let listener = bind_control_plane_socket(Path::new(socket_path)).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let recovery_socket_path = control_plane_clock_recovery_socket_path(Path::new(socket_path));
    let recovery_listener = bind_control_plane_clock_recovery_socket(&recovery_socket_path)
        .unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });
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
        invalidate_authority_clock_restart_checkpoint(Path::new(state_path)).unwrap_or_else(
            |error| {
                eprintln!("failed to invalidate blocked authority clock checkpoint: {error}");
                std::process::exit(1);
            },
        );
    }
    if let Some(previous_authority) = authority.snapshot().lease_grant_horizon_authority() {
        if !authority_clock.resume_single_authority_lease_horizon_generation(previous_authority) {
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
    let authority_clock = Arc::new(Mutex::new(authority_clock));
    let authority_clock_checkpoint_target = Arc::new(AuthorityClockCheckpointTarget {
        path: PathBuf::from(state_path),
        binding: authority_clock_checkpoint_binding,
    });
    let authority = Arc::new(Mutex::new(authority));
    let _checkpoint_loop = spawn_standalone_control_plane_checkpoint_loop(Arc::clone(&authority));
    let active_rpc_workers = Arc::new(AtomicUsize::new(0));
    let active_recovery_rpc_workers = Arc::new(AtomicUsize::new(0));
    let auth_verifier = build_control_plane_unix_auth_verifier(config)
        .unwrap_or_else(|error| {
            eprintln!("failed to configure control-plane auth verifier: {error}");
            std::process::exit(1);
        })
        .map(Arc::new);
    process_info!(
        "argmin-s3 control-plane manager using state {} on {} (clock recovery {}, lease scan {} ms)",
        state_path,
        socket_path,
        recovery_socket_path.display(),
        config.control_plane_lease_scan_interval.as_millis()
    );
    if let Some(auth_verifier) = &auth_verifier {
        process_info!(
            "{}",
            format_control_plane_unix_auth_diagnostics(auth_verifier)
        );
    }

    let _rpc_listener_loop = spawn_control_plane_rpc_listener_loop(
        listener,
        Arc::clone(&authority),
        Some(Arc::clone(&authority_clock)),
        Some(Arc::clone(&authority_clock_checkpoint_target)),
        ControlPlaneRpcWorkerPolicy {
            gate_request_time_with_authority_clock: true,
            active_rpc_workers: Arc::clone(&active_rpc_workers),
            worker_limit: CONTROL_PLANE_RPC_WORKER_LIMIT,
            endpoint: ControlPlaneRpcEndpoint::Ordinary,
            auth_verifier: auth_verifier.clone(),
            raft_authority_admission: None,
            durable_response_publication: None,
        },
    );
    let _clock_recovery_listener_loop = spawn_control_plane_rpc_listener_loop(
        recovery_listener,
        Arc::clone(&authority),
        Some(Arc::clone(&authority_clock)),
        Some(Arc::clone(&authority_clock_checkpoint_target)),
        ControlPlaneRpcWorkerPolicy {
            gate_request_time_with_authority_clock: true,
            active_rpc_workers: active_recovery_rpc_workers,
            worker_limit: CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
            endpoint: ControlPlaneRpcEndpoint::ClockRecovery,
            auth_verifier: auth_verifier.clone(),
            raft_authority_admission: None,
            durable_response_publication: None,
        },
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
            invalidate_blocked_authority_clock_checkpoint(
                &authority_clock,
                Some(&authority_clock_checkpoint_target),
            )?;
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

#[derive(Clone)]
struct ExperimentalRaftDurabilityPublication {
    gate: Arc<(Mutex<ExperimentalRaftDurabilityPublicationState>, Condvar)>,
    poisoned: Arc<AtomicBool>,
}

#[derive(Default)]
struct ExperimentalRaftDurabilityPublicationState {
    active_responses: usize,
    poison_requested: bool,
}

struct ExperimentalRaftResponsePublicationPermit<'a> {
    publication: &'a ExperimentalRaftDurabilityPublication,
}

impl Drop for ExperimentalRaftResponsePublicationPermit<'_> {
    fn drop(&mut self) {
        let (gate, responses_drained) = &*self.publication.gate;
        let mut state = gate
            .lock()
            .expect("experimental OpenRaft response publication mutex poisoned");
        state.active_responses = state
            .active_responses
            .checked_sub(1)
            .expect("response publication permit count should be positive");
        if state.active_responses == 0 {
            responses_drained.notify_all();
        }
    }
}

impl ExperimentalRaftDurabilityPublication {
    fn new() -> Self {
        Self {
            gate: Arc::new((
                Mutex::new(ExperimentalRaftDurabilityPublicationState::default()),
                Condvar::new(),
            )),
            poisoned: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    fn publish_poison(&self, before_publish: impl FnOnce()) {
        let (gate, responses_drained) = &*self.gate;
        let mut state = gate
            .lock()
            .expect("experimental OpenRaft response publication mutex poisoned");
        if state.poison_requested {
            while !self.poisoned.load(Ordering::Acquire) {
                state = responses_drained
                    .wait(state)
                    .expect("experimental OpenRaft response publication mutex poisoned");
            }
            return;
        }
        state.poison_requested = true;
        while state.active_responses != 0 {
            state = responses_drained
                .wait(state)
                .expect("experimental OpenRaft response publication mutex poisoned");
        }
        before_publish();
        self.poisoned.store(true, Ordering::Release);
        responses_drained.notify_all();
    }

    fn publish<T>(
        &self,
        publish: impl FnOnce() -> Result<T, ControlPlaneError>,
    ) -> Result<T, ControlPlaneError> {
        let _permit = self.response_publication_permit()?;
        publish()
    }

    fn response_publication_permit(
        &self,
    ) -> Result<ExperimentalRaftResponsePublicationPermit<'_>, ControlPlaneError> {
        let (gate, _) = &*self.gate;
        let mut state = gate
            .lock()
            .expect("experimental OpenRaft response publication mutex poisoned");
        if state.poison_requested || self.poisoned.load(Ordering::Acquire) {
            return Err(ControlPlaneError::RpcRemote {
                message: "experimental OpenRaft durable authority was poisoned before response publication"
                    .to_owned(),
            });
        }
        state.active_responses = state
            .active_responses
            .checked_add(1)
            .expect("response publication permit count overflow");
        Ok(ExperimentalRaftResponsePublicationPermit { publication: self })
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
    durable_poison: Arc<Mutex<Option<String>>>,
    durable_publication: ExperimentalRaftDurabilityPublication,
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
                    .ok_or(ControlPlaneError::RpcRemote {
                        message: "local OpenRaft test authority is not the serving leader"
                            .to_string(),
                    })?;
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
            return Err(ControlPlaneError::RpcRemote {
                message: format!(
                    "local OpenRaft authority is not the serving leader: {:?}",
                    status.linearized_authority_readiness()
                ),
            });
        }
        self.authority_time_and_lease_horizon_binding_for_status(status)
    }

    fn authority_time_and_lease_horizon_binding_for_status(
        &self,
        status: ControlPlaneRaftAuthorityStatus,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        let current_term = status.current_term().ok_or(ControlPlaneError::RpcRemote {
            message: "local OpenRaft leader has no current term".to_string(),
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

    fn durable_poison_error(&self) -> Option<ControlPlaneError> {
        self.durable_poison
            .lock()
            .expect("experimental OpenRaft durable poison mutex poisoned")
            .clone()
            .map(|message| ControlPlaneError::RpcRemote { message })
    }

    fn ensure_not_durably_poisoned(&self) -> Result<(), ControlPlaneError> {
        if let Some(error) = self.durable_poison_error() {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn poison_durable_authority(&self, message: String) {
        self.durable_publication.publish_poison(|| {
            *self
                .durable_poison
                .lock()
                .expect("experimental OpenRaft durable poison mutex poisoned") = Some(message);
        });
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
                return Err(ControlPlaneError::RpcRemote {
                    message: format!(
                        "durable OpenRaft serving read observed poisoned WAL state: {reason}"
                    ),
                });
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
        let outcome = submitted.into_outcome();
        if checkpoint_after_commit {
            if let Err(error) = self.store_durable_restart_artifact() {
                self.poison_durable_authority(format!(
                    "experimental OpenRaft control-plane durability checkpoint failed after a \
                     committed command; refusing to serve until restart: {error}"
                ));
                return Err(error);
            }
        }
        match outcome {
            ControlPlaneRaftCommandOutcome::Applied(response) => Ok(response),
            ControlPlaneRaftCommandOutcome::Rejected(error) => Err(error),
        }
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
        let preflight_authority = preflight_authority.ok_or(ControlPlaneError::RpcRemote {
            message: "OpenRaft heartbeat expiry has no serving lease-horizon authority".to_string(),
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
        let authority = lease_horizon_authority.ok_or(ControlPlaneError::RpcRemote {
            message: "OpenRaft heartbeat expiry has no serving lease-horizon authority".to_string(),
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
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command(ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
            pg_id,
            acting_set,
            transfer,
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
            return Err(ControlPlaneError::RpcRemote {
                message: format!(
                    "OpenRaft startup did not apply through committed state within {timeout:?}: \
                     {message}"
                ),
            });
        }
        source
            .wait_for_applied(committed, deadline.saturating_duration_since(now), message)
            .await?;
    }
}

struct ExperimentalRaftPeerListener {
    listener: UnixListener,
    policy: Arc<ControlPlaneRaftPeerTransportPolicy>,
}

fn build_experimental_raft_peer_transport_policy(
    config: &ServerConfig,
    cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<Option<ControlPlaneRaftPeerTransportPolicy>, String> {
    let Some(local_peer_socket_path) = config.control_plane_raft_peer_socket_path.as_deref() else {
        return Ok(None);
    };
    let peer_endpoints: Vec<_> = if config.control_plane_raft_peer_sockets.is_empty() {
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
        ControlPlaneRaftPeerTransportLimits::default(),
    );
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

    let local_configured = latest_auth_credential_by_version_then_id(
        config
            .control_plane_raft_auth_credentials
            .iter()
            .filter(|credential| credential.node_id == local_node_id),
        |credential| credential.credential_id.as_str(),
        |credential| credential.credential_version,
    )
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
        secret: configured.secret.as_str().as_bytes().to_vec(),
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

fn format_control_plane_unix_auth_diagnostics(verifier: &ControlPlaneUnixAuthVerifier) -> String {
    let status = verifier.status_snapshot();
    let metrics = status.metrics();
    let mut diagnostics = format!(
        "control_plane_unix_auth required={} storage_node_heartbeat_required={} frontend_runtime_map_required={} admin_control_plane_required={} cluster_id={} storage_node_credentials={} frontend_credentials={} admin_credentials={} accepted_total={} rejected_total={}",
        status.required(),
        status.storage_node_heartbeat_required(),
        status.frontend_runtime_map_required(),
        status.admin_control_plane_required(),
        status.cluster_id(),
        status.storage_node_credentials().len(),
        status.frontend_credentials().len(),
        status.admin_credentials().len(),
        metrics.accepted_total(),
        metrics.rejected_total()
    );
    for credential in status.storage_node_credentials() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "control_plane_unix_auth storage_node_credential{{node_id=\"{}\",credential_id=\"{}\",credential_version=\"{}\"}} 1",
            credential.node_id().as_u32(),
            credential.credential_id(),
            credential.credential_version()
        )
        .expect("write to String should not fail");
    }
    for credential in status.frontend_credentials() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "control_plane_unix_auth frontend_credential{{instance_id=\"{}\",credential_id=\"{}\",credential_version=\"{}\"}} 1",
            credential.instance_id(),
            credential.credential_id(),
            credential.credential_version()
        )
        .expect("write to String should not fail");
    }
    for credential in status.admin_credentials() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "control_plane_unix_auth admin_credential{{instance_id=\"{}\",credential_id=\"{}\",credential_version=\"{}\"}} 1",
            credential.instance_id(),
            credential.credential_id(),
            credential.credential_version()
        )
        .expect("write to String should not fail");
    }
    for (operation, count) in metrics.accepted_by_operation() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "control_plane_unix_auth accepted_by_operation{{operation=\"{operation:?}\"}} {count}"
        )
        .expect("write to String should not fail");
    }
    for (operation, count) in metrics.rejected_by_operation() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "control_plane_unix_auth rejected_by_operation{{operation=\"{operation:?}\"}} {count}"
        )
        .expect("write to String should not fail");
    }
    for (reason, count) in metrics.rejected_by_reason() {
        diagnostics.push('\n');
        write!(
            &mut diagnostics,
            "control_plane_unix_auth rejected_by_reason{{reason=\"{reason:?}\"}} {count}"
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

fn experimental_raft_startup_bootstrap_requires_local_serving(
    peer_policy: Option<&ControlPlaneRaftPeerTransportPolicy>,
) -> bool {
    peer_policy.is_some_and(|policy| policy.peers().len() > 1)
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
    Err(ControlPlaneError::RpcRemote {
        message: format!(
            "local OpenRaft authority did not become serving within {timeout:?}: {message}"
        ),
    })
}

#[cfg(test)]
fn bind_experimental_raft_peer_listener(
    config: &ServerConfig,
    cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<Option<ExperimentalRaftPeerListener>, String> {
    let policy =
        build_experimental_raft_peer_transport_policy(config, cluster_name, local_node_id)?;
    bind_experimental_raft_peer_listener_with_policy(config, policy)
}

fn bind_experimental_raft_peer_listener_with_policy(
    config: &ServerConfig,
    policy: Option<ControlPlaneRaftPeerTransportPolicy>,
) -> Result<Option<ExperimentalRaftPeerListener>, String> {
    let Some(peer_socket_path) = config.control_plane_raft_peer_socket_path.as_deref() else {
        return Ok(None);
    };
    let Some(policy) = policy else {
        return Err("configured OpenRaft peer socket is missing peer transport policy".to_string());
    };
    let listener = bind_control_plane_raft_peer_socket(Path::new(peer_socket_path))?;
    Ok(Some(ExperimentalRaftPeerListener {
        listener,
        policy: Arc::new(policy),
    }))
}

#[derive(Clone)]
struct ExperimentalRaftPeerRpcWorkerContext {
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    local_node_id: ControlPlaneRaftNodeId,
    policy: Arc<ControlPlaneRaftPeerTransportPolicy>,
    durability: Option<ExperimentalRaftPeerDurabilityContext>,
    active_workers: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct ExperimentalRaftPeerDurabilityContext {
    artifact_path: Option<Arc<PathBuf>>,
    checkpoint_lock: Arc<Mutex<()>>,
    publication: ExperimentalRaftDurabilityPublication,
}

#[derive(Clone, Copy)]
struct ExperimentalRaftPeerRpcDurability<'a> {
    artifact_path: Option<&'a Path>,
    checkpoint_lock: Option<&'a Arc<Mutex<()>>>,
    publication: Option<&'a ExperimentalRaftDurabilityPublication>,
}

impl<'a> ExperimentalRaftPeerRpcDurability<'a> {
    const NONE: Self = Self {
        artifact_path: None,
        checkpoint_lock: None,
        publication: None,
    };

    fn from_context(context: &'a ExperimentalRaftPeerDurabilityContext) -> Self {
        Self {
            artifact_path: context.artifact_path.as_deref().map(PathBuf::as_path),
            checkpoint_lock: Some(&context.checkpoint_lock),
            publication: Some(&context.publication),
        }
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

fn spawn_experimental_raft_peer_rpc_worker(
    stream: UnixStream,
    context: ExperimentalRaftPeerRpcWorkerContext,
) {
    let ExperimentalRaftPeerRpcWorkerContext {
        runtime,
        authority,
        local_node_id,
        policy,
        durability,
        active_workers,
    } = context;
    if durability
        .as_ref()
        .is_some_and(|durability| durability.publication.is_poisoned())
    {
        eprintln!(
            "experimental OpenRaft control-plane peer RPC rejected: durable authority is poisoned"
        );
        return;
    }
    match active_workers.fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
        (active < CONTROL_PLANE_RAFT_PEER_RPC_WORKER_LIMIT).then_some(active + 1)
    }) {
        Ok(_) => {}
        Err(_) => {
            eprintln!(
                "experimental OpenRaft control-plane peer RPC rejected: worker limit {} reached",
                CONTROL_PLANE_RAFT_PEER_RPC_WORKER_LIMIT
            );
            return;
        }
    }

    thread::spawn(move || {
        let mut stream = stream;
        let rpc_durability = durability
            .as_ref()
            .map_or(ExperimentalRaftPeerRpcDurability::NONE, |durability| {
                ExperimentalRaftPeerRpcDurability::from_context(durability)
            });
        match handle_experimental_raft_peer_rpc_before_ack(
            &runtime,
            &authority,
            &mut stream,
            local_node_id,
            &policy,
            rpc_durability,
        ) {
            Ok(()) => {}
            Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(error)) => {
                eprintln!("experimental OpenRaft control-plane peer RPC failed: {error}");
            }
            Err(ExperimentalRaftPeerRpcWorkerError::Checkpoint(error)) => {
                if let Some(durability) = &durability {
                    durability.publication.publish_poison(|| {});
                }
                eprintln!(
                    "experimental OpenRaft control-plane durability checkpoint failed before peer RPC response; exiting to avoid acknowledging volatile Raft state: {error}"
                );
                std::process::exit(1);
            }
        }
        active_workers.fetch_sub(1, Ordering::AcqRel);
    });
}

#[derive(Debug)]
enum ExperimentalRaftPeerRpcWorkerError {
    PeerRpc(ControlPlaneError),
    Checkpoint(ControlPlaneError),
}

#[derive(Clone, Copy)]
struct ExperimentalRaftValidatedPeerRequest<'a> {
    frame: &'a [u8],
    kind: ControlPlaneRaftPeerFrameKind,
    identity: &'a ControlPlaneRaftPeerFrameIdentity,
    operation: ControlPlaneAuthOperation,
}

fn handle_experimental_raft_peer_rpc_before_ack(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    stream: &mut UnixStream,
    local_node_id: ControlPlaneRaftNodeId,
    policy: &ControlPlaneRaftPeerTransportPolicy,
    durability: ExperimentalRaftPeerRpcDurability<'_>,
) -> Result<(), ExperimentalRaftPeerRpcWorkerError> {
    handle_experimental_raft_peer_rpc_with_response_writer(
        runtime,
        authority,
        stream,
        local_node_id,
        policy,
        durability,
        write_control_plane_raft_peer_transport_frame,
    )
}

fn handle_experimental_raft_peer_rpc_with_response_writer<WriteResponse>(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    stream: &mut UnixStream,
    local_node_id: ControlPlaneRaftNodeId,
    policy: &ControlPlaneRaftPeerTransportPolicy,
    durability: ExperimentalRaftPeerRpcDurability<'_>,
    write_response: WriteResponse,
) -> Result<(), ExperimentalRaftPeerRpcWorkerError>
where
    WriteResponse: FnOnce(&mut UnixStream, &[u8]) -> Result<(), ControlPlaneError>,
{
    ensure_experimental_raft_peer_not_durably_poisoned(durability.publication)?;
    stream
        .set_read_timeout(Some(CONTROL_PLANE_RAFT_PEER_RPC_IO_TIMEOUT))
        .map_err(|source| ControlPlaneError::Io {
            context: "set control-plane OpenRaft peer stream read timeout",
            source,
        })
        .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;
    stream
        .set_write_timeout(Some(CONTROL_PLANE_RAFT_PEER_RPC_IO_TIMEOUT))
        .map_err(|source| ControlPlaneError::Io {
            context: "set control-plane OpenRaft peer stream write timeout",
            source,
        })
        .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;

    let received_frame =
        read_control_plane_raft_peer_transport_frame(stream, policy.limits().max_frame_bytes)
            .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;
    let request_frame = if let Some(auth_policy) = policy.auth_policy() {
        let envelope = match ControlPlaneAuthEnvelope::decode_frame(
            &received_frame,
            policy.limits().max_frame_bytes,
        ) {
            Ok(envelope) => envelope,
            Err(error) => {
                auth_policy.record_peer_frame_rejection_without_operation(
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(error));
            }
        };
        // Header fields are decoded before verification only to select the
        // expected metric bucket and peer identity. The safety boundary is in
        // verify_peer_frame(): credential lookup, MAC verification, and the
        // authenticated payload identity/operation binding check.
        let operation = envelope.header().operation();
        let identity = match experimental_raft_peer_auth_envelope_identity(
            &envelope,
            policy.cluster_name(),
            local_node_id,
        ) {
            Ok(identity) => identity,
            Err(error) => {
                auth_policy.record_peer_frame_rejection(
                    operation,
                    ControlPlaneAuthRejectionReason::Malformed,
                );
                return Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(error));
            }
        };
        auth_policy
            .verify_peer_frame(
                &received_frame,
                &identity,
                operation,
                policy.limits().max_frame_bytes,
            )
            .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?
    } else {
        received_frame
    };
    let frame_kind = decode_control_plane_raft_peer_request_frame_kind(&request_frame)
        .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;
    let identity = decode_control_plane_raft_peer_request_frame_identity(&request_frame)
        .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;
    let operation = decode_control_plane_raft_peer_request_auth_operation(&request_frame)
        .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;
    policy
        .validate_incoming_frame_identity(&identity, local_node_id)
        .map_err(|error| ControlPlaneError::RpcProtocol {
            message: error.to_string(),
        })
        .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;

    let response_frame = handle_experimental_raft_peer_rpc_validated_frame_before_ack(
        runtime,
        authority,
        ExperimentalRaftValidatedPeerRequest {
            frame: &request_frame,
            kind: frame_kind,
            identity: &identity,
            operation,
        },
        policy,
        durability,
    )?;

    publish_experimental_raft_peer_response(durability.publication, || {
        write_response(stream, &response_frame)
    })
}

fn publish_experimental_raft_peer_response<T>(
    publication: Option<&ExperimentalRaftDurabilityPublication>,
    publish: impl FnOnce() -> Result<T, ControlPlaneError>,
) -> Result<T, ExperimentalRaftPeerRpcWorkerError> {
    match publication {
        Some(publication) => publication
            .publish(publish)
            .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc),
        None => publish().map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc),
    }
}

fn experimental_raft_peer_auth_envelope_identity(
    envelope: &ControlPlaneAuthEnvelope,
    expected_cluster_name: &str,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<ControlPlaneRaftPeerFrameIdentity, ControlPlaneError> {
    let source = match envelope.header().source() {
        ControlPlaneAuthPrincipal::RaftPeer { node_id } => *node_id,
        principal => {
            return Err(ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane OpenRaft peer auth source is not a RaftPeer principal: {principal:?}"
                ),
            });
        }
    };
    match envelope.header().target() {
        ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer { .. }) => {}
        target => {
            return Err(ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane OpenRaft peer auth target is not a RaftPeer principal: {target:?}"
                ),
            });
        }
    }
    Ok(ControlPlaneRaftPeerFrameIdentity::new(
        expected_cluster_name.to_string(),
        source,
        local_node_id,
    ))
}

fn handle_experimental_raft_peer_rpc_validated_frame_before_ack(
    runtime: &Handle,
    authority: &ControlPlaneRaftAuthority,
    request: ExperimentalRaftValidatedPeerRequest<'_>,
    policy: &ControlPlaneRaftPeerTransportPolicy,
    durability: ExperimentalRaftPeerRpcDurability<'_>,
) -> Result<Vec<u8>, ExperimentalRaftPeerRpcWorkerError> {
    ensure_experimental_raft_peer_not_durably_poisoned(durability.publication)?;
    let raw_response_frame = block_on_control_plane_raft(runtime, async {
        match request.kind {
            ControlPlaneRaftPeerFrameKind::OrdinaryRpc => {
                handle_control_plane_raft_peer_rpc_frame(
                    authority.raft(),
                    request.frame,
                    request.identity,
                )
                .await
            }
            ControlPlaneRaftPeerFrameKind::Snapshot => {
                handle_control_plane_raft_peer_snapshot_frame(
                    authority.raft(),
                    request.frame,
                    policy.limits().max_frame_bytes,
                    policy.limits().max_snapshot_bytes,
                    request.identity,
                )
                .await
            }
        }
    })
    .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?;
    let response_frame = if let Some(auth_policy) = policy.auth_policy() {
        auth_policy
            .sign_peer_frame(
                &ControlPlaneRaftPeerFrameIdentity::new(
                    request.identity.cluster_name.clone(),
                    request.identity.target,
                    request.identity.source,
                ),
                request.operation,
                raw_response_frame,
            )
            .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)?
    } else {
        raw_response_frame
    };

    let checkpoint_before_response =
        matches!(request.kind, ControlPlaneRaftPeerFrameKind::Snapshot);
    if checkpoint_before_response {
        let Some(path) = durability.artifact_path else {
            return Err(ExperimentalRaftPeerRpcWorkerError::Checkpoint(
                ControlPlaneError::RpcRemote {
                    message:
                        "experimental OpenRaft snapshot peer RPC requires durable checkpoint path before response"
                            .to_string(),
                },
            ));
        };
        if let Err(error) = store_experimental_raft_durable_restart_artifact(
            runtime,
            authority,
            path,
            durability.checkpoint_lock,
        ) {
            return Err(ExperimentalRaftPeerRpcWorkerError::Checkpoint(error));
        }
    }

    ensure_experimental_raft_peer_not_durably_poisoned(durability.publication)?;
    Ok(response_frame)
}

fn ensure_experimental_raft_peer_not_durably_poisoned(
    publication: Option<&ExperimentalRaftDurabilityPublication>,
) -> Result<(), ExperimentalRaftPeerRpcWorkerError> {
    if publication.is_some_and(ExperimentalRaftDurabilityPublication::is_poisoned) {
        return Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(
            ControlPlaneError::RpcRemote {
                message: "experimental OpenRaft control-plane durable authority is poisoned; refusing peer RPC until restart".to_string(),
            },
        ));
    }
    Ok(())
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
        return Err(ControlPlaneError::RpcRemote {
            message: format!(
                "durable OpenRaft peer checkpoint scheduler observed poisoned WAL state: {reason}"
            ),
        });
    }
    let wal_metrics = wal_status.metrics();
    let successful_append_total = wal_metrics
        .append_total
        .saturating_sub(wal_metrics.append_error_total);
    let offsets = wal_status.offsets();
    let wal_suffix_bytes = offsets
        .clean_len()
        .checked_sub(offsets.base_offset())
        .ok_or(ControlPlaneError::RpcRemote {
            message: format!(
                "durable OpenRaft WAL clean offset {} precedes base offset {}",
                offsets.clean_len(),
                offsets.base_offset()
            ),
        })?;
    let observation = ExperimentalRaftPeerCheckpointObservation {
        wal_suffix_bytes,
        successful_append_total,
    };
    if tracker.observe(now, observation).is_none() {
        return Ok(false);
    }
    let path = durability
        .artifact_path
        .as_deref()
        .ok_or(ControlPlaneError::RpcRemote {
            message: "durable OpenRaft peer checkpoint scheduler requires an artifact path"
                .to_string(),
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
            if durability.publication.is_poisoned() {
                return;
            }
            if let Err(error) = checkpoint_experimental_raft_peer_wal_if_due(
                &runtime,
                &authority,
                &durability,
                &mut tracker,
                Instant::now(),
            ) {
                publish_experimental_raft_checkpoint_monitor_poison(&durability);
                eprintln!(
                    "experimental OpenRaft control-plane bounded peer WAL checkpoint failed; exiting to avoid serving after durability failure: {error}"
                );
                std::process::exit(1);
            }
            thread::sleep(tracker.policy.poll_interval);
        }
    })
}

fn publish_experimental_raft_checkpoint_monitor_poison(
    durability: &ExperimentalRaftPeerDurabilityContext,
) {
    durability.publication.publish_poison(|| {});
}

fn spawn_experimental_raft_peer_listener_loop(
    listener: ExperimentalRaftPeerListener,
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    local_node_id: ControlPlaneRaftNodeId,
    durability: ExperimentalRaftPeerDurabilityContext,
    active_workers: Arc<AtomicUsize>,
) -> thread::JoinHandle<()> {
    listener
        .listener
        .set_nonblocking(false)
        .unwrap_or_else(|error| {
            eprintln!(
                "control-plane OpenRaft peer socket failed to enter blocking accept mode: {error}"
            );
            std::process::exit(1);
        });
    thread::spawn(move || loop {
        match listener.listener.accept() {
            Ok((stream, _addr)) => {
                spawn_experimental_raft_peer_rpc_worker(
                    stream,
                    ExperimentalRaftPeerRpcWorkerContext {
                        runtime: runtime.clone(),
                        authority: Arc::clone(&authority),
                        local_node_id,
                        policy: Arc::clone(&listener.policy),
                        durability: Some(durability.clone()),
                        active_workers: Arc::clone(&active_workers),
                    },
                );
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                eprintln!("control-plane OpenRaft peer socket accept failed: {error}");
                std::process::exit(1);
            }
        }
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
    let artifact_existed = path.exists();
    let checkpoint =
        block_on_control_plane_raft(runtime, authority.capture_durable_restart_checkpoint())?;
    let committed_timestamp_high_water_ms =
        authority.persist_durable_restart_checkpoint(checkpoint, path)?;
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
    let listener = bind_control_plane_socket(Path::new(socket_path)).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(1);
    });
    let recovery_socket_path = control_plane_clock_recovery_socket_path(Path::new(socket_path));
    let recovery_listener = bind_control_plane_clock_recovery_socket(&recovery_socket_path)
        .unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });

    let runtime = Handle::current();
    let node_id: ControlPlaneRaftNodeId = config.control_plane_raft_node_id.unwrap_or(1);
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
    let durable_checkpoint_lock = Arc::new(Mutex::new(()));
    let durable_artifact_path = Arc::new(PathBuf::from(state_path));
    let authority_clock_checkpoint_binding =
        ControlPlaneAuthorityClockCheckpointBinding::for_raft(&cluster_name, node_id);
    let restart_clock_checkpoint = load_process_authority_clock_restart_checkpoint(
        &durable_artifact_path,
        authority_clock_checkpoint_binding,
    )
    .unwrap_or_else(|error| {
        eprintln!("failed to load experimental OpenRaft authority clock checkpoint: {error}");
        std::process::exit(1);
    });
    let durable_wal_path = durable_artifact_wal_path(&durable_artifact_path);
    let authority = block_on_control_plane_raft(&runtime, async {
        let authority = if let Some(policy) = raft_peer_policy.clone() {
            ControlPlaneRaftAuthority::new_experimental_unix_peer_durable_with_wal(
                cluster_name.clone(),
                node_id,
                Path::new(state_path),
                &durable_wal_path,
                policy,
                CONTROL_PLANE_RAFT_PEER_RPC_IO_TIMEOUT,
            )
            .await?
        } else {
            ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
                cluster_name.clone(),
                node_id,
                Path::new(state_path),
                &durable_wal_path,
            )
            .await?
        };
        Ok::<_, ControlPlaneError>(Arc::new(authority))
    })
    .unwrap_or_else(|error| {
        eprintln!("failed to initialize experimental OpenRaft control-plane: {error}");
        std::process::exit(1);
    });
    let raft_peer_listener =
        bind_experimental_raft_peer_listener_with_policy(config, raft_peer_policy.clone())
            .unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(1);
            });
    let active_raft_peer_rpc_workers = Arc::new(AtomicUsize::new(0));
    let durable_publication = ExperimentalRaftDurabilityPublication::new();
    let multi_node_raft_peer_mode = raft_peer_policy
        .as_ref()
        .is_some_and(|policy| policy.peers().len() > 1);
    let raft_peer_durability = ExperimentalRaftPeerDurabilityContext {
        artifact_path: Some(Arc::clone(&durable_artifact_path)),
        checkpoint_lock: Arc::clone(&durable_checkpoint_lock),
        publication: durable_publication.clone(),
    };
    let _raft_checkpoint_loop = spawn_experimental_raft_peer_checkpoint_loop(
        runtime.clone(),
        Arc::clone(&authority),
        raft_peer_durability.clone(),
        ExperimentalRaftPeerCheckpointPolicy::default(),
    );
    let _raft_peer_listener_loop = raft_peer_listener.map(|listener| {
        spawn_experimental_raft_peer_listener_loop(
            listener,
            runtime.clone(),
            Arc::clone(&authority),
            node_id,
            raft_peer_durability,
            Arc::clone(&active_raft_peer_rpc_workers),
        )
    });
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
        } else {
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
        durable_poison: Arc::new(Mutex::new(None)),
        durable_publication: durable_publication.clone(),
        #[cfg(test)]
        after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
    };
    let should_bootstrap_control_plane_state =
        if experimental_raft_startup_bootstrap_requires_local_serving(raft_peer_policy.as_ref()) {
            block_on_control_plane_raft(&runtime, async {
                experimental_raft_local_authority_serving_within(&authority, Duration::from_secs(1))
                    .await
        })
        .unwrap_or_else(|error| {
            eprintln!(
                "failed to determine experimental OpenRaft control-plane bootstrap leadership: {error}"
            );
            std::process::exit(1);
        })
        } else {
            true
        };
    if should_bootstrap_control_plane_state {
        bootstrap_empty_experimental_raft_control_plane(&mut control_plane, config).unwrap_or_else(
            |error| {
                eprintln!("failed to bootstrap experimental OpenRaft control-plane state: {error}");
                std::process::exit(1);
            },
        );
    }
    let raft_authority = Arc::clone(&authority);
    let mut authority = control_plane;
    let authority_clock_checkpoint_target = Arc::new(AuthorityClockCheckpointTarget {
        path: durable_artifact_path.as_ref().clone(),
        binding: authority_clock_checkpoint_binding,
    });
    let active_rpc_workers = Arc::new(AtomicUsize::new(0));
    let active_recovery_rpc_workers = Arc::new(AtomicUsize::new(0));
    let auth_verifier = build_control_plane_unix_auth_verifier(config)
        .unwrap_or_else(|error| {
            eprintln!("failed to configure control-plane auth verifier: {error}");
            std::process::exit(1);
        })
        .map(Arc::new);
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
    if let Some(auth_verifier) = &auth_verifier {
        process_info!(
            "{}",
            format_control_plane_unix_auth_diagnostics(auth_verifier)
        );
    }

    let rpc_runtime = runtime.clone();
    let rpc_raft_authority = Arc::clone(&raft_authority);
    let raft_authority_admission: ControlPlaneRaftRpcAuthorityAdmission =
        Arc::new(move |request| {
            if !request.requires_raft_authority_confirmation() {
                return Ok(());
            }
            block_on_control_plane_raft(
                &rpc_runtime,
                rpc_raft_authority.confirmed_linearized_authority_status(),
            )
            .map(|_| ())
        });

    let _rpc_listener_loop = spawn_cloned_control_plane_rpc_listener_loop(
        listener,
        authority.clone(),
        Some(Arc::clone(&authority_clock)),
        Some(Arc::clone(&authority_clock_checkpoint_target)),
        ControlPlaneRpcWorkerPolicy {
            gate_request_time_with_authority_clock: false,
            active_rpc_workers: Arc::clone(&active_rpc_workers),
            worker_limit: CONTROL_PLANE_RPC_WORKER_LIMIT,
            endpoint: ControlPlaneRpcEndpoint::Ordinary,
            auth_verifier: auth_verifier.clone(),
            raft_authority_admission: Some(Arc::clone(&raft_authority_admission)),
            durable_response_publication: Some(durable_publication.clone()),
        },
    );
    let _clock_recovery_listener_loop = spawn_cloned_control_plane_rpc_listener_loop(
        recovery_listener,
        authority.clone(),
        Some(Arc::clone(&authority_clock)),
        Some(Arc::clone(&authority_clock_checkpoint_target)),
        ControlPlaneRpcWorkerPolicy {
            gate_request_time_with_authority_clock: false,
            active_rpc_workers: active_recovery_rpc_workers,
            worker_limit: CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
            endpoint: ControlPlaneRpcEndpoint::ClockRecovery,
            auth_verifier: auth_verifier.clone(),
            raft_authority_admission: Some(raft_authority_admission),
            durable_response_publication: Some(durable_publication),
        },
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
            if multi_node_raft_peer_mode {
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
            invalidate_blocked_authority_clock_checkpoint(
                &authority_clock,
                Some(&authority_clock_checkpoint_target),
            )
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
) -> Result<(), String> {
    if experimental_raft_control_plane_has_bootstrap_state(authority)? {
        return Ok(());
    }
    if config.storage_node_sockets.is_empty() {
        return Ok(());
    }

    let nodes: Vec<(NodeId, String)> = config
        .storage_node_sockets
        .iter()
        .map(|entry| (NodeId::new(entry.node_id), entry.socket_path.clone()))
        .collect();
    let pg_ids: Vec<storage::PgId> = config
        .storage_pg_ids
        .iter()
        .copied()
        .map(storage::PgId::new)
        .collect();
    let node_count = nodes.len();
    match authority
        .submit_raft_command(ControlPlaneCommand::BootstrapInitialClusterMap { nodes, pg_ids })
    {
        Ok(_) => {}
        Err(error)
            if experimental_raft_bootstrap_submit_error_was_concurrent_success(
                authority, &error,
            )? => {}
        Err(error) => return Err(error.to_string()),
    }
    let epoch = authority
        .current_snapshot()
        .map_err(|error| error.to_string())?
        .cluster_epoch();
    process_info!(
        "experimental OpenRaft control-plane bootstrapped {} nodes and {} PG acting sets at epoch {}",
        node_count,
        config.storage_pg_ids.len(),
        epoch
    );
    Ok(())
}

fn experimental_raft_control_plane_has_bootstrap_state(
    authority: &ExperimentalRaftControlPlane,
) -> Result<bool, String> {
    Ok(authority
        .current_snapshot()
        .map_err(|error| error.to_string())?
        .nodes()
        .next()
        .is_some())
}

fn experimental_raft_bootstrap_submit_error_was_concurrent_success(
    authority: &ExperimentalRaftControlPlane,
    error: &ControlPlaneError,
) -> Result<bool, String> {
    Ok(
        experimental_raft_bootstrap_submit_error_can_be_concurrent_success(error)
            && experimental_raft_control_plane_has_bootstrap_state(authority)?,
    )
}

fn experimental_raft_bootstrap_submit_error_can_be_concurrent_success(
    error: &ControlPlaneError,
) -> bool {
    matches!(error, ControlPlaneError::BootstrapRequiresEmptyState)
        || experimental_raft_error_is_non_local_leader(error)
}

fn bootstrap_empty_control_plane(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    config: &ServerConfig,
) -> Result<(), String> {
    if authority.snapshot().nodes().next().is_some() {
        return Ok(());
    }
    if config.storage_node_sockets.is_empty() {
        return Ok(());
    }

    let nodes: Vec<(NodeId, String)> = config
        .storage_node_sockets
        .iter()
        .map(|entry| (NodeId::new(entry.node_id), entry.socket_path.clone()))
        .collect();
    let pg_ids: Vec<storage::PgId> = config
        .storage_pg_ids
        .iter()
        .copied()
        .map(storage::PgId::new)
        .collect();
    let node_count = nodes.len();
    authority
        .bootstrap_initial_cluster_map(nodes, pg_ids)
        .map_err(|error| error.to_string())?;
    process_info!(
        "control-plane bootstrapped {} nodes and {} PG acting sets at epoch {}",
        node_count,
        config.storage_pg_ids.len(),
        authority.snapshot().cluster_epoch()
    );
    Ok(())
}

fn spawn_control_plane_rpc_listener_loop<T>(
    listener: UnixListener,
    authority: Arc<Mutex<T>>,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    authority_clock_checkpoint_target: Option<Arc<AuthorityClockCheckpointTarget>>,
    worker_policy: ControlPlaneRpcWorkerPolicy,
) -> thread::JoinHandle<()>
where
    T: ControlPlaneAdmin
        + ControlPlaneHeartbeatRuntimeMapSource
        + ControlPlaneRuntimeMapSource
        + Send
        + 'static,
{
    listener.set_nonblocking(false).unwrap_or_else(|error| {
        eprintln!("control-plane socket failed to enter blocking accept mode: {error}");
        std::process::exit(1);
    });
    thread::spawn(move || loop {
        match listener.accept() {
            Ok((stream, _addr)) => spawn_control_plane_rpc_worker(
                stream,
                ControlPlaneRpcWorkerAuthority::Shared(Arc::clone(&authority)),
                authority_clock.clone(),
                authority_clock_checkpoint_target.clone(),
                worker_policy.clone(),
            ),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                eprintln!("control-plane socket accept failed: {error}");
                std::process::exit(1);
            }
        }
    })
}

fn spawn_cloned_control_plane_rpc_listener_loop<T>(
    listener: UnixListener,
    authority: T,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    authority_clock_checkpoint_target: Option<Arc<AuthorityClockCheckpointTarget>>,
    worker_policy: ControlPlaneRpcWorkerPolicy,
) -> thread::JoinHandle<()>
where
    T: Clone
        + ControlPlaneAdmin
        + ControlPlaneHeartbeatRuntimeMapSource
        + ControlPlaneRuntimeMapSource
        + Send
        + 'static,
{
    listener.set_nonblocking(false).unwrap_or_else(|error| {
        eprintln!("control-plane socket failed to enter blocking accept mode: {error}");
        std::process::exit(1);
    });
    thread::spawn(move || loop {
        match listener.accept() {
            Ok((stream, _addr)) => spawn_control_plane_rpc_worker(
                stream,
                ControlPlaneRpcWorkerAuthority::PerWorker(authority.clone()),
                authority_clock.clone(),
                authority_clock_checkpoint_target.clone(),
                worker_policy.clone(),
            ),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                eprintln!("control-plane socket accept failed: {error}");
                std::process::exit(1);
            }
        }
    })
}

enum ControlPlaneRpcWorkerAuthority<T> {
    Shared(Arc<Mutex<T>>),
    PerWorker(T),
}

impl<T> ControlPlaneRpcWorkerAuthority<T> {
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

type ControlPlaneRaftRpcAuthorityAdmission = Arc<
    dyn Fn(&storage::control_plane::VerifiedControlPlaneRpcRequest) -> Result<(), ControlPlaneError>
        + Send
        + Sync,
>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ControlPlaneRpcEndpoint {
    Ordinary,
    ClockRecovery,
}

impl ControlPlaneRpcEndpoint {
    fn accepts(self, request: &storage::control_plane::VerifiedControlPlaneRpcRequest) -> bool {
        match self {
            Self::Ordinary => !request.is_authority_clock_admin(),
            Self::ClockRecovery => request.is_authority_clock_admin(),
        }
    }
}

#[derive(Clone)]
struct ControlPlaneRpcWorkerPolicy {
    gate_request_time_with_authority_clock: bool,
    active_rpc_workers: Arc<AtomicUsize>,
    worker_limit: usize,
    endpoint: ControlPlaneRpcEndpoint,
    auth_verifier: Option<Arc<ControlPlaneUnixAuthVerifier>>,
    raft_authority_admission: Option<ControlPlaneRaftRpcAuthorityAdmission>,
    durable_response_publication: Option<ExperimentalRaftDurabilityPublication>,
}

enum ControlPlaneRpcAdmissionFailure {
    Unauthenticated(Box<ControlPlaneError>),
    Authenticated {
        request: Box<storage::control_plane::VerifiedControlPlaneRpcRequest>,
        error: Box<ControlPlaneError>,
    },
}

fn authenticate_and_admit_control_plane_rpc(
    request: storage::control_plane::ControlPlaneRpcRequest,
    worker_policy: &ControlPlaneRpcWorkerPolicy,
    authority_now_ms: u64,
) -> Result<storage::control_plane::VerifiedControlPlaneRpcRequest, ControlPlaneRpcAdmissionFailure>
{
    let request = verify_control_plane_unix_request(
        request,
        worker_policy.auth_verifier.as_deref(),
        authority_now_ms,
    )
    .map_err(|error| ControlPlaneRpcAdmissionFailure::Unauthenticated(Box::new(error)))?;
    if !worker_policy.endpoint.accepts(&request) {
        let error = ControlPlaneError::RpcProtocol {
            message: match worker_policy.endpoint {
                ControlPlaneRpcEndpoint::Ordinary => {
                    "authority-clock administration requires the dedicated recovery endpoint"
                        .to_owned()
                }
                ControlPlaneRpcEndpoint::ClockRecovery => {
                    "dedicated authority-clock recovery endpoint rejects ordinary control-plane RPCs"
                        .to_owned()
                }
            },
        };
        return Err(ControlPlaneRpcAdmissionFailure::Authenticated {
            request: Box::new(request),
            error: Box::new(error),
        });
    }
    if let Some(admission) = &worker_policy.raft_authority_admission {
        if let Err(error) = admission(&request) {
            return Err(ControlPlaneRpcAdmissionFailure::Authenticated {
                request: Box::new(request),
                error: Box::new(error),
            });
        }
    }
    Ok(request)
}

fn spawn_control_plane_rpc_worker<T>(
    mut stream: UnixStream,
    mut authority: ControlPlaneRpcWorkerAuthority<T>,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    authority_clock_checkpoint_target: Option<Arc<AuthorityClockCheckpointTarget>>,
    worker_policy: ControlPlaneRpcWorkerPolicy,
) where
    T: ControlPlaneAdmin
        + ControlPlaneHeartbeatRuntimeMapSource
        + ControlPlaneRuntimeMapSource
        + Send
        + 'static,
{
    if !reserve_control_plane_rpc_worker(
        &worker_policy.active_rpc_workers,
        worker_policy.worker_limit,
    ) {
        eprintln!("control-plane RPC rejected: worker limit reached");
        return;
    }
    thread::spawn(move || {
        let _guard = ControlPlaneRpcWorkerGuard {
            active_rpc_workers: Arc::clone(&worker_policy.active_rpc_workers),
        };
        if let Err(error) = stream.set_nonblocking(false) {
            eprintln!("control-plane RPC failed to set blocking mode: {error}");
            return;
        }
        if let Err(error) = stream.set_read_timeout(Some(CONTROL_PLANE_RPC_IO_TIMEOUT)) {
            eprintln!("control-plane RPC failed to set read timeout: {error}");
            return;
        }
        if let Err(error) = stream.set_write_timeout(Some(CONTROL_PLANE_RPC_IO_TIMEOUT)) {
            eprintln!("control-plane RPC failed to set write timeout: {error}");
            return;
        }
        let request = match read_control_plane_unix_request(&mut stream) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("control-plane RPC request read failed: {error}");
                return;
            }
        };
        let metrics_kind = request.metrics_kind();
        let request = match authenticate_and_admit_control_plane_rpc(
            request,
            &worker_policy,
            storage::clock::current_time_millis(),
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
                    storage::clock::current_time_millis(),
                );
                write_control_plane_rpc_admission_response(&mut stream, metrics_kind, response);
                return;
            }
        };
        let response = (|| {
            if request.is_authority_clock_admin() {
                let _operation_timer =
                    observability::control_plane_rpc_operation_timer(metrics_kind);
                let authority_clock =
                    authority_clock
                        .as_ref()
                        .ok_or_else(|| ControlPlaneError::RpcProtocol {
                            message:
                                "authority-clock administration requires a process-local clock gate"
                                    .to_owned(),
                        })?;
                // OpenRaft/state-machine reads may wait for convergence. Keep
                // them outside the process-local clock gate so status and
                // recovery confirmation remain observable while they wait.
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
                            persist_established_authority_clock_checkpoint(
                                context,
                                authority_clock,
                                authority_clock_checkpoint_target.as_deref(),
                            )
                        },
                        || Ok(storage::clock::current_time_millis()),
                    );
                invalidate_blocked_authority_clock_checkpoint(
                    &authority_clock,
                    authority_clock_checkpoint_target.as_deref(),
                )?;
                response
            } else if request.is_refresh_node_heartbeat() {
                let prepared = {
                    let _operation_timer =
                        observability::control_plane_rpc_operation_timer(metrics_kind);
                    authority.with_mut(metrics_kind, |authority| {
                        let (now_ms, lease_horizon_authority) = match &authority_clock {
                            Some(authority_clock)
                                if worker_policy.gate_request_time_with_authority_clock =>
                            {
                                let mut authority_clock = authority_clock
                                    .lock()
                                    .expect("control-plane authority clock mutex poisoned");
                                let now_ms = authority_clock.effective_process_now_ms();
                                invalidate_blocked_authority_clock_checkpoint(
                                    &authority_clock,
                                    authority_clock_checkpoint_target.as_deref(),
                                )?;
                                let now_ms = now_ms?;
                                let lease_horizon_authority =
                                    authority_clock.lease_horizon_authority_binding(None)?;
                                (now_ms, Some(lease_horizon_authority))
                            }
                            _ => (storage::clock::current_time_millis(), None),
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
                                authority,
                                request,
                                now_ms,
                            ),
                        }
                    })
                };
                prepared.and_then(|prepared| {
                    finish_control_plane_heartbeat_response(prepared, || {
                        Ok(storage::clock::current_time_millis())
                    })
                })
            } else {
                let _operation_timer =
                    observability::control_plane_rpc_operation_timer(metrics_kind);
                authority.with_mut(metrics_kind, |authority| {
                    let now_ms = match &authority_clock {
                        Some(authority_clock)
                            if worker_policy.gate_request_time_with_authority_clock =>
                        {
                            let mut authority_clock = authority_clock
                                .lock()
                                .expect("control-plane authority clock mutex poisoned");
                            let now_ms = authority_clock.effective_process_now_ms();
                            invalidate_blocked_authority_clock_checkpoint(
                                &authority_clock,
                                authority_clock_checkpoint_target.as_deref(),
                            )?;
                            now_ms?
                        }
                        _ => storage::clock::current_time_millis(),
                    };
                    build_control_plane_unix_response_from_verified(
                        authority,
                        request,
                        now_ms,
                        || Ok(storage::clock::current_time_millis()),
                    )
                })
            }
        })();
        if let Some(authority_clock) = &authority_clock {
            let authority_clock = authority_clock
                .lock()
                .expect("control-plane authority clock mutex poisoned");
            if let Err(error) = invalidate_blocked_authority_clock_checkpoint(
                &authority_clock,
                authority_clock_checkpoint_target.as_deref(),
            ) {
                eprintln!(
                    "failed to invalidate blocked control-plane authority clock checkpoint: {error}"
                );
                std::process::exit(1);
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
        let response_result = match &worker_policy.durable_response_publication {
            Some(publication) => {
                publication.publish(|| write_control_plane_unix_response(&mut stream, response))
            }
            None => write_control_plane_unix_response(&mut stream, response),
        };
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
        }
    });
}

fn reserve_control_plane_rpc_worker(active_rpc_workers: &AtomicUsize, worker_limit: usize) -> bool {
    active_rpc_workers
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
            (active < worker_limit).then_some(active + 1)
        })
        .is_ok()
}

fn write_control_plane_rpc_admission_response(
    stream: &mut UnixStream,
    metrics_kind: observability::ControlPlaneRpcMetricKind,
    response: Result<storage::control_plane::ControlPlaneRpcResponse, ControlPlaneError>,
) {
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            eprintln!("control-plane RPC admission response build failed: {error}");
            return;
        }
    };
    let response_write_started = Instant::now();
    let response_result = write_control_plane_unix_response(stream, response);
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

fn control_plane_rpc_response_write_error_kind(
    error: &ControlPlaneError,
) -> observability::ControlPlaneRpcResponseWriteErrorKind {
    let ControlPlaneError::Io { source, .. } = error else {
        return observability::ControlPlaneRpcResponseWriteErrorKind::Other;
    };
    match source.kind() {
        io::ErrorKind::BrokenPipe => {
            observability::ControlPlaneRpcResponseWriteErrorKind::BrokenPipe
        }
        io::ErrorKind::ConnectionReset => {
            observability::ControlPlaneRpcResponseWriteErrorKind::ConnectionReset
        }
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => {
            observability::ControlPlaneRpcResponseWriteErrorKind::Timeout
        }
        _ => observability::ControlPlaneRpcResponseWriteErrorKind::Other,
    }
}

#[derive(Debug)]
struct AuthorityClockCheckpointTarget {
    path: PathBuf,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
}

fn persist_established_authority_clock_checkpoint(
    context: ControlPlaneAuthorityClockContext,
    authority_clock: &mut ControlPlaneAuthorityClock,
    checkpoint_target: Option<&AuthorityClockCheckpointTarget>,
) -> Result<(), ControlPlaneError> {
    if !authority_clock.status(context).established() {
        return Ok(());
    }
    let checkpoint_target = checkpoint_target.ok_or_else(|| ControlPlaneError::RpcProtocol {
        message: "authority-clock administration requires a durable checkpoint path".to_owned(),
    })?;
    let persistence_started = Instant::now();
    let mut invalidate_elapsed = Duration::ZERO;
    let mut store_elapsed = Duration::ZERO;
    let persistence_result = (|| {
        let invalidate_started = Instant::now();
        invalidate_authority_clock_restart_checkpoint(&checkpoint_target.path)?;
        invalidate_elapsed = invalidate_started.elapsed();
        let store_started = Instant::now();
        store_validated_authority_clock_restart_checkpoint(
            &checkpoint_target.path,
            checkpoint_target.binding,
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

fn invalidate_blocked_authority_clock_checkpoint(
    authority_clock: &ControlPlaneAuthorityClock,
    checkpoint_target: Option<&AuthorityClockCheckpointTarget>,
) -> Result<(), ControlPlaneError> {
    if authority_clock.is_established() {
        return Ok(());
    }
    let checkpoint_target = checkpoint_target.ok_or_else(|| ControlPlaneError::RpcProtocol {
        message: "blocked authority clock requires a durable checkpoint path".to_owned(),
    })?;
    invalidate_authority_clock_restart_checkpoint(&checkpoint_target.path)
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

struct ControlPlaneRpcWorkerGuard {
    active_rpc_workers: Arc<AtomicUsize>,
}

impl Drop for ControlPlaneRpcWorkerGuard {
    fn drop(&mut self) {
        self.active_rpc_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

enum StorageNodeControlPlaneClient {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

enum FrontendControlPlaneClient {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

enum AdminControlPlaneClient {
    Plain(UnixControlPlaneClient),
    Authenticated(AuthenticatedUnixControlPlaneClient),
}

impl ControlPlaneHeartbeatRuntimeMapSource for StorageNodeControlPlaneClient {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: storage::control_plane::NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.refresh_node_heartbeat(heartbeat, authority_now_ms),
            Self::Authenticated(client) => {
                client.refresh_node_heartbeat(heartbeat, authority_now_ms)
            }
        }
    }
}

impl ControlPlaneRuntimeMapSource for FrontendControlPlaneClient {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.runtime_map_snapshot(authority_now_ms),
            Self::Authenticated(client) => client.runtime_map_snapshot(authority_now_ms),
        }
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<storage::control_plane::ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.runtime_map_status(authority_now_ms),
            Self::Authenticated(client) => client.runtime_map_status(authority_now_ms),
        }
    }

    fn pending_metadata_command_recoveries(
        &self,
        authority_now_ms: u64,
    ) -> Result<storage::control_plane::PendingMetadataCommandRecoveryListing, ControlPlaneError>
    {
        match self {
            Self::Plain(client) => client.pending_metadata_command_recoveries(),
            Self::Authenticated(client) => {
                client.pending_metadata_command_recoveries(authority_now_ms)
            }
        }
    }

    fn pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.pg_runtime_map_snapshot(pg_id, authority_now_ms),
            Self::Authenticated(client) => client.pg_runtime_map_snapshot(pg_id, authority_now_ms),
        }
    }
}

impl FrontendControlPlaneClient {
    fn runtime_map_diagnostics(
        &self,
    ) -> Result<storage::control_plane::ControlPlaneRuntimeMapDiagnostics, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.runtime_map_diagnostics(),
            Self::Authenticated(client) => {
                client.runtime_map_diagnostics(storage::clock::current_time_millis())
            }
        }
    }

    fn runtime_map_status_with_check_applied_timeout(
        &self,
    ) -> Result<storage::control_plane::ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.runtime_map_status_with_check_applied_timeout(),
            Self::Authenticated(client) => client.runtime_map_status_with_check_applied_timeout(
                storage::clock::current_time_millis(),
            ),
        }
    }
}

impl AdminControlPlaneClient {
    fn authority_clock_status(
        &self,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        match self {
            Self::Plain(_) => Err(ControlPlaneError::RpcProtocol {
                message: "authority-clock status requires authenticated admin credentials"
                    .to_owned(),
            }),
            Self::Authenticated(client) => {
                client.authority_clock_status(storage::clock::current_time_millis())
            }
        }
    }

    fn reestablish_authority_clock(
        &self,
    ) -> Result<ControlPlaneAuthorityClockStatus, ControlPlaneError> {
        match self {
            Self::Plain(_) => Err(ControlPlaneError::RpcProtocol {
                message:
                    "authority-clock re-establishment requires authenticated admin credentials"
                        .to_owned(),
            }),
            Self::Authenticated(client) => {
                client.reestablish_authority_clock(storage::clock::current_time_millis())
            }
        }
    }

    fn set_pg_acting_set_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.set_pg_acting_set_checked(pg_id, acting_set),
            Self::Authenticated(client) => client.set_pg_acting_set_checked(
                pg_id,
                acting_set,
                storage::clock::current_time_millis(),
            ),
        }
    }

    fn transfer_raft_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        match self {
            Self::Plain(client) => client.transfer_raft_leadership_to(node_id),
            Self::Authenticated(client) => {
                client.transfer_raft_leadership_to(node_id, storage::clock::current_time_millis())
            }
        }
    }

    fn trigger_raft_snapshot_and_purge(&self) -> Result<Option<u64>, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.trigger_raft_snapshot_and_purge(),
            Self::Authenticated(client) => {
                client.trigger_raft_snapshot_and_purge(storage::clock::current_time_millis())
            }
        }
    }

    fn trigger_raft_election(&self) -> Result<(), ControlPlaneError> {
        match self {
            Self::Plain(client) => client.trigger_raft_election(),
            Self::Authenticated(client) => {
                client.trigger_raft_election(storage::clock::current_time_millis())
            }
        }
    }

    fn fence_pg_for_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.fence_pg_for_metadata_transfer_runtime_map_checked(pg_id),
            Self::Authenticated(client) => client
                .fence_pg_for_metadata_transfer_runtime_map_checked(
                    pg_id,
                    storage::clock::current_time_millis(),
                ),
        }
    }

    fn fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
        &self,
        pg_id: PgId,
    ) -> Result<storage::control_plane::FencedPgMetadataTransferRuntimeMap, ControlPlaneError> {
        match self {
            Self::Plain(client) => {
                client.fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(pg_id)
            }
            Self::Authenticated(client) => client
                .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
                    pg_id,
                    storage::clock::current_time_millis(),
                ),
        }
    }

    fn set_pg_acting_set_with_metadata_transfer_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        min_cluster_epoch: ClusterEpoch,
    ) -> Result<ClusterEpoch, ControlPlaneError> {
        match self {
            Self::Plain(client) => client.set_pg_acting_set_with_metadata_transfer_checked(
                pg_id,
                acting_set,
                transfer,
                min_cluster_epoch,
            ),
            Self::Authenticated(client) => client.set_pg_acting_set_with_metadata_transfer_checked(
                pg_id,
                acting_set,
                transfer,
                min_cluster_epoch,
                storage::clock::current_time_millis(),
            ),
        }
    }

    fn set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
        &self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        min_cluster_epoch: ClusterEpoch,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        match self {
            Self::Plain(client) => client
                .set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    min_cluster_epoch,
                ),
            Self::Authenticated(client) => client
                .set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
                    pg_id,
                    acting_set,
                    transfer,
                    min_cluster_epoch,
                    storage::clock::current_time_millis(),
                ),
        }
    }
}

fn configured_storage_node_auth_credential(
    configured: &ConfiguredControlPlaneStorageAuthCredential,
) -> Result<ControlPlaneStorageNodeAuthCredential, String> {
    ControlPlaneStorageNodeAuthCredential::new(ControlPlaneStorageNodeAuthCredentialInput {
        node_id: NodeId::new(configured.node_id),
        credential_id: configured.credential_id.clone(),
        credential_version: configured.credential_version,
        secret: configured.secret.as_str().as_bytes().to_vec(),
    })
    .map_err(|error| {
        format!(
            "invalid ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS credential for node {}: {error}",
            configured.node_id
        )
    })
}

fn configured_frontend_auth_credential(
    configured: &ConfiguredControlPlaneFrontendAuthCredential,
) -> Result<ControlPlaneFrontendAuthCredential, String> {
    ControlPlaneFrontendAuthCredential::new(ControlPlaneFrontendAuthCredentialInput {
        instance_id: configured.instance_id.clone(),
        credential_id: configured.credential_id.clone(),
        credential_version: configured.credential_version,
        secret: configured.secret.as_str().as_bytes().to_vec(),
    })
    .map_err(|error| {
        format!(
            "invalid ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS credential for instance {}: {error}",
            configured.instance_id
        )
    })
}

fn configured_admin_auth_credential(
    configured: &ConfiguredControlPlaneAdminAuthCredential,
) -> Result<ControlPlaneAdminAuthCredential, String> {
    ControlPlaneAdminAuthCredential::new(ControlPlaneAdminAuthCredentialInput {
        instance_id: configured.instance_id.clone(),
        credential_id: configured.credential_id.clone(),
        credential_version: configured.credential_version,
        secret: configured.secret.as_str().as_bytes().to_vec(),
    })
    .map_err(|error| {
        format!(
            "invalid ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS credential for instance {}: {error}",
            configured.instance_id
        )
    })
}

fn build_control_plane_unix_auth_verifier(
    config: &ServerConfig,
) -> Result<Option<ControlPlaneUnixAuthVerifier>, String> {
    if config.control_plane_storage_auth_credentials.is_empty()
        && config.control_plane_frontend_auth_credentials.is_empty()
        && config.control_plane_admin_auth_credentials.is_empty()
    {
        return Ok(None);
    }
    let cluster_id = config
        .control_plane_auth_cluster_id
        .as_deref()
        .expect("control-plane Unix auth credentials require control-plane auth cluster id");
    let storage_credentials = config
        .control_plane_storage_auth_credentials
        .iter()
        .map(configured_storage_node_auth_credential)
        .collect::<Result<Vec<_>, _>>()?;
    let frontend_credentials = config
        .control_plane_frontend_auth_credentials
        .iter()
        .map(configured_frontend_auth_credential)
        .collect::<Result<Vec<_>, _>>()?;
    let admin_credentials = config
        .control_plane_admin_auth_credentials
        .iter()
        .map(configured_admin_auth_credential)
        .collect::<Result<Vec<_>, _>>()?;
    if admin_credentials.is_empty() {
        return Err(
            "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS is required when any Unix control-plane auth credentials are configured for the control-plane verifier"
                .to_string(),
        );
    }
    if let Some(instance_id) = config.control_plane_admin_auth_instance_id.as_deref() {
        if !config
            .control_plane_admin_auth_credentials
            .iter()
            .any(|credential| credential.instance_id == instance_id)
        {
            return Err(format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS must include local admin instance id {instance_id}"
            ));
        }
    }
    let verifier = if storage_credentials.is_empty() {
        ControlPlaneUnixAuthVerifier::new_empty(cluster_id)
    } else {
        ControlPlaneUnixAuthVerifier::new(cluster_id, storage_credentials)
    }
    .map_err(|error| format!("invalid control-plane Unix auth credential verifier: {error}"))?;
    verifier
        .with_frontend_credentials(frontend_credentials)
        .map_err(|error| {
            format!("invalid ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS verifier: {error}")
        })?
        .with_admin_credentials(admin_credentials)
        .map(Some)
        .map_err(|error| {
            format!("invalid ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS verifier: {error}")
        })
}

fn build_frontend_control_plane_client(
    config: &ServerConfig,
    control_plane_socket_path: &str,
) -> Result<FrontendControlPlaneClient, String> {
    let client = build_configured_unix_control_plane_client(config, control_plane_socket_path)?;
    if config.control_plane_frontend_auth_credentials.is_empty() {
        return Ok(FrontendControlPlaneClient::Plain(client));
    }
    let cluster_id = config
        .control_plane_auth_cluster_id
        .as_deref()
        .expect("frontend auth credentials require control-plane auth cluster id");
    let instance_id = config
        .control_plane_frontend_auth_instance_id
        .as_deref()
        .expect("frontend auth credentials require local frontend instance id");
    let configured = latest_frontend_auth_credential_for_instance(
        &config.control_plane_frontend_auth_credentials,
        instance_id,
    )?;
    build_authenticated_frontend_control_plane_client_with_inner(
        client,
        cluster_id,
        instance_id,
        configured,
    )
}

fn build_configured_unix_control_plane_client(
    config: &ServerConfig,
    primary_socket_path: &str,
) -> Result<UnixControlPlaneClient, String> {
    let socket_paths = if config.control_plane_client_socket_paths.is_empty() {
        vec![PathBuf::from(primary_socket_path)]
    } else {
        config
            .control_plane_client_socket_paths
            .iter()
            .map(PathBuf::from)
            .collect()
    };
    UnixControlPlaneClient::with_socket_paths(socket_paths)
        .map_err(|error| format!("invalid control-plane client socket paths: {error}"))
}

fn build_frontend_control_plane_client_from_runtime_map_auth_env(
    control_plane_socket_path: &Path,
) -> Result<FrontendControlPlaneClient, String> {
    let client = build_command_unix_control_plane_client(control_plane_socket_path)?;
    let auth_config = ConfiguredControlPlaneFrontendRuntimeMapAuth::from_env()?;
    match auth_config {
        Some(auth_config) => {
            let configured = latest_frontend_auth_credential_for_instance(
                &auth_config.credentials,
                &auth_config.instance_id,
            )
            .expect("auth-only frontend config validates local instance credential");
            build_authenticated_frontend_control_plane_client_with_inner(
                client,
                &auth_config.cluster_id,
                &auth_config.instance_id,
                configured,
            )
        }
        None => Ok(FrontendControlPlaneClient::Plain(client)),
    }
}

fn build_command_unix_control_plane_client(
    primary_socket_path: &Path,
) -> Result<UnixControlPlaneClient, String> {
    let configured = std::env::var("ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS").ok();
    let Some(configured) = configured else {
        return Ok(UnixControlPlaneClient::new(primary_socket_path));
    };
    let primary_socket_path = primary_socket_path.to_str().ok_or_else(|| {
        "control-plane command socket path must be UTF-8 when ARGMIN_CONTROL_PLANE_CLIENT_SOCKET_PATHS is set"
            .to_owned()
    })?;
    let socket_paths = config::parse_control_plane_client_socket_paths(
        Some(configured),
        Some(primary_socket_path),
    )?;
    UnixControlPlaneClient::with_socket_paths(socket_paths.into_iter().map(PathBuf::from))
        .map_err(|error| format!("invalid control-plane client socket paths: {error}"))
}

fn build_admin_control_plane_client_from_command_auth_env(
    control_plane_socket_path: &Path,
) -> Result<AdminControlPlaneClient, String> {
    let client = build_command_unix_control_plane_client(control_plane_socket_path)?;
    build_admin_control_plane_client_with_command_auth_env(client)
}

fn build_admin_clock_recovery_client_from_command_auth_env(
    control_plane_socket_path: &Path,
) -> Result<AdminControlPlaneClient, String> {
    let client = build_command_unix_control_plane_client(control_plane_socket_path)?;
    let client = UnixControlPlaneClient::with_socket_paths(
        client
            .socket_paths()
            .iter()
            .map(|path| control_plane_clock_recovery_socket_path(path)),
    )
    .map_err(|error| format!("invalid control-plane clock recovery socket paths: {error}"))?;
    build_admin_control_plane_client_with_command_auth_env(client)
}

fn build_admin_control_plane_client_with_command_auth_env(
    client: UnixControlPlaneClient,
) -> Result<AdminControlPlaneClient, String> {
    match ConfiguredControlPlaneAdminCommandAuth::from_env()? {
        Some(auth_config) => {
            let configured = latest_admin_auth_credential_for_instance(
                &auth_config.credentials,
                &auth_config.instance_id,
            )
            .expect("auth-only admin config validates local instance credential");
            let credential = configured_admin_auth_credential(configured)?
                .scoped_for_cluster(&auth_config.cluster_id)
                .map_err(|error| {
                    format!(
                        "invalid ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS scoped credential for instance {}: {error}",
                        auth_config.instance_id
                    )
                })?;
            Ok(AdminControlPlaneClient::Authenticated(
                AuthenticatedUnixControlPlaneClient::new(client, credential),
            ))
        }
        None => Ok(AdminControlPlaneClient::Plain(client)),
    }
}

fn build_authenticated_frontend_control_plane_client_with_inner(
    client: UnixControlPlaneClient,
    cluster_id: &str,
    instance_id: &str,
    configured: &ConfiguredControlPlaneFrontendAuthCredential,
) -> Result<FrontendControlPlaneClient, String> {
    let credential = configured_frontend_auth_credential(configured)?
        .scoped_for_cluster(cluster_id)
        .map_err(|error| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS scoped credential for instance {instance_id}: {error}"
            )
        })?;
    Ok(FrontendControlPlaneClient::Authenticated(
        AuthenticatedUnixControlPlaneClient::new(client, credential),
    ))
}

fn build_storage_node_control_plane_client(
    config: &ServerConfig,
    control_plane_socket_path: &str,
    node_id: NodeId,
    node_incarnation: u64,
) -> Result<StorageNodeControlPlaneClient, String> {
    let client = build_configured_unix_control_plane_client(config, control_plane_socket_path)?;
    if config.control_plane_storage_auth_credentials.is_empty() {
        return Ok(StorageNodeControlPlaneClient::Plain(client));
    }
    let cluster_id = config
        .control_plane_auth_cluster_id
        .as_deref()
        .expect("storage auth credentials require control-plane auth cluster id");
    let configured = latest_storage_node_auth_credential_for_node(
        &config.control_plane_storage_auth_credentials,
        node_id.as_u32(),
    )?;
    let credential = configured_storage_node_auth_credential(configured)?
        .scoped_for_cluster_and_incarnation(cluster_id, node_incarnation)
        .map_err(|error| {
            format!(
                "invalid ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS scoped credential for node {} incarnation {node_incarnation}: {error}",
                node_id.as_u32()
            )
        })?;
    Ok(StorageNodeControlPlaneClient::Authenticated(
        AuthenticatedUnixControlPlaneClient::new(client, credential),
    ))
}

fn latest_storage_node_auth_credential_for_node(
    credentials: &[ConfiguredControlPlaneStorageAuthCredential],
    node_id: u32,
) -> Result<&ConfiguredControlPlaneStorageAuthCredential, String> {
    latest_auth_credential_by_version_then_id(
        credentials
            .iter()
            .filter(|credential| credential.node_id == node_id),
        |credential| credential.credential_id.as_str(),
        |credential| credential.credential_version,
    )
    .ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_STORAGE_AUTH_CREDENTIALS must include local storage node id {node_id}"
            )
        })
}

fn latest_frontend_auth_credential_for_instance<'a>(
    credentials: &'a [ConfiguredControlPlaneFrontendAuthCredential],
    instance_id: &str,
) -> Result<&'a ConfiguredControlPlaneFrontendAuthCredential, String> {
    latest_auth_credential_by_version_then_id(
        credentials
            .iter()
            .filter(|credential| credential.instance_id == instance_id),
        |credential| credential.credential_id.as_str(),
        |credential| credential.credential_version,
    )
    .ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_FRONTEND_AUTH_CREDENTIALS must include local frontend instance id {instance_id}"
            )
        })
}

fn latest_admin_auth_credential_for_instance<'a>(
    credentials: &'a [ConfiguredControlPlaneAdminAuthCredential],
    instance_id: &str,
) -> Result<&'a ConfiguredControlPlaneAdminAuthCredential, String> {
    latest_auth_credential_by_version_then_id(
        credentials
            .iter()
            .filter(|credential| credential.instance_id == instance_id),
        |credential| credential.credential_id.as_str(),
        |credential| credential.credential_version,
    )
    .ok_or_else(|| {
            format!(
                "ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS must include local admin instance id {instance_id}"
            )
        })
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

fn bind_control_plane_socket(socket_path: &Path) -> Result<UnixListener, String> {
    bind_control_plane_unix_socket(socket_path, "ARGMIN_CONTROL_PLANE_SOCKET_PATH")
}

fn bind_control_plane_clock_recovery_socket(socket_path: &Path) -> Result<UnixListener, String> {
    bind_control_plane_unix_socket(socket_path, "derived control-plane clock recovery socket")
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
    let server = Arc::new(bound.server);
    let control_plane_refresh_loop = maybe_spawn_storage_node_control_plane_refresh_loop(
        Arc::clone(&server),
        config,
        control_plane_node_incarnation,
    );
    std::thread::spawn(move || {
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
}

type BuiltStorageNodeProcessConfig = (PreparedStorageNodeServer, Option<u64>);

fn bind_storage_node_process(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> BoundStorageNodeProcess {
    let (prepared_server, control_plane_node_incarnation) =
        build_storage_node_process_config(config, ec_config).unwrap_or_else(|e| {
            eprintln!("storage-node configuration error: {e}");
            std::process::exit(1);
        });
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
        control_plane_node_incarnation,
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
            acting_set: acting_set.clone(),
        })
        .collect();
    Ok((
        PreparedStorageNodeServer::new(
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
            .map_err(|error| error.to_string())?,
        ),
        None,
    ))
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
        match control_plane
            .refresh_node_heartbeat(heartbeat, storage::clock::current_time_millis())
            .map_err(|error| {
                format!(
                    "failed to refresh control-plane runtime map from {control_plane_socket_path}: {error}"
                )
            }) {
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
                if control_plane_startup_error_is_retryable(&error)
                    && started_at.elapsed() < retry_deadline =>
            {
                if attempts == 1 || attempts.is_multiple_of(10) {
                    eprintln!(
                        "argmin-s3 storage-node waiting for control-plane runtime map during startup: {error}"
                    );
                }
                thread::sleep(retry_delay);
            }
            Err(error) => return Err(error),
        }
    };
    let (_lease, runtime_map) = refresh.into_parts();
    let prepared_server = bootstrap
        .prepare(&runtime_map)
        .map_err(|error| error.to_string())?;
    Ok((prepared_server, Some(node_incarnation)))
}

async fn run_legacy_local_frontend(config: ServerConfig, host_id: String, ec_config: EcConfig) {
    let opened_storage_cluster = build_legacy_local_storage_cluster(&config, &ec_config)
        .unwrap_or_else(|e| {
            eprintln!("failed to open local storage cluster: {e}");
            std::process::exit(1);
        });
    let storage_cluster = opened_storage_cluster.cluster();

    run_frontend_server(
        config,
        host_id,
        storage_cluster,
        server_core::coordinator::BackgroundWorkerMode::all(),
    )
    .await;
    drop(opened_storage_cluster);
}

struct OpenedLegacyLocalStorageCluster {
    cluster: Arc<StorageCluster>,
    _static_storage_runtime_lock: Option<static_cluster_state::StaticStorageRuntimeLock>,
}

impl OpenedLegacyLocalStorageCluster {
    fn cluster(&self) -> Arc<StorageCluster> {
        Arc::clone(&self.cluster)
    }
}

impl std::ops::Deref for OpenedLegacyLocalStorageCluster {
    type Target = StorageCluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

fn build_legacy_local_storage_cluster(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<OpenedLegacyLocalStorageCluster, String> {
    let pg_ids: Vec<u32> = (0..config.pg_count).collect();
    let data_dir = Path::new(&config.data_dir);
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
    let mut static_storage_runtime_lock = None;
    let storage_cluster = if node_ids.len() == 1 {
        let node_id = node_ids[0];
        let node_data_dir = config
            .storage_node_data_dir
            .as_deref()
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join(format!("node-{:04}", node_id.as_u32())));
        if let Some(identity) = &config.static_cluster_identity {
            static_storage_runtime_lock = Some(
                static_cluster_state::lock_and_verify_standalone_storage_startup(
                    identity,
                    node_id.as_u32(),
                    &node_data_dir,
                    &pg_ids,
                )?,
            );
        }
        let cluster_epoch = ClusterEpoch::new(config.storage_cluster_epoch)
            .ok_or_else(|| "configured storage cluster epoch must be > 0".to_string())?;
        let local_map = LocalClusterMap::open_with_configs_and_epoch(
            node_id,
            [storage::LocalNodeStoreConfig::new(node_id, node_data_dir)],
            &pg_ids,
            ec_shape,
            cluster_epoch,
        )
        .map_err(|error| error.to_string())?;
        StorageCluster::from_local_map(Arc::new(local_map)).map_err(|error| error.to_string())?
    } else {
        StorageCluster::open_local_nodes(data_dir, &node_ids, &pg_ids, ec_shape)
            .map_err(|error| error.to_string())?
    };
    Ok(OpenedLegacyLocalStorageCluster {
        cluster: storage_cluster,
        _static_storage_runtime_lock: static_storage_runtime_lock,
    })
}

async fn run_remote_frontend(config: ServerConfig, host_id: String, ec_config: EcConfig) {
    let storage_cluster =
        build_remote_frontend_storage_cluster_retrying_startup(&config, &ec_config)
            .await
            .unwrap_or_else(|e| {
                eprintln!("failed to open remote frontend storage cluster: {e}");
                std::process::exit(1);
            });
    run_frontend_server(
        config,
        host_id,
        storage_cluster,
        server_core::coordinator::BackgroundWorkerMode::remote_frontend_phase_10_6(),
    )
    .await;
}

async fn build_remote_frontend_storage_cluster_retrying_startup(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<Arc<StorageCluster>, String> {
    if config.control_plane_socket_path.is_none() {
        return build_remote_frontend_storage_cluster(config, ec_config);
    }

    let retry_deadline = frontend_control_plane_startup_retry_deadline(config);
    let retry_delay = frontend_control_plane_startup_retry_delay(config);
    let started_at = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        match build_remote_frontend_storage_cluster(config, ec_config) {
            Ok(storage_cluster) => {
                if attempts > 1 {
                    process_info!(
                        "argmin-s3 frontend control-plane runtime map became ready after {} attempts",
                        attempts
                    );
                }
                return Ok(storage_cluster);
            }
            Err(error)
                if frontend_control_plane_startup_error_is_retryable(&error)
                    && started_at.elapsed() < retry_deadline =>
            {
                if attempts == 1 || attempts.is_multiple_of(10) {
                    eprintln!(
                        "argmin-s3 frontend waiting for control-plane runtime map during startup: {error}"
                    );
                }
                tokio::time::sleep(retry_delay).await;
            }
            Err(error) => return Err(error),
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

fn frontend_control_plane_startup_error_is_retryable(error: &str) -> bool {
    control_plane_startup_error_is_retryable(error)
}

fn control_plane_startup_error_is_retryable(error: &str) -> bool {
    error.starts_with("failed to fetch control-plane runtime map from ")
        || error.starts_with("failed to refresh control-plane runtime map from ")
        || control_plane_runtime_map_not_ready_for_serving(error)
        || error == "control-plane runtime map has no routed nodes"
}

fn build_remote_frontend_storage_cluster(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> Result<Arc<StorageCluster>, String> {
    if let Some(socket_path) = config.control_plane_socket_path.as_deref() {
        return build_control_plane_frontend_storage_cluster(config, ec_config, socket_path);
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
            })
        })
        .map_err(|e| e.to_string())?;
    StorageCluster::from_local_map(Arc::new(local_map)).map_err(|e| e.to_string())
}

fn build_control_plane_frontend_storage_cluster(
    config: &ServerConfig,
    ec_config: &EcConfig,
    control_plane_socket_path: &str,
) -> Result<Arc<StorageCluster>, String> {
    let control_plane = build_frontend_control_plane_client(config, control_plane_socket_path)?;
    let runtime_map = control_plane
        .runtime_map_snapshot(storage::clock::current_time_millis())
        .map_err(|error| {
            format!(
                "failed to fetch control-plane runtime map from {control_plane_socket_path}: {error}"
            )
        })?;
    ensure_frontend_startup_runtime_map_is_serving(&runtime_map)?;
    build_frontend_storage_cluster_from_runtime_map(config, ec_config, &runtime_map)
}

fn ensure_frontend_startup_runtime_map_is_serving(
    runtime_map: &ClusterRuntimeMapSnapshot,
) -> Result<(), String> {
    if runtime_map.pg_routes().is_empty() {
        return Err("control-plane runtime map has no routed PGs".to_string());
    }
    for route in runtime_map.pg_routes() {
        if route.state() != PgState::Active || route.primary_lease_deadline_ms().is_none() {
            return Err(format!(
                "PG {} has no serving primary in cluster epoch {}",
                route.pg_id().get(),
                runtime_map.cluster_epoch().get()
            ));
        }
    }
    Ok(())
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
    StorageCluster::from_runtime_map_with_unix_storage_node_client_admission_settings(
        metadata_primary_node_id,
        runtime_map,
        EcShape {
            k: ec_config.data_shards(),
            m: ec_config.parity_shards(),
        },
        unix_storage_node_client_admission_settings(config),
    )
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
    storage_cluster: Arc<StorageCluster>,
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

    let storage_cluster_handle = StorageClusterRuntimeMapHandle::new(storage_cluster);
    let frontend_runtime_map_refresh_loop =
        maybe_spawn_frontend_control_plane_refresh_loop(storage_cluster_handle.clone(), &config);
    let frontend_runtime_map_refresh_status = frontend_runtime_map_refresh_loop
        .as_ref()
        .map(storage::StorageClusterRuntimeMapRefreshLoop::status_handle);

    // Build frontend pool sharing the same storage cluster and identity-provider handles.
    let identity_provider = auth::IdentityProvider::in_memory(build_credential_store(&config));
    let mut frontends = Vec::with_capacity(config.workers as usize);
    for _ in 0..config.workers {
        let coordinator =
            Coordinator::new_with_managed_key_provider_for_storage_cluster_runtime_map_handle_with_background_worker_mode(
                storage_cluster_handle.clone(),
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

fn maybe_spawn_frontend_control_plane_refresh_loop(
    storage_cluster_handle: StorageClusterRuntimeMapHandle,
    config: &ServerConfig,
) -> Option<storage::StorageClusterRuntimeMapRefreshLoop> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use auth::SecretKey;
    use config::{ConfiguredControlPlaneRaftAuthCredential, SecretConfigValue};
    use openraft::impls::{BasicNode, Vote};
    use openraft::raft::{TransferLeaderRequest, VoteRequest};
    use storage::control_plane::{
        build_control_plane_unix_response_with_auth_and_response_clock,
        handle_control_plane_unix_stream, ControlPlaneHeartbeatSink, NodeAvailabilityState,
        NodeHeartbeat, NodeMembershipState, NodePgHeartbeatObservation, PgMetadataProof,
    };
    use storage::control_plane_auth::{
        ControlPlaneAuthEnvelope, ControlPlaneAuthSignInput, ControlPlaneAuthTarget,
    };
    use storage::control_plane_raft::{
        ControlPlaneRaftLeaderId, ControlPlaneRaftPeerFrameIdentity, ControlPlaneRaftPeerRpcRequest,
    };

    #[test]
    fn authority_clock_recovery_workers_are_reserved_at_connection_admission() {
        let ordinary_workers = AtomicUsize::new(CONTROL_PLANE_RPC_WORKER_LIMIT);
        let recovery_workers = AtomicUsize::new(0);

        assert!(!reserve_control_plane_rpc_worker(
            &ordinary_workers,
            CONTROL_PLANE_RPC_WORKER_LIMIT
        ));
        assert!(reserve_control_plane_rpc_worker(
            &recovery_workers,
            CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT
        ));
        assert_eq!(recovery_workers.load(Ordering::Acquire), 1);
    }

    #[test]
    fn post_admission_raft_wait_does_not_block_recovery_authority_access() {
        #[derive(Clone)]
        struct TestRaftAuthority;

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let ordinary_release = Arc::clone(&release);
        let mut ordinary = ControlPlaneRpcWorkerAuthority::PerWorker(TestRaftAuthority);
        let ordinary_worker = thread::spawn(move || {
            ordinary.with_mut(
                observability::ControlPlaneRpcMetricKind::RuntimeMapSnapshot,
                |_| {
                    entered_tx
                        .send(())
                        .expect("ordinary worker should report entering quorum wait");
                    let (released, wake) = &*ordinary_release;
                    let mut released = released
                        .lock()
                        .expect("quorum-wait test mutex should not be poisoned");
                    while !*released {
                        released = wake
                            .wait(released)
                            .expect("quorum-wait test mutex should not be poisoned");
                    }
                },
            );
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("ordinary worker should reach its post-admission quorum wait");

        let (recovery_tx, recovery_rx) = std::sync::mpsc::channel();
        let recovery_worker = thread::spawn(move || {
            let mut recovery = ControlPlaneRpcWorkerAuthority::PerWorker(TestRaftAuthority);
            recovery.with_mut(
                observability::ControlPlaneRpcMetricKind::AuthorityClockStatus,
                |_| (),
            );
            recovery_tx
                .send(())
                .expect("recovery worker completion should be observed");
        });
        recovery_rx.recv_timeout(Duration::from_secs(1)).expect(
            "recovery authority access must not wait for an ordinary worker's quorum timeout",
        );

        let (released, wake) = &*release;
        *released
            .lock()
            .expect("quorum-wait test mutex should not be poisoned") = true;
        wake.notify_one();
        ordinary_worker
            .join()
            .expect("ordinary quorum-wait worker should exit");
        recovery_worker
            .join()
            .expect("recovery authority worker should exit");
    }

    #[test]
    fn saturated_ordinary_worker_pool_does_not_block_clock_recovery_endpoint() {
        let tmp = test_util::tempdir();
        let authority = Arc::new(Mutex::new(
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
                tmp.path().join("control-plane.state"),
            ))
            .unwrap(),
        ));
        let now_ms = storage::clock::current_time_millis();
        let authority_clock = Arc::new(Mutex::new(
            ControlPlaneAuthorityClock::new(
                None,
                now_ms,
                storage::clock::clock_health_time_millis(),
            )
            .unwrap(),
        ));
        let admin_credential =
            ControlPlaneAdminAuthCredential::new(ControlPlaneAdminAuthCredentialInput {
                instance_id: "admin-1".to_owned(),
                credential_id: "admin-1".to_owned(),
                credential_version: 1,
                secret: b"admin-test-secret".to_vec(),
            })
            .unwrap();
        let verifier = Arc::new(
            ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
                .unwrap()
                .with_admin_credentials(vec![admin_credential.clone()])
                .unwrap(),
        );
        let ordinary_workers = Arc::new(AtomicUsize::new(0));
        let ordinary_policy = ControlPlaneRpcWorkerPolicy {
            gate_request_time_with_authority_clock: false,
            active_rpc_workers: Arc::clone(&ordinary_workers),
            worker_limit: CONTROL_PLANE_RPC_WORKER_LIMIT,
            endpoint: ControlPlaneRpcEndpoint::Ordinary,
            auth_verifier: Some(Arc::clone(&verifier)),
            raft_authority_admission: None,
            durable_response_publication: None,
        };
        let mut incomplete_clients = Vec::new();
        for _ in 0..CONTROL_PLANE_RPC_WORKER_LIMIT {
            let (server, client) = UnixStream::pair().unwrap();
            spawn_control_plane_rpc_worker(
                server,
                ControlPlaneRpcWorkerAuthority::Shared(Arc::clone(&authority)),
                Some(Arc::clone(&authority_clock)),
                None,
                ordinary_policy.clone(),
            );
            incomplete_clients.push(client);
        }
        let deadline = Instant::now() + Duration::from_millis(500);
        while ordinary_workers.load(Ordering::Acquire) != CONTROL_PLANE_RPC_WORKER_LIMIT {
            assert!(
                Instant::now() < deadline,
                "ordinary worker pool did not saturate"
            );
            thread::yield_now();
        }

        let recovery_socket = tmp.path().join("clock-recovery.sock");
        let recovery_listener = UnixListener::bind(&recovery_socket).unwrap();
        let client_credential = admin_credential.scoped_for_cluster("auth-cluster").unwrap();
        let client = thread::spawn(move || {
            AuthenticatedUnixControlPlaneClient::new(
                UnixControlPlaneClient::new(recovery_socket),
                client_credential,
            )
            .authority_clock_status(now_ms)
        });
        let (recovery_stream, _) = recovery_listener.accept().unwrap();
        let recovery_workers = Arc::new(AtomicUsize::new(0));
        spawn_control_plane_rpc_worker(
            recovery_stream,
            ControlPlaneRpcWorkerAuthority::Shared(Arc::clone(&authority)),
            Some(Arc::clone(&authority_clock)),
            None,
            ControlPlaneRpcWorkerPolicy {
                gate_request_time_with_authority_clock: false,
                active_rpc_workers: Arc::clone(&recovery_workers),
                worker_limit: CONTROL_PLANE_CLOCK_RECOVERY_RPC_WORKER_LIMIT,
                endpoint: ControlPlaneRpcEndpoint::ClockRecovery,
                auth_verifier: Some(verifier),
                raft_authority_admission: None,
                durable_response_publication: None,
            },
        );

        assert!(client.join().unwrap().unwrap().established());
        assert_eq!(ordinary_workers.load(Ordering::Acquire), 64);
        drop(incomplete_clients);
    }

    #[test]
    fn clock_recovery_socket_is_distinct_and_shorter_than_standard_process_socket() {
        let socket = Path::new("/tmp/private/control-plane-101.sock");
        let recovery = control_plane_clock_recovery_socket_path(socket);

        assert_eq!(recovery.parent(), socket.parent());
        assert_ne!(recovery, socket);
        assert!(recovery.as_os_str().as_bytes().len() <= socket.as_os_str().as_bytes().len());
    }

    #[test]
    fn invalid_control_plane_auth_cannot_invoke_raft_admission() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let credential = |secret: &[u8]| {
            ControlPlaneFrontendAuthCredential::new(ControlPlaneFrontendAuthCredentialInput {
                instance_id: "frontend-1".to_owned(),
                credential_id: "frontend-1".to_owned(),
                credential_version: 1,
                secret: secret.to_vec(),
            })
            .unwrap()
        };
        let verifier = ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
            .unwrap()
            .with_frontend_credentials(vec![credential(b"expected-secret")])
            .unwrap();
        let client_credential = credential(b"wrong-secret")
            .scoped_for_cluster("auth-cluster")
            .unwrap();
        let client_socket_path = socket_path.clone();
        let client = thread::spawn(move || {
            AuthenticatedUnixControlPlaneClient::new(
                UnixControlPlaneClient::new(client_socket_path),
                client_credential,
            )
            .runtime_map_status_with_check_applied_timeout(2_000)
        });
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let admission_calls_for_policy = Arc::clone(&admission_calls);
        let policy = ControlPlaneRpcWorkerPolicy {
            gate_request_time_with_authority_clock: false,
            active_rpc_workers: Arc::new(AtomicUsize::new(0)),
            worker_limit: CONTROL_PLANE_RPC_WORKER_LIMIT,
            endpoint: ControlPlaneRpcEndpoint::Ordinary,
            auth_verifier: Some(Arc::new(verifier)),
            raft_authority_admission: Some(Arc::new(move |_| {
                admission_calls_for_policy.fetch_add(1, Ordering::AcqRel);
                Ok(())
            })),
            durable_response_publication: None,
        };

        assert!(matches!(
            authenticate_and_admit_control_plane_rpc(request, &policy, 2_000),
            Err(ControlPlaneRpcAdmissionFailure::Unauthenticated(_))
        ));
        assert_eq!(admission_calls.load(Ordering::Acquire), 0);
        let invalid_magic = [0; b"argmin-control-plane-rpc".len()];
        std::io::Write::write_all(&mut stream, &invalid_magic).unwrap();
        drop(stream);
        assert!(client.join().unwrap().is_err());
    }

    fn durable_raft_artifact_vote(path: &Path) -> Option<Vote<ControlPlaneRaftLeaderId>> {
        let artifact =
            storage::control_plane_raft::ControlPlaneRaftRestartArtifact::load_durable_artifact(
                path,
            )
            .ok()?;
        let (log_store, _state_machine) = artifact.restore().ok()?;
        log_store.persisted_vote().ok().flatten()
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
        let checkpoint_target = AuthorityClockCheckpointTarget {
            path: state_path.clone(),
            binding,
        };

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
            persist_established_authority_clock_checkpoint(
                context,
                &mut authority_clock,
                Some(&checkpoint_target),
            )
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
        let target = AuthorityClockCheckpointTarget {
            path: blocked_parent.join("authority.state"),
            binding: ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1),
        };

        storage::clock::with_time_override(1_000, || {
            assert!(persist_established_authority_clock_checkpoint(
                context,
                &mut authority_clock,
                Some(&target),
            )
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
        let target = AuthorityClockCheckpointTarget {
            path: state_path.clone(),
            binding,
        };

        invalidate_blocked_authority_clock_checkpoint(&authority_clock, Some(&target)).unwrap();
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
        let target = AuthorityClockCheckpointTarget {
            path: state_path.clone(),
            binding,
        };
        storage::clock::with_time_override(1_000, || {
            assert!(persist_established_authority_clock_checkpoint(
                context,
                &mut authority_clock,
                Some(&target),
            )
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
            storage_node_socket_path: Some("/tmp/argmin-test/node-0002.sock".to_string()),
            storage_node_sockets: Vec::new(),
            storage_node_rpc_admission_limit:
                LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT,
            storage_node_rpc_admission_wait_timeout:
                LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            storage_node_rpc_control_admission_wait_timeout:
                LocalUnixStorageNodeClientConfig::DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
            control_plane_state_path: None,
            control_plane_socket_path: None,
            control_plane_client_socket_paths: Vec::new(),
            control_plane_auth_cluster_id: None,
            control_plane_storage_auth_credentials: Vec::new(),
            control_plane_frontend_auth_instance_id: None,
            control_plane_frontend_auth_credentials: Vec::new(),
            control_plane_admin_auth_instance_id: None,
            control_plane_admin_auth_credentials: Vec::new(),
            control_plane_experimental_raft: false,
            control_plane_raft_cluster_name: None,
            control_plane_raft_node_id: None,
            control_plane_raft_peer_socket_path: None,
            control_plane_raft_peer_sockets: Vec::new(),
            control_plane_raft_auth_credentials: Vec::new(),
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
    fn storage_node_control_plane_client_uses_authenticated_client_when_configured() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_storage_auth_credentials = vec![
            ConfiguredControlPlaneStorageAuthCredential {
                node_id: 2,
                credential_id: "storage-node".to_string(),
                credential_version: 7,
                secret: SecretConfigValue::new("storage-node-2-secret".to_string()),
            },
            ConfiguredControlPlaneStorageAuthCredential {
                node_id: 2,
                credential_id: "storage-node".to_string(),
                credential_version: 8,
                secret: SecretConfigValue::new("storage-node-2-new-secret".to_string()),
            },
        ];

        let client = build_storage_node_control_plane_client(
            &config,
            "/tmp/argmin-control-plane.sock",
            NodeId::new(2),
            55,
        )
        .expect("storage-node auth client should build");

        let StorageNodeControlPlaneClient::Authenticated(client) = client else {
            panic!("storage-node auth client should be authenticated");
        };
        assert_eq!(client.credential().credential_version(), 8);
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
                secret: SecretConfigValue::new("storage-node-2-secret".to_string()),
            }];
        let storage_credential = configured_storage_node_auth_credential(
            &config.control_plane_storage_auth_credentials[0],
        )
        .expect("test storage-node credential should build");
        let verifier = ControlPlaneUnixAuthVerifier::new("control-auth", vec![storage_credential])
            .expect("test storage-node verifier should build");
        let verifier_for_assert = verifier.clone();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let store = FileControlPlaneStore::new(state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(node_id, storage::control_plane::NodeMembershipState::Active)
            .unwrap();
        let (server_ready_tx, server_ready_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            server_ready_tx.send(()).unwrap();
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let response = build_control_plane_unix_response_with_auth_and_response_clock(
                &mut authority,
                request,
                2_000,
                Some(&verifier),
                || Ok(2_000),
            )
            .unwrap();
            write_control_plane_unix_response(&mut stream, response).unwrap();
        });
        server_ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("control-plane test server should be ready");

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
        let refresh = client.refresh_node_heartbeat(heartbeat, 2_000).unwrap();

        server.join().unwrap();
        assert_eq!(refresh.lease().node_id(), node_id);
        assert_eq!(refresh.lease().lease_deadline_ms(), 2_100);
        assert_eq!(
            refresh.runtime_map().nodes()[0].endpoint(),
            "/tmp/argmin-node-2.sock"
        );
        let metrics = verifier_for_assert.metrics_snapshot();
        assert_eq!(metrics.accepted_total(), 1);
        assert_eq!(
            metrics.accepted_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
            1
        );
        assert_eq!(metrics.rejected_total(), 0);
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
                secret: SecretConfigValue::new("storage-node-1-secret".to_string()),
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
                secret: SecretConfigValue::new("frontend-1-secret".to_string()),
            },
            ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 8,
                secret: SecretConfigValue::new("frontend-1-new-secret".to_string()),
            },
        ];

        let client = build_frontend_control_plane_client(&config, "/tmp/argmin-control-plane.sock")
            .expect("frontend auth client should build");

        let FrontendControlPlaneClient::Authenticated(client) = client else {
            panic!("frontend auth client should be authenticated");
        };
        assert_eq!(client.credential().credential_version(), 8);
    }

    #[test]
    fn admin_control_plane_client_selects_latest_local_auth_credential() {
        let credentials = vec![
            ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 7,
                secret: SecretConfigValue::new("admin-1-secret".to_string()),
            },
            ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 8,
                secret: SecretConfigValue::new("admin-1-new-secret".to_string()),
            },
        ];

        let configured = latest_admin_auth_credential_for_instance(&credentials, "admin-1")
            .expect("admin auth credential should be selected");

        assert_eq!(configured.credential_version, 8);
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
                secret: SecretConfigValue::new("frontend-1-secret".to_string()),
            }];

        let result = build_frontend_control_plane_client(&config, "/tmp/argmin-control-plane.sock");
        let Err(err) = result else {
            panic!("frontend control-plane client should reject missing local credential");
        };

        assert!(err.contains("local frontend instance id frontend-2"));
    }

    #[test]
    fn control_plane_unix_auth_verifier_includes_frontend_credentials() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_frontend_auth_credentials =
            vec![ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 7,
                secret: SecretConfigValue::new("frontend-1-secret".to_string()),
            }];
        config.control_plane_admin_auth_credentials =
            vec![ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 8,
                secret: SecretConfigValue::new("admin-1-secret".to_string()),
            }];

        let verifier = build_control_plane_unix_auth_verifier(&config)
            .expect("frontend auth verifier should build")
            .expect("frontend auth verifier should be enabled");
        let status = verifier.status_snapshot();

        assert!(status.required());
        assert!(!status.storage_node_heartbeat_required());
        assert!(status.frontend_runtime_map_required());
        assert!(status.admin_control_plane_required());
        assert_eq!(status.storage_node_credentials().len(), 0);
        assert_eq!(status.frontend_credentials().len(), 1);
        assert_eq!(status.frontend_credentials()[0].instance_id(), "frontend-1");
        assert_eq!(status.admin_credentials().len(), 1);
        assert_eq!(status.admin_credentials()[0].instance_id(), "admin-1");
    }

    #[test]
    fn control_plane_unix_auth_verifier_rejects_frontend_without_admin_credentials() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_frontend_auth_credentials =
            vec![ConfiguredControlPlaneFrontendAuthCredential {
                instance_id: "frontend-1".to_string(),
                credential_id: "frontend".to_string(),
                credential_version: 7,
                secret: SecretConfigValue::new("frontend-1-secret".to_string()),
            }];

        let error = build_control_plane_unix_auth_verifier(&config)
            .expect_err("frontend auth verifier should require admin credentials");

        assert!(error.contains("ARGMIN_CONTROL_PLANE_ADMIN_AUTH_CREDENTIALS is required"));
    }

    #[test]
    fn control_plane_unix_auth_verifier_includes_admin_credentials() {
        let mut config = test_server_config();
        config.control_plane_auth_cluster_id = Some("control-auth".to_string());
        config.control_plane_admin_auth_credentials =
            vec![ConfiguredControlPlaneAdminAuthCredential {
                instance_id: "admin-1".to_string(),
                credential_id: "admin".to_string(),
                credential_version: 7,
                secret: SecretConfigValue::new("admin-1-secret".to_string()),
            }];

        let verifier = build_control_plane_unix_auth_verifier(&config)
            .expect("admin auth verifier should build")
            .expect("admin auth verifier should be enabled");
        let status = verifier.status_snapshot();

        assert!(status.required());
        assert!(!status.storage_node_heartbeat_required());
        assert!(!status.frontend_runtime_map_required());
        assert!(status.admin_control_plane_required());
        assert_eq!(status.storage_node_credentials().len(), 0);
        assert_eq!(status.frontend_credentials().len(), 0);
        assert_eq!(status.admin_credentials().len(), 1);
        assert_eq!(status.admin_credentials()[0].instance_id(), "admin-1");
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
    fn raft_non_leader_classifier_covers_status_submission_race() {
        assert!(experimental_raft_error_is_non_local_leader(
            &ControlPlaneError::RpcRemote {
                message: "local OpenRaft authority is not the serving leader".to_string(),
            }
        ));
        assert!(experimental_raft_error_is_non_local_leader(
            &ControlPlaneError::RpcRemote {
                message: "local OpenRaft authority is not the serving leader: NotLocalLeader"
                    .to_string(),
            }
        ));
        assert!(experimental_raft_error_is_non_local_leader(
            &ControlPlaneError::RpcRemote {
                message: "local OpenRaft authority is not the serving leader: command-authority read-index failed: not enough for a quorum"
                    .to_string(),
            }
        ));
        assert!(experimental_raft_error_is_non_local_leader(
            &ControlPlaneError::RpcRemote {
                message: "OpenRaft client-write failed: has to forward request to: Some(102)"
                    .to_string(),
            }
        ));
        assert!(experimental_raft_lease_expiry_error_is_transient(
            &ControlPlaneError::LeaseGrantHorizonAuthorityTermMismatch {
                authority_term: Some(2),
                committed_term: Some(3),
            }
        ));
        assert!(!experimental_raft_error_is_non_local_leader(
            &ControlPlaneError::RpcRemote {
                message: "unrelated control-plane failure".to_string(),
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let handle = runtime.handle().clone();
        let authority = runtime.block_on(async {
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                format!("argmin-s3-experimental-raft-{name}-{}", std::process::id()),
                1,
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
            durable_poison: Arc::new(Mutex::new(None)),
            durable_publication: ExperimentalRaftDurabilityPublication::new(),
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        ExperimentalRaftTestHarness {
            runtime,
            authority,
            control_plane,
        }
    }

    #[test]
    fn durability_publication_allows_concurrent_responses_before_exclusive_poison() {
        let publication = ExperimentalRaftDurabilityPublication::new();
        let first_publication = publication.clone();
        let (first_started_tx, first_started_rx) = std::sync::mpsc::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let first = thread::spawn(move || {
            first_publication.publish(|| {
                first_started_tx
                    .send(())
                    .expect("first response should report publication start");
                release_first_rx
                    .recv()
                    .expect("first response should be released");
                Ok(())
            })
        });
        first_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first response should acquire a publication permit");

        let second_publication = publication.clone();
        let (second_finished_tx, second_finished_rx) = std::sync::mpsc::channel();
        let second = thread::spawn(move || {
            let result = second_publication.publish(|| Ok(()));
            second_finished_tx
                .send(())
                .expect("second response should report completion");
            result
        });
        second_finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("an unrelated response must not wait for the first socket write");
        second
            .join()
            .expect("second response worker should exit")
            .expect("second response should publish");

        let poison_publication = publication.clone();
        let poisoner = thread::spawn(move || poison_publication.publish_poison(|| {}));
        let poison_wait_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let poison_requested = publication
                .gate
                .0
                .lock()
                .expect("response publication state should lock")
                .poison_requested;
            if poison_requested {
                break;
            }
            assert!(
                Instant::now() < poison_wait_deadline,
                "poison publication should become pending"
            );
            thread::yield_now();
        }
        assert!(!publication.is_poisoned());
        let error = publication
            .publish(|| Ok(()))
            .expect_err("a response arriving after poison was requested must be suppressed");
        assert!(error
            .to_string()
            .contains("poisoned before response publication"));

        release_first_tx
            .send(())
            .expect("first response should resume");
        first
            .join()
            .expect("first response worker should exit")
            .expect("first response should publish");
        poisoner
            .join()
            .expect("poison publication worker should exit");
        assert!(publication.is_poisoned());
    }

    #[test]
    fn cloned_raft_wrapper_suppresses_response_publication_after_concurrent_poison() {
        let harness = experimental_raft_test_harness("cloned-response-poison");
        let in_flight = harness.control_plane.clone();
        let poisoner = harness.control_plane.clone();
        let publication = in_flight.durable_publication.clone();
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
            publication.publish(|| {
                worker_published.store(true, Ordering::Release);
                Ok(())
            })
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
        assert!(error
            .to_string()
            .contains("poisoned before response publication"));
        assert!(!published.load(Ordering::Acquire));
        assert!(harness.control_plane.ensure_not_durably_poisoned().is_err());
        harness.shutdown();
    }

    #[test]
    fn wal_checkpoint_monitor_poison_suppresses_parked_peer_response_publication() {
        let harness = experimental_raft_test_harness("monitor-response-poison");
        let publication = harness.control_plane.durable_publication.clone();
        let durability = ExperimentalRaftPeerDurabilityContext {
            artifact_path: None,
            checkpoint_lock: Arc::new(Mutex::new(())),
            publication: publication.clone(),
        };
        let published = Arc::new(AtomicBool::new(false));
        let worker_published = Arc::clone(&published);
        let (checked_tx, checked_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            assert!(!publication.is_poisoned());
            checked_tx
                .send(())
                .expect("response worker should report its early poison check");
            resume_rx
                .recv()
                .expect("response worker should resume after monitor poison");
            publish_experimental_raft_peer_response(Some(&publication), || {
                worker_published.store(true, Ordering::Release);
                Ok(())
            })
        });
        checked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("response worker should pass its early poison check");

        publish_experimental_raft_checkpoint_monitor_poison(&durability);
        resume_tx
            .send(())
            .expect("response worker should resume after monitor poison");
        let error = worker
            .join()
            .expect("response worker should exit")
            .expect_err("monitor poison must suppress parked response publication");
        assert!(matches!(
            error,
            ExperimentalRaftPeerRpcWorkerError::PeerRpc(error)
                if error.to_string().contains("poisoned before response publication")
        ));
        assert!(!published.load(Ordering::Acquire));
        assert!(harness.control_plane.durable_publication.is_poisoned());
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

    fn experimental_raft_signed_peer_frame(
        cluster_name: &str,
        credential: ControlPlaneScopedCredential,
        target_node_id: ControlPlaneRaftNodeId,
        operation: ControlPlaneAuthOperation,
        payload: Vec<u8>,
    ) -> Vec<u8> {
        credential
            .sign_envelope(ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                    node_id: target_node_id,
                }),
                operation,
                issued_at_ms: None,
                expires_at_ms: None,
                sequence: None,
                nonce: Vec::new(),
                payload,
            })
            .unwrap_or_else(|error| {
                panic!("test auth envelope should sign for cluster {cluster_name}: {error}")
            })
            .encode_frame()
            .expect("test auth envelope should encode")
    }

    fn experimental_raft_durable_test_harness(
        name: &str,
        state_path: &Path,
    ) -> ExperimentalRaftTestHarness {
        experimental_raft_durable_test_harness_inner(name, state_path, None)
    }

    fn experimental_raft_durable_wal_test_harness(
        name: &str,
        state_path: &Path,
    ) -> ExperimentalRaftTestHarness {
        let wal_path = durable_artifact_wal_path(state_path);
        experimental_raft_durable_test_harness_inner(name, state_path, Some(&wal_path))
    }

    fn experimental_raft_durable_test_harness_inner(
        name: &str,
        state_path: &Path,
        wal_path: Option<&Path>,
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
            let authority = match wal_path {
                Some(wal_path) => {
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
                        cluster_name,
                        1,
                        state_path,
                        wal_path,
                    )
                    .await
                }
                None => {
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        cluster_name,
                        1,
                        state_path,
                    )
                    .await
                }
            }
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
                    .store_durable_restart_artifact(state_path)
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
                .store_durable_restart_artifact(state_path)
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
            durable_poison: Arc::new(Mutex::new(None)),
            durable_publication: ExperimentalRaftDurabilityPublication::new(),
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        ExperimentalRaftTestHarness {
            runtime,
            authority,
            control_plane,
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
        let mut control_plane = ExperimentalRaftControlPlane {
            runtime: harness.runtime.handle().clone(),
            authority: Arc::clone(&harness.authority),
            durable_artifact_path: None,
            durable_checkpoint_lock: None,
            durable_serving_checkpoint: Arc::new(Mutex::new(None)),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: Arc::new(Mutex::new(None)),
            durable_publication: ExperimentalRaftDurabilityPublication::new(),
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        std::thread::spawn(move || {
            for request_index in 0..request_count {
                let (mut stream, _addr) = listener.accept().unwrap();
                handle_control_plane_unix_stream(
                    &mut control_plane,
                    &mut stream,
                    authority_now_ms + u64::try_from(request_index).unwrap(),
                )
                .unwrap();
            }
        })
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

        let listener =
            bind_experimental_raft_peer_listener(&config, "process-peer-listener-test", 1)
                .expect("peer listener should bind")
                .expect("configured peer listener should be present");

        assert!(peer_socket_path.exists());
        drop(listener);
        fs::remove_file(&peer_socket_path).unwrap();
        fs::remove_dir(&test_dir).unwrap();
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
    fn experimental_raft_control_plane_durable_restart_restores_bootstrap_state() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-durable-restart");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).unwrap();
        let state_path = state_dir.join("control-plane.state");
        let storage_nodes = vec![(
            NodeId::new(1),
            "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        )];
        let pg_ids = vec![PgId::new(0)];
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![0];
        let cluster_name = format!(
            "argmin-s3-experimental-raft-durable-restart-{}",
            std::process::id()
        );
        let checkpoint_binding =
            ControlPlaneAuthorityClockCheckpointBinding::for_raft(&cluster_name, 1);
        let expected =
            storage::control_plane_raft::ControlPlaneRaftRestartArtifact::store_single_node_committed_ahead_bootstrap_artifact_for_test(
                &state_path,
                cluster_name.clone(),
                1,
                storage_nodes,
                pg_ids,
            )
            .expect("committed-ahead durable raft artifact should be stored");
        store_authority_clock_restart_checkpoint(
            &state_path,
            checkpoint_binding,
            1,
            expected.max_committed_timestamp_ms(),
        )
        .expect("test restart artifact should have a paired authority-clock checkpoint");
        assert!(state_path.exists());

        let restarted_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("restarted test runtime should build");
        let handle = restarted_runtime.handle().clone();
        let authority = restarted_runtime.block_on(async {
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                cluster_name,
                1,
                &state_path,
            )
            .await
            .expect("durable experimental raft authority should restore");
            assert!(authority
                .is_initialized()
                .await
                .expect("restored durable raft initialization status should read"));
            authority
                .wait_for_current_leader(
                    1,
                    Duration::from_secs(1),
                    "restarted durable experimental process test leadership",
                )
                .await
                .expect("single-node raft should become leader");
            wait_for_experimental_raft_local_authority_serving(
                &authority,
                Duration::from_secs(1),
                "restarted durable experimental process test committed replay",
            )
            .await
            .expect("restarted durable raft should apply committed suffix");
            authority
                .store_durable_restart_artifact(&state_path)
                .await
                .expect("restarted durable raft should checkpoint caught-up state");
            Arc::new(authority)
        });
        store_experimental_raft_durable_restart_artifact(&handle, &authority, &state_path, None)
            .expect("process checkpoint should pair Raft state with a clock checkpoint");
        let restart_clock_checkpoint =
            load_authority_clock_restart_checkpoint(&state_path, checkpoint_binding)
                .expect("Raft clock checkpoint should load")
                .expect("Raft clock checkpoint should exist");
        let restored_snapshot = restarted_runtime
            .block_on(authority.current_control_plane_snapshot())
            .expect("restored snapshot should read for clock checkpoint validation");
        assert_eq!(
            restart_clock_checkpoint.committed_timestamp_high_water_ms(),
            restored_snapshot.max_committed_timestamp_ms()
        );
        let mut control_plane = ExperimentalRaftControlPlane {
            runtime: handle,
            authority: Arc::clone(&authority),
            durable_artifact_path: Some(Arc::new(state_path.clone())),
            durable_checkpoint_lock: Some(Arc::new(Mutex::new(()))),
            durable_serving_checkpoint: Arc::new(Mutex::new(None)),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: Arc::new(Mutex::new(None)),
            durable_publication: ExperimentalRaftDurabilityPublication::new(),
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        bootstrap_empty_experimental_raft_control_plane(&mut control_plane, &config)
            .expect("durable experimental raft control-plane bootstrap should succeed");
        let restarted = control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read after bootstrap");
        restarted_runtime
            .block_on(authority.shutdown())
            .expect("durable experimental raft authority should shut down");
        assert_eq!(restarted, expected);
    }

    #[test]
    fn experimental_raft_control_plane_checkpoint_failure_poisons_durable_authority() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-checkpoint-poison");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).unwrap();
        let startup_state_path = state_dir.join("startup.state");
        let invalid_checkpoint_path = state_dir.to_path_buf();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let handle = runtime.handle().clone();
        let authority = runtime.block_on(async {
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                format!(
                    "argmin-s3-experimental-raft-checkpoint-poison-{}",
                    std::process::id()
                ),
                1,
                &startup_state_path,
            )
            .await
            .expect("durable experimental raft authority should initialize");
            authority
                .initialize_single_node_membership(1)
                .await
                .expect("single-node raft membership should initialize");
            authority
                .wait_for_current_leader(
                    1,
                    Duration::from_secs(1),
                    "checkpoint poison test leadership",
                )
                .await
                .expect("single-node raft should become leader");
            wait_for_experimental_raft_local_authority_serving(
                &authority,
                Duration::from_secs(1),
                "checkpoint poison test committed membership",
            )
            .await
            .expect("single-node raft should apply committed membership");
            Arc::new(authority)
        });
        let mut control_plane = ExperimentalRaftControlPlane {
            runtime: handle,
            authority: Arc::clone(&authority),
            durable_artifact_path: Some(Arc::new(invalid_checkpoint_path)),
            durable_checkpoint_lock: Some(Arc::new(Mutex::new(()))),
            durable_serving_checkpoint: Arc::new(Mutex::new(None)),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: Arc::new(Mutex::new(None)),
            durable_publication: ExperimentalRaftDurabilityPublication::new(),
            after_heartbeat_commit_hook: Arc::new(Mutex::new(None)),
        };
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![0];

        let err = bootstrap_empty_experimental_raft_control_plane(&mut control_plane, &config)
            .expect_err("checkpoint failure should reject the bootstrap response");
        assert!(err.contains("durable restart artifact"));
        assert!(control_plane
            .durable_poison
            .lock()
            .expect("durable poison mutex should not be poisoned")
            .is_some());
        assert!(control_plane.durable_publication.is_poisoned());

        let runtime_map_err = ControlPlaneRuntimeMapSource::runtime_map_snapshot(
            &control_plane,
            storage::clock::current_time_millis(),
        )
        .expect_err("poisoned durable authority should reject runtime-map service");
        assert!(runtime_map_err
            .to_string()
            .contains("durability checkpoint failed"));

        let admin_err = control_plane
            .set_pg_acting_set(PgId::new(0), vec![NodeId::new(1)])
            .expect_err("poisoned durable authority should reject admin mutation");
        assert!(admin_err.to_string().contains("refusing to serve"));

        runtime
            .block_on(authority.shutdown())
            .expect("durable experimental raft authority should shut down");
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
    fn experimental_raft_bootstrap_concurrent_success_requires_initialized_state() {
        let harness = experimental_raft_test_harness("bootstrap-concurrent-empty-test");
        assert!(
            !experimental_raft_bootstrap_submit_error_was_concurrent_success(
                &harness.control_plane,
                &ControlPlaneError::BootstrapRequiresEmptyState,
            )
            .expect("empty experimental raft control-plane snapshot should read"),
            "potentially benign bootstrap rejection must not be accepted while state is empty"
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_bootstrap_concurrent_success_is_idempotent_after_state_exists() {
        let mut harness = experimental_raft_test_harness("bootstrap-concurrent-success-test");
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![0];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("experimental raft control-plane bootstrap should succeed");

        assert!(
            experimental_raft_bootstrap_submit_error_was_concurrent_success(
                &harness.control_plane,
                &ControlPlaneError::BootstrapRequiresEmptyState,
            )
            .expect("bootstrapped experimental raft control-plane snapshot should read"),
            "potentially benign bootstrap rejection is accepted only after state exists"
        );

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
            .block_on(
                harness
                    .authority
                    .raft()
                    .with_state_machine(|state_machine| {
                        let deadline = state_machine
                            .inner()
                            .snapshot()
                            .pg(PgId::new(7))
                            .and_then(|pg| pg.previous_primary_lease_deadline_ms());
                        Box::pin(async move { deadline })
                    }),
            )
            .expect("durable availability fence state should read");
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
                .block_on(
                    harness
                        .authority
                        .raft()
                        .with_state_machine(|state_machine| {
                            let snapshot = state_machine.inner().snapshot().clone();
                            Box::pin(async move { snapshot })
                        }),
                )
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
        let wal_path = durable_artifact_wal_path(&state_path);
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
        let large_artifact_bytes = fs::metadata(&state_path)
            .expect("large restart artifact metadata should read")
            .len();
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
            publication: harness.control_plane.durable_publication.clone(),
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
            let artifact_bytes = fs::metadata(&state_path)
                .expect("post-purge artifact metadata should read")
                .len();
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
        let read_artifact_before =
            fs::read(&state_path).expect("pre-serving-read restart artifact should read");
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
        assert_eq!(
            fs::read(&state_path).expect("post-serving-read restart artifact should read"),
            read_artifact_before,
            "a serving read must retain liveness changes solely in the synced WAL suffix"
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
        let artifact_before = fs::read(&state_path).expect("restart artifact should read");
        let wal_before = fs::read(&wal_path).expect("Raft WAL should read");
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
        let artifact_after_steady = fs::read(&state_path).expect("restart artifact should reread");
        let wal_after_steady = fs::read(&wal_path).expect("Raft WAL should reread");
        let durability_metrics_after_steady = harness.authority.durability_metric_snapshots();
        assert_eq!(applied_after_steady, applied_before);
        assert_eq!(artifact_after_steady, artifact_before);
        assert_eq!(wal_after_steady, wal_before);
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
            "control_plane_write_amplification_release pgs={PG_COUNT} retained_epochs={RETAINED_HISTORY_EPOCHS} retained_artifact_bytes={large_artifact_bytes} storage_nodes={STORAGE_NODE_COUNT} sustained_peering_intervals={SUSTAINED_PEERING_INTERVALS} sustained_peering_rounds_per_interval={SUSTAINED_PEERING_ROUNDS} post_purge_artifact_bytes={post_purge_artifact_bytes:?} sustained_peering_checkpoint_bytes={sustained_checkpoint_bytes} sustained_peering_wal_bytes={sustained_wal_bytes} sustained_peering_durable_bytes_per_second={sustained_durable_bytes_per_second} steady_heartbeats={steady_heartbeat_requests} compact_status_reads={STEADY_HEARTBEAT_ROUNDS} wal_monitor_polls={monitor_poll_total} wal_monitor_poll_us_total={monitor_poll_us_total} wal_monitor_poll_us_max={monitor_poll_us_max} steady_checkpoint_stores=0 steady_checkpoint_syncs=0 steady_wal_appends=0 steady_wal_syncs=0 horizon_probe_heartbeats={horizon_probe_heartbeats} checkpoint_stores={checkpoint_store_delta} checkpoint_store_us_total={checkpoint_store_us_total_delta} checkpoint_store_us_lifetime_max={} checkpoint_file_syncs={checkpoint_file_sync_delta} checkpoint_file_sync_us_total={checkpoint_file_sync_us_total_delta} checkpoint_file_sync_us_lifetime_max={} checkpoint_directory_syncs={checkpoint_directory_sync_delta} checkpoint_directory_sync_us_total={checkpoint_directory_sync_us_total_delta} checkpoint_directory_sync_us_lifetime_max={} checkpoint_bytes={checkpoint_bytes} horizon_wal_appends={wal_append_delta} horizon_wal_append_us_total={wal_append_us_total_delta} horizon_wal_append_us_lifetime_max={} horizon_wal_file_syncs={wal_file_sync_delta} horizon_wal_file_sync_us_total={wal_file_sync_us_total_delta} horizon_wal_file_sync_us_lifetime_max={} horizon_wal_directory_syncs={wal_directory_sync_delta} horizon_wal_directory_sync_us_total={wal_directory_sync_us_total_delta} horizon_wal_directory_sync_us_lifetime_max={} horizon_wal_bytes_appended={wal_bytes_appended} horizon_wal_offset_advance={wal_offset_advance} horizon_extension_interval_ms={extension_interval_ms} checkpoint_amortization_interval_ms={checkpoint_amortization_interval_ms} amortized_durable_bytes_per_second={amortized_durable_bytes_per_second} concurrent_checkpoints={CONCURRENT_CHECKPOINT_COUNT} concurrent_checkpoint_call_us_total={concurrent_checkpoint_call_us_total} concurrent_checkpoint_batch_us={concurrent_checkpoint_batch_us} concurrent_checkpoint_us_max={concurrent_checkpoint_us_max} concurrent_heartbeats={} concurrent_heartbeat_us_max={concurrent_heartbeat_us_max} concurrent_status_reads={CONCURRENT_HEARTBEAT_ROUNDS} concurrent_status_us_max={concurrent_status_us_max}",
            final_checkpoint_metrics.store_us_max,
            final_checkpoint_metrics.file_sync_us_max,
            final_checkpoint_metrics.directory_sync_us_max,
            wal_metrics_after.append_us_max,
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
            .block_on(
                harness
                    .authority
                    .raft()
                    .with_state_machine(|state_machine| {
                        let deadline = state_machine
                            .inner()
                            .snapshot()
                            .node(NodeId::new(1))
                            .and_then(|node| node.lease_deadline_ms());
                        Box::pin(async move { deadline })
                    }),
            )
            .expect("durable state should retain promotion before rejected command");
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
    fn experimental_raft_peer_rpc_rejects_durable_poison_before_dispatch() {
        let harness = experimental_raft_test_harness("peer-poison-before-dispatch");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-poison-before-dispatch-{}",
            std::process::id()
        );
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1);
        let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(3, 1),
            last_log_id: None,
            leadership_transfer: false,
        });
        let request_frame = request
            .encode_frame_for_peer(&identity)
            .expect("peer request should encode");
        let (mut client_stream, mut server_stream) =
            UnixStream::pair().expect("test UnixStream pair should create");
        write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
            .expect("client should write request frame");
        let publication = ExperimentalRaftDurabilityPublication::new();
        publication.publish_poison(|| {});
        let runtime_handle = harness.runtime.handle().clone();

        let result = handle_experimental_raft_peer_rpc_before_ack(
            &runtime_handle,
            &harness.authority,
            &mut server_stream,
            1,
            &policy,
            ExperimentalRaftPeerRpcDurability {
                artifact_path: None,
                checkpoint_lock: None,
                publication: Some(&publication),
            },
        );
        assert!(
            matches!(result, Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(_))),
            "poisoned peer RPC should fail before dispatch: {result:?}"
        );
        drop(server_stream);

        let response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            response.is_err(),
            "poisoned peer RPC must not write a response"
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_peer_rpc_rejects_durable_poison_after_validation_before_dispatch() {
        let harness = experimental_raft_test_harness("peer-poison-after-validation");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-poison-after-validation-{}",
            std::process::id()
        );
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1);
        let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(3, 1),
            last_log_id: None,
            leadership_transfer: false,
        });
        let request_frame = request
            .encode_frame_for_peer(&identity)
            .expect("peer request should encode");
        let frame_kind = decode_control_plane_raft_peer_request_frame_kind(&request_frame)
            .expect("request kind should decode");
        let decoded_identity =
            decode_control_plane_raft_peer_request_frame_identity(&request_frame)
                .expect("request identity should decode");
        let operation = decode_control_plane_raft_peer_request_auth_operation(&request_frame)
            .expect("request operation should decode");
        policy
            .validate_incoming_frame_identity(&decoded_identity, 1)
            .expect("request identity should validate");
        let before_status = harness
            .runtime
            .block_on(harness.authority.status())
            .expect("status should read before poisoned dispatch");
        let publication = ExperimentalRaftDurabilityPublication::new();
        publication.publish_poison(|| {});

        let result = handle_experimental_raft_peer_rpc_validated_frame_before_ack(
            harness.runtime.handle(),
            &harness.authority,
            ExperimentalRaftValidatedPeerRequest {
                frame: &request_frame,
                kind: frame_kind,
                identity: &decoded_identity,
                operation,
            },
            &policy,
            ExperimentalRaftPeerRpcDurability {
                artifact_path: None,
                checkpoint_lock: None,
                publication: Some(&publication),
            },
        );
        assert!(
            matches!(result, Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(_))),
            "poisoned validated peer RPC should fail before OpenRaft dispatch: {result:?}"
        );
        let after_status = harness
            .runtime
            .block_on(harness.authority.status())
            .expect("status should read after poisoned dispatch");
        assert_eq!(
            after_status.persisted_vote(),
            before_status.persisted_vote(),
            "poisoned validated peer RPC must not mutate the Raft vote"
        );

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_peer_rpc_rejects_bad_auth_before_dispatch() {
        let harness = experimental_raft_test_harness("peer-auth-before-dispatch");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-auth-before-dispatch-{}",
            std::process::id()
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name.clone(), 1, 2);
        let request_frame = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: Vote::<ControlPlaneRaftLeaderId>::new(3, 1),
            last_log_id: None,
            leadership_transfer: false,
        })
        .encode_frame_for_peer(&identity)
        .expect("peer request should encode");

        let valid_source_credential = experimental_raft_peer_auth_credential(
            &cluster_name,
            1,
            "raft-node-1",
            1,
            "node-1-test-secret",
        );
        let valid_signed_frame = experimental_raft_signed_peer_frame(
            &cluster_name,
            valid_source_credential.clone(),
            2,
            ControlPlaneAuthOperation::RaftVote,
            request_frame.clone(),
        );
        let valid_envelope = ControlPlaneAuthEnvelope::decode_frame(
            &valid_signed_frame,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("valid test auth envelope should decode");
        let mut tampered_authenticator = valid_envelope.authenticator().to_vec();
        tampered_authenticator[0] ^= 0x01;
        let bad_mac_frame = ControlPlaneAuthEnvelope::new(
            storage::control_plane_auth::ControlPlaneAuthEnvelopeInput {
                header: valid_envelope.header().clone(),
                payload: valid_envelope.payload().to_vec(),
                authenticator: tampered_authenticator,
            },
        )
        .expect("tampered test auth envelope should rebuild")
        .encode_frame()
        .expect("tampered test auth envelope should encode");

        let wrong_cluster_credential = experimental_raft_peer_auth_credential(
            "wrong-cluster",
            1,
            "raft-node-1",
            1,
            "node-1-test-secret",
        );
        let wrong_cluster_frame = experimental_raft_signed_peer_frame(
            "wrong-cluster",
            wrong_cluster_credential,
            2,
            ControlPlaneAuthOperation::RaftVote,
            request_frame.clone(),
        );
        let wrong_source_credential = experimental_raft_peer_auth_credential(
            &cluster_name,
            2,
            "raft-node-2",
            1,
            "node-2-test-secret",
        );
        let wrong_source_frame = experimental_raft_signed_peer_frame(
            &cluster_name,
            wrong_source_credential,
            2,
            ControlPlaneAuthOperation::RaftVote,
            request_frame.clone(),
        );
        let wrong_target_frame = experimental_raft_signed_peer_frame(
            &cluster_name,
            valid_source_credential.clone(),
            1,
            ControlPlaneAuthOperation::RaftVote,
            request_frame.clone(),
        );
        let wrong_role_frame = experimental_raft_signed_peer_frame(
            &cluster_name,
            valid_source_credential.clone(),
            2,
            ControlPlaneAuthOperation::RaftPreVote,
            request_frame.clone(),
        );
        let unknown_credential = experimental_raft_peer_auth_credential(
            &cluster_name,
            1,
            "unknown-raft-node-1",
            1,
            "node-1-test-secret",
        );
        let unknown_credential_frame = experimental_raft_signed_peer_frame(
            &cluster_name,
            unknown_credential,
            2,
            ControlPlaneAuthOperation::RaftVote,
            request_frame.clone(),
        );
        let mut tampered_payload = valid_envelope.payload().to_vec();
        let last_payload_byte = tampered_payload
            .last_mut()
            .expect("vote request payload should be non-empty");
        *last_payload_byte ^= 0x01;
        let payload_bitflip_frame = ControlPlaneAuthEnvelope::new(
            storage::control_plane_auth::ControlPlaneAuthEnvelopeInput {
                header: valid_envelope.header().clone(),
                payload: tampered_payload,
                authenticator: valid_envelope.authenticator().to_vec(),
            },
        )
        .expect("payload-bitflip test auth envelope should rebuild")
        .encode_frame()
        .expect("payload-bitflip test auth envelope should encode");
        let stale_credential = experimental_raft_peer_auth_credential(
            &cluster_name,
            1,
            "raft-node-1",
            1,
            "node-1-test-secret",
        );
        let stale_credential_frame = experimental_raft_signed_peer_frame(
            &cluster_name,
            stale_credential,
            2,
            ControlPlaneAuthOperation::RaftVote,
            request_frame.clone(),
        );

        for (case_name, policy_node_1_version, frame) in [
            ("missing-auth", 1, request_frame),
            ("wrong-cluster", 1, wrong_cluster_frame),
            ("wrong-source", 1, wrong_source_frame),
            ("wrong-target", 1, wrong_target_frame),
            ("wrong-role", 1, wrong_role_frame),
            ("bad-mac", 1, bad_mac_frame),
            ("payload-bitflip", 1, payload_bitflip_frame),
            ("stale-credential", 2, stale_credential_frame),
            ("unknown-credential", 1, unknown_credential_frame),
        ] {
            let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
                cluster_name.clone(),
                [(1, "node-1".to_string()), (2, "node-2".to_string())],
                ControlPlaneRaftPeerTransportLimits::default(),
            )
            .with_auth_policy(experimental_raft_peer_auth_policy(
                &cluster_name,
                2,
                policy_node_1_version,
            ));
            let before_status = harness
                .runtime
                .block_on(harness.authority.status())
                .unwrap_or_else(|error| panic!("{case_name}: status should read before: {error}"));
            let (mut client_stream, mut server_stream) =
                UnixStream::pair().expect("test UnixStream pair should create");
            write_control_plane_raft_peer_transport_frame(&mut client_stream, &frame)
                .unwrap_or_else(|error| panic!("{case_name}: client should write frame: {error}"));
            client_stream
                .set_read_timeout(Some(Duration::from_millis(50)))
                .expect("client stream read timeout should set");

            let result = handle_experimental_raft_peer_rpc_before_ack(
                harness.runtime.handle(),
                &harness.authority,
                &mut server_stream,
                2,
                &policy,
                ExperimentalRaftPeerRpcDurability {
                    artifact_path: None,
                    checkpoint_lock: None,
                    publication: None,
                },
            );
            assert!(
                matches!(result, Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(_))),
                "{case_name}: bad auth peer RPC should fail before dispatch: {result:?}"
            );
            let after_status = harness
                .runtime
                .block_on(harness.authority.status())
                .unwrap_or_else(|error| panic!("{case_name}: status should read after: {error}"));
            assert_eq!(
                after_status.persisted_vote(),
                before_status.persisted_vote(),
                "{case_name}: bad auth peer RPC must not mutate persisted vote"
            );
            assert_eq!(
                after_status.current_term(),
                before_status.current_term(),
                "{case_name}: bad auth peer RPC must not mutate current term"
            );
            drop(server_stream);

            let response = read_control_plane_raft_peer_transport_frame(
                &mut client_stream,
                ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
            );
            assert!(
                response.is_err(),
                "{case_name}: bad auth peer RPC must not write a response"
            );

            let metrics = policy
                .auth_policy()
                .expect("test policy should have auth policy")
                .metrics_snapshot();
            assert_eq!(
                metrics.accepted_total(),
                0,
                "{case_name}: bad auth peer RPC must not count as accepted"
            );
            assert_eq!(
                metrics.rejected_total(),
                1,
                "{case_name}: bad auth peer RPC must be counted as rejected"
            );
            assert_eq!(
                metrics.rejected_without_operation_total(),
                u64::from(case_name == "missing-auth"),
                "{case_name}: only malformed envelope frames should lack operation attribution"
            );
        }

        harness.shutdown();
    }

    #[test]
    fn experimental_raft_peer_rpc_rejects_expired_transfer_leader_auth_before_dispatch() {
        let harness = experimental_raft_test_harness("peer-transfer-leader-auth-before-dispatch");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-transfer-leader-auth-before-dispatch-{}",
            std::process::id()
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name.clone(), 1, 2);
        let request_frame = ControlPlaneRaftPeerRpcRequest::TransferLeader(
            TransferLeaderRequest::new(Vote::<ControlPlaneRaftLeaderId>::new(3, 1), 2, None),
        )
        .encode_frame_for_peer(&identity)
        .expect("transfer-leader peer request should encode");
        let expired_frame = experimental_raft_peer_auth_credential(
            &cluster_name,
            1,
            "raft-node-1",
            1,
            "node-1-test-secret",
        )
        .sign_envelope(ControlPlaneAuthSignInput {
            target: ControlPlaneAuthTarget::Principal(ControlPlaneAuthPrincipal::RaftPeer {
                node_id: 2,
            }),
            operation: ControlPlaneAuthOperation::RaftTransferLeader,
            issued_at_ms: Some(1),
            expires_at_ms: Some(2),
            sequence: None,
            nonce: Vec::new(),
            payload: request_frame,
        })
        .expect("expired transfer-leader auth envelope should sign")
        .encode_frame()
        .expect("expired transfer-leader auth envelope should encode");
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string()), (2, "node-2".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        )
        .with_auth_policy(experimental_raft_peer_auth_policy(&cluster_name, 2, 1));
        let before_status = harness
            .runtime
            .block_on(harness.authority.status())
            .expect("status should read before expired transfer-leader auth");
        let (mut client_stream, mut server_stream) =
            UnixStream::pair().expect("test UnixStream pair should create");
        write_control_plane_raft_peer_transport_frame(&mut client_stream, &expired_frame)
            .expect("client should write expired transfer-leader auth frame");
        client_stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("client stream read timeout should set");

        let result = handle_experimental_raft_peer_rpc_before_ack(
            harness.runtime.handle(),
            &harness.authority,
            &mut server_stream,
            2,
            &policy,
            ExperimentalRaftPeerRpcDurability {
                artifact_path: None,
                checkpoint_lock: None,
                publication: None,
            },
        );
        assert!(
            matches!(result, Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(_))),
            "expired transfer-leader auth should fail before dispatch: {result:?}"
        );
        let after_status = harness
            .runtime
            .block_on(harness.authority.status())
            .expect("status should read after expired transfer-leader auth");
        assert_eq!(
            after_status.persisted_vote(),
            before_status.persisted_vote(),
            "expired transfer-leader auth must not mutate persisted vote"
        );
        assert_eq!(
            after_status.current_term(),
            before_status.current_term(),
            "expired transfer-leader auth must not mutate current term"
        );
        drop(server_stream);

        let response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            response.is_err(),
            "expired transfer-leader auth must not write a response"
        );
        let metrics = policy
            .auth_policy()
            .expect("test policy should have auth policy")
            .metrics_snapshot();
        assert_eq!(metrics.accepted_total(), 0);
        assert_eq!(metrics.rejected_total(), 1);
        assert_eq!(
            metrics.rejected_for_operation(ControlPlaneAuthOperation::RaftTransferLeader),
            1
        );
        assert_eq!(
            metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure),
            1
        );

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
                secret: SecretConfigValue::new("node-2-old-test-secret".to_string()),
            },
            ConfiguredControlPlaneRaftAuthCredential {
                node_id: 2,
                credential_id: "raft-node-2".to_string(),
                credential_version: 2,
                secret: SecretConfigValue::new("node-2-new-test-secret".to_string()),
            },
            ConfiguredControlPlaneRaftAuthCredential {
                node_id: 1,
                credential_id: "raft-node-1".to_string(),
                credential_version: 1,
                secret: SecretConfigValue::new("node-1-test-secret".to_string()),
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
    fn control_plane_unix_auth_diagnostics_are_redacted() {
        let verifier = ControlPlaneUnixAuthVerifier::new(
            "control-auth",
            vec![
                ControlPlaneStorageNodeAuthCredential::new(
                    ControlPlaneStorageNodeAuthCredentialInput {
                        node_id: NodeId::new(7),
                        credential_id: "storage-node-7".to_owned(),
                        credential_version: 3,
                        secret: b"storage-node-7-test-secret".to_vec(),
                    },
                )
                .expect("test storage-node credential should build"),
                ControlPlaneStorageNodeAuthCredential::new(
                    ControlPlaneStorageNodeAuthCredentialInput {
                        node_id: NodeId::new(7),
                        credential_id: "storage-node-7".to_owned(),
                        credential_version: 4,
                        secret: b"storage-node-7-new-test-secret".to_vec(),
                    },
                )
                .expect("test rotated storage-node credential should build"),
            ],
        )
        .expect("test Unix auth verifier should build")
        .with_frontend_credentials(vec![
            ControlPlaneFrontendAuthCredential::new(ControlPlaneFrontendAuthCredentialInput {
                instance_id: "frontend-1".to_owned(),
                credential_id: "frontend".to_owned(),
                credential_version: 5,
                secret: b"frontend-1-test-secret".to_vec(),
            })
            .expect("test frontend credential should build"),
            ControlPlaneFrontendAuthCredential::new(ControlPlaneFrontendAuthCredentialInput {
                instance_id: "frontend-1".to_owned(),
                credential_id: "frontend".to_owned(),
                credential_version: 6,
                secret: b"frontend-1-new-test-secret".to_vec(),
            })
            .expect("test rotated frontend credential should build"),
        ])
        .expect("test frontend credentials should install")
        .with_admin_credentials(vec![
            ControlPlaneAdminAuthCredential::new(ControlPlaneAdminAuthCredentialInput {
                instance_id: "admin-1".to_owned(),
                credential_id: "admin".to_owned(),
                credential_version: 7,
                secret: b"admin-1-test-secret".to_vec(),
            })
            .expect("test admin credential should build"),
            ControlPlaneAdminAuthCredential::new(ControlPlaneAdminAuthCredentialInput {
                instance_id: "admin-1".to_owned(),
                credential_id: "admin".to_owned(),
                credential_version: 8,
                secret: b"admin-1-new-test-secret".to_vec(),
            })
            .expect("test rotated admin credential should build"),
        ])
        .expect("test admin credentials should install");

        let error = verifier
            .verify_storage_node_heartbeat_request_payload(b"not an auth envelope", 2_000)
            .expect_err("missing auth envelope should reject");
        assert!(
            error.to_string().contains("auth magic"),
            "unexpected error: {error}"
        );

        let diagnostics = format_control_plane_unix_auth_diagnostics(&verifier);
        assert!(diagnostics.contains("required=true"), "{diagnostics}");
        assert!(
            diagnostics.contains("storage_node_heartbeat_required=true"),
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
            diagnostics.contains("cluster_id=control-auth"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("storage_node_credentials=2"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("frontend_credentials=2"),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("admin_credentials=2"), "{diagnostics}");
        assert!(
            diagnostics.contains(
                "storage_node_credential{node_id=\"7\",credential_id=\"storage-node-7\",credential_version=\"3\"} 1"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "storage_node_credential{node_id=\"7\",credential_id=\"storage-node-7\",credential_version=\"4\"} 1"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "frontend_credential{instance_id=\"frontend-1\",credential_id=\"frontend\",credential_version=\"5\"} 1"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "frontend_credential{instance_id=\"frontend-1\",credential_id=\"frontend\",credential_version=\"6\"} 1"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "admin_credential{instance_id=\"admin-1\",credential_id=\"admin\",credential_version=\"7\"} 1"
            ),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(
                "admin_credential{instance_id=\"admin-1\",credential_id=\"admin\",credential_version=\"8\"} 1"
            ),
            "{diagnostics}"
        );
        assert!(diagnostics.contains("accepted_total=0"), "{diagnostics}");
        assert!(diagnostics.contains("rejected_total=1"), "{diagnostics}");
        assert!(
            diagnostics.contains("rejected_by_operation{operation=\"StorageRuntimeMapRefresh\"} 1"),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains("rejected_by_reason{reason=\"Missing\"} 1"),
            "{diagnostics}"
        );
        assert!(!diagnostics.contains("storage-node-7-test-secret"));
        assert!(!diagnostics.contains("storage-node-7-new-test-secret"));
        assert!(!diagnostics.contains("frontend-1-test-secret"));
        assert!(!diagnostics.contains("frontend-1-new-test-secret"));
        assert!(!diagnostics.contains("admin-1-test-secret"));
        assert!(!diagnostics.contains("admin-1-new-test-secret"));
        assert!(!diagnostics.contains("payload"));
        assert!(!diagnostics.contains("authenticator"));
        assert!(!diagnostics.contains("not an auth envelope"));
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
        let wal_path = durable_artifact_wal_path(&state_path);
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-wal-ack-{}",
            std::process::id()
        );
        let authority = runtime.block_on(async {
            let authority =
                ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
                    cluster_name.clone(),
                    1,
                    &state_path,
                    &wal_path,
                )
                .await
                .expect("WAL-backed durable authority should initialize");
            authority
                .store_durable_restart_artifact(&state_path)
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

        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name.clone(), 1, 1);
        let expected_vote = Vote::<ControlPlaneRaftLeaderId>::new(3, 1);
        let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: expected_vote,
            last_log_id: None,
            leadership_transfer: false,
        });
        let request_frame = request
            .encode_frame_for_peer(&identity)
            .expect("peer request should encode");
        let (mut client_stream, mut server_stream) =
            UnixStream::pair().expect("test UnixStream pair should create");
        write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
            .expect("client should write request frame");
        let checkpoint_lock = Arc::new(Mutex::new(()));
        let publication = ExperimentalRaftDurabilityPublication::new();
        let durability_context = ExperimentalRaftPeerDurabilityContext {
            artifact_path: Some(Arc::new(state_path.clone())),
            checkpoint_lock,
            publication,
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

        let result = handle_experimental_raft_peer_rpc_before_ack(
            runtime.handle(),
            &authority,
            &mut server_stream,
            1,
            &policy,
            ExperimentalRaftPeerRpcDurability::from_context(&durability_context),
        );
        assert!(
            result.is_ok(),
            "peer RPC should acknowledge after its WAL mutation is durable: {result:?}"
        );
        let response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("peer RPC should write a response frame");
        assert!(
            !response.is_empty(),
            "peer RPC response frame should be non-empty"
        );
        assert_eq!(
            durable_raft_artifact_vote(&state_path),
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
            durable_raft_artifact_vote(&state_path),
            Some(expected_vote),
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

        let failed_response_vote = Vote::<ControlPlaneRaftLeaderId>::new(4, 1);
        let failed_response_request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: failed_response_vote,
            last_log_id: None,
            leadership_transfer: false,
        });
        let failed_response_frame = failed_response_request
            .encode_frame_for_peer(&identity)
            .expect("second peer request should encode");
        let (mut second_client_stream, mut second_server_stream) =
            UnixStream::pair().expect("second test UnixStream pair should create");
        write_control_plane_raft_peer_transport_frame(
            &mut second_client_stream,
            &failed_response_frame,
        )
        .expect("second client should write request frame");
        let failed_response = handle_experimental_raft_peer_rpc_with_response_writer(
            runtime.handle(),
            &authority,
            &mut second_server_stream,
            1,
            &policy,
            ExperimentalRaftPeerRpcDurability::from_context(&durability_context),
            |_stream, _response_frame| {
                Err(ControlPlaneError::Io {
                    context: "write injected failed peer response",
                    source: io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "injected post-dispatch response failure",
                    ),
                })
            },
        );
        assert!(
            matches!(
                &failed_response,
                Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(
                    ControlPlaneError::Io {
                        context: "write injected failed peer response",
                        source,
                    }
                )) if source.kind() == io::ErrorKind::BrokenPipe
            ),
            "injected post-dispatch response write should fail: {failed_response:?}"
        );
        assert!(
            checkpoint_experimental_raft_peer_wal_if_due(
                runtime.handle(),
                &authority,
                &durability_context,
                &mut checkpoint_tracker,
                Instant::now(),
            )
            .expect("failed-response checkpoint should succeed"),
            "WAL observer should see a durable mutation despite response failure"
        );
        assert_eq!(
            durable_raft_artifact_vote(&state_path),
            Some(failed_response_vote),
            "failed response must not strand its durable WAL mutation outside checkpoint scheduling"
        );

        let no_op_metrics_before = authority.durability_metric_snapshots();
        let no_op_offsets_before = runtime
            .block_on(authority.status())
            .expect("status before no-op vote should read")
            .durable_wal_offsets();
        let request = ControlPlaneRaftPeerRpcRequest::Vote(VoteRequest {
            vote: failed_response_vote,
            last_log_id: None,
            leadership_transfer: false,
        });
        let request_frame = request
            .encode_frame_for_peer(&identity)
            .expect("peer request should encode");
        let (mut client_stream, mut server_stream) =
            UnixStream::pair().expect("test UnixStream pair should create");
        write_control_plane_raft_peer_transport_frame(&mut client_stream, &request_frame)
            .expect("client should write request frame");
        let result = handle_experimental_raft_peer_rpc_before_ack(
            runtime.handle(),
            &authority,
            &mut server_stream,
            1,
            &policy,
            ExperimentalRaftPeerRpcDurability::from_context(&durability_context),
        );
        assert!(
            result.is_ok(),
            "unchanged durable Raft state should not require a new checkpoint: {result:?}"
        );
        read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("no-op peer vote should receive a response");
        assert_eq!(
            authority.durability_metric_snapshots().wal,
            no_op_metrics_before.wal,
            "no-op peer vote must not append a WAL record"
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
    fn experimental_raft_wal_monitor_does_not_enter_state_machine() {
        let state_dir = short_unix_socket_test_dir("experimental-raft-wal-monitor-lock");
        let state_path = state_dir.0.path().join("control-plane.state");
        let harness = experimental_raft_durable_wal_test_harness(
            "wal-monitor-state-machine-lock",
            &state_path,
        );
        let expected_offsets = harness
            .authority
            .durable_wal_monitor_snapshot()
            .expect("baseline WAL monitor snapshot should read")
            .offsets();

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let runtime_handle = harness.runtime.handle().clone();
        let boundary_authority = Arc::clone(&harness.authority);
        let boundary_thread = thread::spawn(move || {
            runtime_handle
                .block_on(
                    boundary_authority
                        .raft()
                        .with_state_machine(move |_state_machine| {
                            entered_tx
                                .send(())
                                .expect("state-machine boundary entry should signal");
                            Box::pin(async move {
                                release_rx
                                    .recv()
                                    .expect("state-machine boundary release should arrive");
                            })
                        }),
                )
                .expect("state-machine boundary should remain available");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("state-machine boundary should be held");

        let (monitor_tx, monitor_rx) = std::sync::mpsc::channel();
        let monitor_authority = Arc::clone(&harness.authority);
        let monitor_thread = thread::spawn(move || {
            let result = monitor_authority.durable_wal_monitor_snapshot();
            monitor_tx
                .send(result)
                .expect("WAL monitor result should be observed");
        });
        let monitor_result = monitor_rx.recv_timeout(Duration::from_secs(1));
        release_tx
            .send(())
            .expect("state-machine boundary should release");
        boundary_thread
            .join()
            .expect("state-machine boundary thread should finish");
        monitor_thread
            .join()
            .expect("WAL monitor thread should finish");
        let monitor_snapshot = monitor_result
            .expect("WAL monitor must not wait for the state-machine boundary")
            .expect("WAL monitor snapshot should read");
        assert_eq!(monitor_snapshot.offsets(), expected_offsets);
        assert_eq!(monitor_snapshot.poisoned(), None);

        harness.shutdown();
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
        let wal_path = durable_artifact_wal_path(&state_path);
        let cluster_name = format!(
            "argmin-s3-experimental-raft-local-election-checkpoint-{}",
            std::process::id()
        );
        let (authority, initial_vote) = runtime.block_on(async {
            let authority =
                ControlPlaneRaftAuthority::new_experimental_single_node_durable_with_wal(
                    cluster_name,
                    1,
                    &state_path,
                    &wal_path,
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
                .store_durable_restart_artifact(&state_path)
                .await
                .expect("leader baseline authority state should checkpoint");
            let status = authority
                .status()
                .await
                .expect("baseline authority status should read before step-down");
            let initial_term = status
                .current_term()
                .expect("baseline authority should have a current term");
            let response = authority
                .raft()
                .vote(VoteRequest {
                    vote: Vote::new(initial_term + 1, 2),
                    last_log_id: status.last_log_id(),
                    leadership_transfer: true,
                })
                .await
                .expect("higher peer vote should step down the local leader");
            assert!(response.vote_granted);
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
        let publication = ExperimentalRaftDurabilityPublication::new();
        let checkpoint_loop = spawn_experimental_raft_peer_checkpoint_loop(
            runtime.handle().clone(),
            Arc::clone(&authority),
            ExperimentalRaftPeerDurabilityContext {
                artifact_path: Some(Arc::new(state_path.clone())),
                checkpoint_lock: Arc::new(Mutex::new(())),
                publication: publication.clone(),
            },
            ExperimentalRaftPeerCheckpointPolicy {
                max_wal_suffix_bytes: u64::MAX,
                max_mutations: u64::MAX,
                max_delay: Duration::from_millis(100),
                poll_interval: Duration::from_millis(10),
            },
        );

        runtime.block_on(async {
            authority
                .raft()
                .trigger()
                .elect(false)
                .await
                .expect("local election should trigger");
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    let vote = authority
                        .status()
                        .await
                        .expect("authority status should read during local election")
                        .persisted_vote()
                        .expect("local election should persist a vote");
                    if vote.leader_id.term > initial_vote.leader_id.term {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("local election should advance the persisted vote");
        });
        let elected_vote = runtime
            .block_on(authority.status())
            .expect("post-election authority status should read")
            .persisted_vote()
            .expect("post-election authority should have a persisted vote");
        let checkpoint_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let status = runtime
                .block_on(authority.status())
                .expect("authority status should read while awaiting checkpoint");
            let offsets = status
                .durable_wal_offsets()
                .expect("WAL-backed authority should report offsets");
            if durable_raft_artifact_vote(&state_path) == Some(elected_vote)
                && offsets.base_offset() == offsets.clean_len()
            {
                break;
            }
            assert!(
                Instant::now() < checkpoint_deadline,
                "WAL observer did not checkpoint locally initiated election"
            );
            thread::sleep(Duration::from_millis(10));
        }

        publication.publish_poison(|| {});
        checkpoint_loop
            .join()
            .expect("checkpoint observer should stop after poison gate closes");
        runtime
            .block_on(authority.shutdown())
            .expect("WAL-backed authority should shut down");
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn experimental_raft_captured_checkpoint_persists_outside_state_machine_boundary() {
        let state_dir = short_unix_socket_test_dir("captured-checkpoint-isolation");
        let state_path = state_dir.0.path().join("control-plane.state");
        let mut harness = experimental_raft_durable_wal_test_harness(
            "captured-checkpoint-state-machine-isolation",
            &state_path,
        );
        let mut config = test_server_config();
        config.storage_node_sockets = vec![config::ConfiguredStorageNodeSocket {
            node_id: 1,
            socket_path: "/tmp/argmin-checkpoint-isolation-node-1.sock".to_string(),
        }];
        config.storage_pg_ids = vec![0];
        bootstrap_empty_experimental_raft_control_plane(&mut harness.control_plane, &config)
            .expect("checkpoint-isolation authority should bootstrap");
        let checkpoint = harness
            .control_plane
            .block_on(harness.authority.capture_durable_restart_checkpoint())
            .expect("restart checkpoint should capture before holding the state machine");
        let state_path = harness
            .control_plane
            .durable_artifact_path
            .as_ref()
            .expect("durable harness should retain an artifact path")
            .as_ref()
            .clone();

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let runtime_handle = harness.runtime.handle().clone();
        let boundary_authority = Arc::clone(&harness.authority);
        let boundary_thread = thread::spawn(move || {
            runtime_handle
                .block_on(
                    boundary_authority
                        .raft()
                        .with_state_machine(move |_state_machine| {
                            entered_tx
                                .send(())
                                .expect("state-machine boundary entry should signal");
                            Box::pin(async move {
                                release_rx
                                    .recv()
                                    .expect("state-machine boundary release should arrive");
                            })
                        }),
                )
                .expect("state-machine boundary should remain available");
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("state-machine boundary should be held");

        let (persist_tx, persist_rx) = std::sync::mpsc::channel();
        let persist_authority = Arc::clone(&harness.authority);
        let persist_thread = thread::spawn(move || {
            let result =
                persist_authority.persist_durable_restart_checkpoint(checkpoint, &state_path);
            persist_tx
                .send(result)
                .expect("checkpoint persistence result should be observed");
        });
        let persist_result = persist_rx.recv_timeout(Duration::from_secs(2));
        release_tx
            .send(())
            .expect("state-machine boundary should release");
        boundary_thread
            .join()
            .expect("state-machine boundary thread should finish");
        persist_thread
            .join()
            .expect("checkpoint persistence thread should finish");
        persist_result
            .expect("captured checkpoint persistence must not wait for the state machine")
            .expect("captured checkpoint should persist");

        harness.shutdown();
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
        let checkpoint_before =
            fs::read(&state_path).expect("baseline restart artifact should read");
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
            fs::read(&state_path).expect("restart artifact should remain readable"),
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
        let artifact_before_purge =
            fs::read(&state_path).expect("pre-purge snapshot artifact should read");
        harness
            .control_plane
            .block_on(
                harness
                    .authority
                    .purge_log_through_snapshot(snapshot_log_id),
            )
            .expect("snapshot-covered log prefix should purge");
        assert_eq!(
            fs::read(&state_path).expect("artifact should remain readable after purge"),
            artifact_before_purge,
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
            .block_on(
                harness
                    .authority
                    .raft()
                    .with_state_machine(|state_machine| {
                        let snapshot = state_machine.inner().snapshot().clone();
                        Box::pin(async move { snapshot })
                    }),
            )
            .expect("pre-crash state-machine snapshot should read");
        harness.shutdown();

        let restarted =
            experimental_raft_durable_wal_test_harness("wal-snapshot-purge-restart", &state_path);
        let restored = restarted
            .control_plane
            .block_on(
                restarted
                    .authority
                    .raft()
                    .with_state_machine(|state_machine| {
                        let snapshot = state_machine.inner().snapshot().clone();
                        Box::pin(async move { snapshot })
                    }),
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
            ControlPlaneError::RpcRemote { message } if message.contains("unknown node 99")
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
        let changed_epoch =
            set_control_plane_pg_acting_set_live(&socket_path, PgId::new(19), vec![NodeId::new(2)])
                .expect("live acting-set helper should succeed");
        server.join().unwrap();

        assert!(changed_epoch > bootstrap_epoch);
        let snapshot = harness
            .control_plane
            .current_snapshot()
            .expect("experimental snapshot should read after acting-set helper");
        assert_eq!(snapshot.cluster_epoch(), changed_epoch);
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
            ControlPlaneError::RpcRemote { message }
                if message.contains("PG 19 acting set references unknown node 99")
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
            .block_on(
                harness
                    .authority
                    .raft()
                    .with_state_machine(|state_machine| {
                        let deadline = state_machine
                            .inner()
                            .snapshot()
                            .pg(PgId::new(13))
                            .and_then(|pg| pg.metadata_transfer_fence_source_lease_deadline_ms());
                        Box::pin(async move { deadline })
                    }),
            )
            .expect("durable metadata-transfer fence state should read");
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
        let transfer_runtime_map = client
            .set_pg_acting_set_with_metadata_transfer_runtime_map(
                PgId::new(13),
                vec![NodeId::new(2)],
                transfer,
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
        let transfer_snapshot = harness
            .control_plane
            .set_pg_acting_set_with_metadata_transfer(PgId::new(13), vec![NodeId::new(2)], transfer)
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
        let fenced_epoch =
            fence_control_plane_pg_for_metadata_transfer_live(&socket_path, PgId::new(14))
                .expect("live metadata-transfer fence helper should succeed");
        fence_server.join().unwrap();
        assert!(fenced_epoch > active_epoch);

        std::fs::remove_file(&socket_path).unwrap();
        let transfer = PgMetadataTransferProof::new(active_epoch, active_proof);
        let install_server =
            spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 41_300);
        let installed_epoch = set_control_plane_pg_acting_set_with_metadata_transfer_live(
            &socket_path,
            PgId::new(14),
            vec![NodeId::new(2)],
            transfer,
        )
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
        assert_eq!(snapshot.cluster_epoch(), installed_epoch);
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
        let args = ["7", "12", "20", "30", "40", "20", "31", "40", "2", "3"]
            .into_iter()
            .map(OsString::from);

        let (pg_id, transfer, acting_set) =
            parse_control_plane_pg_acting_set_with_metadata_transfer_args(args).unwrap();

        assert_eq!(pg_id, PgId::new(7));
        assert_eq!(transfer.source_epoch(), ClusterEpoch::new(12).unwrap());
        assert_eq!(
            transfer.source_metadata_proof(),
            PgMetadataProof {
                applied_log_index: 20,
                applied_log_hash: 30,
                state_digest: 40
            }
        );
        assert_eq!(
            transfer.metadata_proof(),
            PgMetadataProof {
                applied_log_index: 20,
                applied_log_hash: 31,
                state_digest: 40
            }
        );
        assert_eq!(acting_set, vec![NodeId::new(2), NodeId::new(3)]);
    }

    #[test]
    fn metadata_transfer_retry_treats_active_peering_inspection_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::StorageRpc {
            node_id: 0,
            operation: "metadata command replica state",
            code: StorageRpcErrorCode::MetadataTransferHistoricalRouteActive,
            message: "historical peering inspection for PG 0 at epoch 28 requires Peering route, got active".to_string(),
        });

        assert!(metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_retry_treats_transport_timeout_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::StorageRpc {
            node_id: 0,
            operation: "read storage RPC response",
            code: StorageRpcErrorCode::TransportTimeout,
            message: "storage RPC stream I/O error: timed out".to_string(),
        });

        assert!(metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_retry_treats_transport_closed_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::StorageRpc {
            node_id: 0,
            operation: "read storage RPC response",
            code: StorageRpcErrorCode::TransportClosed,
            message: "storage RPC stream I/O error: early eof".to_string(),
        });

        assert!(metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_retry_treats_metadata_command_contention_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::StorageRpc {
            node_id: 2,
            operation: "metadata command pending envelope",
            code: StorageRpcErrorCode::MetadataCommandContention,
            message: "metadata command lock wait for PG 9 exceeded 500ms".to_string(),
        });

        assert!(metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_retry_treats_expired_route_map_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::RouteMapExpired {
            cluster_epoch: ClusterEpoch::new(31).unwrap(),
            valid_until_ms: 2_000,
            now_ms: 2_001,
        });

        assert!(metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_retry_treats_apply_route_expiry_as_transient() {
        let error = PgMetadataTransferError::Apply(storage::BucketSnapshotLoadError::Store(
            StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::new(32).unwrap(),
                valid_until_ms: 3_000,
                now_ms: 3_001,
            },
        ));

        assert!(metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_import_refreshes_expired_destination_route() {
        let tmp = short_unix_socket_test_dir("metadata-transfer-import-refresh");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let store = FileControlPlaneStore::new(tmp.join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        let source_node_id = NodeId::new(1);
        let destination_node_id = NodeId::new(2);
        let pg_id = PgId::new(7);
        let source_proof = PgMetadataProof::empty();
        let mut now_ms = storage::clock::current_time_millis();

        for node_id in [source_node_id, destination_node_id] {
            authority
                .set_node_membership(node_id, NodeMembershipState::Active)
                .unwrap();
            authority
                .submit_node_heartbeat(
                    NodeHeartbeat {
                        node_id,
                        node_incarnation: 1,
                        endpoint: tmp
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .display()
                            .to_string(),
                        observed_epoch: authority.snapshot().cluster_epoch(),
                        requested_lease_duration_ms: 10_000,
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: Vec::new(),
                    },
                    now_ms,
                )
                .unwrap();
            now_ms += 1;
        }
        authority
            .set_pg_acting_set(pg_id, vec![source_node_id])
            .unwrap();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id: source_node_id,
                    node_incarnation: 1,
                    endpoint: tmp.join("node-1.sock").display().to_string(),
                    observed_epoch: authority.snapshot().cluster_epoch(),
                    requested_lease_duration_ms: 10_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Peering,
                        metadata_proof: source_proof,
                        pending_metadata_command: None,
                    }],
                },
                now_ms,
            )
            .unwrap();
        now_ms += 1;
        authority
            .complete_pg_peering(pg_id, source_node_id, 1, now_ms)
            .unwrap();
        let source_epoch = authority.snapshot().cluster_epoch();
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id: source_node_id,
                    node_incarnation: 1,
                    endpoint: tmp.join("node-1.sock").display().to_string(),
                    observed_epoch: source_epoch,
                    requested_lease_duration_ms: 10_000,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id,
                        state: PgState::Active,
                        metadata_proof: source_proof,
                        pending_metadata_command: None,
                    }],
                },
                now_ms + 1,
            )
            .unwrap();
        authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
        let imported_proof = PgMetadataProof {
            applied_log_index: 0,
            applied_log_hash: 0,
            state_digest: 0x1234,
        };
        let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
            source_epoch,
            source_proof,
            imported_proof,
        );
        let transfer_snapshot = authority
            .set_pg_acting_set_with_metadata_transfer(pg_id, vec![destination_node_id], transfer)
            .unwrap();
        let destination_epoch = transfer_snapshot.cluster_epoch();
        let runtime_map = ControlPlaneRuntimeMapSource::pg_runtime_map_snapshot(
            &authority,
            pg_id,
            storage::clock::current_time_millis(),
        )
        .unwrap();
        let mut config = test_server_config();
        config.process_role = ProcessRole::Frontend;
        config.ec_k = 1;
        config.ec_m = 0;
        let ec_config = EcConfig::new(1, 0).unwrap();
        let initial_cluster =
            build_frontend_storage_cluster_from_runtime_map(&config, &ec_config, &runtime_map)
                .unwrap();
        let initial_cluster_ptr = Arc::as_ptr(&initial_cluster);
        let mut attempts = 0;
        let context = MetadataTransferImportContext {
            config: &config,
            ec_config: &ec_config,
            pg_id,
            acting_set: &[destination_node_id],
            destination_epoch,
            expected_transfer: transfer,
            imported_proof,
        };
        let mismatched_context = MetadataTransferImportContext {
            config: &config,
            ec_config: &ec_config,
            pg_id,
            acting_set: &[destination_node_id],
            destination_epoch,
            expected_transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
                source_epoch,
                source_proof,
                PgMetadataProof {
                    state_digest: imported_proof.state_digest + 1,
                    ..imported_proof
                },
            ),
            imported_proof,
        };
        let mismatch =
            match refresh_pg_metadata_transfer_import_route(&authority, &mismatched_context) {
                Err(error) => error,
                Ok(_) => panic!("mismatched transfer proof must fail closed"),
            };
        assert!(mismatch.contains(&format!(
            "expected epoch {}, state Peering, acting set {:?}, transfer {:?}",
            destination_epoch.get(),
            [destination_node_id],
            mismatched_context.expected_transfer,
        )));
        assert!(mismatch.contains(&format!(
            "actual epoch {}, state Peering, acting set {:?}, transfer {:?}",
            destination_epoch.get(),
            [destination_node_id],
            Some(transfer),
        )));

        let result =
            retry_pg_metadata_transfer_import(&authority, initial_cluster, &context, |cluster| {
                attempts += 1;
                if attempts == 1 {
                    return Err(PgMetadataTransferError::Store(
                        StoreError::RouteMapExpired {
                            cluster_epoch: destination_epoch,
                            valid_until_ms: 1,
                            now_ms: 2,
                        },
                    ));
                }
                assert!(
                    !std::ptr::eq(cluster, initial_cluster_ptr),
                    "retry must not reuse the expired destination cluster"
                );
                assert!(cluster.is_route_map_valid_at(storage::clock::current_time_millis()));
                Ok(imported_proof)
            })
            .unwrap();

        assert_eq!(result, imported_proof);
        assert_eq!(attempts, 2);
        drop(authority);
        std::fs::remove_dir_all(&tmp).unwrap();
    }

    #[test]
    fn metadata_transfer_retry_does_not_treat_active_route_mismatch_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::StorageRpc {
            node_id: 0,
            operation: "metadata command replica state",
            code: StorageRpcErrorCode::InactivePgRoute,
            message: "historical peering inspection for PG 0 at epoch 28 requires Peering route, got peering".to_string(),
        });

        assert!(!metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_active_check_treats_incomplete_runtime_map_as_retryable() {
        assert!(
            control_plane_metadata_transfer_observation_error_is_retryable(
                &ControlPlaneError::RpcRemote {
                    message: "PG 1 has no serving primary in cluster epoch 26".to_string(),
                }
            )
        );
        assert!(
            control_plane_metadata_transfer_observation_error_is_retryable(
                &ControlPlaneError::RpcRemote {
                    message:
                        "PG 0 primary node 2 has not reported active state in cluster epoch 31"
                            .to_string(),
                }
            )
        );
        assert!(control_plane_metadata_transfer_observation_error_is_retryable(
            &ControlPlaneError::RpcRemote {
                message:
                    "node 1 reported unresolved pending metadata command for PG 27 in cluster epoch 83"
                        .to_string(),
            }
        ));
        assert!(
            control_plane_metadata_transfer_observation_error_is_retryable(
                &ControlPlaneError::RpcRemote {
                    message: "control-plane runtime map has no routed PGs".to_string(),
                }
            )
        );
        assert!(
            control_plane_metadata_transfer_observation_error_is_retryable(
                &ControlPlaneError::Io {
                    context: "read control-plane RPC magic",
                    source: io::Error::from(io::ErrorKind::WouldBlock),
                }
            )
        );
        assert!(
            !control_plane_metadata_transfer_observation_error_is_retryable(
                &ControlPlaneError::RpcRemote {
                    message: "unknown PG 99".to_string(),
                }
            )
        );
    }

    #[test]
    fn metadata_transfer_active_check_uses_pg_scoped_runtime_map() {
        let tmp = short_unix_socket_test_dir("mpg");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("cp.sock");
        let endpoint = tmp.join("n0.sock");
        let server = serve_control_plane_with_active_pg_and_unserved_pg(
            socket_path.clone(),
            NodeId::new(0),
            endpoint.display().to_string(),
            2,
        );
        let control_plane = UnixControlPlaneClient::new(socket_path);

        let full_map_error = control_plane.runtime_map_snapshot(2_000).unwrap_err();
        assert!(
            control_plane_runtime_map_not_ready_for_serving(&full_map_error.to_string()),
            "expected unrelated PG to make full runtime map fail, got {full_map_error}"
        );
        assert!(
            matches!(
                refresh_pg_metadata_transfer_import_route(
                    &control_plane,
                    &MetadataTransferImportContext {
                        config: &test_server_config(),
                        ec_config: &EcConfig::new(1, 0).unwrap(),
                        pg_id: PgId::new(0),
                        acting_set: &[NodeId::new(0)],
                        destination_epoch: ClusterEpoch::new(1).unwrap(),
                        expected_transfer: PgMetadataTransferProof::new(
                            ClusterEpoch::new(1).unwrap(),
                            PgMetadataProof::empty(),
                        ),
                        imported_proof: PgMetadataProof::empty(),
                    },
                )
                .unwrap(),
                MetadataTransferImportRouteRefresh::Completed
            ),
            "target PG should be confirmable without requiring unrelated PGs to serve"
        );

        server.join().unwrap();
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn frontend_startup_retries_transient_control_plane_runtime_map_errors() {
        assert!(frontend_control_plane_startup_error_is_retryable(
            "failed to fetch control-plane runtime map from /tmp/control-plane.sock: control-plane RPC remote error: PG 1 has no serving primary in cluster epoch 35"
        ));
        assert!(frontend_control_plane_startup_error_is_retryable(
            "failed to fetch control-plane runtime map from /tmp/control-plane.sock: No such file or directory"
        ));
        assert!(frontend_control_plane_startup_error_is_retryable(
            "control-plane runtime map has no routed nodes"
        ));
        assert!(frontend_control_plane_startup_error_is_retryable(
            "control-plane runtime map has no routed PGs"
        ));
        assert!(!frontend_control_plane_startup_error_is_retryable(
            "ARGMIN_STORAGE_CLUSTER_EPOCH must be > 0"
        ));
    }

    #[test]
    fn metadata_transfer_source_lease_wait_duration_waits_until_deadline() {
        assert_eq!(
            metadata_transfer_source_lease_wait_duration(1_000, 1_250),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            metadata_transfer_source_lease_wait_duration(1_200, 1_250),
            Some(Duration::from_millis(50))
        );
        assert_eq!(
            metadata_transfer_source_lease_wait_duration(1_250, 1_250),
            None
        );
        assert_eq!(
            metadata_transfer_source_lease_wait_duration(1_251, 1_250),
            None
        );
    }

    #[test]
    fn control_plane_response_write_errors_use_bounded_io_categories() {
        let classify = |kind| {
            control_plane_rpc_response_write_error_kind(&ControlPlaneError::Io {
                context: "write test response",
                source: io::Error::from(kind),
            })
        };

        assert_eq!(
            classify(io::ErrorKind::BrokenPipe),
            observability::ControlPlaneRpcResponseWriteErrorKind::BrokenPipe
        );
        assert_eq!(
            classify(io::ErrorKind::ConnectionReset),
            observability::ControlPlaneRpcResponseWriteErrorKind::ConnectionReset
        );
        assert_eq!(
            classify(io::ErrorKind::WouldBlock),
            observability::ControlPlaneRpcResponseWriteErrorKind::Timeout
        );
        assert_eq!(
            classify(io::ErrorKind::PermissionDenied),
            observability::ControlPlaneRpcResponseWriteErrorKind::Other
        );
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
                "control_plane_raft_wal append_total=12 append_error_total=0 append_us_total=0 append_us_max=0 lock_wait_us_total=0 lock_wait_us_max=0 frame_bytes_total=0 frame_bytes_last=345 frame_bytes_max=0 file_sync_total=13 file_sync_us_total=0 file_sync_us_max=0 directory_sync_total=14"
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

        let (prepared_server, control_plane_node_incarnation) =
            build_storage_node_process_config(&config, &ec_config).unwrap();
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
    fn legacy_local_storage_cluster_preserves_explicit_node_id_and_data_dir() {
        let tmp = test_util::tempdir();
        let node_data_dir = tmp.path().join("manifest-node-data");
        let ec_config = EcConfig::new(1, 0).unwrap();
        let mut config = test_server_config();
        config.data_dir = tmp.path().join("unused-dense-layout").display().to_string();
        config.pg_count = 2;
        config.storage_node_ids = vec![17];
        config.storage_node_id = Some(17);
        config.storage_node_data_dir = Some(node_data_dir.display().to_string());

        let cluster = build_legacy_local_storage_cluster(&config, &ec_config).unwrap();

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
        assert!(!Path::new(&config.data_dir).exists());
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
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        std::thread::spawn(move || {
            use storage::control_plane::{
                handle_control_plane_unix_stream, ControlPlaneHeartbeatSink, NodeHeartbeat,
                NodeMembershipState,
            };
            use storage::PgId;

            let state_path = socket_path.with_extension("state");
            let store = FileControlPlaneStore::new(state_path);
            let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
            authority
                .set_node_membership(node_id, NodeMembershipState::Active)
                .unwrap();
            authority
                .set_pg_acting_set(PgId::new(0), vec![node_id])
                .unwrap();
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
                            pg_observations: pg_observations.clone(),
                        },
                        now_ms,
                    )
                    .unwrap();
                if lease.serving() {
                    break;
                }
                assert!(now_ms < 1_003, "authority did not grant serving lease");
            }
            if serving_pg_routes {
                authority
                    .complete_pg_peering(PgId::new(0), node_id, 1, 1_004)
                    .unwrap();
                authority
                    .submit_node_heartbeat(
                        NodeHeartbeat {
                            node_id,
                            node_incarnation: 1,
                            endpoint: endpoint.clone(),
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
                        1_005,
                    )
                    .unwrap();
            }
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_006).unwrap();
        })
    }

    fn serve_control_plane_with_active_pg_and_unserved_pg(
        socket_path: PathBuf,
        node_id: NodeId,
        endpoint: String,
        request_count: usize,
    ) -> std::thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        std::thread::spawn(move || {
            use storage::control_plane::{
                handle_control_plane_unix_stream, ControlPlaneHeartbeatSink, NodeHeartbeat,
                NodeMembershipState,
            };

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

            for request_index in 0..request_count {
                let (mut stream, _addr) = listener.accept().unwrap();
                handle_control_plane_unix_stream(
                    &mut authority,
                    &mut stream,
                    1_008 + u64::try_from(request_index).unwrap(),
                )
                .unwrap();
            }
        })
    }

    fn serve_control_plane_storage_node_startup_refreshes(
        socket_path: PathBuf,
        node_id: NodeId,
        request_count: usize,
    ) -> std::thread::JoinHandle<Vec<u64>> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        std::thread::spawn(move || {
            use storage::control_plane::{handle_control_plane_unix_stream, NodeMembershipState};
            use storage::PgId;

            let state_path = socket_path.with_extension("state");
            let store = FileControlPlaneStore::new(state_path);
            let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
            authority
                .set_node_membership(node_id, NodeMembershipState::Active)
                .unwrap();
            authority
                .set_pg_acting_set(PgId::new(0), vec![node_id])
                .unwrap();

            let mut observed_incarnations = Vec::with_capacity(request_count);
            for request_index in 0..request_count {
                let (mut stream, _addr) = listener.accept().unwrap();
                handle_control_plane_unix_stream(
                    &mut authority,
                    &mut stream,
                    1_000 + u64::try_from(request_index).unwrap(),
                )
                .unwrap();
                observed_incarnations.push(
                    authority
                        .snapshot()
                        .node(node_id)
                        .unwrap()
                        .node_incarnation(),
                );
            }
            observed_incarnations
        })
    }

    fn serve_control_plane_storage_node_startup_refresh_after_dropped_connection(
        socket_path: PathBuf,
        node_id: NodeId,
    ) -> std::thread::JoinHandle<Vec<u64>> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        std::thread::spawn(move || {
            use storage::control_plane::{handle_control_plane_unix_stream, NodeMembershipState};
            use storage::PgId;

            let state_path = socket_path.with_extension("state");
            let store = FileControlPlaneStore::new(state_path);
            let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
            authority
                .set_node_membership(node_id, NodeMembershipState::Active)
                .unwrap();
            authority
                .set_pg_acting_set(PgId::new(0), vec![node_id])
                .unwrap();

            let (dropped_stream, _addr) = listener.accept().unwrap();
            drop(dropped_stream);

            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_000).unwrap();
            vec![authority
                .snapshot()
                .node(node_id)
                .unwrap()
                .node_incarnation()]
        })
    }

    fn serve_frontend_control_plane_runtime_map_refresh(
        socket_path: PathBuf,
        node_id: NodeId,
        endpoint: String,
    ) -> std::thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        std::thread::spawn(move || {
            use storage::control_plane::{
                handle_control_plane_unix_stream, ControlPlaneHeartbeatSink, NodeHeartbeat,
            };
            use storage::PgId;

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

            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_004).unwrap();

            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_005).unwrap();

            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_006).unwrap();
        })
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

        let handle = StorageClusterRuntimeMapHandle::new(cluster);
        let mut refresh_loop =
            maybe_spawn_frontend_control_plane_refresh_loop(handle.clone(), &config).unwrap();
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

        let handle = StorageClusterRuntimeMapHandle::new(cluster);
        let mut refresh_loop =
            maybe_spawn_frontend_control_plane_refresh_loop(handle.clone(), &frontend_config)
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

        let (prepared_server, control_plane_node_incarnation) =
            build_storage_node_process_config(&config, &ec_config).unwrap();

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

        let (first_prepared, first_incarnation) =
            build_storage_node_process_config(&config, &ec_config).unwrap();
        let first_config = first_prepared.config().clone();
        drop(first_prepared);
        let (second_prepared, second_incarnation) =
            build_storage_node_process_config(&config, &ec_config).unwrap();
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

        let (prepared_server, control_plane_node_incarnation) =
            build_storage_node_process_config(&config, &ec_config).unwrap();

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
