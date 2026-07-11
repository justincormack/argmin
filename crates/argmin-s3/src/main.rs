mod config;

use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use auth::{AccountIdentity, CredentialRecord, CredentialStore};
use ec::EcConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use server_core::coordinator::Coordinator;
use server_core::sse::{
    ManagedWrappingKeyConfig, SseCustomerValidatorConfig, StaticManagedKeyProvider,
};
use storage::control_plane::{
    build_control_plane_authority_clock_admin_response, build_control_plane_unix_response,
    build_control_plane_unix_response_with_auth_and_response_clock,
    finish_control_plane_heartbeat_response, load_authority_clock_restart_checkpoint,
    prepare_control_plane_heartbeat_response, read_control_plane_unix_request,
    store_authority_clock_restart_checkpoint, store_validated_authority_clock_restart_checkpoint,
    write_control_plane_unix_response, AuthenticatedUnixControlPlaneClient, ClusterControlSnapshot,
    ClusterRuntimeMapSnapshot, ControlPlaneAdmin, ControlPlaneAdminAuthCredential,
    ControlPlaneAdminAuthCredentialInput, ControlPlaneAuthorityClock,
    ControlPlaneAuthorityClockAdminSample, ControlPlaneAuthorityClockCheckpointBinding,
    ControlPlaneAuthorityClockContext, ControlPlaneAuthorityClockStatus, ControlPlaneError,
    ControlPlaneFrontendAuthCredential, ControlPlaneFrontendAuthCredentialInput,
    ControlPlaneHeartbeatRefresh, ControlPlaneHeartbeatRuntimeMapSource,
    ControlPlaneRuntimeMapSource, ControlPlaneStorageNodeAuthCredential,
    ControlPlaneStorageNodeAuthCredentialInput, ControlPlaneUnixAuthVerifier,
    FencedPgMetadataTransferSnapshot, FileControlPlaneStore, PgMetadataProof,
    PgMetadataTransferProof, PgRouteSnapshot, SingleAuthorityControlPlane, UnixControlPlaneClient,
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
    ControlPlaneRaftNodeId, ControlPlaneRaftPeerAuthPolicy, ControlPlaneRaftPeerFrameIdentity,
    ControlPlaneRaftPeerFrameKind, ControlPlaneRaftPeerTransportLimits,
    ControlPlaneRaftPeerTransportPolicy,
};
use storage::storage_node_server::{
    advance_storage_node_incarnation, StorageNodeControlPlaneRefreshLoop, StorageNodeDataDirGuard,
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts, StorageNodeServer,
};
use storage::{
    CanonicalUserId, ClusterEpoch, EcShape, LocalClusterMap,
    LocalUnixStorageNodeClientAdmissionSettings, LocalUnixStorageNodeClientConfig, NodeId, PgId,
    PgMetadataTransferArtifact, PgMetadataTransferError, PgState, RouteMapValidity,
    SharedStorageNode, StorageCluster, StorageClusterRuntimeMapHandle, StorageRpcErrorCode,
    StoreError,
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
const CONTROL_PLANE_ACCEPT_BATCH_LIMIT: usize = 32;
const CONTROL_PLANE_RPC_WORKER_LIMIT: usize = 64;
const CONTROL_PLANE_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);
const CONTROL_PLANE_RAFT_PEER_RPC_WORKER_LIMIT: usize = 64;
const CONTROL_PLANE_RAFT_PEER_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);

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
    credentials.add_record(CredentialRecord {
        access_key_id: credential.access_key_id.clone(),
        secret_key: credential.secret_access_key.clone(),
        account: AccountIdentity::new(
            credential.principal.clone(),
            CanonicalUserId::from_principal(&credential.account_id),
            credential.display_name.clone(),
        ),
        authorization_profile: authorization_profile(credential.authorization_profile),
        expires_at_epoch_secs: None,
        enabled: true,
    });
}

fn build_credential_store(config: &ServerConfig) -> CredentialStore {
    let mut credentials = CredentialStore::new();
    let account = AccountIdentity::new(
        config.account_id.clone(),
        CanonicalUserId::from_principal(&config.account_id),
        config.account_id.clone(),
    );
    credentials.add_record(CredentialRecord {
        access_key_id: config.access_key_id.clone(),
        secret_key: config.secret_access_key.clone(),
        account,
        authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
        expires_at_epoch_secs: None,
        enabled: true,
    });
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
    if let Some(parent) = lock_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create control-plane lock directory {}: {error}",
                parent.display()
            )
        })?;
    }
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
    let config = match ServerConfig::from_env() {
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
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .authority_clock_status()
        .map_err(|error| format!("failed to read control-plane authority-clock status: {error}"))
}

fn reestablish_control_plane_authority_clock(
    socket_path: &Path,
) -> Result<ControlPlaneAuthorityClockStatus, String> {
    build_admin_control_plane_client_from_command_auth_env(socket_path)?
        .reestablish_authority_clock()
        .map_err(|error| format!("failed to re-establish control-plane authority clock: {error}"))
}

fn format_authority_clock_status(status: ControlPlaneAuthorityClockStatus) -> String {
    format!(
        "generation={} established={} blocked_reason={:?} committed_timestamp_high_water_ms={} bound_raft_term={} current_raft_term={} local_raft_authority_serving={}",
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
    let config =
        ServerConfig::from_env().map_err(|error| format!("configuration error: {error}"))?;
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
    let runtime_map = control_plane
        .runtime_map_snapshot(storage::clock::current_time_millis())
        .map_err(|error| format!("control-plane runtime map is not ready: {error}"))?;
    Ok(format_control_plane_runtime_map_diagnostics(&runtime_map))
}

fn format_control_plane_runtime_map_diagnostics(runtime_map: &ClusterRuntimeMapSnapshot) -> String {
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
    let mut diagnostics = format!(
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
        diagnostics.push('\n');
        diagnostics.push_str(&format!(
            "node_id={} incarnation={} endpoint={} storage_history_floor_epoch={}",
            node.node_id().as_u32(),
            node.node_incarnation(),
            node.endpoint(),
            format_optional_epoch(node.cluster_map_history_floor_epoch()),
        ));
    }
    diagnostics
}

fn format_optional_epoch(epoch: Option<ClusterEpoch>) -> String {
    epoch
        .map(|epoch| epoch.get().to_string())
        .unwrap_or_else(|| "-".to_owned())
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
    let authority_clock =
        ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
            authority.snapshot().max_committed_timestamp_ms(),
            restart_clock_checkpoint,
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to initialize control-plane authority clock: {error}");
            std::process::exit(1);
        });
    let authority_clock = Arc::new(Mutex::new(authority_clock));
    let authority_clock_checkpoint_target = Arc::new(AuthorityClockCheckpointTarget {
        path: PathBuf::from(state_path),
        binding: authority_clock_checkpoint_binding,
    });
    let authority = Arc::new(Mutex::new(authority));
    let active_rpc_workers = Arc::new(AtomicUsize::new(0));
    let auth_verifier = build_control_plane_unix_auth_verifier(config)
        .unwrap_or_else(|error| {
            eprintln!("failed to configure control-plane auth verifier: {error}");
            std::process::exit(1);
        })
        .map(Arc::new);
    process_info!(
        "argmin-s3 control-plane manager using state {} on {} (lease scan {} ms)",
        state_path,
        socket_path,
        config.control_plane_lease_scan_interval.as_millis()
    );
    if let Some(auth_verifier) = &auth_verifier {
        process_info!(
            "{}",
            format_control_plane_unix_auth_diagnostics(auth_verifier)
        );
    }

    loop {
        for _ in 0..CONTROL_PLANE_ACCEPT_BATCH_LIMIT {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    spawn_control_plane_rpc_worker(
                        stream,
                        Arc::clone(&authority),
                        Some(Arc::clone(&authority_clock)),
                        Some(Arc::clone(&authority_clock_checkpoint_target)),
                        true,
                        Arc::clone(&active_rpc_workers),
                        auth_verifier.clone(),
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    eprintln!("control-plane socket accept failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        let expiry = {
            let mut authority = authority
                .lock()
                .expect("control-plane authority mutex poisoned");
            authority_clock
                .lock()
                .expect("control-plane authority clock mutex poisoned")
                .effective_process_now_ms()
                .and_then(|effective_now_ms| authority.expire_heartbeat_leases(effective_now_ms))
        };
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

struct ExperimentalRaftControlPlane {
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    durable_artifact_path: Option<Arc<PathBuf>>,
    durable_checkpoint_lock: Option<Arc<Mutex<()>>>,
    durable_serving_checkpoint: Mutex<Option<ExperimentalRaftServingCheckpointMarker>>,
    checkpoint_serving_reads: bool,
    resample_authority_time: bool,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    durable_poison: Option<String>,
    durable_poison_gate: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExperimentalRaftServingCheckpointMarker {
    current_leader: Option<ControlPlaneRaftNodeId>,
    persisted_vote: Option<(u64, ControlPlaneRaftNodeId, bool)>,
    current_term: Option<u64>,
    committed: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    applied: Option<(u64, ControlPlaneRaftNodeId, u64)>,
}

impl ExperimentalRaftServingCheckpointMarker {
    fn from_status(status: &ControlPlaneRaftAuthorityStatus) -> Self {
        Self {
            current_leader: status.current_leader(),
            persisted_vote: status
                .persisted_vote()
                .map(|vote| (vote.leader_id.term, vote.leader_id.node_id, vote.committed)),
            current_term: status.current_term(),
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
        }
    }
}

impl ExperimentalRaftControlPlane {
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        block_on_control_plane_raft(&self.runtime, future)
    }

    fn authority_now_ms(&self, supplied_now_ms: u64) -> Result<u64, ControlPlaneError> {
        if !self.resample_authority_time {
            return Ok(supplied_now_ms);
        }
        let status = self.block_on(self.authority.status())?;
        if !status.local_leader() {
            return Err(ControlPlaneError::RpcRemote {
                message: "local OpenRaft authority is not the serving leader".to_string(),
            });
        }
        let max_committed_timestamp_ms = self.current_snapshot()?.max_committed_timestamp_ms();
        let mut authority_clock = self
            .authority_clock
            .as_ref()
            .expect("resampled authority time requires a clock gate")
            .lock()
            .expect("control-plane authority clock mutex poisoned");
        authority_clock.observe_committed_timestamp_high_water(max_committed_timestamp_ms);
        let current_term = status.current_term().ok_or(ControlPlaneError::RpcRemote {
            message: "local OpenRaft leader has no current term".to_string(),
        })?;
        authority_clock.validate_raft_leadership_term(current_term)?;
        authority_clock.effective_process_now_ms()
    }

    fn durable_poison_error(&self) -> Option<ControlPlaneError> {
        self.durable_poison
            .as_ref()
            .map(|message| ControlPlaneError::RpcRemote {
                message: message.clone(),
            })
    }

    fn ensure_not_durably_poisoned(&self) -> Result<(), ControlPlaneError> {
        if let Some(error) = self.durable_poison_error() {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn poison_durable_authority(&mut self, message: String) {
        self.durable_poison = Some(message);
        self.durable_poison_gate.store(true, Ordering::Release);
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

        let status = self.block_on(self.authority.status()).ok();
        let marker = status
            .as_ref()
            .filter(|status| status.linearized_authority_serving())
            .map(ExperimentalRaftServingCheckpointMarker::from_status);
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
        self.ensure_not_durably_poisoned()?;
        let submitted = self.block_on(self.authority.submit_control_plane_command(command))?;
        let outcome = submitted.into_outcome();
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "experimental OpenRaft control-plane durability checkpoint failed after a \
                 committed command; refusing to serve until restart: {error}"
            ));
            return Err(error);
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
        let now_ms = self.authority_now_ms(now_ms)?;
        let snapshot = self.current_snapshot()?;
        let expire_at_ms = snapshot.heartbeat_lease_expiry_timestamp(now_ms);
        let response =
            self.submit_raft_command(ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms })?;
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
}

impl ControlPlaneHeartbeatRuntimeMapSource for ExperimentalRaftControlPlane {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: storage::control_plane::NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let node_id = heartbeat.node_id;
        let requested_observed_epoch = heartbeat.observed_epoch;
        let requested_lease_duration_ms = heartbeat.requested_lease_duration_ms;
        let pre_record_snapshot = self.current_snapshot()?;
        let previous_observed_epoch = pre_record_snapshot
            .node(node_id)
            .and_then(|node| node.last_observed_epoch());
        let lease_deadline_ms = pre_record_snapshot.heartbeat_lease_deadline(
            node_id,
            authority_now_ms,
            requested_lease_duration_ms,
        )?;
        let pre_record_epoch = pre_record_snapshot.cluster_epoch();
        self.submit_raft_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: authority_now_ms,
            lease_deadline_ms,
        })?;
        let snapshot = self.current_snapshot()?;
        let mut lease = snapshot.heartbeat_lease_after_record(
            node_id,
            requested_observed_epoch,
            pre_record_epoch,
            lease_deadline_ms,
            authority_now_ms,
        )?;
        let ready = snapshot.ready_pg_peering_completions(authority_now_ms)?;
        if !ready.is_empty() {
            self.submit_raft_command(ControlPlaneCommand::CompleteReadyPgPeerings {
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
        Ok(ControlPlaneHeartbeatRefresh::new(lease, runtime_map))
    }
}

impl ControlPlaneAdmin for ExperimentalRaftControlPlane {
    fn authority_clock_context(
        &self,
    ) -> Result<ControlPlaneAuthorityClockContext, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let status = self.block_on(self.authority.status())?;
        let snapshot = self.current_snapshot()?;
        Ok(ControlPlaneAuthorityClockContext::new(
            snapshot.max_committed_timestamp_ms(),
            status.current_term(),
            status.linearized_authority_serving(),
        ))
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
            self.submit_raft_command(ControlPlaneCommand::FencePgForMetadataTransfer { pg_id })?;
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
        let snapshot_log_id = self.block_on(self.authority.trigger_snapshot_and_purge_applied())?;
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "experimental OpenRaft control-plane durability checkpoint failed after a \
                 snapshot purge; refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        Ok(snapshot_log_id.map(|log_id| log_id.index()))
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
    let Some(committed) = authority.status().await?.committed() else {
        return Ok(());
    };
    authority
        .wait_for_applied_log_id(committed, timeout, message)
        .await
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

async fn maybe_trigger_experimental_raft_seed_election(
    authority: &ControlPlaneRaftAuthority,
    peer_policy: Option<&ControlPlaneRaftPeerTransportPolicy>,
    local_node_id: ControlPlaneRaftNodeId,
) -> Result<(), ControlPlaneError> {
    if !experimental_raft_startup_initializes_membership(peer_policy, local_node_id) {
        return Ok(());
    }
    if peer_policy.is_none_or(|policy| policy.peers().len() <= 1) {
        return Ok(());
    }
    if authority.status().await?.current_leader().is_some() {
        return Ok(());
    }
    authority
        .raft()
        .trigger()
        .elect(true)
        .await
        .map_err(|error| ControlPlaneError::RpcRemote {
            message: format!("OpenRaft startup election trigger failed: {error:?}"),
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
    poison_gate: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
struct ExperimentalRaftPeerRpcDurability<'a> {
    artifact_path: Option<&'a Path>,
    checkpoint_lock: Option<&'a Arc<Mutex<()>>>,
    poison_gate: Option<&'a AtomicBool>,
    checkpoint_ordinary_rpc: bool,
}

impl<'a> ExperimentalRaftPeerRpcDurability<'a> {
    const NONE: Self = Self {
        artifact_path: None,
        checkpoint_lock: None,
        poison_gate: None,
        checkpoint_ordinary_rpc: false,
    };

    fn from_context(context: &'a ExperimentalRaftPeerDurabilityContext) -> Self {
        Self {
            artifact_path: context.artifact_path.as_deref().map(PathBuf::as_path),
            checkpoint_lock: Some(&context.checkpoint_lock),
            poison_gate: Some(context.poison_gate.as_ref()),
            checkpoint_ordinary_rpc: true,
        }
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
        .is_some_and(|durability| durability.poison_gate.load(Ordering::Acquire))
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
    ensure_experimental_raft_peer_not_durably_poisoned(durability.poison_gate)?;
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

    write_control_plane_raft_peer_transport_frame(stream, &response_frame)
        .map_err(ExperimentalRaftPeerRpcWorkerError::PeerRpc)
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
    ensure_experimental_raft_peer_not_durably_poisoned(durability.poison_gate)?;
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

    let checkpoint_before_response = durability.checkpoint_ordinary_rpc
        || matches!(request.kind, ControlPlaneRaftPeerFrameKind::Snapshot);
    if checkpoint_before_response {
        let Some(path) = durability.artifact_path else {
            return Err(ExperimentalRaftPeerRpcWorkerError::Checkpoint(
                ControlPlaneError::RpcRemote {
                    message:
                        "experimental OpenRaft peer RPC requires durable checkpoint path before response"
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

    ensure_experimental_raft_peer_not_durably_poisoned(durability.poison_gate)?;
    Ok(response_frame)
}

fn ensure_experimental_raft_peer_not_durably_poisoned(
    durable_poison_gate: Option<&AtomicBool>,
) -> Result<(), ExperimentalRaftPeerRpcWorkerError> {
    if durable_poison_gate.is_some_and(|gate| gate.load(Ordering::Acquire)) {
        return Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(
            ControlPlaneError::RpcRemote {
                message: "experimental OpenRaft control-plane durable authority is poisoned; refusing peer RPC until restart".to_string(),
            },
        ));
    }
    Ok(())
}

fn spawn_experimental_raft_peer_listener_loop(
    listener: ExperimentalRaftPeerListener,
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    local_node_id: ControlPlaneRaftNodeId,
    durability: ExperimentalRaftPeerDurabilityContext,
    active_workers: Arc<AtomicUsize>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        for _ in 0..CONTROL_PLANE_ACCEPT_BATCH_LIMIT {
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
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    eprintln!("control-plane OpenRaft peer socket accept failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        thread::sleep(Duration::from_millis(1));
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
    let artifact_existed = path.exists();
    let committed_timestamp_high_water_ms =
        block_on_control_plane_raft(runtime, authority.store_durable_restart_artifact(path))?;
    let binding = authority.authority_clock_checkpoint_binding();
    if !artifact_existed && load_authority_clock_restart_checkpoint(path, binding)?.is_none() {
        store_authority_clock_restart_checkpoint(path, binding, committed_timestamp_high_water_ms)?;
    }
    Ok(())
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
    let durable_poison_gate = Arc::new(AtomicBool::new(false));
    let multi_node_raft_peer_mode = raft_peer_policy
        .as_ref()
        .is_some_and(|policy| policy.peers().len() > 1);
    let _raft_peer_listener_loop = raft_peer_listener.map(|listener| {
        spawn_experimental_raft_peer_listener_loop(
            listener,
            runtime.clone(),
            Arc::clone(&authority),
            node_id,
            ExperimentalRaftPeerDurabilityContext {
                artifact_path: Some(Arc::clone(&durable_artifact_path)),
                checkpoint_lock: Arc::clone(&durable_checkpoint_lock),
                poison_gate: Arc::clone(&durable_poison_gate),
            },
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
        }
        wait_for_experimental_raft_startup_catch_up(
            &authority,
            Duration::from_secs(1),
            "experimental control-plane startup committed replay",
        )
        .await?;
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
        durable_serving_checkpoint: Mutex::new(None),
        checkpoint_serving_reads: multi_node_raft_peer_mode,
        resample_authority_time: true,
        authority_clock: Some(Arc::clone(&authority_clock)),
        durable_poison: None,
        durable_poison_gate: Arc::clone(&durable_poison_gate),
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
    let authority = Arc::new(Mutex::new(control_plane));
    let authority_clock_checkpoint_target = Arc::new(AuthorityClockCheckpointTarget {
        path: durable_artifact_path.as_ref().clone(),
        binding: authority_clock_checkpoint_binding,
    });
    let active_rpc_workers = Arc::new(AtomicUsize::new(0));
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
        "argmin-s3 experimental durable OpenRaft control-plane manager using state {} on {} (raft node {}, peer socket {}, configured peers {}, configured auth credentials {}, lease scan {} ms)",
        state_path,
        socket_path,
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

    loop {
        for _ in 0..CONTROL_PLANE_ACCEPT_BATCH_LIMIT {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    spawn_control_plane_rpc_worker(
                        stream,
                        Arc::clone(&authority),
                        Some(Arc::clone(&authority_clock)),
                        Some(Arc::clone(&authority_clock_checkpoint_target)),
                        false,
                        Arc::clone(&active_rpc_workers),
                        auth_verifier.clone(),
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    eprintln!("control-plane socket accept failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        if multi_node_raft_peer_mode {
            block_on_control_plane_raft(&runtime, async {
                maybe_trigger_experimental_raft_seed_election(
                    &raft_authority,
                    raft_peer_policy.as_ref(),
                    node_id,
                )
                .await
            })
            .unwrap_or_else(|error| {
                eprintln!("experimental OpenRaft control-plane election trigger failed: {error}");
                std::process::exit(1);
            });
        }
        let local_raft_authority_serving = if multi_node_raft_peer_mode {
            block_on_control_plane_raft(&runtime, async {
                raft_authority
                    .status()
                    .await
                    .map(|status| status.linearized_authority_serving())
            })
            .unwrap_or_else(|error| {
                eprintln!("experimental OpenRaft control-plane status check failed: {error}");
                std::process::exit(1);
            })
        } else {
            true
        };
        let expiry = if local_raft_authority_serving {
            let mut authority = authority
                .lock()
                .expect("control-plane authority mutex poisoned");
            if multi_node_raft_peer_mode {
                bootstrap_empty_experimental_raft_control_plane(&mut authority, config)
                    .unwrap_or_else(|error| {
                        eprintln!(
                            "failed to bootstrap experimental OpenRaft control-plane state: {error}"
                        );
                        std::process::exit(1);
                    });
            }
            authority.expire_heartbeat_leases(storage::clock::current_time_millis())
        } else {
            Ok((ClusterEpoch::INITIAL, 0, 0))
        };
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
            Err(error) if experimental_raft_error_is_forward_to_leader(&error) => {}
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

fn control_plane_lease_expiry_error_is_clock_wait(error: &ControlPlaneError) -> bool {
    matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression { .. }
            | ControlPlaneError::CommittedTimestampTooFarAhead { .. }
            | ControlPlaneError::AuthorityClockLeadershipChanged { .. }
            | ControlPlaneError::AuthorityClockSourceUnavailable
            | ControlPlaneError::AuthorityClockNotEstablished { .. }
    )
}

fn experimental_raft_error_is_forward_to_leader(error: &ControlPlaneError) -> bool {
    matches!(
        error,
        ControlPlaneError::RpcRemote { message }
            if message.contains("OpenRaft client-write failed")
                && message.contains("has to forward request to")
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
        || experimental_raft_error_is_forward_to_leader(error)
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

fn spawn_control_plane_rpc_worker(
    mut stream: UnixStream,
    authority: Arc<
        Mutex<
            impl ControlPlaneAdmin
                + ControlPlaneHeartbeatRuntimeMapSource
                + ControlPlaneRuntimeMapSource
                + Send
                + 'static,
        >,
    >,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    authority_clock_checkpoint_target: Option<Arc<AuthorityClockCheckpointTarget>>,
    gate_request_time_with_authority_clock: bool,
    active_rpc_workers: Arc<AtomicUsize>,
    auth_verifier: Option<Arc<ControlPlaneUnixAuthVerifier>>,
) {
    match active_rpc_workers.fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
        (active < CONTROL_PLANE_RPC_WORKER_LIMIT).then_some(active + 1)
    }) {
        Ok(_) => {}
        Err(_) => {
            eprintln!("control-plane RPC rejected: worker limit reached");
            return;
        }
    }
    thread::spawn(move || {
        let _guard = ControlPlaneRpcWorkerGuard {
            active_rpc_workers: Arc::clone(&active_rpc_workers),
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
        let response = (|| {
            if request.is_authority_clock_admin() {
                let authority = authority
                    .lock()
                    .expect("control-plane authority mutex poisoned");
                let authority_clock =
                    authority_clock
                        .as_ref()
                        .ok_or_else(|| ControlPlaneError::RpcProtocol {
                            message:
                                "authority-clock administration requires a process-local clock gate"
                                    .to_owned(),
                        })?;
                let mut authority_clock = authority_clock
                    .lock()
                    .expect("control-plane authority clock mutex poisoned");
                build_control_plane_authority_clock_admin_response(
                    &*authority,
                    &mut authority_clock,
                    request,
                    auth_verifier.as_deref(),
                    ControlPlaneAuthorityClockAdminSample::from_process_clock()?,
                    |authority, authority_clock| {
                        persist_established_authority_clock_checkpoint(
                            authority,
                            authority_clock,
                            authority_clock_checkpoint_target.as_deref(),
                        )
                    },
                    || Ok(storage::clock::current_time_millis()),
                )
            } else if request.is_refresh_node_heartbeat() {
                let prepared = {
                    let mut authority = authority
                        .lock()
                        .expect("control-plane authority mutex poisoned");
                    let now_ms = match &authority_clock {
                        Some(authority_clock) if gate_request_time_with_authority_clock => {
                            authority_clock
                                .lock()
                                .expect("control-plane authority clock mutex poisoned")
                                .effective_process_now_ms()?
                        }
                        _ => storage::clock::current_time_millis(),
                    };
                    prepare_control_plane_heartbeat_response(
                        &mut *authority,
                        request,
                        now_ms,
                        auth_verifier.as_deref(),
                    )
                };
                prepared.and_then(|prepared| {
                    finish_control_plane_heartbeat_response(prepared, || {
                        Ok(storage::clock::current_time_millis())
                    })
                })
            } else {
                let mut authority = authority
                    .lock()
                    .expect("control-plane authority mutex poisoned");
                let now_ms = match &authority_clock {
                    Some(authority_clock) if gate_request_time_with_authority_clock => {
                        authority_clock
                            .lock()
                            .expect("control-plane authority clock mutex poisoned")
                            .effective_process_now_ms()?
                    }
                    _ => storage::clock::current_time_millis(),
                };
                match auth_verifier.as_deref() {
                    Some(auth_verifier) => {
                        build_control_plane_unix_response_with_auth_and_response_clock(
                            &mut *authority,
                            request,
                            now_ms,
                            Some(auth_verifier),
                            || Ok(storage::clock::current_time_millis()),
                        )
                    }
                    None => build_control_plane_unix_response(&mut *authority, request, now_ms),
                }
            }
        })();
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                eprintln!("control-plane RPC response build failed: {error}");
                return;
            }
        };
        if let Err(error) = write_control_plane_unix_response(&mut stream, response) {
            eprintln!("control-plane RPC response failed: {error}");
        }
    });
}

#[derive(Debug)]
struct AuthorityClockCheckpointTarget {
    path: PathBuf,
    binding: ControlPlaneAuthorityClockCheckpointBinding,
}

fn persist_established_authority_clock_checkpoint(
    authority: &impl ControlPlaneAdmin,
    authority_clock: &mut ControlPlaneAuthorityClock,
    checkpoint_target: Option<&AuthorityClockCheckpointTarget>,
) -> Result<(), ControlPlaneError> {
    let context = authority.authority_clock_context()?;
    if !authority_clock.status(context).established() {
        return Ok(());
    }
    let checkpoint_target = checkpoint_target.ok_or_else(|| ControlPlaneError::RpcProtocol {
        message: "authority-clock administration requires a durable checkpoint path".to_owned(),
    })?;
    if let Err(error) = store_validated_authority_clock_restart_checkpoint(
        &checkpoint_target.path,
        checkpoint_target.binding,
        context.committed_timestamp_high_water_ms(),
        authority_clock,
    ) {
        if authority_clock.status(context).established() {
            authority_clock.fail_closed_after_checkpoint_persistence_failure()?;
        }
        return Err(error);
    }
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
    if config.control_plane_frontend_auth_credentials.is_empty() {
        return Ok(FrontendControlPlaneClient::Plain(
            UnixControlPlaneClient::new(control_plane_socket_path),
        ));
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
    build_authenticated_frontend_control_plane_client(
        control_plane_socket_path,
        cluster_id,
        instance_id,
        configured,
    )
}

fn build_frontend_control_plane_client_from_runtime_map_auth_env(
    control_plane_socket_path: &Path,
) -> Result<FrontendControlPlaneClient, String> {
    let auth_config = ConfiguredControlPlaneFrontendRuntimeMapAuth::from_env()?;
    match auth_config {
        Some(auth_config) => {
            let configured = latest_frontend_auth_credential_for_instance(
                &auth_config.credentials,
                &auth_config.instance_id,
            )
            .expect("auth-only frontend config validates local instance credential");
            build_authenticated_frontend_control_plane_client(
                control_plane_socket_path,
                &auth_config.cluster_id,
                &auth_config.instance_id,
                configured,
            )
        }
        None => Ok(FrontendControlPlaneClient::Plain(
            UnixControlPlaneClient::new(control_plane_socket_path),
        )),
    }
}

fn build_admin_control_plane_client_from_command_auth_env(
    control_plane_socket_path: &Path,
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
                AuthenticatedUnixControlPlaneClient::new(
                    UnixControlPlaneClient::new(control_plane_socket_path),
                    credential,
                ),
            ))
        }
        None => Ok(AdminControlPlaneClient::Plain(UnixControlPlaneClient::new(
            control_plane_socket_path,
        ))),
    }
}

fn build_authenticated_frontend_control_plane_client(
    control_plane_socket_path: impl Into<std::path::PathBuf>,
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
        AuthenticatedUnixControlPlaneClient::new(
            UnixControlPlaneClient::new(control_plane_socket_path),
            credential,
        ),
    ))
}

fn build_storage_node_control_plane_client(
    config: &ServerConfig,
    control_plane_socket_path: &str,
    node_id: NodeId,
    node_incarnation: u64,
) -> Result<StorageNodeControlPlaneClient, String> {
    let client = UnixControlPlaneClient::new(control_plane_socket_path);
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

type BuiltStorageNodeProcessConfig = (
    StorageNodeProcessConfig,
    Option<u64>,
    Option<StorageNodeDataDirGuard>,
);

fn bind_storage_node_process(
    config: &ServerConfig,
    ec_config: &EcConfig,
) -> BoundStorageNodeProcess {
    let (storage_config, control_plane_node_incarnation, data_dir_guard) =
        build_storage_node_process_config(config, ec_config).unwrap_or_else(|e| {
            eprintln!("storage-node configuration error: {e}");
            std::process::exit(1);
        });
    let node_id = storage_config.node_id();
    let socket_path = storage_config.socket_path().to_path_buf();
    let server = match data_dir_guard {
        Some(data_dir_guard) => {
            StorageNodeServer::bind_with_data_dir_guard(storage_config, data_dir_guard)
        }
        None => StorageNodeServer::bind(storage_config),
    }
    .unwrap_or_else(|e| {
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
    let control_plane_client = build_storage_node_control_plane_client(
        config,
        socket_path,
        NodeId::new(
            config
                .storage_node_id
                .expect("storage node id is required for storage roles"),
        ),
        node_incarnation,
    )
    .unwrap_or_else(|error| {
        eprintln!("failed to configure storage-node control-plane auth client: {error}");
        std::process::exit(1);
    });
    let loop_handle = server
        .spawn_control_plane_refresh_loop(
            control_plane_client,
            node_incarnation,
            lease_ms,
            config.control_plane_refresh_interval,
            storage::clock::current_time_millis,
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to start storage-node control-plane refresh loop: {error}");
            std::process::exit(1);
        });
    process_info!(
        "argmin-s3 storage-node control-plane refresh using {} (incarnation {}, refresh {} ms, lease {} ms)",
        socket_path,
        node_incarnation,
        config.control_plane_refresh_interval.as_millis(),
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
    let acting_set: Vec<NodeId> = (0..config.local_node_count).map(NodeId::new).collect();
    let pg_routes = pg_ids
        .iter()
        .map(|&pg_id| StorageNodePgRoute {
            pg_id,
            cluster_epoch,
            state: PgState::Active,
            primary_node_id: NodeId::new(0),
            acting_set: acting_set.clone(),
        })
        .collect();
    Ok((
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
        None,
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
    let data_dir_guard = StorageNodeDataDirGuard::acquire(node_data_dir_path).map_err(|error| {
        format!("failed to lock storage node data directory for startup heartbeat: {error}")
    })?;
    let node_incarnation =
        advance_storage_node_incarnation(node_data_dir_path).map_err(|error| {
            format!("failed to advance storage-node control-plane incarnation: {error}")
        })?;
    let default_ec_shape = EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };
    let node = SharedStorageNode::open_with_default_ec_shape(
        node_data_dir_path,
        &config.storage_pg_ids,
        default_ec_shape,
    )
    .map_err(|error| format!("failed to open storage node for startup heartbeat: {error}"))?;
    node.recover_pg_metadata_command_state(node_id)
        .map_err(|error| {
            format!("failed to recover storage node for startup heartbeat: {error}")
        })?;
    // Control-plane managed storage nodes learn their runtime map from the
    // control plane. On restart, use the last locally installed runtime config
    // as the heartbeat baseline so the authority can return a bounded delta
    // instead of replaying all history since the oldest durable placement.
    let startup_runtime_config = StorageNodeProcessConfig::load_control_plane_runtime_config(
        node_data_dir_path,
        node_id,
        default_ec_shape,
        configured_socket_path,
    )
    .map_err(|error| format!("failed to load storage-node startup runtime config: {error}"))?;
    let lease_ms = u64::try_from(config.control_plane_heartbeat_lease_duration.as_millis())
        .map_err(|_| "ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS is too large".to_string())?;
    let retry_deadline = control_plane_startup_retry_deadline(config);
    let retry_delay = control_plane_startup_retry_delay(config);
    let started_at = Instant::now();
    let mut attempts = 0_u32;
    let refresh = loop {
        attempts = attempts.saturating_add(1);
        let heartbeat = match startup_runtime_config.as_ref() {
            Some(runtime_config) => runtime_config
                .control_plane_heartbeat(&node, node_incarnation, lease_ms)
                .map_err(|error| {
                    format!("failed to build storage-node startup heartbeat: {error}")
                })?,
            None => node
                .control_plane_heartbeat(
                    node_id,
                    node_incarnation,
                    configured_socket_path,
                    ClusterEpoch::INITIAL,
                    lease_ms,
                    std::iter::empty(),
                )
                .map_err(|error| {
                    format!("failed to build storage-node startup heartbeat: {error}")
                })?,
        };
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
    let node_config = match startup_runtime_config.as_ref() {
        Some(runtime_config) => {
            let history_reference_summary =
                node.cluster_map_history_reference_summary()
                    .map_err(|error| {
                        format!(
                        "failed to read storage-node startup history reference summary: {error}"
                    )
                    })?;
            StorageNodeProcessConfig::from_runtime_map_refresh(
                runtime_config,
                &runtime_map,
                history_reference_summary,
            )
        }
        None => StorageNodeProcessConfig::from_runtime_map(
            node_id,
            node_data_dir_path.to_path_buf(),
            default_ec_shape,
            &runtime_map,
        ),
    }
    .map_err(|error| error.to_string())?;
    if node_config.socket_path() != Path::new(configured_socket_path) {
        return Err(format!(
            "ARGMIN_STORAGE_NODE_SOCKET_PATH {} must match control-plane endpoint {} for node {}",
            configured_socket_path,
            node_config.socket_path().display(),
            node_id.as_u32()
        ));
    }
    node_config
        .persist_control_plane_runtime_config()
        .map_err(|error| {
            format!("failed to persist storage-node startup runtime config: {error}")
        })?;
    Ok((node_config, Some(node_incarnation), Some(data_dir_guard)))
}

async fn run_legacy_local_frontend(config: ServerConfig, host_id: String, ec_config: EcConfig) {
    let pg_ids: Vec<u32> = (0..config.pg_count).collect();
    let data_dir = Path::new(&config.data_dir);
    let ec_shape = storage::EcShape {
        k: ec_config.data_shards(),
        m: ec_config.parity_shards(),
    };

    let node_ids: Vec<NodeId> = (0..config.local_node_count).map(NodeId::new).collect();
    let storage_cluster = StorageCluster::open_local_nodes(data_dir, &node_ids, &pg_ids, ec_shape)
        .unwrap_or_else(|e| {
            eprintln!("failed to open local storage cluster: {e}");
            std::process::exit(1);
        });

    run_frontend_server(
        config,
        host_id,
        storage_cluster,
        server_core::coordinator::BackgroundWorkerMode::all(),
    )
    .await;
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
    control_plane_startup_retry_deadline(config)
}

fn control_plane_startup_retry_deadline(config: &ServerConfig) -> Duration {
    let refresh_budget = config.control_plane_refresh_interval.saturating_mul(20);
    let lease_budget = config
        .control_plane_heartbeat_lease_duration
        .saturating_mul(2);
    Duration::from_secs(30)
        .max(refresh_budget)
        .max(lease_budget)
}

fn frontend_control_plane_startup_retry_delay(config: &ServerConfig) -> Duration {
    control_plane_startup_retry_delay(config)
}

fn control_plane_startup_retry_delay(config: &ServerConfig) -> Duration {
    config
        .control_plane_refresh_interval
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
    let node_ids: Vec<NodeId> = (0..config.local_node_count).map(NodeId::new).collect();
    let mut local_map = LocalClusterMap::open_frontend_topology_only_with_epoch(
        NodeId::new(0),
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
    let _frontend_runtime_map_refresh_loop =
        maybe_spawn_frontend_control_plane_refresh_loop(storage_cluster_handle.clone(), &config);

    // Build frontend pool sharing the same storage cluster handle.
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
            credentials: build_credential_store(&config),
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
            config.control_plane_refresh_interval,
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
        config.control_plane_refresh_interval.as_millis(),
    );
    Some(loop_handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::SecretKey;
    use config::{ConfiguredControlPlaneRaftAuthCredential, SecretConfigValue};
    use openraft::impls::Vote;
    use openraft::raft::{TransferLeaderRequest, VoteRequest};
    use storage::control_plane::{
        handle_control_plane_unix_stream, ControlPlaneHeartbeatSink, NodeAvailabilityState,
        NodeHeartbeat, NodeMembershipState, NodePgHeartbeatObservation, PgMetadataProof,
    };
    use storage::control_plane_auth::{
        ControlPlaneAuthEnvelope, ControlPlaneAuthSignInput, ControlPlaneAuthTarget,
    };
    use storage::control_plane_raft::{
        ControlPlaneRaftLeaderId, ControlPlaneRaftPeerFrameIdentity, ControlPlaneRaftPeerRpcRequest,
    };

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
        authority
            .expire_heartbeat_leases(1_000)
            .expect("timestamp high-water should advance");
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
                &authority,
                &mut authority_clock,
                Some(&checkpoint_target),
            )
            .expect("established clock checkpoint should persist");
        });
        let checkpoint = load_authority_clock_restart_checkpoint(&state_path, binding)
            .expect("checkpoint should load")
            .expect("checkpoint should exist");
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
            store_authority_clock_restart_checkpoint(&state_path, binding, Some(999)).unwrap();
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
                &authority,
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

    fn wait_for_experimental_raft_vote(
        runtime: &Handle,
        authority: &ControlPlaneRaftAuthority,
        expected_vote: Vote<ControlPlaneRaftLeaderId>,
        message: &'static str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let status = runtime
                .block_on(authority.status())
                .expect("experimental OpenRaft authority status should read");
            if status.persisted_vote() == Some(expected_vote) {
                return;
            }
            if Instant::now() >= deadline {
                panic!(
                    "{message}: expected vote {expected_vote:?}, got {:?}",
                    status.persisted_vote()
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn short_unix_socket_test_dir(name: &str) -> PathBuf {
        Path::new("/tmp").join(format!("as3-{}-{name}", std::process::id()))
    }

    fn test_server_config() -> ServerConfig {
        ServerConfig {
            process_role: ProcessRole::StorageNode,
            listen_addr: "127.0.0.1:9000".to_string(),
            tls_cert_path: None,
            tls_key_path: None,
            data_dir: "/tmp/argmin-test".to_string(),
            pg_count: 8,
            local_node_count: 6,
            storage_node_id: Some(2),
            storage_node_data_dir: Some("/tmp/argmin-test/node-0002".to_string()),
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
            control_plane_refresh_interval: std::time::Duration::from_millis(250),
            control_plane_heartbeat_lease_duration: std::time::Duration::from_millis(1000),
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
        let server = std::thread::spawn(move || {
            let store = FileControlPlaneStore::new(state_path);
            let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
            authority
                .set_node_membership(node_id, storage::control_plane::NodeMembershipState::Active)
                .unwrap();
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
            cluster_map_history_reference_summary:
                storage::PgClusterMapHistoryReferenceSummary::default(),
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
        assert!(!control_plane_lease_expiry_error_is_clock_wait(
            &ControlPlaneError::InvalidLeaseDuration
        ));
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
            Arc::new(authority)
        });
        let control_plane = ExperimentalRaftControlPlane {
            runtime: handle,
            authority: Arc::clone(&authority),
            durable_artifact_path: None,
            durable_checkpoint_lock: None,
            durable_serving_checkpoint: Mutex::new(None),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: None,
            durable_poison_gate: Arc::new(AtomicBool::new(false)),
        };
        ExperimentalRaftTestHarness {
            runtime,
            authority,
            control_plane,
        }
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

    fn experimental_raft_uninitialized_test_harness(name: &str) -> ExperimentalRaftTestHarness {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let handle = runtime.handle().clone();
        let authority = runtime.block_on(async {
            Arc::new(
                ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                    format!(
                        "argmin-s3-experimental-raft-uninitialized-{name}-{}",
                        std::process::id()
                    ),
                    1,
                )
                .await
                .expect("experimental raft authority should initialize"),
            )
        });
        let control_plane = ExperimentalRaftControlPlane {
            runtime: handle,
            authority: Arc::clone(&authority),
            durable_artifact_path: None,
            durable_checkpoint_lock: None,
            durable_serving_checkpoint: Mutex::new(None),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: None,
            durable_poison_gate: Arc::new(AtomicBool::new(false)),
        };
        ExperimentalRaftTestHarness {
            runtime,
            authority,
            control_plane,
        }
    }

    fn experimental_raft_durable_test_harness(
        name: &str,
        state_path: &Path,
    ) -> ExperimentalRaftTestHarness {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let handle = runtime.handle().clone();
        let authority = runtime.block_on(async {
            let authority = ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                format!(
                    "argmin-s3-experimental-durable-raft-{name}-{}",
                    std::process::id()
                ),
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
            wait_for_experimental_raft_startup_catch_up(
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
            durable_serving_checkpoint: Mutex::new(None),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: None,
            durable_poison_gate: Arc::new(AtomicBool::new(false)),
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
            durable_serving_checkpoint: Mutex::new(None),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: None,
            durable_poison_gate: Arc::new(AtomicBool::new(false)),
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
            wait_for_experimental_raft_startup_catch_up(
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
            durable_serving_checkpoint: Mutex::new(None),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: None,
            durable_poison_gate: Arc::new(AtomicBool::new(false)),
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
        let invalid_checkpoint_path = state_dir.clone();

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
            Arc::new(authority)
        });
        let mut control_plane = ExperimentalRaftControlPlane {
            runtime: handle,
            authority: Arc::clone(&authority),
            durable_artifact_path: Some(Arc::new(invalid_checkpoint_path)),
            durable_checkpoint_lock: Some(Arc::new(Mutex::new(()))),
            durable_serving_checkpoint: Mutex::new(None),
            checkpoint_serving_reads: false,
            resample_authority_time: false,
            authority_clock: None,
            durable_poison: None,
            durable_poison_gate: Arc::new(AtomicBool::new(false)),
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
        assert!(control_plane.durable_poison.is_some());
        assert!(control_plane.durable_poison_gate.load(Ordering::Acquire));

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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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

        let active_refresh = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: active_epoch,
                    requested_lease_duration_ms: 600,
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                20_000,
            )
        })
        .expect("experimental raft heartbeat should refresh");

        assert_eq!(refresh.lease().lease_deadline_ms(), 30_500);

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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
        let shortened = harness
            .control_plane
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-experimental-raft-node-1.sock".to_string(),
                    observed_epoch: refreshed_epoch,
                    requested_lease_duration_ms: 100,
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                40_100,
            )
            .expect("experimental raft heartbeat should preserve longer existing lease");
        assert_eq!(shortened.lease().lease_deadline_ms(), 41_000);
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
        let protected_floor = storage::PgClusterMapHistoryReferenceSummary {
            oldest_live_placement_epoch: Some(protected_epoch),
            oldest_durable_backfill_epoch: None,
            oldest_pending_metadata_command_epoch: None,
        };
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
                    cluster_map_history_reference_summary: protected_floor,
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
            cluster_map_history_reference_summary: protected_floor,
            pg_observations: Vec::new(),
        };
        harness
            .control_plane
            .submit_raft_command(ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: restart_heartbeat.clone(),
                heartbeat_at_ms: 62_000,
                lease_deadline_ms: 63_000,
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
    fn experimental_raft_peer_rpc_checkpoint_failure_writes_no_response() {
        let harness = experimental_raft_test_harness("peer-checkpoint-before-ack");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-checkpoint-before-ack-{}",
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
        let state_dir = short_unix_socket_test_dir("experimental-raft-peer-checkpoint-fail");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).expect("checkpoint failure target directory should exist");
        let runtime_handle = harness.runtime.handle().clone();

        let result = handle_experimental_raft_peer_rpc_before_ack(
            &runtime_handle,
            &harness.authority,
            &mut server_stream,
            1,
            &policy,
            ExperimentalRaftPeerRpcDurability {
                artifact_path: Some(&state_dir),
                checkpoint_lock: None,
                poison_gate: None,
                checkpoint_ordinary_rpc: true,
            },
        );
        assert!(
            matches!(
                result,
                Err(ExperimentalRaftPeerRpcWorkerError::Checkpoint(_))
            ),
            "peer RPC should fail at checkpoint before response: {result:?}"
        );
        drop(server_stream);

        let response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            response.is_err(),
            "checkpoint failure must not acknowledge peer RPC before durability"
        );

        harness.shutdown();
        let _ = fs::remove_dir_all(&state_dir);
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
        let poison_gate = AtomicBool::new(true);
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
                poison_gate: Some(&poison_gate),
                checkpoint_ordinary_rpc: false,
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
        let poison_gate = AtomicBool::new(true);

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
                poison_gate: Some(&poison_gate),
                checkpoint_ordinary_rpc: false,
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
                    poison_gate: None,
                    checkpoint_ordinary_rpc: false,
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
                poison_gate: None,
                checkpoint_ordinary_rpc: false,
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
    fn experimental_raft_peer_rpc_poison_before_ack_writes_no_response() {
        let harness = experimental_raft_test_harness("peer-poison-before-ack");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-poison-before-ack-{}",
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
        client_stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("client stream read timeout should set");

        let state_dir = short_unix_socket_test_dir("experimental-raft-peer-poison-before-ack");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).expect("checkpoint directory should exist");
        let state_path = state_dir.join("control-plane.state");
        let checkpoint_lock = Arc::new(Mutex::new(()));
        let checkpoint_guard = checkpoint_lock
            .lock()
            .expect("checkpoint lock should acquire");
        let poison_gate = Arc::new(AtomicBool::new(false));
        let runtime_handle = harness.runtime.handle().clone();
        let authority = Arc::clone(&harness.authority);
        let worker_lock = Arc::clone(&checkpoint_lock);
        let worker_poison_gate = Arc::clone(&poison_gate);
        let worker_state_path = state_path.clone();
        let worker = thread::spawn(move || {
            handle_experimental_raft_peer_rpc_before_ack(
                &runtime_handle,
                &authority,
                &mut server_stream,
                1,
                &policy,
                ExperimentalRaftPeerRpcDurability {
                    artifact_path: Some(&worker_state_path),
                    checkpoint_lock: Some(&worker_lock),
                    poison_gate: Some(worker_poison_gate.as_ref()),
                    checkpoint_ordinary_rpc: true,
                },
            )
        });

        let blocked_response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            blocked_response.is_err(),
            "peer RPC must not respond before durable checkpoint completes"
        );
        poison_gate.store(true, Ordering::Release);
        drop(checkpoint_guard);
        let result = worker.join().expect("peer RPC worker should not panic");
        assert!(
            matches!(result, Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(_))),
            "peer RPC should fail closed after poison flips before ack: {result:?}"
        );

        client_stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("client stream read timeout should update");
        let response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            response.is_err(),
            "peer RPC must not acknowledge after durable poison flips"
        );

        harness.shutdown();
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn experimental_raft_peer_vote_response_waits_until_vote_is_durable() {
        let harness = experimental_raft_uninitialized_test_harness("peer-vote-durable-before-ack");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-vote-durable-before-ack-{}",
            std::process::id()
        );
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1);
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
        client_stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("client stream read timeout should set");

        let state_dir = short_unix_socket_test_dir("experimental-raft-peer-vote-durable");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).expect("checkpoint directory should exist");
        let state_path = state_dir.join("control-plane.state");
        let checkpoint_lock = Arc::new(Mutex::new(()));
        let checkpoint_guard = checkpoint_lock
            .lock()
            .expect("checkpoint lock should acquire");
        let runtime_handle = harness.runtime.handle().clone();
        let authority = Arc::clone(&harness.authority);
        let worker_lock = Arc::clone(&checkpoint_lock);
        let worker_state_path = state_path.clone();
        let worker = thread::spawn(move || {
            handle_experimental_raft_peer_rpc_before_ack(
                &runtime_handle,
                &authority,
                &mut server_stream,
                1,
                &policy,
                ExperimentalRaftPeerRpcDurability {
                    artifact_path: Some(&worker_state_path),
                    checkpoint_lock: Some(&worker_lock),
                    poison_gate: None,
                    checkpoint_ordinary_rpc: true,
                },
            )
        });

        let blocked_response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            blocked_response.is_err(),
            "peer vote RPC must not respond before durable checkpoint completes"
        );
        wait_for_experimental_raft_vote(
            harness.runtime.handle(),
            &harness.authority,
            expected_vote,
            "peer vote should be volatile before checkpoint lock release",
        );
        assert_eq!(
            durable_raft_artifact_vote(&state_path),
            None,
            "checkpoint lock should keep the volatile peer vote out of the artifact"
        );

        drop(checkpoint_guard);
        let result = worker.join().expect("peer RPC worker should not panic");
        assert!(
            result.is_ok(),
            "peer vote RPC should complete after checkpoint lock release: {result:?}"
        );
        assert_eq!(
            durable_raft_artifact_vote(&state_path),
            Some(expected_vote),
            "peer response is released only after the durable artifact contains the vote"
        );

        client_stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("client stream read timeout should update");
        read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("peer vote RPC should respond after durable checkpoint completes");

        harness.shutdown();
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn experimental_raft_peer_vote_response_default_durable_context_checkpoints_and_compacts() {
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
        let poison_gate = AtomicBool::new(false);
        let durability_context = ExperimentalRaftPeerDurabilityContext {
            artifact_path: Some(Arc::new(state_path.clone())),
            checkpoint_lock,
            poison_gate: Arc::new(poison_gate),
        };

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
            "peer RPC should acknowledge after the default durable checkpoint: {result:?}"
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
            Some(expected_vote),
            "default durable peer context must checkpoint ordinary peer RPC state before ack"
        );
        let status = runtime
            .block_on(authority.status())
            .expect("post-peer RPC WAL-backed authority status should read");
        let wal_offsets = status
            .durable_wal_offsets()
            .expect("post-peer RPC WAL-backed authority should report WAL offsets");
        assert_eq!(
            wal_offsets.base_offset(),
            wal_offsets.clean_len(),
            "ordinary peer RPC checkpoint should compact the acknowledged WAL suffix"
        );

        runtime
            .block_on(authority.shutdown())
            .expect("WAL-backed authority should shut down");
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn experimental_raft_peer_vote_poison_before_ack_writes_no_response_after_checkpoint() {
        let harness = experimental_raft_uninitialized_test_harness("peer-vote-poison-before-ack");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-vote-poison-before-ack-{}",
            std::process::id()
        );
        let policy = ControlPlaneRaftPeerTransportPolicy::from_peer_endpoints(
            cluster_name.clone(),
            [(1, "node-1".to_string())],
            ControlPlaneRaftPeerTransportLimits::default(),
        );
        let identity = ControlPlaneRaftPeerFrameIdentity::new(cluster_name, 1, 1);
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
        client_stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("client stream read timeout should set");

        let state_dir = short_unix_socket_test_dir("experimental-raft-peer-vote-poison");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).expect("checkpoint directory should exist");
        let state_path = state_dir.join("control-plane.state");
        let checkpoint_lock = Arc::new(Mutex::new(()));
        let checkpoint_guard = checkpoint_lock
            .lock()
            .expect("checkpoint lock should acquire");
        let poison_gate = Arc::new(AtomicBool::new(false));
        let runtime_handle = harness.runtime.handle().clone();
        let authority = Arc::clone(&harness.authority);
        let worker_lock = Arc::clone(&checkpoint_lock);
        let worker_poison_gate = Arc::clone(&poison_gate);
        let worker_state_path = state_path.clone();
        let worker = thread::spawn(move || {
            handle_experimental_raft_peer_rpc_before_ack(
                &runtime_handle,
                &authority,
                &mut server_stream,
                1,
                &policy,
                ExperimentalRaftPeerRpcDurability {
                    artifact_path: Some(&worker_state_path),
                    checkpoint_lock: Some(&worker_lock),
                    poison_gate: Some(worker_poison_gate.as_ref()),
                    checkpoint_ordinary_rpc: true,
                },
            )
        });

        let blocked_response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            blocked_response.is_err(),
            "peer vote RPC must not respond before durable checkpoint completes"
        );
        wait_for_experimental_raft_vote(
            harness.runtime.handle(),
            &harness.authority,
            expected_vote,
            "peer vote should be volatile before checkpoint lock release",
        );
        poison_gate.store(true, Ordering::Release);
        drop(checkpoint_guard);
        let result = worker.join().expect("peer RPC worker should not panic");
        assert!(
            matches!(result, Err(ExperimentalRaftPeerRpcWorkerError::PeerRpc(_))),
            "peer vote RPC should fail closed after poison flips before ack: {result:?}"
        );
        assert_eq!(
            durable_raft_artifact_vote(&state_path),
            Some(expected_vote),
            "poison before ack still leaves the volatile vote durably checkpointed"
        );

        client_stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("client stream read timeout should update");
        let response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            response.is_err(),
            "poisoned peer vote RPC must not acknowledge after checkpoint"
        );

        harness.shutdown();
        let _ = fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn experimental_raft_peer_rpc_checkpoint_lock_delays_response() {
        let harness = experimental_raft_test_harness("peer-checkpoint-lock");
        let cluster_name = format!(
            "argmin-s3-experimental-raft-peer-checkpoint-lock-{}",
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
        client_stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("client stream read timeout should set");

        let state_dir = short_unix_socket_test_dir("experimental-raft-peer-checkpoint-lock");
        let _ = fs::remove_dir_all(&state_dir);
        fs::create_dir_all(&state_dir).expect("checkpoint directory should exist");
        let state_path = state_dir.join("control-plane.state");
        let checkpoint_lock = Arc::new(Mutex::new(()));
        let checkpoint_guard = checkpoint_lock
            .lock()
            .expect("checkpoint lock should acquire");
        let runtime_handle = harness.runtime.handle().clone();
        let authority = Arc::clone(&harness.authority);
        let worker_lock = Arc::clone(&checkpoint_lock);
        let worker_state_path = state_path.clone();
        let worker = thread::spawn(move || {
            handle_experimental_raft_peer_rpc_before_ack(
                &runtime_handle,
                &authority,
                &mut server_stream,
                1,
                &policy,
                ExperimentalRaftPeerRpcDurability {
                    artifact_path: Some(&worker_state_path),
                    checkpoint_lock: Some(&worker_lock),
                    poison_gate: None,
                    checkpoint_ordinary_rpc: true,
                },
            )
        });

        let blocked_response = read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        );
        assert!(
            blocked_response.is_err(),
            "peer RPC must not respond before acquiring the durable checkpoint lock"
        );
        drop(checkpoint_guard);
        let result = worker.join().expect("peer RPC worker should not panic");
        assert!(
            result.is_ok(),
            "peer RPC should complete after checkpoint lock release: {result:?}"
        );

        client_stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("client stream read timeout should update");
        read_control_plane_raft_peer_transport_frame(
            &mut client_stream,
            ControlPlaneRaftPeerTransportLimits::DEFAULT_MAX_FRAME_BYTES,
        )
        .expect("peer RPC should respond after durable checkpoint completes");

        harness.shutdown();
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
        let expected = harness
            .control_plane
            .current_snapshot()
            .expect("durable experimental snapshot should read before restart");
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
        assert_eq!(restored, expected);

        let runtime_map =
            ControlPlaneRuntimeMapSource::runtime_map_snapshot(&restarted.control_plane, 20_300)
                .expect("durable experimental raft runtime map should read after restart");
        let restored_route = runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(7))
            .expect("restored runtime map should include PG route");
        assert_eq!(restored_route.state(), PgState::Active);
        assert_eq!(restored_route.primary_node_id(), NodeId::new(1));
        assert_eq!(restored_route.primary_lease_deadline_ms(), Some(20_800));

        restarted.shutdown();
        fs::remove_dir_all(&state_dir).unwrap();
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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

        let no_expiry = harness
            .control_plane
            .expire_heartbeat_leases(50_899)
            .expect("pre-deadline expiry should apply as a no-op");
        assert_eq!(no_expiry, (active_cluster_epoch, 0, 0));

        let expiry = harness
            .control_plane
            .expire_heartbeat_leases(50_900)
            .expect("deadline expiry should apply through raft");
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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

        let tmp = short_unix_socket_test_dir("experimental-raft-unix-transfer-admin");
        std::fs::create_dir_all(&tmp).unwrap();
        let socket_path = tmp.join("control-plane.sock");
        let fence_server = spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 40_300);
        let client = UnixControlPlaneClient::new(&socket_path);
        let fenced = client
            .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(PgId::new(13))
            .expect("Unix metadata-transfer fence should succeed");
        fence_server.join().unwrap();
        assert_eq!(fenced.source_primary_lease_deadline_ms(), Some(40_900));
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

        std::fs::remove_file(&socket_path).unwrap();
        let transfer = PgMetadataTransferProof::new(active_epoch, active_proof);
        let install_server =
            spawn_experimental_raft_unix_rpc_server(&harness, &socket_path, 40_400);
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                        cluster_map_history_reference_summary:
                            storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
        authority
            .submit_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(2),
                    node_incarnation: 7,
                    endpoint: "node-2.sock".to_owned(),
                    observed_epoch: floor_epoch,
                    requested_lease_duration_ms: 1_000,
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary {
                            oldest_live_placement_epoch: Some(floor_epoch),
                            oldest_durable_backfill_epoch: None,
                            oldest_pending_metadata_command_epoch: None,
                        },
                    pg_observations: Vec::new(),
                },
                1_000,
            )
            .unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_000).unwrap();

        let diagnostics = format_control_plane_runtime_map_diagnostics(&runtime_map);

        assert!(diagnostics.contains("nodes=1"), "{diagnostics}");
        assert!(
            diagnostics.contains(&format!(
                "oldest_storage_history_floor_epoch={}",
                floor_epoch.get()
            )),
            "{diagnostics}"
        );
        assert!(
            diagnostics.contains(&format!(
                "node_id=2 incarnation=7 endpoint=node-2.sock storage_history_floor_epoch={}",
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

        let (storage_config, control_plane_node_incarnation, data_dir_guard) =
            build_storage_node_process_config(&config, &ec_config).unwrap();

        assert_eq!(control_plane_node_incarnation, None);
        assert!(data_dir_guard.is_none());
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
        config.local_node_count = 1;
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
                            cluster_map_history_reference_summary:
                                storage::PgClusterMapHistoryReferenceSummary::default(),
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
                            cluster_map_history_reference_summary:
                                storage::PgClusterMapHistoryReferenceSummary::default(),
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
                            cluster_map_history_reference_summary:
                                storage::PgClusterMapHistoryReferenceSummary::default(),
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
                            cluster_map_history_reference_summary:
                                storage::PgClusterMapHistoryReferenceSummary::default(),
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
                        cluster_map_history_reference_summary:
                            storage::PgClusterMapHistoryReferenceSummary::default(),
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
                            cluster_map_history_reference_summary:
                                storage::PgClusterMapHistoryReferenceSummary::default(),
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
                        cluster_map_history_reference_summary:
                            storage::PgClusterMapHistoryReferenceSummary::default(),
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
        config.local_node_count = 1;
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
        config.local_node_count = 4;
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
        config.local_node_count = 1;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = None;
        config.storage_node_socket_path = None;
        config.storage_node_sockets.clear();
        config.control_plane_socket_path = Some(socket_path.display().to_string());
        config.control_plane_refresh_interval = std::time::Duration::from_millis(5);

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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
                    cluster_map_history_reference_summary:
                        storage::PgClusterMapHistoryReferenceSummary::default(),
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
        frontend_config.local_node_count = 2;
        frontend_config.pg_count = 1;
        frontend_config.storage_pg_ids = vec![0];
        frontend_config.storage_node_id = None;
        frontend_config.storage_node_socket_path = None;
        frontend_config.storage_node_sockets.clear();
        frontend_config.control_plane_socket_path = Some(socket_path.display().to_string());
        frontend_config.control_plane_refresh_interval = std::time::Duration::from_millis(5);

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
        config.local_node_count = 1;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(tmp.join("node-0-data").display().to_string());
        config.storage_node_socket_path = Some(endpoint.display().to_string());
        config.control_plane_socket_path = Some(socket_path.display().to_string());

        let (node_config, control_plane_node_incarnation, data_dir_guard) =
            build_storage_node_process_config(&config, &ec_config).unwrap();

        server.join().unwrap();
        assert_eq!(control_plane_node_incarnation, Some(1));
        assert!(data_dir_guard.is_some());
        assert_eq!(node_config.node_id(), NodeId::new(0));
        assert_eq!(node_config.socket_path(), endpoint);
        assert_eq!(node_config.pg_ids(), &[0]);
        assert_eq!(node_config.pg_routes()[0].state, PgState::Peering);
        drop(data_dir_guard);
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
        config.local_node_count = 1;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(tmp.join("node-0-data").display().to_string());
        config.storage_node_socket_path = Some(endpoint.display().to_string());
        config.control_plane_socket_path = Some(socket_path.display().to_string());

        let (first_config, first_incarnation, first_guard) =
            build_storage_node_process_config(&config, &ec_config).unwrap();
        drop(first_guard);
        let (second_config, second_incarnation, second_guard) =
            build_storage_node_process_config(&config, &ec_config).unwrap();

        let observed_incarnations = server.join().unwrap();
        assert_eq!(first_incarnation, Some(1));
        assert_eq!(second_incarnation, Some(2));
        assert!(second_guard.is_some());
        assert_eq!(observed_incarnations, vec![1, 2]);
        for node_config in [first_config, second_config] {
            assert_eq!(node_config.node_id(), NodeId::new(0));
            assert_eq!(node_config.socket_path(), endpoint);
            assert_eq!(node_config.pg_ids(), &[0]);
            assert_eq!(node_config.pg_routes()[0].state, PgState::Peering);
        }
        drop(second_guard);
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
        config.local_node_count = 1;
        config.pg_count = 1;
        config.storage_pg_ids = vec![0];
        config.storage_node_id = Some(0);
        config.storage_node_data_dir = Some(tmp.join("node-0-data").display().to_string());
        config.storage_node_socket_path = Some(endpoint.display().to_string());
        config.control_plane_socket_path = Some(socket_path.display().to_string());
        config.control_plane_refresh_interval = std::time::Duration::from_millis(1);

        let (node_config, control_plane_node_incarnation, data_dir_guard) =
            build_storage_node_process_config(&config, &ec_config).unwrap();

        let observed_incarnations = server.join().unwrap();
        assert_eq!(control_plane_node_incarnation, Some(1));
        assert!(data_dir_guard.is_some());
        assert_eq!(observed_incarnations, vec![1]);
        assert_eq!(node_config.node_id(), NodeId::new(0));
        assert_eq!(node_config.socket_path(), endpoint);
        assert_eq!(node_config.pg_ids(), &[0]);
        drop(data_dir_guard);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
