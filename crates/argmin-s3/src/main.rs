mod config;

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use auth::{AccountIdentity, CredentialRecord, CredentialStore, SecretKey};
use ec::EcConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use server_core::coordinator::Coordinator;
use server_core::sse::{
    ManagedWrappingKeyConfig, SseCustomerValidatorConfig, StaticManagedKeyProvider,
};
use storage::control_plane::{
    build_control_plane_unix_response, read_control_plane_unix_request,
    write_control_plane_unix_response, ClusterRuntimeMapSnapshot, ControlPlaneRuntimeMapSource,
    FileControlPlaneStore, PgMetadataProof, PgMetadataTransferProof, PgRouteSnapshot,
    SingleAuthorityControlPlane, UnixControlPlaneClient,
};
use storage::storage_node_server::{
    StorageNodeControlPlaneRefreshLoop, StorageNodePgRoute, StorageNodeProcessConfig,
    StorageNodeServer,
};
use storage::{
    CanonicalUserId, ClusterEpoch, EcShape, LocalClusterMap,
    LocalUnixStorageNodeClientAdmissionSettings, LocalUnixStorageNodeClientConfig, NodeId, PgId,
    PgMetadataTransferArtifact, PgMetadataTransferError, PgState, StorageCluster,
    StorageClusterRuntimeMapHandle, StoreError,
};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use config::{ConfiguredCredential, ConfiguredCredentialProfile, ProcessRole, ServerConfig};
use server_http::http::HttpFrontend;

const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;
const CONTROL_PLANE_ACCEPT_BATCH_LIMIT: usize = 32;
const CONTROL_PLANE_RPC_WORKER_LIMIT: usize = 64;
const CONTROL_PLANE_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);

extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
    fn getuid() -> u32;
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
        secret_key: SecretKey::new(credential.secret_access_key.clone()),
        account: AccountIdentity::new(
            credential.principal.clone(),
            CanonicalUserId::from_principal(&credential.account_id),
            credential.display_name.clone(),
        ),
        authorization_profile: authorization_profile(credential.authorization_profile),
        session_token: None,
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
        secret_key: SecretKey::new(config.secret_access_key.clone()),
        account,
        authorization_profile: auth::AuthorizationProfile::OwnerAccountAdmin,
        session_token: None,
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

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if let Some(exit_code) = maybe_run_control_plane_admin_command() {
        std::process::exit(exit_code);
    }
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
    UnixControlPlaneClient::new(socket_path)
        .set_pg_acting_set(pg_id, acting_set)
        .map_err(|error| format!("failed to set live PG acting set: {error}"))
}

fn fence_control_plane_pg_for_metadata_transfer_live(
    socket_path: &Path,
    pg_id: PgId,
) -> Result<ClusterEpoch, String> {
    UnixControlPlaneClient::new(socket_path)
        .fence_pg_for_metadata_transfer(pg_id)
        .map_err(|error| format!("failed to fence live PG for metadata transfer: {error}"))
}

fn set_control_plane_pg_acting_set_with_metadata_transfer_live(
    socket_path: &Path,
    pg_id: PgId,
    acting_set: Vec<NodeId>,
    transfer: PgMetadataTransferProof,
) -> Result<ClusterEpoch, String> {
    UnixControlPlaneClient::new(socket_path)
        .set_pg_acting_set_with_metadata_transfer(pg_id, acting_set, transfer)
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
    let control_plane = UnixControlPlaneClient::new(socket_path);
    let failpoint = MetadataTransferLiveFailpoint::from_env()?;
    if let Some(summary) =
        completed_metadata_transfer_live_summary(&control_plane, pg_id, &acting_set)?
    {
        return Ok(summary);
    }
    let fenced = control_plane
        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease(pg_id)
        .map_err(|error| format!("failed to fence live PG for metadata transfer: {error}"))?;
    let (fenced_runtime, source_lease_deadline_ms) = fenced.into_parts();
    let source_node_id = metadata_transfer_peering_source_node_id(&fenced_runtime, pg_id)?;
    if let Some(source_lease_deadline_ms) = source_lease_deadline_ms {
        wait_for_metadata_transfer_source_lease_to_expire(source_lease_deadline_ms);
    }
    let source_runtime = control_plane
        .fence_pg_for_metadata_transfer_runtime_map(pg_id)
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
            let destination_runtime = control_plane
                .set_pg_acting_set_with_metadata_transfer_runtime_map(
                    pg_id,
                    acting_set.clone(),
                    transfer,
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
        &control_plane,
        &destination_cluster,
        &artifact,
        pg_id,
        &acting_set,
        import_epoch,
        imported_proof,
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
    control_plane: &UnixControlPlaneClient,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<Option<MetadataTransferLiveSummary>, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let runtime_map =
            match control_plane.runtime_map_snapshot(storage::clock::current_time_millis()) {
                Ok(runtime_map) => runtime_map,
                Err(error) => {
                    let message = error.to_string();
                    if control_plane_runtime_map_not_ready_for_metadata_transfer(&message) {
                        if Instant::now() >= deadline {
                            return Ok(None);
                        }
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                    return Err(format!(
                    "failed to fetch control-plane runtime map before metadata transfer: {error}"
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
            imported_proof: PgMetadataProof::new(0, 0, 0),
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

fn import_pg_metadata_transfer_artifact_retrying_stale_route(
    control_plane: &UnixControlPlaneClient,
    destination_cluster: &StorageCluster,
    artifact: &PgMetadataTransferArtifact,
    pg_id: PgId,
    acting_set: &[NodeId],
    destination_epoch: ClusterEpoch,
    imported_proof: PgMetadataProof,
) -> Result<PgMetadataProof, String> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match destination_cluster.import_pg_metadata_transfer_artifact_from_retained_log(artifact) {
            Ok(proof) => return Ok(proof),
            Err(error) if metadata_transfer_error_is_transient_route_refresh(&error) => {
                if control_plane_pg_active_with_acting_set(
                    control_plane,
                    pg_id,
                    acting_set,
                    destination_epoch,
                )? {
                    return Ok(imported_proof);
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

fn control_plane_pg_active_with_acting_set(
    control_plane: &UnixControlPlaneClient,
    pg_id: PgId,
    acting_set: &[NodeId],
    min_cluster_epoch: ClusterEpoch,
) -> Result<bool, String> {
    let runtime_map =
        match control_plane.runtime_map_snapshot(storage::clock::current_time_millis()) {
            Ok(runtime_map) => runtime_map,
            Err(error) => {
                let message = error.to_string();
                if control_plane_runtime_map_not_ready_for_metadata_transfer(&message) {
                    return Ok(false);
                }
                return Err(format!(
                    "failed to verify live PG metadata transfer state: {error}"
                ));
            }
        };
    if runtime_map.cluster_epoch() < min_cluster_epoch {
        return Ok(false);
    }
    let Some(route) = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == pg_id)
    else {
        return Ok(false);
    };
    Ok(route.state() == PgState::Active && route.acting_set() == acting_set)
}

fn control_plane_runtime_map_not_ready_for_metadata_transfer(message: &str) -> bool {
    message.contains(" has no serving primary in cluster epoch ")
        || (message.contains(" primary node ")
            && message.contains(" has not reported active state in cluster epoch "))
        || (message.contains(" reported unresolved pending metadata command for PG ")
            && message.contains(" in cluster epoch "))
}

fn metadata_transfer_error_is_transient_route_refresh(error: &PgMetadataTransferError) -> bool {
    match error {
        PgMetadataTransferError::Store(store_error) => {
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
        StoreError::StaleShardLocation { .. } => true,
        StoreError::StorageRpc { message, .. } => {
            message.starts_with("StaleShardLocation: ")
                || (message.starts_with("InactivePgRoute: historical peering inspection ")
                    && message.contains(" requires Peering route, got active"))
                || (message.starts_with("InactivePgRoute: historical metadata log read ")
                    && message.contains(" requires Peering route, got active"))
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
    let runtime_map = UnixControlPlaneClient::new(socket_path)
        .runtime_map_snapshot(storage::clock::current_time_millis())
        .map_err(|error| format!("control-plane runtime map is not ready: {error}"))?;
    let pg_routes = runtime_map.pg_routes().len();
    let active_serving_pg_routes = runtime_map
        .pg_routes()
        .iter()
        .filter(|route| {
            route.state() == PgState::Active && route.primary_lease_deadline_ms().is_some()
        })
        .count();
    Ok((
        runtime_map.cluster_epoch(),
        pg_routes,
        active_serving_pg_routes,
    ))
}

fn run_control_plane_process(config: &ServerConfig) -> ! {
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
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap_or_else(|error| {
        eprintln!("failed to open control-plane state {state_path}: {error}");
        std::process::exit(1);
    });
    bootstrap_empty_control_plane(&mut authority, config).unwrap_or_else(|error| {
        eprintln!("failed to bootstrap control-plane state: {error}");
        std::process::exit(1);
    });
    let authority = Arc::new(Mutex::new(authority));
    let active_rpc_workers = Arc::new(AtomicUsize::new(0));
    eprintln!(
        "argmin-s3 control-plane manager using state {} on {} (lease scan {} ms)",
        state_path,
        socket_path,
        config.control_plane_lease_scan_interval.as_millis()
    );

    loop {
        for _ in 0..CONTROL_PLANE_ACCEPT_BATCH_LIMIT {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    spawn_control_plane_rpc_worker(
                        stream,
                        Arc::clone(&authority),
                        Arc::clone(&active_rpc_workers),
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    eprintln!("control-plane socket accept failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        let now_ms = storage::clock::current_time_millis();
        let expiry = authority
            .lock()
            .expect("control-plane authority mutex poisoned")
            .expire_heartbeat_leases(now_ms);
        match expiry {
            Ok(expiry) if !expiry.expired_nodes().is_empty() => {
                eprintln!(
                    "control-plane expired {} node leases at epoch {} and moved {} PGs to peering",
                    expiry.expired_nodes().len(),
                    expiry.cluster_epoch(),
                    expiry.peering_pgs().len()
                );
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("control-plane lease expiry failed: {error}");
                std::process::exit(1);
            }
        }
        thread::sleep(config.control_plane_lease_scan_interval);
    }
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
    eprintln!(
        "control-plane bootstrapped {} nodes and {} PG acting sets at epoch {}",
        node_count,
        config.storage_pg_ids.len(),
        authority.snapshot().cluster_epoch()
    );
    Ok(())
}

fn spawn_control_plane_rpc_worker(
    mut stream: UnixStream,
    authority: Arc<Mutex<SingleAuthorityControlPlane<FileControlPlaneStore>>>,
    active_rpc_workers: Arc<AtomicUsize>,
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
        let now_ms = storage::clock::current_time_millis();
        let response = {
            let mut authority = authority
                .lock()
                .expect("control-plane authority mutex poisoned");
            build_control_plane_unix_response(&mut *authority, request, now_ms)
        };
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

struct ControlPlaneRpcWorkerGuard {
    active_rpc_workers: Arc<AtomicUsize>,
}

impl Drop for ControlPlaneRpcWorkerGuard {
    fn drop(&mut self) {
        self.active_rpc_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn bind_control_plane_socket(socket_path: &Path) -> Result<UnixListener, String> {
    if !socket_path.is_absolute() {
        return Err(format!(
            "ARGMIN_CONTROL_PLANE_SOCKET_PATH {} must be absolute",
            socket_path.display()
        ));
    }
    let parent = socket_path.parent().ok_or_else(|| {
        format!(
            "ARGMIN_CONTROL_PLANE_SOCKET_PATH {} is missing a parent directory",
            socket_path.display()
        )
    })?;
    socket_path.file_name().ok_or_else(|| {
        format!(
            "ARGMIN_CONTROL_PLANE_SOCKET_PATH {} is missing a file name",
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
    let server = Arc::new(bind_storage_node_process(config, ec_config));
    let _control_plane_refresh_loop =
        maybe_spawn_storage_node_control_plane_refresh_loop(Arc::clone(&server), config);
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
    let server = Arc::new(bind_storage_node_process(config, ec_config));
    let control_plane_refresh_loop =
        maybe_spawn_storage_node_control_plane_refresh_loop(Arc::clone(&server), config);
    std::thread::spawn(move || {
        let _control_plane_refresh_loop = control_plane_refresh_loop;
        if let Err(error) = server.serve_forever() {
            eprintln!("storage-node server failed: {error}");
            std::process::exit(1);
        }
    })
}

fn bind_storage_node_process(config: &ServerConfig, ec_config: &EcConfig) -> StorageNodeServer {
    let storage_config = build_storage_node_process_config(config, ec_config).unwrap_or_else(|e| {
        eprintln!("storage-node configuration error: {e}");
        std::process::exit(1);
    });
    let node_id = storage_config.node_id;
    let socket_path = storage_config.socket_path.clone();
    let server = StorageNodeServer::bind(storage_config).unwrap_or_else(|e| {
        eprintln!("failed to start storage-node server: {e}");
        std::process::exit(1);
    });
    eprintln!(
        "argmin-s3 storage-node {} listening on {}",
        node_id.as_u32(),
        socket_path.display()
    );
    server
}

fn maybe_spawn_storage_node_control_plane_refresh_loop(
    server: Arc<StorageNodeServer>,
    config: &ServerConfig,
) -> Option<StorageNodeControlPlaneRefreshLoop> {
    let socket_path = config.control_plane_socket_path.as_deref()?;
    let node_incarnation = server
        .advance_control_plane_node_incarnation()
        .unwrap_or_else(|error| {
            eprintln!("failed to advance storage-node control-plane incarnation: {error}");
            std::process::exit(1);
        });
    let lease_ms = u64::try_from(config.control_plane_heartbeat_lease_duration.as_millis())
        .unwrap_or_else(|_| {
            eprintln!("ARGMIN_CONTROL_PLANE_HEARTBEAT_LEASE_MS is too large");
            std::process::exit(1);
        });
    let loop_handle = server
        .spawn_control_plane_refresh_loop(
            UnixControlPlaneClient::new(socket_path),
            node_incarnation,
            lease_ms,
            config.control_plane_refresh_interval,
            storage::clock::current_time_millis,
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to start storage-node control-plane refresh loop: {error}");
            std::process::exit(1);
        });
    eprintln!(
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
) -> Result<StorageNodeProcessConfig, String> {
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
    Ok(StorageNodeProcessConfig {
        node_id,
        cluster_epoch,
        route_map_valid_until_ms: None,
        data_dir: Path::new(&node_data_dir).to_path_buf(),
        default_ec_shape: EcShape {
            k: ec_config.data_shards,
            m: ec_config.parity_shards,
        },
        pg_ids,
        socket_path: Path::new(&socket_path).to_path_buf(),
        pg_routes,

        historical_pg_routes: Vec::new(),
    })
}

fn build_control_plane_storage_node_process_config(
    config: &ServerConfig,
    ec_config: &EcConfig,
    control_plane_socket_path: &str,
) -> Result<StorageNodeProcessConfig, String> {
    let node_id = NodeId::new(
        config
            .storage_node_id
            .ok_or_else(|| "ARGMIN_STORAGE_NODE_ID is required for storage roles".to_string())?,
    );
    let node_data_dir = config
        .storage_node_data_dir
        .clone()
        .unwrap_or_else(|| format!("{}/node-{:04}", config.data_dir, node_id.as_u32()));
    let runtime_map =
        UnixControlPlaneClient::new(control_plane_socket_path)
            .runtime_map_snapshot(storage::clock::current_time_millis())
            .map_err(|error| {
                format!(
                    "failed to fetch control-plane runtime map from {control_plane_socket_path}: {error}"
                )
            })?;
    let node_config = StorageNodeProcessConfig::from_runtime_map(
        node_id,
        Path::new(&node_data_dir).to_path_buf(),
        EcShape {
            k: ec_config.data_shards,
            m: ec_config.parity_shards,
        },
        &runtime_map,
    )
    .map_err(|error| error.to_string())?;
    let configured_socket_path = config.storage_node_socket_path.as_deref().ok_or_else(|| {
        "ARGMIN_STORAGE_NODE_SOCKET_PATH is required for storage roles".to_string()
    })?;
    if node_config.socket_path != Path::new(configured_socket_path) {
        return Err(format!(
            "ARGMIN_STORAGE_NODE_SOCKET_PATH {} must match control-plane endpoint {} for node {}",
            configured_socket_path,
            node_config.socket_path.display(),
            node_id.as_u32()
        ));
    }
    Ok(node_config)
}

async fn run_legacy_local_frontend(config: ServerConfig, host_id: String, ec_config: EcConfig) {
    let pg_ids: Vec<u32> = (0..config.pg_count).collect();
    let data_dir = Path::new(&config.data_dir);
    let ec_shape = storage::EcShape {
        k: ec_config.data_shards,
        m: ec_config.parity_shards,
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
    let storage_cluster = build_remote_frontend_storage_cluster(&config, &ec_config)
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
        k: ec_config.data_shards,
        m: ec_config.parity_shards,
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
        .install_unix_storage_node_clients(config.storage_node_sockets.iter().map(|entry| {
            LocalUnixStorageNodeClientConfig::with_rpc_admission(
                NodeId::new(entry.node_id),
                entry.socket_path.clone(),
                config.storage_node_rpc_admission_limit,
                config.storage_node_rpc_admission_wait_timeout,
                config.storage_node_rpc_control_admission_wait_timeout,
            )
        }))
        .map_err(|e| e.to_string())?;
    StorageCluster::from_local_map(Arc::new(local_map)).map_err(|e| e.to_string())
}

fn build_control_plane_frontend_storage_cluster(
    config: &ServerConfig,
    ec_config: &EcConfig,
    control_plane_socket_path: &str,
) -> Result<Arc<StorageCluster>, String> {
    let runtime_map =
        UnixControlPlaneClient::new(control_plane_socket_path)
            .runtime_map_snapshot(storage::clock::current_time_millis())
            .map_err(|error| {
                format!(
                    "failed to fetch control-plane runtime map from {control_plane_socket_path}: {error}"
                )
            })?;
    build_frontend_storage_cluster_from_runtime_map(config, ec_config, &runtime_map)
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
            k: ec_config.data_shards,
            m: ec_config.parity_shards,
        },
        unix_storage_node_client_admission_settings(config),
    )
    .map_err(|error| error.to_string())
}

fn unix_storage_node_client_admission_settings(
    config: &ServerConfig,
) -> LocalUnixStorageNodeClientAdmissionSettings {
    LocalUnixStorageNodeClientAdmissionSettings::new(
        config.storage_node_rpc_admission_limit,
        config.storage_node_rpc_admission_wait_timeout,
        config.storage_node_rpc_control_admission_wait_timeout,
    )
}

async fn run_frontend_server(
    config: ServerConfig,
    host_id: String,
    storage_cluster: Arc<StorageCluster>,
    background_worker_mode: server_core::coordinator::BackgroundWorkerMode,
) {
    let sse_c_validator = config
        .sse_c_validator_key_b64
        .as_deref()
        .map(|key| SseCustomerValidatorConfig::from_base64(1, key))
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("invalid SSE-C validator key: {e}");
            std::process::exit(1);
        });
    let managed_key_provider =
        ManagedWrappingKeyConfig::from_base64(1, &config.sse_s3_wrapping_key_b64)
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

    eprintln!(
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
    let admission_settings = LocalUnixStorageNodeClientAdmissionSettings::new(
        config.storage_node_rpc_admission_limit,
        config.storage_node_rpc_admission_wait_timeout,
        config.storage_node_rpc_control_admission_wait_timeout,
    );
    let loop_handle = storage_cluster_handle
        .spawn_control_plane_refresh_loop_with_unix_storage_node_clients(
            UnixControlPlaneClient::new(socket_path),
            config.control_plane_refresh_interval,
            storage::clock::current_time_millis,
            admission_settings,
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to start frontend control-plane runtime-map refresh loop: {error}");
            std::process::exit(1);
        });
    eprintln!(
        "argmin-s3 frontend control-plane runtime-map refresh using {} (refresh {} ms)",
        socket_path,
        config.control_plane_refresh_interval.as_millis(),
    );
    Some(loop_handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::control_plane::{
        NodeMembershipState, NodePgHeartbeatObservation, PgMetadataProof,
    };

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
            control_plane_lease_scan_interval: std::time::Duration::from_millis(250),
            control_plane_refresh_interval: std::time::Duration::from_millis(250),
            control_plane_heartbeat_lease_duration: std::time::Duration::from_millis(1000),
            storage_cluster_epoch: 9,
            storage_pg_ids: vec![1, 3, 5],
            ec_k: 4,
            ec_m: 2,
            account_id: String::new(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            uat_credentials: Vec::new(),
            host_id: None,
            sse_c_validator_key_b64: None,
            sse_s3_wrapping_key_b64: String::new(),
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
            PgMetadataProof::new(20, 30, 40)
        );
        assert_eq!(transfer.metadata_proof(), PgMetadataProof::new(20, 31, 40));
        assert_eq!(acting_set, vec![NodeId::new(2), NodeId::new(3)]);
    }

    #[test]
    fn metadata_transfer_retry_treats_active_peering_inspection_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::StorageRpc {
            node_id: 0,
            operation: "metadata command validate replay state preserving pending",
            message: "InactivePgRoute: historical peering inspection for PG 0 at epoch 28 requires Peering route, got active".to_string(),
        });

        assert!(metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_retry_does_not_treat_active_route_mismatch_as_transient() {
        let error = PgMetadataTransferError::Store(StoreError::StorageRpc {
            node_id: 0,
            operation: "metadata command validate replay state preserving pending",
            message: "InactivePgRoute: historical peering inspection for PG 0 at epoch 28 requires Peering route, got peering".to_string(),
        });

        assert!(!metadata_transfer_error_is_transient_route_refresh(&error));
    }

    #[test]
    fn metadata_transfer_active_check_treats_incomplete_runtime_map_as_retryable() {
        assert!(control_plane_runtime_map_not_ready_for_metadata_transfer(
            "control-plane RPC remote error: PG 1 has no serving primary in cluster epoch 26"
        ));
        assert!(control_plane_runtime_map_not_ready_for_metadata_transfer(
            "control-plane RPC remote error: PG 0 primary node 2 has not reported active state in cluster epoch 31"
        ));
        assert!(control_plane_runtime_map_not_ready_for_metadata_transfer(
            "control-plane RPC remote error: node 1 reported unresolved pending metadata command for PG 27 in cluster epoch 83"
        ));
        assert!(!control_plane_runtime_map_not_ready_for_metadata_transfer(
            "control-plane RPC remote error: unknown PG 99"
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

        let storage_config = build_storage_node_process_config(&config, &ec_config).unwrap();

        assert_eq!(storage_config.node_id, NodeId::new(2));
        assert_eq!(storage_config.cluster_epoch, ClusterEpoch::new(9).unwrap());
        assert_eq!(storage_config.pg_ids, vec![1, 3, 5]);
        assert_eq!(
            storage_config
                .pg_routes
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
                            pg_observations: Vec::new(),
                        },
                        now_ms,
                    )
                    .unwrap();
                if lease.serving() {
                    break;
                }
                assert!(now_ms < 1_003, "authority did not grant serving lease");
            }
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_001).unwrap();
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
                has_pending_metadata_command: false,
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

            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_001).unwrap();

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
                        pg_observations: vec![NodePgHeartbeatObservation {
                            pg_id: PgId::new(0),
                            state: PgState::Active,
                            metadata_proof: PgMetadataProof::empty(),
                            has_pending_metadata_command: false,
                        }],
                    },
                    1_003,
                )
                .unwrap();

            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_004).unwrap();
        })
    }

    #[test]
    fn remote_frontend_storage_cluster_can_bootstrap_from_control_plane_socket() {
        let tmp = short_unix_socket_test_dir("fb");
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
            PgState::Peering
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
        let server = serve_one_control_plane_runtime_map(
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
            PgState::Peering
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

        let node_config = build_storage_node_process_config(&config, &ec_config).unwrap();

        server.join().unwrap();
        assert_eq!(node_config.node_id, NodeId::new(0));
        assert_eq!(node_config.socket_path, endpoint);
        assert_eq!(node_config.pg_ids, vec![0]);
        assert_eq!(node_config.pg_routes[0].state, PgState::Peering);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
