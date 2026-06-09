mod config;

use std::path::Path;
use std::sync::Arc;

use auth::{AccountIdentity, CredentialRecord, CredentialStore, SecretKey};
use ec::EcConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use server_core::coordinator::Coordinator;
use server_core::sse::{
    ManagedWrappingKeyConfig, SseCustomerValidatorConfig, StaticManagedKeyProvider,
};
use storage::storage_node_server::{
    StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeServer,
};
use storage::{
    CanonicalUserId, ClusterEpoch, EcShape, LocalClusterMap, LocalUnixStorageNodeClientConfig,
    NodeId, PgState, StorageCluster,
};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use config::{ConfiguredCredential, ConfiguredCredentialProfile, ProcessRole, ServerConfig};
use server_http::http::HttpFrontend;

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

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
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

fn run_storage_node_process(config: &ServerConfig, ec_config: &EcConfig) -> ! {
    let server = bind_storage_node_process(config, ec_config);
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
    let server = bind_storage_node_process(config, ec_config);
    std::thread::spawn(move || {
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
    let server = StorageNodeServer::bind(storage_config).unwrap_or_else(|e| {
        eprintln!("failed to start storage-node server: {e}");
        std::process::exit(1);
    });
    eprintln!(
        "argmin-s3 storage-node {} listening on {}",
        config
            .storage_node_id
            .expect("storage role must have node id"),
        config
            .storage_node_socket_path
            .as_deref()
            .expect("storage role must have socket path")
    );
    server
}

fn build_storage_node_process_config(
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
        data_dir: Path::new(&node_data_dir).to_path_buf(),
        default_ec_shape: EcShape {
            k: ec_config.data_shards,
            m: ec_config.parity_shards,
        },
        pg_ids,
        socket_path: Path::new(&socket_path).to_path_buf(),
        pg_routes,
    })
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
            LocalUnixStorageNodeClientConfig::new(
                NodeId::new(entry.node_id),
                entry.socket_path.clone(),
            )
        }))
        .map_err(|e| e.to_string())?;
    StorageCluster::from_local_map(Arc::new(local_map)).map_err(|e| e.to_string())
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

    // Build frontend pool sharing the same storage cluster handle.
    let mut frontends = Vec::with_capacity(config.workers as usize);
    for _ in 0..config.workers {
        let coordinator =
            Coordinator::new_with_managed_key_provider_for_storage_cluster_with_background_worker_mode(
                Arc::clone(&storage_cluster),
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
            coordinator,
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

    eprintln!(
        "argmin-s3 listening on {}://{} (EC {},{}, {} PGs, {} workers, max {} conns, max {} in-flight, read chunk {} bytes, panic-on-500 {}, abort-on-500 {}, local-debug {}, region {}, host id {})",
        scheme,
        config.listen_addr,
        config.ec_k,
        config.ec_m,
        config.pg_count,
        config.workers,
        config.max_connections,
        config.max_inflight_requests,
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
                config.max_inflight_requests,
                serve_config,
            )
            .await;
        }
        None => {
            server_http::http::serve::serve(
                listener,
                frontends,
                config.max_connections,
                config.max_inflight_requests,
                serve_config,
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
