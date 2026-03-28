mod config;

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use auth::{AccountIdentity, CredentialRecord, CredentialStore, SecretKey};
use ec::EcConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use server_core::coordinator::Coordinator;
use server_core::sse::SseCustomerValidatorConfig;
use storage::CanonicalUserId;
use storage::SharedStorageNode;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use config::ServerConfig;
use server_http::http::HttpFrontend;

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open TLS cert {path}: {e}"))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to read TLS cert {path}: {e}"))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|e| format!("failed to open TLS key {path}: {e}"))?;
    let mut reader = BufReader::new(file);
    let Some(key) = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("failed to read TLS key {path}: {e}"))?
    else {
        return Err(format!("no private key found in {path}"));
    };
    Ok(key)
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

    // EC self-test
    if let Err(e) = ec::self_test() {
        eprintln!("EC self-test failed: {e}");
        std::process::exit(1);
    }

    // Build EC config
    let ec_config = match EcConfig::new(config.ec_k, config.ec_m) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("invalid EC config: {e}");
            std::process::exit(1);
        }
    };

    let pg_ids: Vec<u32> = (0..config.pg_count).collect();
    let data_dir = Path::new(&config.data_dir);
    let sse_c_validator = config
        .sse_c_validator_key_b64
        .as_deref()
        .map(|key| SseCustomerValidatorConfig::from_base64(1, key))
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("invalid SSE-C validator key: {e}");
            std::process::exit(1);
        });

    // Create one shared storage node for all workers.
    let storage_node = match SharedStorageNode::open(data_dir, &pg_ids) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("failed to open storage: {e}");
            std::process::exit(1);
        }
    };

    // Build frontend pool sharing the same storage node
    // (PG access serialized by mutex).
    let mut frontends = Vec::with_capacity(config.workers as usize);
    for _ in 0..config.workers {
        let coordinator = match Coordinator::new_with_sse_c_validator(
            Arc::clone(&storage_node),
            ec_config,
            config.region.clone(),
            sse_c_validator.clone(),
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("failed to create coordinator: {e}");
                std::process::exit(1);
            }
        };
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
            session_token: None,
            expires_at_epoch_secs: None,
            enabled: true,
        });
        frontends.push(HttpFrontend {
            coordinator,
            credentials,
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
        "argmin-s3 listening on {}://{} (EC {},{}, {} PGs, {} workers, max {} conns, max {} in-flight, read chunk {} bytes, region {})",
        scheme,
        config.listen_addr,
        config.ec_k,
        config.ec_m,
        config.pg_count,
        config.workers,
        config.max_connections,
        config.max_inflight_requests,
        config.stream_read_chunk_size,
        config.region
    );

    let serve_config = server_http::http::serve::ServeConfig {
        stream_read_chunk_size: config.stream_read_chunk_size,
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
