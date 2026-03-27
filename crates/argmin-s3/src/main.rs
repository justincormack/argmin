mod config;

use std::path::Path;
use std::sync::Arc;

use auth::{CredentialStore, SecretKey};
use ec::EcConfig;
use server_core::coordinator::Coordinator;
use server_core::sse::SseCustomerValidatorConfig;
use storage::SharedStorageNode;
use tokio::net::TcpListener;

use config::ServerConfig;
use server_http::http::HttpFrontend;

#[tokio::main]
async fn main() {
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
        credentials.add(
            config.access_key_id.clone(),
            SecretKey::new(config.secret_access_key.clone()),
        );
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

    eprintln!(
        "argmin-s3 listening on {} (EC {},{}, {} PGs, {} workers, max {} conns, max {} in-flight, read chunk {} bytes, region {})",
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

    server_http::http::serve::serve(
        listener,
        frontends,
        config.max_connections,
        config.max_inflight_requests,
        server_http::http::serve::ServeConfig {
            stream_read_chunk_size: config.stream_read_chunk_size,
            ..server_http::http::serve::ServeConfig::default()
        },
    )
    .await;
}
