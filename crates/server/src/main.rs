use std::path::Path;

use auth::{CredentialStore, SecretKey};
use ec::EcConfig;
use storage::{LocalStorageNode, SqliteBucketDb};
use tokio::net::TcpListener;

use server::config::ServerConfig;
use server::coordinator::Coordinator;
use server::http::HttpFrontend;

#[tokio::main]
async fn main() {
    let config = match ServerConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
    };

    // EC self-test
    if let Err(e) = ec::self_test() {
        eprintln!("EC self-test failed: {}", e);
        std::process::exit(1);
    }

    // Build EC config
    let ec_config = match EcConfig::new(config.ec_k, config.ec_m) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("invalid EC config: {}", e);
            std::process::exit(1);
        }
    };

    let pg_ids: Vec<u32> = (0..config.pg_count).collect();
    let data_dir = Path::new(&config.data_dir);
    let bucket_db_path = data_dir.join("buckets.db");

    // Build frontend pool — each frontend gets its own SQLite connections.
    let mut frontends = Vec::with_capacity(config.workers as usize);
    for _ in 0..config.workers {
        let storage_node = match LocalStorageNode::open(data_dir, &pg_ids) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open storage: {}", e);
                std::process::exit(1);
            }
        };
        let bucket_db = match SqliteBucketDb::open(&bucket_db_path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to open bucket database: {}", e);
                std::process::exit(1);
            }
        };
        let coordinator = match Coordinator::new(
            storage_node,
            bucket_db,
            ec_config,
            config.pg_count,
            config.region.clone(),
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("failed to create coordinator: {}", e);
                std::process::exit(1);
            }
        };
        let mut credentials = CredentialStore::new();
        credentials.add(
            config.access_key_id.clone(),
            SecretKey(config.secret_access_key.clone()),
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
        "argmin-s3 listening on {} (EC {},{}, {} PGs, {} workers, region {})",
        config.listen_addr,
        config.ec_k,
        config.ec_m,
        config.pg_count,
        config.workers,
        config.region
    );

    server::http::serve::serve(
        listener,
        frontends,
        config.max_connections,
        server::http::serve::ServeConfig::default(),
    )
    .await;
}
