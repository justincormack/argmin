use std::{collections::BTreeSet, io::Write, path::Path};

use s3_tests::{
    aws_sdk_s3::{
        error::{ProvideErrorMetadata, SdkError},
        primitives::ByteStream,
        types::{BucketVersioningStatus, VersioningConfiguration},
    },
    build_client_with_ca, cleanup_versioned_bucket, delete_bucket_retrying_operation_aborted,
    delete_object_retrying_operation_aborted, get_object_body_retrying_operation_aborted,
    put_object_retrying_operation_aborted, retrying_operation_aborted_result, unique_bucket,
    SendRetryingOperationAborted, RT,
};
use storage::{BucketName, GenerationId, ObjectKey, PgTopology};

fn usage() -> ! {
    eprintln!(
        "usage: uat_pg_backfill_smoke create-put <bucket-file> <key> <body-file> | create-versioned-put-distinct-data-pg <bucket-file> <key-file> <data-pg-file> <key-prefix> <body-file> [target-data-pg] [excluded-metadata-pg-csv] | create-put-distinct-data-pg <bucket-file> <key-file> <data-pg-file> <key-prefix> <body-file> [target-data-pg] [excluded-metadata-pg-csv] | create-put-metadata-pg <bucket-file> <key-file> <metadata-pg-file> <key-prefix> <body-file> <target-metadata-pg> | put-for-data-pg <bucket-file> <key-file> <key-prefix> <body-file> <data-pg> [excluded-metadata-pg-csv] | put-for-metadata-pg <bucket-file> <key-file> <key-prefix> <body-file> <target-metadata-pg> | put <bucket-file> <key> <body-file> | put-expect-failure <bucket-file> <key> <body-file> | get <bucket-file> <key> <body-file> | get-expect-failure <bucket-file> <key> | head <bucket-file> <key> <body-file> | head-expect-failure <bucket-file> <key> | list-contains <bucket-file> <key>... | list-expect-failure <bucket-file> | list-versions-contains <bucket-file> <key>... | cleanup <bucket-file> <key>... | cleanup-versioned <bucket-file> | cleanup-versioned-stress <bucket-count> <keys-per-bucket> <versions-per-key> <key-prefix> <body-file> [bucket-log-file]"
    );
    std::process::exit(2);
}

fn read_bucket(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read bucket file {}: {error}", path.display()))
        .trim()
        .to_string()
}

fn read_body(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|error| panic!("read body file {}: {error}", path.display()))
}

fn run<F: std::future::Future>(future: F) -> F::Output {
    RT.block_on(future)
}

fn expect_s3_service_error<E>(context: &str, error: &SdkError<E>, require_code: bool)
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let Some(service_error) = error.as_service_error() else {
        panic!("{context} failed below S3 API layer: {error:?}");
    };
    if require_code && service_error.code().is_none() {
        panic!("{context} returned S3 service error without code: {error:?}");
    }
    let code = service_error.code().unwrap_or("<none>");
    eprintln!("expected S3 failure for {context}: code={code} error={error:?}");
}

fn client_from_env() -> s3_tests::aws_sdk_s3::Client {
    let endpoint = std::env::var("S3_TEST_ENDPOINT").expect("S3_TEST_ENDPOINT is required");
    let access_key = std::env::var("S3_TEST_ACCESS_KEY").expect("S3_TEST_ACCESS_KEY is required");
    let secret_key = std::env::var("S3_TEST_SECRET_KEY").expect("S3_TEST_SECRET_KEY is required");
    let region = std::env::var("S3_TEST_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let tls_ca_pem = std::env::var("S3_TEST_TLS_CA_CERT_PATH").ok().map(|path| {
        std::fs::read(&path).unwrap_or_else(|error| {
            panic!("read S3_TEST_TLS_CA_CERT_PATH {path}: {error}");
        })
    });
    build_client_with_ca(
        &endpoint,
        &access_key,
        &secret_key,
        &region,
        tls_ca_pem.as_deref(),
    )
}

async fn create_bucket(client: &s3_tests::aws_sdk_s3::Client, bucket: &str) {
    retrying_operation_aborted_result(|| {
        let request = client.create_bucket().bucket(bucket);
        Box::pin(async move { request.send().await.map(|_| ()) })
    })
    .await
    .unwrap_or_else(|error| panic!("create UAT bucket {bucket}: {error:?}"));
}

async fn enable_bucket_versioning(client: &s3_tests::aws_sdk_s3::Client, bucket: &str) {
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send_retrying_operation_aborted("enable UAT bucket versioning")
        .await
        .unwrap_or_else(|error| panic!("enable UAT bucket versioning {bucket}: {error:?}"));
}

async fn put_object(client: &s3_tests::aws_sdk_s3::Client, bucket: &str, key: &str, body: Vec<u8>) {
    put_object_retrying_operation_aborted(client, bucket, key, body).await;
}

async fn get_object_body(
    client: &s3_tests::aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Vec<u8> {
    get_object_body_retrying_operation_aborted(
        client,
        bucket,
        key,
        None,
        "uat PG backfill get object",
    )
    .await
}

async fn cleanup_bucket(client: &s3_tests::aws_sdk_s3::Client, bucket: &str, keys: &[String]) {
    for key in keys {
        match delete_object_retrying_operation_aborted(client, bucket, key).await {
            Ok(_) => {}
            Err(error) if error.code() == Some("NoSuchBucket") => return,
            Err(error) => panic!("delete UAT object {bucket}/{key}: {error:?}"),
        }
    }
    delete_bucket_retrying_operation_aborted(client, bucket).await;
}

async fn head_object_matches_len(
    client: &s3_tests::aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_len: usize,
) {
    let response = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send_retrying_operation_aborted("head UAT object")
        .await
        .unwrap_or_else(|error| panic!("head UAT object {bucket}/{key}: {error:?}"));
    assert_eq!(
        response.content_length().unwrap_or_default(),
        expected_len as i64,
        "object content length mismatch for {key}",
    );
}

async fn list_contains_keys(client: &s3_tests::aws_sdk_s3::Client, bucket: &str, keys: &[String]) {
    let response = client
        .list_objects_v2()
        .bucket(bucket)
        .send_retrying_operation_aborted("list UAT objects")
        .await
        .unwrap_or_else(|error| panic!("list UAT objects in {bucket}: {error:?}"));
    let listed: BTreeSet<&str> = response
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    for key in keys {
        assert!(
            listed.contains(key.as_str()),
            "list_objects_v2 did not contain {key}; listed={listed:?}",
        );
    }
}

async fn list_versions_contains_keys(
    client: &s3_tests::aws_sdk_s3::Client,
    bucket: &str,
    keys: &[String],
) {
    let response = client
        .list_object_versions()
        .bucket(bucket)
        .send_retrying_operation_aborted("list UAT object versions")
        .await
        .unwrap_or_else(|error| panic!("list UAT object versions in {bucket}: {error:?}"));
    let listed: BTreeSet<&str> = response
        .versions()
        .iter()
        .filter_map(|object| object.key())
        .chain(
            response
                .delete_markers()
                .iter()
                .filter_map(|marker| marker.key()),
        )
        .collect();
    for key in keys {
        assert!(
            listed.contains(key.as_str()),
            "list_object_versions did not contain {key}; listed={listed:?}",
        );
    }
}

fn pg_topology_from_env() -> PgTopology {
    let pg_count = std::env::var("ARGMIN_PG_COUNT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    let pg_ids: Vec<u32> = (0..pg_count).collect();
    PgTopology::new(&pg_ids).expect("UAT PG topology must be valid")
}

fn pg_ids_from_env() -> Vec<u32> {
    let pg_count = std::env::var("ARGMIN_PG_COUNT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);
    assert!(pg_count > 0, "ARGMIN_PG_COUNT must be greater than zero");
    (0..pg_count).collect()
}

fn parse_pg_csv(value: Option<String>) -> BTreeSet<u32> {
    let Some(value) = value else {
        return BTreeSet::new();
    };
    if value.trim().is_empty() {
        return BTreeSet::new();
    }
    value
        .split(',')
        .map(|part| {
            part.parse::<u32>()
                .unwrap_or_else(|_| panic!("invalid PG id in excluded metadata PG list: {part}"))
        })
        .collect()
}

fn find_key_with_distinct_data_pg(
    bucket: &str,
    key_prefix: &str,
    target_data_pg: Option<u32>,
    excluded_metadata_pgs: &BTreeSet<u32>,
) -> Option<(String, u32)> {
    let topology = pg_topology_from_env();
    find_key_with_distinct_data_pg_in_topology(
        &topology,
        bucket,
        key_prefix,
        target_data_pg,
        excluded_metadata_pgs,
    )
}

fn find_key_with_distinct_data_pg_in_topology(
    topology: &PgTopology,
    bucket: &str,
    key_prefix: &str,
    target_data_pg: Option<u32>,
    excluded_metadata_pgs: &BTreeSet<u32>,
) -> Option<(String, u32)> {
    let bucket_name = BucketName::try_from(bucket.to_string()).expect("UAT bucket must be valid");
    let bucket_pg = topology.bucket_metadata_pg_for(&bucket_name).get();
    if excluded_metadata_pgs.contains(&bucket_pg) {
        return None;
    }
    let pg_count = topology.pg_count();
    if target_data_pg.is_some_and(|target| target >= pg_count) {
        return None;
    }
    let eligible_metadata_pg_count = pg_count.saturating_sub(
        excluded_metadata_pgs
            .iter()
            .filter(|pg_id| **pg_id < pg_count)
            .count() as u32,
    );
    if eligible_metadata_pg_count == 0 {
        return None;
    }
    let generation_id = GenerationId::new(1).expect("first object generation id is valid");
    let search_limit = distinct_data_pg_key_search_limit(
        pg_count,
        eligible_metadata_pg_count,
        target_data_pg.is_some(),
    );

    for suffix in 0..search_limit {
        let key = format!("{key_prefix}-{suffix:04}");
        let object_key = ObjectKey::try_from(key.clone()).expect("UAT key must be valid");
        let object_pg = topology
            .object_metadata_pg_for(&bucket_name, &object_key)
            .get();
        let data_pg = topology
            .object_generation_segment_data_pg(&bucket_name, &object_key, generation_id, 0)
            .get();
        if data_pg != bucket_pg
            && data_pg != object_pg
            && !excluded_metadata_pgs.contains(&object_pg)
            && target_data_pg.is_none_or(|target| data_pg == target)
        {
            return Some((key, data_pg));
        }
    }
    None
}

fn distinct_data_pg_key_search_limit(
    pg_count: u32,
    eligible_metadata_pg_count: u32,
    targets_exact_data_pg: bool,
) -> u32 {
    const MIN_SEARCH_LIMIT: u64 = 10_000;
    const EXPECTED_MATCH_SAFETY_FACTOR: u64 = 128;

    if !targets_exact_data_pg {
        return MIN_SEARCH_LIMIT as u32;
    }
    let estimated_attempts_per_match = u64::from(pg_count)
        .saturating_mul(u64::from(pg_count))
        .div_ceil(u64::from(eligible_metadata_pg_count));
    MIN_SEARCH_LIMIT
        .max(estimated_attempts_per_match.saturating_mul(EXPECTED_MATCH_SAFETY_FACTOR))
        .min(u64::from(u32::MAX)) as u32
}

fn choose_key_with_distinct_data_pg(
    bucket: &str,
    key_prefix: &str,
    target_data_pg: Option<u32>,
    excluded_metadata_pgs: &BTreeSet<u32>,
) -> (String, u32) {
    if let Some(key) =
        find_key_with_distinct_data_pg(bucket, key_prefix, target_data_pg, excluded_metadata_pgs)
    {
        return key;
    }
    panic!("could not find UAT key with distinct bucket/object metadata PG and data PG");
}

fn key_with_metadata_pg_and_distinct_data_pg(
    topology: &PgTopology,
    bucket: &str,
    key_prefix: &str,
    target_metadata_pg: u32,
) -> Option<(String, u32)> {
    let bucket_name = BucketName::try_from(bucket.to_string()).expect("UAT bucket must be valid");
    let generation_id = GenerationId::new(1).expect("first object generation id is valid");
    for suffix in 0..10_000u32 {
        let key = format!("{key_prefix}-{suffix:04}");
        let object_key = ObjectKey::try_from(key.clone()).expect("UAT key must be valid");
        let object_pg = topology
            .object_metadata_pg_for(&bucket_name, &object_key)
            .get();
        let data_pg = topology
            .object_generation_segment_data_pg(&bucket_name, &object_key, generation_id, 0)
            .get();
        if object_pg == target_metadata_pg && data_pg != target_metadata_pg {
            return Some((key, data_pg));
        }
    }
    None
}

fn choose_bucket_key_with_metadata_pg_and_distinct_data_pg(
    key_prefix: &str,
    target_metadata_pg: u32,
) -> (String, String, u32) {
    let topology = pg_topology_from_env();
    for _ in 0..10_000u32 {
        let bucket = unique_bucket();
        let bucket_name = BucketName::try_from(bucket.clone()).expect("UAT bucket must be valid");
        if topology.bucket_metadata_pg_for(&bucket_name).get() != target_metadata_pg {
            continue;
        }
        if let Some((key, data_pg)) = key_with_metadata_pg_and_distinct_data_pg(
            &topology,
            &bucket,
            key_prefix,
            target_metadata_pg,
        ) {
            return (bucket, key, data_pg);
        }
    }
    panic!("could not find UAT bucket/key with bucket and object metadata PG {target_metadata_pg}");
}

fn choose_bucket_with_metadata_pg(target_metadata_pg: u32) -> String {
    let topology = pg_topology_from_env();
    for _ in 0..10_000u32 {
        let bucket = unique_bucket();
        let bucket_name = BucketName::try_from(bucket.clone()).expect("UAT bucket must be valid");
        if topology.bucket_metadata_pg_for(&bucket_name).get() == target_metadata_pg {
            return bucket;
        }
    }
    panic!("could not find UAT bucket with bucket metadata PG {target_metadata_pg}");
}

fn choose_existing_bucket_key_with_metadata_pg_and_distinct_data_pg(
    bucket: &str,
    key_prefix: &str,
    target_metadata_pg: u32,
) -> (String, u32) {
    let topology = pg_topology_from_env();
    let bucket_name = BucketName::try_from(bucket.to_string()).expect("UAT bucket must be valid");
    let bucket_pg = topology.bucket_metadata_pg_for(&bucket_name).get();
    assert_eq!(
        bucket_pg, target_metadata_pg,
        "bucket metadata PG must match requested metadata PG"
    );
    key_with_metadata_pg_and_distinct_data_pg(&topology, bucket, key_prefix, target_metadata_pg)
        .unwrap_or_else(|| {
            panic!(
                "could not find UAT key with object metadata PG {target_metadata_pg} in bucket {bucket}"
            )
        })
}

fn stress_key_for_pg(
    topology: &PgTopology,
    bucket: &str,
    key_prefix: &str,
    target_metadata_pg: u32,
) -> (String, Option<u32>) {
    if let Some((key, data_pg)) =
        key_with_metadata_pg_and_distinct_data_pg(topology, bucket, key_prefix, target_metadata_pg)
    {
        return (key, Some(data_pg));
    }
    (key_prefix.to_string(), None)
}

async fn cleanup_versioned_stress(
    client: &s3_tests::aws_sdk_s3::Client,
    bucket_count: usize,
    keys_per_bucket: usize,
    versions_per_key: usize,
    key_prefix: &str,
    body: &[u8],
    bucket_log_path: Option<&Path>,
) {
    assert!(bucket_count > 0, "bucket-count must be greater than zero");
    assert!(
        keys_per_bucket > 0,
        "keys-per-bucket must be greater than zero"
    );
    assert!(
        versions_per_key > 0,
        "versions-per-key must be greater than zero"
    );
    let topology = pg_topology_from_env();
    let pg_ids = pg_ids_from_env();

    for bucket_index in 0..bucket_count {
        let target_bucket_pg = pg_ids[bucket_index % pg_ids.len()];
        let bucket = choose_bucket_with_metadata_pg(target_bucket_pg);
        if let Some(bucket_log_path) = bucket_log_path {
            let mut bucket_log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(bucket_log_path)
                .unwrap_or_else(|error| {
                    panic!("open bucket log {}: {error}", bucket_log_path.display())
                });
            writeln!(bucket_log, "{bucket}").unwrap_or_else(|error| {
                panic!("write bucket log {}: {error}", bucket_log_path.display())
            });
        }
        eprintln!(
            "cleanup-versioned-stress: bucket={} bucket_pg={} index={}/{}",
            bucket,
            target_bucket_pg,
            bucket_index + 1,
            bucket_count
        );
        create_bucket(client, &bucket).await;
        enable_bucket_versioning(client, &bucket).await;

        for key_index in 0..keys_per_bucket {
            let target_object_pg = pg_ids[(bucket_index + key_index + 1) % pg_ids.len()];
            let key_prefix = format!("{key_prefix}-b{bucket_index:03}-k{key_index:03}");
            let (key, data_pg) =
                stress_key_for_pg(&topology, &bucket, &key_prefix, target_object_pg);
            eprintln!(
                "cleanup-versioned-stress: bucket={} key={} target_object_pg={} data_pg={:?}",
                bucket, key, target_object_pg, data_pg
            );
            for version_index in 0..versions_per_key {
                let mut version_body = body.to_vec();
                version_body.extend_from_slice(
                    format!("\nbucket={bucket_index} key={key_index} version={version_index}\n")
                        .as_bytes(),
                );
                put_object(client, &bucket, &key, version_body).await;
            }
        }

        cleanup_versioned_bucket(client, &bucket).await;
    }
}

fn main() {
    let mut args = std::env::args_os().skip(1);
    let Some(command) = args.next().and_then(|arg| arg.into_string().ok()) else {
        usage();
    };

    match command.as_str() {
        "create-put" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = unique_bucket();
                create_bucket(&client, &bucket).await;
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&bucket_file, format!("{bucket}\n")).unwrap_or_else(|error| {
                    panic!("write bucket file {:?}: {error}", bucket_file);
                });
            });
        }
        "create-put-distinct-data-pg" | "create-versioned-put-distinct-data-pg" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key_file) = args.next() else {
                usage();
            };
            let Some(data_pg_file) = args.next() else {
                usage();
            };
            let Some(key_prefix) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            let target_data_pg = args.next().map(|arg| {
                arg.into_string()
                    .ok()
                    .and_then(|value| value.parse::<u32>().ok())
                    .unwrap_or_else(|| usage())
            });
            let excluded_metadata_pgs =
                parse_pg_csv(args.next().and_then(|arg| arg.into_string().ok()));
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let versioned = command == "create-versioned-put-distinct-data-pg";
                let (bucket, key, data_pg) = if target_data_pg.is_some() {
                    let (bucket, key, data_pg) = (0..100)
                        .find_map(|_| {
                            let bucket = unique_bucket();
                            find_key_with_distinct_data_pg(
                                &bucket,
                                &key_prefix,
                                target_data_pg,
                                &excluded_metadata_pgs,
                            )
                            .map(|(key, data_pg)| (bucket, key, data_pg))
                        })
                        .unwrap_or_else(|| {
                            panic!(
                                "could not find UAT bucket/key for target data PG {target_data_pg:?}"
                            )
                        });
                    create_bucket(&client, &bucket).await;
                    if versioned {
                        enable_bucket_versioning(&client, &bucket).await;
                    }
                    (bucket, key, data_pg)
                } else {
                    let bucket = unique_bucket();
                    create_bucket(&client, &bucket).await;
                    if versioned {
                        enable_bucket_versioning(&client, &bucket).await;
                    }
                    let (key, data_pg) = choose_key_with_distinct_data_pg(
                        &bucket,
                        &key_prefix,
                        None,
                        &excluded_metadata_pgs,
                    );
                    (bucket, key, data_pg)
                };
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&bucket_file, format!("{bucket}\n")).unwrap_or_else(|error| {
                    panic!("write bucket file {:?}: {error}", bucket_file);
                });
                std::fs::write(&key_file, format!("{key}\n")).unwrap_or_else(|error| {
                    panic!("write key file {:?}: {error}", key_file);
                });
                std::fs::write(&data_pg_file, format!("{data_pg}\n")).unwrap_or_else(|error| {
                    panic!("write data PG file {:?}: {error}", data_pg_file);
                });
            });
        }
        "create-put-metadata-pg" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key_file) = args.next() else {
                usage();
            };
            let Some(metadata_pg_file) = args.next() else {
                usage();
            };
            let Some(key_prefix) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            let Some(target_metadata_pg) = args
                .next()
                .and_then(|arg| arg.into_string().ok())
                .and_then(|value| value.parse::<u32>().ok())
            else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let (bucket, key, data_pg) =
                    choose_bucket_key_with_metadata_pg_and_distinct_data_pg(
                        &key_prefix,
                        target_metadata_pg,
                    );
                create_bucket(&client, &bucket).await;
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&bucket_file, format!("{bucket}\n")).unwrap_or_else(|error| {
                    panic!("write bucket file {:?}: {error}", bucket_file);
                });
                std::fs::write(&key_file, format!("{key}\n")).unwrap_or_else(|error| {
                    panic!("write key file {:?}: {error}", key_file);
                });
                std::fs::write(&metadata_pg_file, format!("{target_metadata_pg}\n"))
                    .unwrap_or_else(|error| {
                        panic!("write metadata PG file {:?}: {error}", metadata_pg_file);
                    });
                eprintln!(
                    "selected bucket/object metadata PG {target_metadata_pg} with payload data PG {data_pg}"
                );
            });
        }
        "put-for-data-pg" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key_file) = args.next() else {
                usage();
            };
            let Some(key_prefix) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            let Some(data_pg) = args
                .next()
                .and_then(|arg| arg.into_string().ok())
                .and_then(|value| value.parse::<u32>().ok())
            else {
                usage();
            };
            let excluded_metadata_pgs =
                parse_pg_csv(args.next().and_then(|arg| arg.into_string().ok()));
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let (key, actual_data_pg) = choose_key_with_distinct_data_pg(
                    &bucket,
                    &key_prefix,
                    Some(data_pg),
                    &excluded_metadata_pgs,
                );
                assert_eq!(actual_data_pg, data_pg);
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&key_file, format!("{key}\n")).unwrap_or_else(|error| {
                    panic!("write key file {:?}: {error}", key_file);
                });
            });
        }
        "put-for-metadata-pg" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key_file) = args.next() else {
                usage();
            };
            let Some(key_prefix) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            let Some(target_metadata_pg) = args
                .next()
                .and_then(|arg| arg.into_string().ok())
                .and_then(|value| value.parse::<u32>().ok())
            else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let (key, data_pg) =
                    choose_existing_bucket_key_with_metadata_pg_and_distinct_data_pg(
                        &bucket,
                        &key_prefix,
                        target_metadata_pg,
                    );
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
                std::fs::write(&key_file, format!("{key}\n")).unwrap_or_else(|error| {
                    panic!("write key file {:?}: {error}", key_file);
                });
                eprintln!(
                    "selected object metadata PG {target_metadata_pg} with payload data PG {data_pg}"
                );
            });
        }
        "put" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let body = read_body(Path::new(&body_file));
                put_object(&client, &bucket, &key, body).await;
            });
        }
        "put-expect-failure" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let body = read_body(Path::new(&body_file));
                match client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(ByteStream::from(body))
                    .send()
                    .await
                {
                    Ok(_) => panic!("put UAT object {bucket}/{key} unexpectedly succeeded"),
                    Err(error) => expect_s3_service_error(
                        &format!("put UAT object {bucket}/{key}"),
                        &error,
                        true,
                    ),
                }
            });
        }
        "get" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let expected = read_body(Path::new(&body_file));
                let actual = get_object_body(&client, &bucket, &key).await;
                assert_eq!(actual, expected, "object body mismatch for {key}");
            });
        }
        "get-expect-failure" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                match client.get_object().bucket(&bucket).key(&key).send().await {
                    Ok(_) => panic!("get UAT object {bucket}/{key} unexpectedly succeeded"),
                    Err(error) => expect_s3_service_error(
                        &format!("get UAT object {bucket}/{key}"),
                        &error,
                        true,
                    ),
                }
            });
        }
        "head" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                let expected = read_body(Path::new(&body_file));
                head_object_matches_len(&client, &bucket, &key, expected.len()).await;
            });
        }
        "head-expect-failure" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let Some(key) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                match client.head_object().bucket(&bucket).key(&key).send().await {
                    Ok(_) => panic!("head UAT object {bucket}/{key} unexpectedly succeeded"),
                    Err(error) => expect_s3_service_error(
                        &format!("head UAT object {bucket}/{key}"),
                        &error,
                        false,
                    ),
                }
            });
        }
        "list-contains" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let keys: Vec<String> = args
                .map(|arg| arg.into_string().unwrap_or_else(|_| usage()))
                .collect();
            if keys.is_empty() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                list_contains_keys(&client, &bucket, &keys).await;
            });
        }
        "list-expect-failure" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                match client.list_objects_v2().bucket(&bucket).send().await {
                    Ok(_) => panic!("list UAT objects in {bucket} unexpectedly succeeded"),
                    Err(error) => expect_s3_service_error(
                        &format!("list UAT objects in {bucket}"),
                        &error,
                        true,
                    ),
                }
            });
        }
        "list-versions-contains" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let keys: Vec<String> = args
                .map(|arg| arg.into_string().unwrap_or_else(|_| usage()))
                .collect();
            if keys.is_empty() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                list_versions_contains_keys(&client, &bucket, &keys).await;
            });
        }
        "cleanup" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            let keys: Vec<String> = args
                .map(|arg| arg.into_string().unwrap_or_else(|_| usage()))
                .collect();
            if keys.is_empty() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                cleanup_bucket(&client, &bucket, &keys).await;
            });
        }
        "cleanup-versioned" => {
            let Some(bucket_file) = args.next() else {
                usage();
            };
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let bucket = read_bucket(Path::new(&bucket_file));
                cleanup_versioned_bucket(&client, &bucket).await;
            });
        }
        "cleanup-versioned-stress" => {
            let Some(bucket_count) = args
                .next()
                .and_then(|arg| arg.into_string().ok())
                .and_then(|value| value.parse::<usize>().ok())
            else {
                usage();
            };
            let Some(keys_per_bucket) = args
                .next()
                .and_then(|arg| arg.into_string().ok())
                .and_then(|value| value.parse::<usize>().ok())
            else {
                usage();
            };
            let Some(versions_per_key) = args
                .next()
                .and_then(|arg| arg.into_string().ok())
                .and_then(|value| value.parse::<usize>().ok())
            else {
                usage();
            };
            let Some(key_prefix) = args.next().and_then(|arg| arg.into_string().ok()) else {
                usage();
            };
            let Some(body_file) = args.next() else {
                usage();
            };
            let bucket_log_file = args.next();
            if args.next().is_some() {
                usage();
            }
            run(async {
                let client = client_from_env();
                let body = read_body(Path::new(&body_file));
                let bucket_log_path = bucket_log_file.as_deref().map(Path::new);
                cleanup_versioned_stress(
                    &client,
                    bucket_count,
                    keys_per_bucket,
                    versions_per_key,
                    &key_prefix,
                    &body,
                    bucket_log_path,
                )
                .await;
            });
        }
        _ => usage(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_data_pg_search_scales_for_large_excluded_metadata_set() {
        let topology = PgTopology::new(&(0..216).collect::<Vec<_>>()).unwrap();
        let excluded_metadata_pgs = (0..200).collect::<BTreeSet<_>>();
        let result = find_key_with_distinct_data_pg_in_topology(
            &topology,
            "argmin-s3-976110-18cf77b41c5bca93-14",
            "uat-route-change-new-object-61",
            Some(60),
            &excluded_metadata_pgs,
        )
        .expect("scaled search should find the retained soak target");

        assert_eq!(result.1, 60);
        assert!(result.0.starts_with("uat-route-change-new-object-61-"));
        let suffix = result
            .0
            .rsplit_once('-')
            .and_then(|(_, suffix)| suffix.parse::<u32>().ok())
            .expect("generated key should end in a numeric suffix");
        assert!(
            suffix >= 10_000,
            "regression must exceed the old fixed bound"
        );
        assert!(distinct_data_pg_key_search_limit(216, 16, true) > 10_000);
    }
}
