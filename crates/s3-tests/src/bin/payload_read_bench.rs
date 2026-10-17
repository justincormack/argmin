// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use observability::MetricsSnapshot;
use s3_tests::server::{TEST_ACCESS_KEY, TEST_REGION, TEST_SECRET_KEY};

const MIB: usize = 1024 * 1024;

#[derive(Debug, Clone)]
struct Config {
    object_size: usize,
    range_size: usize,
    warmup_iters: usize,
    full_iters: usize,
    range_iters: usize,
    samples: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            object_size: 16 * MIB,
            range_size: 4 * 1024,
            warmup_iters: 4,
            full_iters: 16,
            range_iters: 64,
            samples: 5,
        }
    }
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut config = Self::default();
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--object-mib" => {
                    let mib = parse_usize(&mut args, "--object-mib")?;
                    config.object_size = mib.checked_mul(MIB).ok_or("--object-mib is too large")?;
                }
                "--range-bytes" => {
                    config.range_size = parse_usize(&mut args, "--range-bytes")?;
                }
                "--warmup-iters" => {
                    config.warmup_iters = parse_usize(&mut args, "--warmup-iters")?;
                }
                "--full-iters" => {
                    config.full_iters = parse_usize(&mut args, "--full-iters")?;
                }
                "--range-iters" => {
                    config.range_iters = parse_usize(&mut args, "--range-iters")?;
                }
                "--samples" => {
                    config.samples = parse_usize(&mut args, "--samples")?;
                }
                "--help" | "-h" => return Err(Self::usage()),
                _ => return Err(format!("unknown argument: {arg}\n{}", Self::usage())),
            }
        }

        if config.object_size <= 8 * MIB {
            return Err("--object-mib must be greater than 8 to exercise multiple segments".into());
        }
        let range_start = config.object_size / 2 + 1024;
        if config.range_size == 0 || config.range_size > config.object_size - range_start {
            return Err(
                "--range-bytes must fit between the benchmark range start and object end".into(),
            );
        }
        if config.full_iters == 0 || config.range_iters == 0 || config.samples == 0 {
            return Err("iteration and sample counts must be greater than zero".into());
        }
        Ok(config)
    }

    fn usage() -> String {
        "usage: payload_read_bench [--object-mib N] [--range-bytes N] \
         [--warmup-iters N] [--full-iters N] [--range-iters N] [--samples N]"
            .to_string()
    }
}

fn parse_usize(args: &mut impl Iterator<Item = String>, option: &str) -> Result<usize, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {option}"))?
        .parse()
        .map_err(|_| format!("invalid integer for {option}"))
}

#[derive(Debug, Clone, Copy, Default)]
struct ReadWork {
    logical_bytes: u64,
    stored_bytes: u64,
    shard_bytes: u64,
    reconstructed_bytes: u64,
    decrypted_bytes: u64,
    aead_records: u64,
    crc_bytes: u64,
    buffer_checkouts: u64,
    buffer_allocations: u64,
    buffer_allocated_bytes: u64,
    repair_fallbacks: u64,
}

impl ReadWork {
    fn between(before: &MetricsSnapshot, after: &MetricsSnapshot) -> Self {
        Self {
            logical_bytes: after
                .payload_read_logical_bytes_total
                .saturating_sub(before.payload_read_logical_bytes_total),
            stored_bytes: after
                .payload_read_stored_bytes_total
                .saturating_sub(before.payload_read_stored_bytes_total),
            shard_bytes: after
                .payload_read_shard_bytes_total
                .saturating_sub(before.payload_read_shard_bytes_total),
            reconstructed_bytes: after
                .payload_read_reconstructed_bytes_total
                .saturating_sub(before.payload_read_reconstructed_bytes_total),
            decrypted_bytes: after
                .payload_read_decrypted_bytes_total
                .saturating_sub(before.payload_read_decrypted_bytes_total),
            aead_records: after
                .payload_read_aead_records_total
                .saturating_sub(before.payload_read_aead_records_total),
            crc_bytes: after
                .payload_read_crc_bytes_total
                .saturating_sub(before.payload_read_crc_bytes_total),
            buffer_checkouts: after
                .payload_read_buffer_checkouts_total
                .saturating_sub(before.payload_read_buffer_checkouts_total),
            buffer_allocations: after
                .payload_read_buffer_allocations_total
                .saturating_sub(before.payload_read_buffer_allocations_total),
            buffer_allocated_bytes: after
                .payload_read_buffer_allocated_bytes_total
                .saturating_sub(before.payload_read_buffer_allocated_bytes_total),
            repair_fallbacks: after
                .payload_read_repair_fallback_total
                .saturating_sub(before.payload_read_repair_fallback_total),
        }
    }
}

#[derive(Debug)]
struct Sample {
    elapsed: Duration,
    work: ReadWork,
}

#[derive(Debug, Clone, Copy)]
enum ReadKind {
    Full,
    Range { start: usize, end: usize },
}

impl ReadKind {
    fn name(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Range { .. } => "range",
        }
    }

    fn expected_len(self, object_size: usize) -> usize {
        match self {
            Self::Full => object_size,
            Self::Range { start, end } => end - start + 1,
        }
    }
}

async fn read_once(client: &Client, bucket: &str, key: &str, kind: ReadKind) -> Bytes {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let ReadKind::Range { start, end } = kind {
        request = request.range(format!("bytes={start}-{end}"));
    }
    request
        .send()
        .await
        .expect("GET benchmark object")
        .body
        .collect()
        .await
        .expect("read benchmark response body")
        .into_bytes()
}

async fn measure_case(
    config: &Config,
    client: &Client,
    case_name: &str,
    bucket: &str,
    key: &str,
    payload: &[u8],
    kind: ReadKind,
) {
    let expected = match kind {
        ReadKind::Full => payload,
        ReadKind::Range { start, end } => &payload[start..=end],
    };
    assert_eq!(
        read_once(client, bucket, key, kind).await.as_ref(),
        expected
    );

    for _ in 0..config.warmup_iters {
        let body = read_once(client, bucket, key, kind).await;
        assert_eq!(body.len(), expected.len());
    }

    let iterations = match kind {
        ReadKind::Full => config.full_iters,
        ReadKind::Range { .. } => config.range_iters,
    };
    let mut samples = Vec::with_capacity(config.samples);
    for _ in 0..config.samples {
        let before = observability::metrics_snapshot();
        let started = Instant::now();
        for _ in 0..iterations {
            let body = read_once(client, bucket, key, kind).await;
            assert_eq!(body.len(), expected.len());
        }
        let elapsed = started.elapsed();
        let after = observability::metrics_snapshot();
        samples.push(Sample {
            elapsed,
            work: ReadWork::between(&before, &after),
        });
    }

    for (index, sample) in samples.iter().enumerate() {
        let requests_per_second = iterations as f64 / sample.elapsed.as_secs_f64();
        let logical_mib_per_second =
            (expected.len() * iterations) as f64 / sample.elapsed.as_secs_f64() / MIB as f64;
        println!(
            "sample case={case_name} operation={} index={} iterations={iterations} \
             elapsed_ms={:.3} requests_s={requests_per_second:.3} logical_mib_s={logical_mib_per_second:.3} \
             logical_bytes={} stored_bytes={} shard_bytes={} reconstructed_bytes={} \
             decrypted_bytes={} aead_records={} crc_bytes={} buffer_checkouts={} \
             buffer_allocations={} buffer_allocated_bytes={} repair_fallbacks={}",
            kind.name(),
            index + 1,
            sample.elapsed.as_secs_f64() * 1000.0,
            sample.work.logical_bytes,
            sample.work.stored_bytes,
            sample.work.shard_bytes,
            sample.work.reconstructed_bytes,
            sample.work.decrypted_bytes,
            sample.work.aead_records,
            sample.work.crc_bytes,
            sample.work.buffer_checkouts,
            sample.work.buffer_allocations,
            sample.work.buffer_allocated_bytes,
            sample.work.repair_fallbacks,
        );
    }

    let mut rates = samples
        .iter()
        .map(|sample| iterations as f64 / sample.elapsed.as_secs_f64())
        .collect::<Vec<_>>();
    rates.sort_by(f64::total_cmp);
    let middle = rates.len() / 2;
    let median_requests_per_second = if rates.len().is_multiple_of(2) {
        (rates[middle - 1] + rates[middle]) / 2.0
    } else {
        rates[middle]
    };
    println!(
        "summary case={case_name} operation={} response_bytes={} median_requests_s={:.3} \
         worst_requests_s={:.3} best_requests_s={:.3}",
        kind.name(),
        kind.expected_len(config.object_size),
        median_requests_per_second,
        rates[0],
        rates[rates.len() - 1],
    );
}

async fn put_benchmark_object(client: &Client, bucket: &str, key: &str, payload: &[u8]) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.to_vec()))
        .send()
        .await
        .expect("PUT benchmark object");
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let config = Config::parse()?;
    let ec_config = ec::EcConfig::default();
    if (ec_config.data_shards(), ec_config.parity_shards()) != (4, 2) {
        return Err("payload_read_bench requires the pinned 4+2 test-server EC shape".into());
    }
    let server = s3_tests::TestServer::start_http().await;
    let client = s3_tests::build_client_with_ca(
        server.endpoint(),
        TEST_ACCESS_KEY,
        TEST_SECRET_KEY,
        TEST_REGION,
        None,
    );

    let encrypted_bucket = s3_tests::unique_bucket();
    client
        .create_bucket()
        .bucket(&encrypted_bucket)
        .send()
        .await
        .map_err(|error| format!("create encrypted bucket: {error}"))?;

    let payload = (0..config.object_size)
        .map(|index| (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as u8)
        .collect::<Vec<_>>();
    let key = "payload-read-benchmark";
    put_benchmark_object(&client, &encrypted_bucket, key, &payload).await;

    let encrypted_head = client
        .head_object()
        .bucket(&encrypted_bucket)
        .key(key)
        .send()
        .await
        .map_err(|error| format!("HEAD encrypted object: {error}"))?;
    if encrypted_head
        .server_side_encryption()
        .is_none_or(|algorithm| algorithm.as_str() != "AES256")
    {
        return Err("encrypted benchmark object is not SSE-S3 AES256".into());
    }

    let range_start = config.object_size / 2 + 1024;
    let range_end = range_start + config.range_size - 1;
    let range = ReadKind::Range {
        start: range_start,
        end: range_end,
    };
    println!("benchmark=payload_read_v1");
    println!("transport=http");
    println!(
        "ec_shape={}+{}",
        ec_config.data_shards(),
        ec_config.parity_shards()
    );
    println!("crc64_backend={}", checksum::crc64::backend_name());
    println!("sha256_backend={}", checksum::sha256::backend_name());
    println!("ec_backend={}", ec::backend_name());
    println!("verified_s3_encryption=AES256");
    println!("plaintext_writer_reachable=false");
    println!("object_size_bytes={}", config.object_size);
    println!("range_start={range_start}");
    println!("range_size_bytes={}", config.range_size);
    println!("warmup_iters={}", config.warmup_iters);
    println!("full_iters={}", config.full_iters);
    println!("range_iters={}", config.range_iters);
    println!("samples={}", config.samples);

    measure_case(
        &config,
        &client,
        "sse_s3_aes256",
        &encrypted_bucket,
        key,
        &payload,
        ReadKind::Full,
    )
    .await;
    measure_case(
        &config,
        &client,
        "sse_s3_aes256",
        &encrypted_bucket,
        key,
        &payload,
        range,
    )
    .await;

    s3_tests::delete_all_and_bucket(&client, &encrypted_bucket, &[key.to_string()]).await;
    Ok(())
}
