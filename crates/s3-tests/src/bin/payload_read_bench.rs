// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use observability::MetricsSnapshot;
use s3_tests::server::{
    PayloadReadBenchmarkEcShape, PayloadReadBenchmarkProfile, TEST_ACCESS_KEY, TEST_REGION,
    TEST_SECRET_KEY,
};

const MIB: usize = 1024 * 1024;

#[derive(Debug, Clone)]
struct Config {
    payload_profile: PayloadReadBenchmarkProfile,
    ec_shape: PayloadReadBenchmarkEcShape,
    object_size: usize,
    range_size: usize,
    warmup_iters: usize,
    full_iters: usize,
    range_iters: usize,
    samples: usize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut payload_profile = None;
        let mut ec_shape = None;
        let mut object_size = 16 * MIB;
        let mut range_size = 4 * 1024;
        let mut warmup_iters = 4;
        let mut full_iters = 16;
        let mut range_iters = 64;
        let mut samples = 5;
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--payload-profile" => {
                    let value = args.next().ok_or("missing value for --payload-profile")?;
                    payload_profile = Some(match value.as_str() {
                        "segment-aes-256-gcm-v1" => PayloadReadBenchmarkProfile::SegmentAes256GcmV1,
                        "cobblestone-256-segment-v1" => {
                            PayloadReadBenchmarkProfile::Cobblestone256SegmentV1
                        }
                        _ => {
                            return Err(format!(
                                "invalid --payload-profile value: {value}\n{}",
                                Self::usage()
                            ));
                        }
                    });
                }
                "--ec-shape" => {
                    let value = args.next().ok_or("missing value for --ec-shape")?;
                    ec_shape = Some(match value.as_str() {
                        "1+0" => PayloadReadBenchmarkEcShape::OnePlusZero,
                        "4+2" => PayloadReadBenchmarkEcShape::FourPlusTwo,
                        _ => {
                            return Err(format!(
                                "invalid --ec-shape value: {value}\n{}",
                                Self::usage()
                            ));
                        }
                    });
                }
                "--object-mib" => {
                    let mib = parse_usize(&mut args, "--object-mib")?;
                    object_size = mib.checked_mul(MIB).ok_or("--object-mib is too large")?;
                }
                "--range-bytes" => {
                    range_size = parse_usize(&mut args, "--range-bytes")?;
                }
                "--warmup-iters" => {
                    warmup_iters = parse_usize(&mut args, "--warmup-iters")?;
                }
                "--full-iters" => {
                    full_iters = parse_usize(&mut args, "--full-iters")?;
                }
                "--range-iters" => {
                    range_iters = parse_usize(&mut args, "--range-iters")?;
                }
                "--samples" => {
                    samples = parse_usize(&mut args, "--samples")?;
                }
                "--help" | "-h" => return Err(Self::usage()),
                _ => return Err(format!("unknown argument: {arg}\n{}", Self::usage())),
            }
        }

        let payload_profile = payload_profile
            .ok_or_else(|| format!("--payload-profile is required\n{}", Self::usage()))?;
        let ec_shape =
            ec_shape.ok_or_else(|| format!("--ec-shape is required\n{}", Self::usage()))?;
        if object_size <= 8 * MIB {
            return Err("--object-mib must be greater than 8 to exercise multiple segments".into());
        }
        let range_start = object_size / 2 + 1024;
        if range_size == 0 || range_size > object_size - range_start {
            return Err(
                "--range-bytes must fit between the benchmark range start and object end".into(),
            );
        }
        if full_iters == 0 || range_iters == 0 || samples == 0 {
            return Err("iteration and sample counts must be greater than zero".into());
        }
        Ok(Self {
            payload_profile,
            ec_shape,
            object_size,
            range_size,
            warmup_iters,
            full_iters,
            range_iters,
            samples,
        })
    }

    fn usage() -> String {
        "usage: payload_read_bench --payload-profile \
         <segment-aes-256-gcm-v1|cobblestone-256-segment-v1> \
         --ec-shape <1+0|4+2> [--object-mib N] [--range-bytes N] \
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
    request_timings: Vec<RequestTiming>,
}

#[derive(Debug, Clone, Copy)]
struct RequestTiming {
    headers: Duration,
    body_ttfb: Duration,
    complete: Duration,
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

fn get_request(
    client: &Client,
    bucket: &str,
    key: &str,
    kind: ReadKind,
) -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder {
    let mut request = client.get_object().bucket(bucket).key(key);
    if let ReadKind::Range { start, end } = kind {
        request = request.range(format!("bytes={start}-{end}"));
    }
    request
}

async fn read_exact(client: &Client, bucket: &str, key: &str, kind: ReadKind) -> Bytes {
    get_request(client, bucket, key, kind)
        .send()
        .await
        .expect("GET benchmark object")
        .body
        .collect()
        .await
        .expect("read benchmark response body")
        .into_bytes()
}

async fn read_and_time(
    client: &Client,
    bucket: &str,
    key: &str,
    kind: ReadKind,
) -> (usize, RequestTiming) {
    let started = Instant::now();
    let response = get_request(client, bucket, key, kind)
        .send()
        .await
        .expect("GET benchmark object");
    let headers = started.elapsed();
    let mut body = response.body;
    let mut body_ttfb = None;
    let mut body_len = 0usize;

    while let Some(chunk) = body.next().await {
        let chunk = chunk.expect("read benchmark response body chunk");
        if chunk.is_empty() {
            continue;
        }
        body_ttfb.get_or_insert_with(|| started.elapsed());
        body_len = body_len
            .checked_add(chunk.len())
            .expect("benchmark response length fits usize");
    }

    let complete = started.elapsed();
    let body_ttfb = body_ttfb.expect("non-empty benchmark response body");
    (
        body_len,
        RequestTiming {
            headers,
            body_ttfb,
            complete,
        },
    )
}

fn nearest_rank_percentile(
    timings: &[RequestTiming],
    percentile: usize,
    select: impl Fn(&RequestTiming) -> Duration,
) -> Duration {
    assert!(!timings.is_empty());
    assert!((1..=100).contains(&percentile));
    let mut values = timings.iter().map(select).collect::<Vec<_>>();
    values.sort_unstable();
    let rank = values
        .len()
        .checked_mul(percentile)
        .expect("percentile rank fits usize")
        .div_ceil(100);
    values[rank - 1]
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
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
        read_exact(client, bucket, key, kind).await.as_ref(),
        expected
    );

    for _ in 0..config.warmup_iters {
        let (body_len, _) = read_and_time(client, bucket, key, kind).await;
        assert_eq!(body_len, expected.len());
    }

    let iterations = match kind {
        ReadKind::Full => config.full_iters,
        ReadKind::Range { .. } => config.range_iters,
    };
    let mut samples = Vec::with_capacity(config.samples);
    for _ in 0..config.samples {
        let mut request_timings = Vec::with_capacity(iterations);
        let before = observability::metrics_snapshot();
        let started = Instant::now();
        for _ in 0..iterations {
            let (body_len, timing) = read_and_time(client, bucket, key, kind).await;
            assert_eq!(body_len, expected.len());
            request_timings.push(timing);
        }
        let elapsed = started.elapsed();
        let after = observability::metrics_snapshot();
        samples.push(Sample {
            elapsed,
            work: ReadWork::between(&before, &after),
            request_timings,
        });
    }

    for (index, sample) in samples.iter().enumerate() {
        let requests_per_second = iterations as f64 / sample.elapsed.as_secs_f64();
        let logical_mib_per_second =
            (expected.len() * iterations) as f64 / sample.elapsed.as_secs_f64() / MIB as f64;
        let headers_p50 =
            nearest_rank_percentile(&sample.request_timings, 50, |timing| timing.headers);
        let body_ttfb_p50 =
            nearest_rank_percentile(&sample.request_timings, 50, |timing| timing.body_ttfb);
        let complete_p50 =
            nearest_rank_percentile(&sample.request_timings, 50, |timing| timing.complete);
        println!(
            "sample case={case_name} operation={} index={} iterations={iterations} \
             elapsed_ms={:.3} requests_s={requests_per_second:.3} logical_mib_s={logical_mib_per_second:.3} \
             headers_p50_ms={:.3} body_ttfb_p50_ms={:.3} complete_p50_ms={:.3} \
             logical_bytes={} stored_bytes={} shard_bytes={} reconstructed_bytes={} \
             decrypted_bytes={} aead_records={} crc_bytes={} buffer_checkouts={} \
             buffer_allocations={} buffer_allocated_bytes={} repair_fallbacks={}",
            kind.name(),
            index + 1,
            duration_ms(sample.elapsed),
            duration_ms(headers_p50),
            duration_ms(body_ttfb_p50),
            duration_ms(complete_p50),
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
    let request_timings = samples
        .iter()
        .flat_map(|sample| sample.request_timings.iter().copied())
        .collect::<Vec<_>>();
    let headers_p50 = nearest_rank_percentile(&request_timings, 50, |timing| timing.headers);
    let headers_p95 = nearest_rank_percentile(&request_timings, 95, |timing| timing.headers);
    let headers_p99 = nearest_rank_percentile(&request_timings, 99, |timing| timing.headers);
    let body_ttfb_p50 = nearest_rank_percentile(&request_timings, 50, |timing| timing.body_ttfb);
    let body_ttfb_p95 = nearest_rank_percentile(&request_timings, 95, |timing| timing.body_ttfb);
    let body_ttfb_p99 = nearest_rank_percentile(&request_timings, 99, |timing| timing.body_ttfb);
    let complete_p50 = nearest_rank_percentile(&request_timings, 50, |timing| timing.complete);
    let complete_p95 = nearest_rank_percentile(&request_timings, 95, |timing| timing.complete);
    let complete_p99 = nearest_rank_percentile(&request_timings, 99, |timing| timing.complete);
    println!(
        "summary case={case_name} operation={} response_bytes={} median_requests_s={:.3} \
         worst_requests_s={:.3} best_requests_s={:.3} \
         headers_p50_ms={:.3} headers_p95_ms={:.3} headers_p99_ms={:.3} \
         body_ttfb_p50_ms={:.3} body_ttfb_p95_ms={:.3} body_ttfb_p99_ms={:.3} \
         complete_p50_ms={:.3} complete_p95_ms={:.3} complete_p99_ms={:.3}",
        kind.name(),
        kind.expected_len(config.object_size),
        median_requests_per_second,
        rates[0],
        rates[rates.len() - 1],
        duration_ms(headers_p50),
        duration_ms(headers_p95),
        duration_ms(headers_p99),
        duration_ms(body_ttfb_p50),
        duration_ms(body_ttfb_p95),
        duration_ms(body_ttfb_p99),
        duration_ms(complete_p50),
        duration_ms(complete_p95),
        duration_ms(complete_p99),
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
    let server = s3_tests::TestServer::start_http_for_payload_read_benchmark(
        config.payload_profile,
        config.ec_shape,
    )
    .await;
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

    let persisted_profile = server
        .observe_payload_read_benchmark_profile(&encrypted_bucket, key)
        .map_err(|error| format!("observe durable benchmark payload profile: {error}"))?;
    if persisted_profile != config.payload_profile {
        return Err(format!(
            "benchmark requested payload profile {} but durable object recorded {}",
            config.payload_profile.as_str(),
            persisted_profile.as_str()
        ));
    }

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
    println!("benchmark=payload_read_v3");
    println!("body_ttfb_definition=request_start_to_first_nonempty_sdk_body_bytes");
    println!("latency_percentile_method=nearest_rank");
    println!("transport=http");
    println!("ec_shape={}", config.ec_shape.as_str());
    println!(
        "requested_payload_profile={}",
        config.payload_profile.as_str()
    );
    println!("persisted_payload_profile={}", persisted_profile.as_str());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn timing(milliseconds: u64) -> RequestTiming {
        let duration = Duration::from_millis(milliseconds);
        RequestTiming {
            headers: duration,
            body_ttfb: duration,
            complete: duration,
        }
    }

    #[test]
    fn latency_percentiles_use_nearest_rank() {
        let timings = [timing(50), timing(10), timing(40), timing(20), timing(30)];
        assert_eq!(
            nearest_rank_percentile(&timings, 50, |value| value.body_ttfb),
            Duration::from_millis(30)
        );
        assert_eq!(
            nearest_rank_percentile(&timings, 95, |value| value.body_ttfb),
            Duration::from_millis(50)
        );
        assert_eq!(
            nearest_rank_percentile(&timings[..4], 50, |value| value.body_ttfb),
            Duration::from_millis(20)
        );
    }
}
