use std::hint::black_box;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
enum Algorithm {
    Crc32,
    Crc32c,
}

#[derive(Debug, Clone)]
struct Config {
    algorithm: Algorithm,
    label: String,
    block_size: usize,
    warmup_iters: usize,
    sample_iters: usize,
    samples: usize,
    sweep_small: bool,
    target_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            algorithm: Algorithm::Crc32,
            label: "crc".to_string(),
            block_size: 8 * 1024 * 1024,
            warmup_iters: 32,
            sample_iters: 256,
            samples: 5,
            sweep_small: false,
            target_ms: 100,
        }
    }
}

const SMALL_SWEEP_SIZES: &[usize] = &[
    0, 1, 7, 8, 15, 16, 31, 32, 47, 48, 63, 64, 65, 71, 72, 80, 96, 112, 128, 192, 256, 512, 1024,
];

const MAX_SWEEP_ITERS: usize = 1 << 28;

fn parse_args() -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--algorithm" => {
                cfg.algorithm = match args.next().ok_or("missing value for --algorithm")?.as_str() {
                    "crc32" => Algorithm::Crc32,
                    "crc32c" => Algorithm::Crc32c,
                    other => return Err(format!("unknown algorithm: {other}")),
                };
            }
            "--label" => cfg.label = args.next().ok_or("missing value for --label")?,
            "--size-mib" => {
                let mib: usize = args
                    .next()
                    .ok_or("missing value for --size-mib")?
                    .parse()
                    .map_err(|_| "invalid integer for --size-mib")?;
                cfg.block_size = mib * 1024 * 1024;
            }
            "--warmup-iters" => {
                cfg.warmup_iters = args
                    .next()
                    .ok_or("missing value for --warmup-iters")?
                    .parse()
                    .map_err(|_| "invalid integer for --warmup-iters")?;
            }
            "--sample-iters" => {
                cfg.sample_iters = args
                    .next()
                    .ok_or("missing value for --sample-iters")?
                    .parse()
                    .map_err(|_| "invalid integer for --sample-iters")?;
            }
            "--samples" => {
                cfg.samples = args
                    .next()
                    .ok_or("missing value for --samples")?
                    .parse()
                    .map_err(|_| "invalid integer for --samples")?;
            }
            "--sweep-small" => cfg.sweep_small = true,
            "--target-ms" => {
                cfg.target_ms = args
                    .next()
                    .ok_or("missing value for --target-ms")?
                    .parse()
                    .map_err(|_| "invalid integer for --target-ms")?;
            }
            "--help" | "-h" => {
                return Err(
                    "usage: crc_bench --algorithm crc32|crc32c [--label NAME] [--size-mib N] [--warmup-iters N] [--sample-iters N] [--samples N] [--sweep-small] [--target-ms N]".to_string(),
                );
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    if cfg.block_size == 0 {
        return Err("block size must be > 0".to_string());
    }
    if cfg.sample_iters == 0 || cfg.samples == 0 {
        return Err("sample iters and samples must be > 0".to_string());
    }
    if cfg.target_ms == 0 {
        return Err("target ms must be > 0".to_string());
    }

    Ok(cfg)
}

fn throughput_gib_per_s(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0 * 1024.0)
}

fn checksum(algorithm: Algorithm, data: &[u8]) -> u32 {
    match algorithm {
        Algorithm::Crc32 => checksum::crc32::checksum(data),
        Algorithm::Crc32c => checksum::crc32c::checksum(data),
    }
}

fn backend_override_name(algorithm: Algorithm) -> String {
    match algorithm {
        Algorithm::Crc32 => {
            std::env::var("ARGMIN_CRC32_BENCH_BACKEND").unwrap_or_else(|_| "auto".to_string())
        }
        Algorithm::Crc32c => {
            std::env::var("ARGMIN_CRC32C_BENCH_BACKEND").unwrap_or_else(|_| "auto".to_string())
        }
    }
}

fn backend_name(algorithm: Algorithm) -> &'static str {
    match algorithm {
        Algorithm::Crc32 => checksum::crc32::backend_name(),
        Algorithm::Crc32c => checksum::crc32c::backend_name(),
    }
}

fn run_iterations(algorithm: Algorithm, data: &[u8], iterations: usize) -> Duration {
    let start = Instant::now();
    let mut sink = 0u32;
    for _ in 0..iterations {
        sink ^= black_box(checksum(algorithm, black_box(data)));
    }
    let elapsed = start.elapsed();
    black_box(sink);
    elapsed
}

fn calibrated_iterations(algorithm: Algorithm, data: &[u8], target: Duration) -> usize {
    let mut iterations = 1usize;
    loop {
        let elapsed = run_iterations(algorithm, data, iterations);
        if elapsed >= target || iterations == MAX_SWEEP_ITERS {
            return iterations;
        }
        iterations = iterations.saturating_mul(2).min(MAX_SWEEP_ITERS);
    }
}

fn run_small_sweep(cfg: &Config) {
    let max_size = SMALL_SWEEP_SIZES.iter().copied().max().unwrap_or(0);
    let word_count = (max_size + 7).div_ceil(8).max(1);
    let mut backing = vec![0u64; word_count];
    // SAFETY: every u8 bit pattern is valid, the byte slice covers exactly the initialized u64
    // allocation, and it does not outlive or alias another access to `backing`.
    let bytes = unsafe {
        std::slice::from_raw_parts_mut(
            backing.as_mut_ptr().cast::<u8>(),
            backing.len() * std::mem::size_of::<u64>(),
        )
    };
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as u8;
    }

    let target = Duration::from_millis(cfg.target_ms);
    println!("label={}", cfg.label);
    println!(
        "algorithm={}",
        match cfg.algorithm {
            Algorithm::Crc32 => "crc32",
            Algorithm::Crc32c => "crc32c",
        }
    );
    println!("mode=small-sweep");
    println!("backend_override={}", backend_override_name(cfg.algorithm));
    println!("backend_selected={}", backend_name(cfg.algorithm));
    println!("target_ms={}", cfg.target_ms);
    println!("samples={}", cfg.samples);
    println!(
        "size_bytes,leading_offset,iterations,crc,median_ns_per_call,best_ns_per_call,worst_ns_per_call,median_gib_s"
    );

    for &size in SMALL_SWEEP_SIZES {
        for leading_offset in 0usize..8 {
            let data = &bytes[leading_offset..leading_offset + size];
            let expected = checksum(cfg.algorithm, data);
            for _ in 0..cfg.warmup_iters {
                black_box(checksum(cfg.algorithm, black_box(data)));
            }

            let iterations = calibrated_iterations(cfg.algorithm, data, target);
            let mut ns_per_call = Vec::with_capacity(cfg.samples);
            for _ in 0..cfg.samples {
                let elapsed = run_iterations(cfg.algorithm, data, iterations);
                ns_per_call.push(elapsed.as_secs_f64() * 1_000_000_000.0 / iterations as f64);
            }
            assert_eq!(expected, checksum(cfg.algorithm, data));
            ns_per_call.sort_by(f64::total_cmp);

            let median_ns = ns_per_call[ns_per_call.len() / 2];
            let best_ns = ns_per_call[0];
            let worst_ns = ns_per_call[ns_per_call.len() - 1];
            let median_gib_s = if size == 0 {
                0.0
            } else {
                size as f64 / (median_ns / 1_000_000_000.0) / (1024.0 * 1024.0 * 1024.0)
            };
            println!(
                "{size},{leading_offset},{iterations},0x{expected:08X},{median_ns:.3},{best_ns:.3},{worst_ns:.3},{median_gib_s:.6}"
            );
        }
    }
}

fn main() -> Result<(), String> {
    let cfg = parse_args()?;

    if cfg.sweep_small {
        run_small_sweep(&cfg);
        return Ok(());
    }

    let mut data = vec![0u8; cfg.block_size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as u8;
    }

    let expected = checksum(cfg.algorithm, &data);

    for _ in 0..cfg.warmup_iters {
        black_box(checksum(cfg.algorithm, black_box(&data)));
    }

    let mut samples = Vec::with_capacity(cfg.samples);
    let mut sink = 0u32;
    for _ in 0..cfg.samples {
        let start = Instant::now();
        for _ in 0..cfg.sample_iters {
            sink ^= black_box(checksum(cfg.algorithm, black_box(&data)));
        }
        samples.push(start.elapsed());
    }

    assert_eq!(expected, checksum(cfg.algorithm, &data));
    black_box(sink);

    let total_bytes = cfg.block_size * cfg.sample_iters;
    let mut throughputs: Vec<f64> = samples
        .iter()
        .map(|elapsed| throughput_gib_per_s(total_bytes, *elapsed))
        .collect();
    throughputs.sort_by(f64::total_cmp);

    println!("label={}", cfg.label);
    println!(
        "algorithm={}",
        match cfg.algorithm {
            Algorithm::Crc32 => "crc32",
            Algorithm::Crc32c => "crc32c",
        }
    );
    println!("block_size_bytes={}", cfg.block_size);
    println!("sample_iters={}", cfg.sample_iters);
    println!("samples={}", cfg.samples);
    println!("backend_override={}", backend_override_name(cfg.algorithm));
    println!("backend_selected={}", backend_name(cfg.algorithm));
    println!("crc=0x{expected:08X}");
    for (idx, elapsed) in samples.iter().enumerate() {
        println!(
            "sample_{}: elapsed_ms={:.3} throughput_gib_s={:.3}",
            idx + 1,
            elapsed.as_secs_f64() * 1000.0,
            throughput_gib_per_s(total_bytes, *elapsed),
        );
    }
    println!(
        "median_throughput_gib_s={:.3}",
        throughputs[throughputs.len() / 2]
    );
    println!(
        "best_throughput_gib_s={:.3}",
        throughputs[throughputs.len() - 1]
    );
    println!("worst_throughput_gib_s={:.3}", throughputs[0]);

    Ok(())
}
