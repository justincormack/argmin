use std::hint::black_box;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct Config {
    label: String,
    block_size: usize,
    warmup_iters: usize,
    sample_iters: usize,
    samples: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            label: "crc64".to_string(),
            block_size: 8 * 1024 * 1024,
            warmup_iters: 32,
            sample_iters: 256,
            samples: 5,
        }
    }
}

fn parse_args() -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
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
            "--help" | "-h" => {
                return Err(
                    "usage: crc64_bench [--label NAME] [--size-mib N] [--warmup-iters N] [--sample-iters N] [--samples N]".to_string(),
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

    Ok(cfg)
}

fn throughput_gib_per_s(bytes: usize, elapsed: Duration) -> f64 {
    bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0 * 1024.0)
}

fn main() -> Result<(), String> {
    let cfg = parse_args()?;

    let mut data = vec![0u8; cfg.block_size];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as u8;
    }

    let expected = checksum::crc64::checksum(&data);

    for _ in 0..cfg.warmup_iters {
        black_box(checksum::crc64::checksum(black_box(&data)));
    }

    let mut samples = Vec::with_capacity(cfg.samples);
    let mut sink = 0u64;
    for _ in 0..cfg.samples {
        let start = Instant::now();
        for _ in 0..cfg.sample_iters {
            sink ^= black_box(checksum::crc64::checksum(black_box(&data)));
        }
        samples.push(start.elapsed());
    }

    assert_eq!(expected, checksum::crc64::checksum(&data));
    black_box(sink);

    let total_bytes = cfg.block_size * cfg.sample_iters;
    let mut throughputs: Vec<f64> = samples
        .iter()
        .map(|elapsed| throughput_gib_per_s(total_bytes, *elapsed))
        .collect();
    throughputs.sort_by(f64::total_cmp);

    println!("label={}", cfg.label);
    println!("block_size_bytes={}", cfg.block_size);
    println!("sample_iters={}", cfg.sample_iters);
    println!("samples={}", cfg.samples);
    println!(
        "backend_override={}",
        std::env::var("ARGMIN_CRC64_BENCH_BACKEND").unwrap_or_else(|_| "auto".to_string())
    );
    println!("backend_selected={}", checksum::crc64::backend_name());
    println!("crc=0x{expected:016X}");
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
