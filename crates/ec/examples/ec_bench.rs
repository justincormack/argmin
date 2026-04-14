use std::hint::black_box;
use std::time::Instant;

#[derive(Clone, Copy)]
enum BackendChoice {
    Native,
    IsaL,
}

struct Config {
    backend: BackendChoice,
    label: String,
    size_mib: usize,
    warmup_iters: usize,
    sample_iters: usize,
    samples: usize,
    data_shards: u8,
    parity_shards: u8,
    recover_index: usize,
}

trait Backend {
    type Codec;
    type Config;
    type Error: std::fmt::Display;

    fn config(data_shards: u8, parity_shards: u8) -> Result<Self::Config, Self::Error>;
    fn codec(config: Self::Config) -> Result<Self::Codec, Self::Error>;
    fn encode(
        codec: &Self::Codec,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), Self::Error>;
    fn reconstruct(
        codec: &Self::Codec,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), Self::Error>;
}

#[cfg(feature = "native-backend")]
struct NativeBackend;

#[cfg(feature = "native-backend")]
impl Backend for NativeBackend {
    type Codec = ec_native::ErasureCodec;
    type Config = ec_native::EcConfig;
    type Error = ec_native::EcError;

    fn config(data_shards: u8, parity_shards: u8) -> Result<Self::Config, Self::Error> {
        ec_native::EcConfig::new(data_shards, parity_shards)
    }

    fn codec(config: Self::Config) -> Result<Self::Codec, Self::Error> {
        ec_native::ErasureCodec::new(config)
    }

    fn encode(
        codec: &Self::Codec,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), Self::Error> {
        codec.encode(data, parity)
    }

    fn reconstruct(
        codec: &Self::Codec,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), Self::Error> {
        codec.reconstruct(present_indices, present_data, recover_indices, outputs)
    }
}

#[cfg(feature = "real-backend")]
struct RealBackend;

#[cfg(feature = "real-backend")]
impl Backend for RealBackend {
    type Codec = ec_real::ErasureCodec;
    type Config = ec_real::EcConfig;
    type Error = ec_real::EcError;

    fn config(data_shards: u8, parity_shards: u8) -> Result<Self::Config, Self::Error> {
        ec_real::EcConfig::new(data_shards, parity_shards)
    }

    fn codec(config: Self::Config) -> Result<Self::Codec, Self::Error> {
        ec_real::ErasureCodec::new(config)
    }

    fn encode(
        codec: &Self::Codec,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), Self::Error> {
        codec.encode(data, parity)
    }

    fn reconstruct(
        codec: &Self::Codec,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), Self::Error> {
        codec.reconstruct(present_indices, present_data, recover_indices, outputs)
    }
}

fn parse_args() -> Result<Config, String> {
    let mut backend = None;
    let mut label = None;
    let mut size_mib = 8usize;
    let mut warmup_iters = 32usize;
    let mut sample_iters = 256usize;
    let mut samples = 5usize;
    let mut data_shards = 6u8;
    let mut parity_shards = 2u8;
    let mut recover_index = 0usize;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--backend" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --backend".to_string())?;
                backend = Some(match value.as_str() {
                    "native" => BackendChoice::Native,
                    "isa-l" => BackendChoice::IsaL,
                    _ => return Err(format!("unsupported backend: {value}")),
                });
            }
            "--label" => {
                label = Some(
                    args.next()
                        .ok_or_else(|| "missing value for --label".to_string())?,
                );
            }
            "--size-mib" => {
                size_mib = parse_usize_arg(&arg, args.next())?;
            }
            "--warmup-iters" => {
                warmup_iters = parse_usize_arg(&arg, args.next())?;
            }
            "--sample-iters" => {
                sample_iters = parse_usize_arg(&arg, args.next())?;
            }
            "--samples" => {
                samples = parse_usize_arg(&arg, args.next())?;
            }
            "--data-shards" => {
                data_shards = parse_u8_arg(&arg, args.next())?;
            }
            "--parity-shards" => {
                parity_shards = parse_u8_arg(&arg, args.next())?;
            }
            "--recover-index" => {
                recover_index = parse_usize_arg(&arg, args.next())?;
            }
            "--help" | "-h" => {
                return Err(
                    "usage: ec_bench --backend native|isa-l [--label NAME] [--size-mib N] [--warmup-iters N] [--sample-iters N] [--samples N] [--data-shards N] [--parity-shards N] [--recover-index N]".to_string(),
                );
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    let backend = backend.ok_or_else(|| "missing required --backend".to_string())?;
    let label = label.unwrap_or_else(|| match backend {
        BackendChoice::Native => "ec-native".to_string(),
        BackendChoice::IsaL => "ec-isa-l".to_string(),
    });

    Ok(Config {
        backend,
        label,
        size_mib,
        warmup_iters,
        sample_iters,
        samples,
        data_shards,
        parity_shards,
        recover_index,
    })
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse::<usize>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn parse_u8_arg(flag: &str, value: Option<String>) -> Result<u8, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse::<u8>()
        .map_err(|err| format!("invalid value for {flag}: {err}"))
}

fn make_data(k: usize, shard_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..shard_size)
                .map(|j| ((i * 37 + j * 13 + 7) & 0xFF) as u8)
                .collect()
        })
        .collect()
}

fn present_indices(total: usize, k: usize, recover_index: usize) -> Result<Vec<usize>, String> {
    let indices: Vec<usize> = (0..total)
        .filter(|&index| index != recover_index)
        .take(k)
        .collect();
    if indices.len() != k {
        return Err(format!(
            "not enough present shards after excluding recover index {recover_index}"
        ));
    }
    Ok(indices)
}

fn run_backend<B: Backend>(config: &Config) -> Result<(), String> {
    let k = config.data_shards as usize;
    let m = config.parity_shards as usize;
    let total = k + m;
    let shard_size = config.size_mib * 1024 * 1024;

    if config.parity_shards == 0 {
        return Err("parity_shards must be >= 1 for reconstruction benchmark".to_string());
    }
    if config.recover_index >= total {
        return Err(format!(
            "recover index {} out of range for total shards {}",
            config.recover_index, total
        ));
    }

    let codec_config =
        B::config(config.data_shards, config.parity_shards).map_err(|err| err.to_string())?;
    let codec = B::codec(codec_config).map_err(|err| err.to_string())?;

    let data = make_data(k, shard_size);
    let data_refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
    B::encode(&codec, &data_refs, &mut parity_refs).map_err(|err| err.to_string())?;

    let recover_indices = [config.recover_index];
    let present_indices = present_indices(total, k, config.recover_index)?;
    let present_data: Vec<&[u8]> = present_indices
        .iter()
        .map(|&index| {
            if index < k {
                data[index].as_slice()
            } else {
                parity[index - k].as_slice()
            }
        })
        .collect();

    let expected = if config.recover_index < k {
        data[config.recover_index].as_slice()
    } else {
        parity[config.recover_index - k].as_slice()
    };

    let mut output = vec![0u8; shard_size];
    let bytes_per_sample = config.sample_iters as f64 * shard_size as f64;

    println!("label={}", config.label);
    println!("shard_size_bytes={shard_size}");
    println!("data_shards={}", config.data_shards);
    println!("parity_shards={}", config.parity_shards);
    println!("recover_index={}", config.recover_index);
    println!("sample_iters={}", config.sample_iters);
    println!("samples={}", config.samples);

    for _ in 0..config.warmup_iters {
        let mut outputs = [output.as_mut_slice()];
        B::reconstruct(
            &codec,
            &present_indices,
            &present_data,
            &recover_indices,
            &mut outputs,
        )
        .map_err(|err| err.to_string())?;
        black_box(output[0]);
    }

    let mut throughputs = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let started = Instant::now();
        for _ in 0..config.sample_iters {
            let mut outputs = [output.as_mut_slice()];
            B::reconstruct(
                &codec,
                &present_indices,
                &present_data,
                &recover_indices,
                &mut outputs,
            )
            .map_err(|err| err.to_string())?;
            black_box(output[0]);
        }

        let elapsed = started.elapsed();
        let throughput_gib_s =
            bytes_per_sample / elapsed.as_secs_f64() / (1024.0 * 1024.0 * 1024.0);
        throughputs.push(throughput_gib_s);
        println!(
            "sample_{}: elapsed_ms={:.3} throughput_gib_s={:.3}",
            sample + 1,
            elapsed.as_secs_f64() * 1000.0,
            throughput_gib_s
        );
    }

    if output.as_slice() != expected {
        return Err("reconstructed shard did not match expected contents".to_string());
    }

    let mut sorted = throughputs.clone();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let median = sorted[sorted.len() / 2];
    let best = throughputs
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let worst = throughputs.iter().copied().fold(f64::INFINITY, f64::min);

    println!("median_throughput_gib_s={median:.3}");
    println!("best_throughput_gib_s={best:.3}");
    println!("worst_throughput_gib_s={worst:.3}");
    Ok(())
}

fn main() -> Result<(), String> {
    let config = parse_args()?;

    match config.backend {
        BackendChoice::Native => {
            #[cfg(feature = "native-backend")]
            {
                run_backend::<NativeBackend>(&config)
            }
            #[cfg(not(feature = "native-backend"))]
            {
                Err("native backend not enabled for this build".to_string())
            }
        }
        BackendChoice::IsaL => {
            #[cfg(feature = "real-backend")]
            {
                run_backend::<RealBackend>(&config)
            }
            #[cfg(not(feature = "real-backend"))]
            {
                Err("isa-l backend not enabled for this build".to_string())
            }
        }
    }
}
