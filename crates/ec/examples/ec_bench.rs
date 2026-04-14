use std::hint::black_box;
use std::time::Instant;

#[derive(Clone, Copy)]
enum BenchMode {
    Encode,
    Verify,
    Reconstruct,
}

struct Config {
    mode: BenchMode,
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
    fn verify_scratch_size(codec: &Self::Codec, shard_size: usize) -> usize;
    fn verify(
        codec: &Self::Codec,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
    ) -> Result<bool, Self::Error>;
    fn reconstruct(
        codec: &Self::Codec,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), Self::Error>;
}

struct NativeBackend;

impl Backend for NativeBackend {
    type Codec = ec::ErasureCodec;
    type Config = ec::EcConfig;
    type Error = ec::EcError;

    fn config(data_shards: u8, parity_shards: u8) -> Result<Self::Config, Self::Error> {
        ec::EcConfig::new(data_shards, parity_shards)
    }

    fn codec(config: Self::Config) -> Result<Self::Codec, Self::Error> {
        ec::ErasureCodec::new(config)
    }

    fn encode(
        codec: &Self::Codec,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), Self::Error> {
        codec.encode(data, parity)
    }

    fn verify_scratch_size(codec: &Self::Codec, shard_size: usize) -> usize {
        codec.verify_scratch_size(shard_size)
    }

    fn verify(
        codec: &Self::Codec,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
    ) -> Result<bool, Self::Error> {
        codec
            .verify(data, parity, scratch)
            .map(|result| matches!(result, ec::VerifyResult::Ok))
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
    let mut mode = BenchMode::Reconstruct;
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
            "--mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| "missing value for --mode".to_string())?;
                mode = match value.as_str() {
                    "encode" => BenchMode::Encode,
                    "verify" => BenchMode::Verify,
                    "reconstruct" => BenchMode::Reconstruct,
                    _ => return Err(format!("unsupported mode: {value}")),
                };
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
                    "usage: ec_bench [--mode encode|verify|reconstruct] [--label NAME] [--size-mib N] [--warmup-iters N] [--sample-iters N] [--samples N] [--data-shards N] [--parity-shards N] [--recover-index N]".to_string(),
                );
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    let label = label.unwrap_or_else(|| "ec".to_string());

    Ok(Config {
        mode,
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

fn mode_name(mode: BenchMode) -> &'static str {
    match mode {
        BenchMode::Encode => "encode",
        BenchMode::Verify => "verify",
        BenchMode::Reconstruct => "reconstruct",
    }
}

fn black_box_first_byte(bytes: &[u8]) {
    if let Some(&byte) = bytes.first() {
        black_box(byte);
    } else {
        black_box(bytes.len());
    }
}

fn run_backend<B: Backend>(config: &Config) -> Result<(), String> {
    let k = config.data_shards as usize;
    let m = config.parity_shards as usize;
    let total = k + m;
    let shard_size = config.size_mib * 1024 * 1024;

    if matches!(config.mode, BenchMode::Reconstruct) {
        if config.parity_shards == 0 {
            return Err("parity_shards must be >= 1 for reconstruction benchmark".to_string());
        }
        if config.recover_index >= total {
            return Err(format!(
                "recover index {} out of range for total shards {}",
                config.recover_index, total
            ));
        }
    }

    let codec_config =
        B::config(config.data_shards, config.parity_shards).map_err(|err| err.to_string())?;
    let codec = B::codec(codec_config).map_err(|err| err.to_string())?;

    let data = make_data(k, shard_size);
    let data_refs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
    B::encode(&codec, &data_refs, &mut parity_refs).map_err(|err| err.to_string())?;
    let parity_read_refs: Vec<&[u8]> = parity.iter().map(Vec::as_slice).collect();

    let recover_indices = [config.recover_index];
    let present_indices = if matches!(config.mode, BenchMode::Reconstruct) {
        present_indices(total, k, config.recover_index)?
    } else {
        Vec::new()
    };
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

    let expected = if matches!(config.mode, BenchMode::Reconstruct) {
        Some(if config.recover_index < k {
            data[config.recover_index].as_slice()
        } else {
            parity[config.recover_index - k].as_slice()
        })
    } else {
        None
    };

    let mut output = vec![0u8; shard_size];
    let mut _encode_buffers: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
    let mut encode_output_refs: Vec<&mut [u8]> =
        _encode_buffers.iter_mut().map(Vec::as_mut_slice).collect();
    let mut verify_scratch = vec![0u8; B::verify_scratch_size(&codec, shard_size)];
    let bytes_per_iter = match config.mode {
        BenchMode::Encode | BenchMode::Verify => shard_size as f64 * k as f64,
        BenchMode::Reconstruct => shard_size as f64,
    };
    let bytes_per_sample = config.sample_iters as f64 * bytes_per_iter;

    println!("label={}", config.label);
    println!("mode={}", mode_name(config.mode));
    println!("shard_size_bytes={shard_size}");
    println!("data_shards={}", config.data_shards);
    println!("parity_shards={}", config.parity_shards);
    if matches!(config.mode, BenchMode::Reconstruct) {
        println!("recover_index={}", config.recover_index);
    }
    println!("sample_iters={}", config.sample_iters);
    println!("samples={}", config.samples);

    for _ in 0..config.warmup_iters {
        match config.mode {
            BenchMode::Encode => {
                B::encode(&codec, &data_refs, &mut encode_output_refs)
                    .map_err(|err| err.to_string())?;
                if let Some(first) = encode_output_refs.first() {
                    black_box_first_byte(first);
                } else {
                    black_box(encode_output_refs.len());
                }
            }
            BenchMode::Verify => {
                let ok = B::verify(&codec, &data_refs, &parity_read_refs, &mut verify_scratch)
                    .map_err(|err| err.to_string())?;
                if !ok {
                    return Err("verify reported mismatch".to_string());
                }
                black_box_first_byte(&verify_scratch);
            }
            BenchMode::Reconstruct => {
                let mut outputs = [output.as_mut_slice()];
                B::reconstruct(
                    &codec,
                    &present_indices,
                    &present_data,
                    &recover_indices,
                    &mut outputs,
                )
                .map_err(|err| err.to_string())?;
                black_box_first_byte(&output);
            }
        }
    }

    let mut throughputs = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let started = Instant::now();
        for _ in 0..config.sample_iters {
            match config.mode {
                BenchMode::Encode => {
                    B::encode(&codec, &data_refs, &mut encode_output_refs)
                        .map_err(|err| err.to_string())?;
                    if let Some(first) = encode_output_refs.first() {
                        black_box_first_byte(first);
                    } else {
                        black_box(encode_output_refs.len());
                    }
                }
                BenchMode::Verify => {
                    let ok = B::verify(&codec, &data_refs, &parity_read_refs, &mut verify_scratch)
                        .map_err(|err| err.to_string())?;
                    if !ok {
                        return Err("verify reported mismatch".to_string());
                    }
                    black_box_first_byte(&verify_scratch);
                }
                BenchMode::Reconstruct => {
                    let mut outputs = [output.as_mut_slice()];
                    B::reconstruct(
                        &codec,
                        &present_indices,
                        &present_data,
                        &recover_indices,
                        &mut outputs,
                    )
                    .map_err(|err| err.to_string())?;
                    black_box_first_byte(&output);
                }
            }
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

    if let Some(expected) = expected {
        if output.as_slice() != expected {
            return Err("reconstructed shard did not match expected contents".to_string());
        }
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
    run_backend::<NativeBackend>(&config)
}
