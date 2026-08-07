use std::hint::black_box;
use std::io::{self, Cursor, Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use auth::{
    authenticate_request, CredentialStore, ExpectedSigningRegion, IdentityProvider, SecretKey,
    SigningService,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection};

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const DATE: &str = "20130524";
const AMZ_DATE: &str = "20130524T000000Z";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const METHOD: &str = "GET";
const PATH: &str = "/benchmark/object";
const QUERY: &str = "partNumber=1&uploadId=crypto-benchmark";
const HOST: &str = "benchmark.s3.us-east-1.amazonaws.com";
const STORAGE_RPC_ALPN: &[u8] = b"argmin-storage-rpc/1";

#[derive(Debug, Clone, Copy)]
struct Config {
    block_size: usize,
    warmup_iters: usize,
    sample_iters: usize,
    sigv4_warmup_iters: usize,
    sigv4_sample_iters: usize,
    samples: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            block_size: 8 * 1024 * 1024,
            warmup_iters: 4,
            sample_iters: 32,
            sigv4_warmup_iters: 10_000,
            sigv4_sample_iters: 100_000,
            samples: 5,
        }
    }
}

struct SignedRequest {
    headers: Vec<(String, String)>,
    now_epoch_secs: u64,
}

fn main() -> Result<(), String> {
    let config = parse_args()?;
    let tls_crypto_provider = tls_provider::configured_provider();
    let data = benchmark_data(config.block_size);
    let (provider, request) = signed_request()?;

    println!("tls_crypto_provider={}", tls_provider::provider_name());
    println!("crypto_provider={}", argmin_crypto::provider_name());
    println!("sha256_backend={}", checksum::sha256::backend_name());
    println!("tls_profile=storage-rpc-tls13");
    println!("block_size_bytes={}", config.block_size);
    println!("sample_iters={}", config.sample_iters);
    println!("sigv4_sample_iters={}", config.sigv4_sample_iters);
    println!("samples={}", config.samples);

    println!();
    println!("[sigv4 payload sha256]");
    let payload_samples = measure(
        config.warmup_iters,
        config.sample_iters,
        config.samples,
        || {
            black_box(auth::canonical::sha256_hex(black_box(&data)));
        },
    );
    let payload_median =
        print_bulk_samples(&payload_samples, config.block_size, config.sample_iters);

    println!();
    println!("[sigv4 request verification]");
    let request_samples = measure(
        config.sigv4_warmup_iters,
        config.sigv4_sample_iters,
        config.samples,
        || {
            let context = authenticate_request(
                METHOD,
                PATH,
                QUERY,
                black_box(request.headers.as_slice()),
                &[],
                &provider,
                ExpectedSigningRegion::ExactEndpointRegion(REGION),
                SigningService::S3,
                request.now_epoch_secs,
            )
            .expect("benchmark request must authenticate");
            black_box(context);
        },
    );
    let request_median = print_rate_samples(&request_samples, config.sigv4_sample_iters);

    let (mut seal_client, seal_server) = connected_tls_pair(Arc::clone(&tls_crypto_provider))?;
    print_tls_connection(&seal_client, &seal_server)?;
    println!();
    println!("[peer tls13 seal]");
    let mut sink = io::sink();
    let seal_samples = measure_fallible(
        config.warmup_iters,
        config.sample_iters,
        config.samples,
        || tls_seal(&mut seal_client, &data, &mut sink),
    )?;
    let seal_median = print_bulk_samples(&seal_samples, config.block_size, config.sample_iters);

    let (mut open_client, mut open_server) = connected_tls_pair(tls_crypto_provider)?;
    println!();
    println!("[peer tls13 open]");
    let mut encrypted = Vec::with_capacity(config.block_size + config.block_size / 100);
    let mut plaintext = vec![0_u8; config.block_size];
    for _ in 0..config.warmup_iters {
        encrypted.clear();
        tls_seal(&mut open_client, &data, &mut encrypted).map_err(io_error)?;
        tls_open(&mut open_server, &encrypted, &mut plaintext).map_err(io_error)?;
    }
    let mut open_samples = Vec::with_capacity(config.samples);
    for _ in 0..config.samples {
        let mut elapsed = Duration::ZERO;
        for _ in 0..config.sample_iters {
            encrypted.clear();
            tls_seal(&mut open_client, &data, &mut encrypted).map_err(io_error)?;
            let start = Instant::now();
            tls_open(&mut open_server, &encrypted, &mut plaintext).map_err(io_error)?;
            elapsed += start.elapsed();
        }
        open_samples.push(elapsed);
    }
    assert_eq!(plaintext, data);
    black_box(&plaintext);
    let open_median = print_bulk_samples(&open_samples, config.block_size, config.sample_iters);

    println!();
    println!("summary_sigv4_payload_gib_s={payload_median:.3}");
    println!("summary_sigv4_verify_requests_s={request_median:.0}");
    println!("summary_tls13_seal_gib_s={seal_median:.3}");
    println!("summary_tls13_open_gib_s={open_median:.3}");
    println!(
        "summary_tls13_peer_limit_gib_s={:.3}",
        seal_median.min(open_median)
    );

    Ok(())
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--size-mib" => {
                let size_mib = parse_usize(&arg, args.next())?;
                config.block_size = size_mib
                    .checked_mul(1024 * 1024)
                    .ok_or_else(|| "--size-mib is too large".to_string())?;
            }
            "--warmup-iters" => config.warmup_iters = parse_usize(&arg, args.next())?,
            "--sample-iters" => config.sample_iters = parse_usize(&arg, args.next())?,
            "--sigv4-warmup-iters" => {
                config.sigv4_warmup_iters = parse_usize(&arg, args.next())?;
            }
            "--sigv4-sample-iters" => {
                config.sigv4_sample_iters = parse_usize(&arg, args.next())?;
            }
            "--samples" => config.samples = parse_usize(&arg, args.next())?,
            "--help" | "-h" => {
                return Err(
                    "usage: crypto_bench [--size-mib N] [--warmup-iters N] [--sample-iters N] [--sigv4-warmup-iters N] [--sigv4-sample-iters N] [--samples N]"
                        .to_string(),
                );
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    if config.block_size == 0
        || config.sample_iters == 0
        || config.sigv4_sample_iters == 0
        || config.samples == 0
    {
        return Err("size, sample iterations, and samples must be greater than zero".to_string());
    }
    Ok(config)
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()
        .map_err(|error| format!("invalid value for {flag}: {error}"))
}

fn benchmark_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as u8)
        .collect()
}

fn signed_request() -> Result<(IdentityProvider, SignedRequest), String> {
    let secret = SecretKey::new(SECRET_ACCESS_KEY.to_string());
    let mut store = CredentialStore::new();
    store
        .add(ACCESS_KEY_ID.to_string(), secret.clone())
        .map_err(|error| error.to_string())?;
    let provider = IdentityProvider::in_memory(store).map_err(|error| error.to_string())?;

    let body_hash = auth::canonical::sha256_hex(b"");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let signing_headers = [
        ("host", HOST),
        ("x-amz-content-sha256", body_hash.as_str()),
        ("x-amz-date", AMZ_DATE),
    ];
    let canonical_headers = auth::canonical::canonical_headers(&signing_headers);
    let canonical_query = auth::canonical::canonical_query_string(QUERY);
    let canonical_request = auth::canonical::canonical_request(
        METHOD,
        PATH,
        &canonical_query,
        &canonical_headers,
        signed_headers,
        &body_hash,
    );
    let scope = format!("{DATE}/{REGION}/{SERVICE}/aws4_request");
    let string_to_sign = auth::canonical::string_to_sign(
        AMZ_DATE,
        &scope,
        &auth::canonical::sha256_hex(canonical_request.as_bytes()),
    );
    let signing_key = auth::sigv4::derive_signing_key(&secret, DATE, REGION, SERVICE);
    let signature = hex_lower(&argmin_crypto::hmac::sha256(
        signing_key.as_ref(),
        string_to_sign.as_bytes(),
    ));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY_ID}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    let headers = vec![
        ("host".to_string(), HOST.to_string()),
        ("x-amz-content-sha256".to_string(), body_hash),
        ("x-amz-date".to_string(), AMZ_DATE.to_string()),
        ("authorization".to_string(), authorization),
    ];
    let now_epoch_secs = auth::parse_amz_date(AMZ_DATE)
        .ok_or_else(|| "benchmark timestamp must be valid".to_string())?;

    authenticate_request(
        METHOD,
        PATH,
        QUERY,
        headers.as_slice(),
        &[],
        &provider,
        ExpectedSigningRegion::ExactEndpointRegion(REGION),
        SigningService::S3,
        now_epoch_secs,
    )
    .map_err(|error| format!("generated benchmark request did not authenticate: {error}"))?;

    Ok((
        provider,
        SignedRequest {
            headers,
            now_epoch_secs,
        },
    ))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    output
}

fn connected_tls_pair(
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<(ClientConnection, ServerConnection), String> {
    let certificates = CertificateDer::pem_slice_iter(include_bytes!(
        "../../s3-tests/testdata/localhost-cert.pem"
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|error| error.to_string())?;
    let private_key =
        PrivateKeyDer::from_pem_slice(include_bytes!("../../s3-tests/testdata/localhost-key.pem"))
            .map_err(|error| error.to_string())?;
    let mut roots = RootCertStore::empty();
    let ca = CertificateDer::pem_slice_iter(include_bytes!("../../s3-tests/testdata/ca-cert.pem"))
        .next()
        .ok_or_else(|| "benchmark CA file is empty".to_string())?
        .map_err(|error| error.to_string())?;
    roots.add(ca).map_err(|error| error.to_string())?;

    // Keep this profile aligned with storage_rpc_tls_{client,server}_config.
    let mut client_config = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| error.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![STORAGE_RPC_ALPN.to_vec()];
    let mut server_config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| error.to_string())?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|error| error.to_string())?;
    server_config.alpn_protocols = vec![STORAGE_RPC_ALPN.to_vec()];

    let server_name = ServerName::try_from("localhost")
        .map_err(|error| error.to_string())?
        .to_owned();
    let mut client = ClientConnection::new(Arc::new(client_config), server_name)
        .map_err(|error| error.to_string())?;
    let mut server =
        ServerConnection::new(Arc::new(server_config)).map_err(|error| error.to_string())?;

    while client.is_handshaking() || server.is_handshaking() {
        if client.wants_write() {
            transfer_client_to_server(&mut client, &mut server).map_err(io_error)?;
        }
        if server.wants_write() {
            transfer_server_to_client(&mut server, &mut client).map_err(io_error)?;
        }
    }
    if client.alpn_protocol() != Some(STORAGE_RPC_ALPN)
        || server.alpn_protocol() != Some(STORAGE_RPC_ALPN)
    {
        return Err("benchmark TLS connection did not negotiate storage RPC ALPN".to_string());
    }
    Ok((client, server))
}

fn transfer_client_to_server(
    client: &mut ClientConnection,
    server: &mut ServerConnection,
) -> io::Result<()> {
    let mut encrypted = Vec::new();
    client.write_tls(&mut encrypted)?;
    server.read_tls(&mut Cursor::new(encrypted))?;
    server.process_new_packets().map_err(io::Error::other)?;
    Ok(())
}

fn transfer_server_to_client(
    server: &mut ServerConnection,
    client: &mut ClientConnection,
) -> io::Result<()> {
    let mut encrypted = Vec::new();
    server.write_tls(&mut encrypted)?;
    client.read_tls(&mut Cursor::new(encrypted))?;
    client.process_new_packets().map_err(io::Error::other)?;
    Ok(())
}

fn tls_seal(
    client: &mut ClientConnection,
    mut plaintext: &[u8],
    output: &mut impl Write,
) -> io::Result<()> {
    while !plaintext.is_empty() {
        let written = client.writer().write(plaintext)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "TLS plaintext writer made no progress",
            ));
        }
        plaintext = &plaintext[written..];
        while client.wants_write() {
            client.write_tls(output)?;
        }
    }
    Ok(())
}

fn tls_open(
    server: &mut ServerConnection,
    encrypted: &[u8],
    plaintext: &mut [u8],
) -> io::Result<()> {
    let mut input = Cursor::new(encrypted);
    let mut plaintext_offset = 0_usize;
    while input.position() < encrypted.len() as u64 {
        server.read_tls(&mut input)?;
        let state = server.process_new_packets().map_err(io::Error::other)?;
        let available = state.plaintext_bytes_to_read();
        let end = plaintext_offset.checked_add(available).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "TLS plaintext length overflow")
        })?;
        let output = plaintext.get_mut(plaintext_offset..end).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "TLS connection produced too much plaintext",
            )
        })?;
        server.reader().read_exact(output)?;
        plaintext_offset = end;
    }
    if plaintext_offset != plaintext.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "TLS connection produced too little plaintext",
        ));
    }
    Ok(())
}

fn print_tls_connection(
    client: &ClientConnection,
    server: &ServerConnection,
) -> Result<(), String> {
    let protocol = client
        .protocol_version()
        .ok_or_else(|| "TLS protocol was not negotiated".to_string())?;
    let cipher = client
        .negotiated_cipher_suite()
        .ok_or_else(|| "TLS cipher suite was not negotiated".to_string())?;
    if server.protocol_version() != Some(protocol)
        || server.negotiated_cipher_suite().map(|suite| suite.suite()) != Some(cipher.suite())
    {
        return Err("TLS peers disagree about negotiated parameters".to_string());
    }
    println!("tls_protocol={protocol:?}");
    println!("tls_cipher_suite={:?}", cipher.suite());
    println!("tls_alpn={}", String::from_utf8_lossy(STORAGE_RPC_ALPN));
    Ok(())
}

fn measure(
    mut warmup: usize,
    sample_iters: usize,
    samples: usize,
    mut operation: impl FnMut(),
) -> Vec<Duration> {
    while warmup > 0 {
        operation();
        warmup -= 1;
    }
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        for _ in 0..sample_iters {
            operation();
        }
        durations.push(start.elapsed());
    }
    durations
}

fn measure_fallible(
    mut warmup: usize,
    sample_iters: usize,
    samples: usize,
    mut operation: impl FnMut() -> io::Result<()>,
) -> Result<Vec<Duration>, String> {
    while warmup > 0 {
        operation().map_err(io_error)?;
        warmup -= 1;
    }
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        for _ in 0..sample_iters {
            operation().map_err(io_error)?;
        }
        durations.push(start.elapsed());
    }
    Ok(durations)
}

fn print_bulk_samples(samples: &[Duration], block_size: usize, sample_iters: usize) -> f64 {
    let bytes = block_size
        .checked_mul(sample_iters)
        .expect("validated benchmark byte count must fit usize");
    let values: Vec<f64> = samples
        .iter()
        .map(|elapsed| bytes as f64 / elapsed.as_secs_f64() / GIB)
        .collect();
    print_samples(samples, &values, "throughput_gib_s", 3);
    median(&values)
}

fn print_rate_samples(samples: &[Duration], sample_iters: usize) -> f64 {
    let values: Vec<f64> = samples
        .iter()
        .map(|elapsed| sample_iters as f64 / elapsed.as_secs_f64())
        .collect();
    print_samples(samples, &values, "requests_s", 0);
    median(&values)
}

fn print_samples(samples: &[Duration], values: &[f64], metric: &str, precision: usize) {
    for (index, (elapsed, value)) in samples.iter().zip(values).enumerate() {
        println!(
            "sample_{}: elapsed_ms={:.3} {metric}={value:.precision$}",
            index + 1,
            elapsed.as_secs_f64() * 1000.0,
        );
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    println!("median_{metric}={:.precision$}", sorted[sorted.len() / 2]);
    println!("best_{metric}={:.precision$}", sorted[sorted.len() - 1]);
    println!("worst_{metric}={:.precision$}", sorted[0]);
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

fn io_error(error: io::Error) -> String {
    error.to_string()
}
