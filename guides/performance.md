# Performance Guide

Argmin provides focused benchmarks for the CPU-heavy checksum, erasure-coding,
SigV4, and peer TLS paths. These benchmarks are useful for checking runtime
backend selection and comparing machines or cryptography providers. They are
not end-to-end S3 throughput benchmarks: they exclude storage, networking,
request scheduling, and most protocol processing.

## Benchmark environment

Build and run benchmarks on the machine and operating-system environment that
will run the server. CPU features exposed by a virtual machine or container,
kernel support, and the system OpenSSL build can all change the selected
backend.

For comparable results:

- keep the Rust toolchain, source revision, benchmark settings, and shard
  layout identical;
- leave the machine otherwise idle and avoid thermal or power throttling;
- use the same CPU affinity, frequency policy, and NUMA placement;
- run enough samples to observe variability and compare medians rather than a
  single best result; and
- record the printed CPU, provider, backend, TLS cipher, and block-size fields
  with the results.

The scripts use release builds and `cargo --offline`. Fetch dependencies before
running them on a newly provisioned machine.

## Checksums and erasure coding

Run:

```bash
./scripts/bench-crc
```

Despite its historical name, [`bench-crc`](../scripts/bench-crc) measures both
checksums and erasure coding. It runs CRC64/NVME, CRC32, and CRC32C with the
scalar backend forced and then with normal runtime selection. It also runs
erasure-coding parity generation and single-shard reconstruction in scalar and
automatically selected modes.

The following environment variables adjust the workload:

| Variable | Default | Meaning |
|---|---:|---|
| `SIZE_MIB` | `8` | CRC buffer size and size of each EC shard |
| `WARMUP_ITERS` | `32` | Untimed iterations before each measurement |
| `SAMPLE_ITERS` | `256` | Timed operations in each sample |
| `SAMPLES` | `5` | Number of samples |
| `EC_DATA_SHARDS` | `6` | EC data-shard count |
| `EC_PARITY_SHARDS` | `2` | EC parity-shard count |
| `EC_RECOVER_INDEX` | `0` | Shard reconstructed by the reconstruction benchmark |

For example, a shorter exploratory run is:

```bash
SIZE_MIB=4 WARMUP_ITERS=4 SAMPLE_ITERS=32 SAMPLES=3 ./scripts/bench-crc
```

Check `backend_selected` in every result. An `auto` run reporting `scalar` did
not select an accelerated backend, regardless of the CPU model advertised by
the host. The EC encode rate is normalized to input data bytes; reconstruction
is normalized to the recovered shard bytes. Compare like-for-like shard
layouts.

The large-buffer benchmark primarily describes aligned streaming work. Small,
fragmented, or unaligned inputs can have different acceleration cutoffs. The
CRC32 and CRC32C example supports a small-buffer sweep when that boundary is
important:

```bash
cargo run --offline -p checksum --example crc_bench --release \
  --features bench-select -- --algorithm crc32c --sweep-small
```

## Cryptography and peer TLS

Run the default ring build with:

```bash
./scripts/bench-crypto
```

Run the compile-time OpenSSL alternative with:

```bash
./scripts/bench-crypto --openssl
```

The default invocation of [`bench-crypto`](../scripts/bench-crypto) uses the
ring-based provider for cryptographic primitives and TLS, with the project's
Rust MD5 compatibility implementation. The OpenSSL invocation disables default
features and selects the same dynamically linked `openssl` provider used by a
production OpenSSL build for both primitives and TLS. OpenSSL 3.0 or later is
required. Provider selection is a build-time choice; there is no runtime
environment-variable override. See the [configuration
guide](configuration.md#cryptography-provider) for production build commands.

The workload can be adjusted with:

| Variable | Default | Meaning |
|---|---:|---|
| `SIZE_MIB` | `8` | Payload size for SHA-256 and established TLS records |
| `WARMUP_ITERS` | `4` | Untimed bulk-operation iterations |
| `SAMPLE_ITERS` | `32` | Timed bulk operations in each sample |
| `SIGV4_WARMUP_ITERS` | `10000` | Untimed SigV4 verification operations |
| `SIGV4_SAMPLE_ITERS` | `100000` | Timed SigV4 verifications in each sample |
| `SAMPLES` | `5` | Number of samples |

The summary fields measure distinct parts of the crypto surface:

| Field | What it measures |
|---|---|
| `summary_sigv4_payload_gib_s` | SHA-256 hashing of an S3 request payload |
| `summary_sigv4_verify_requests_s` | Complete verification of a representative signed request |
| `summary_tls13_seal_gib_s` | Encrypting application data through an established peer TLS connection |
| `summary_tls13_open_gib_s` | Decrypting application data through an established peer TLS connection |
| `summary_tls13_peer_limit_gib_s` | The lower of seal and open throughput |

The TLS measurements use the TLS 1.3 profile and ALPN used by internal storage
RPC, but operate in memory. They exclude the handshake, sockets, network
latency, framing outside rustls, and simultaneous bidirectional traffic. The
benchmark also does not individually measure every provider primitive, such as
MD5, HKDF, or random-number generation.

When comparing ring with OpenSSL, verify the printed `crypto_provider` and
`tls_crypto_provider` rather than inferring the provider from the command. For
OpenSSL results, also record `openssl version -a`: distribution patches,
compile options, and detected CPU capabilities can materially affect results.
`openssl speed` is a useful diagnostic for the underlying primitives, but it
does not replace `bench-crypto`, which exercises Argmin's actual SigV4 and
rustls paths.

## Architecture notes

### amd64

Checksum runtime selection can use PCLMULQDQ, VPCLMULQDQ, and AVX-512 variants;
EC can select AVX2 or AVX-512. Virtual-machine CPU models commonly hide some of
these features. Use `backend_selected` to distinguish a real accelerated run
from a scalar fallback. Benchmark ring and OpenSSL on the intended deployment
CPU rather than assuming that either provider is universally faster.

### arm64

Checksum runtime selection can use PMULL and, where available, the SHA3
extension; CRC32 and CRC32C also have an accelerated folding backend. EC uses
NEON. Current benchmarks do not use SVE or SVE2, so their presence alone does
not change the selected backend.

### riscv64

CRC acceleration uses the scalar `Zbc` carry-less-multiply extension and is
reported as `riscv64-zbc`; it does not require the vector extension. EC uses
RVV and is reported as `riscv64-rvv`. The current RVV backend requires a vector
length of at least 256 bits and, on Linux, vector state enabled for the calling
thread. A CPU advertising vectors can therefore still report `scalar` if the
kernel or thread policy does not permit their use.

The default ring provider currently lacks comparable RISC-V cryptographic
acceleration. OpenSSL is therefore the recommended production provider on
RISC-V, provided the installed OpenSSL build detects and uses the machine's
crypto and vector extensions. Confirm this with both `openssl version -a` and
the provider/backend fields printed by `bench-crypto`; do not assume that every
distribution OpenSSL build has the same RISC-V assembly support.

Large aligned CRC buffers and EC shards are representative of Argmin's main
data path, but the benchmark does not describe small request metadata,
fragmented buffers, memory bandwidth shared with other harts, or end-to-end
storage throughput. Measure the complete server workload as a separate step
when sizing a deployment.
