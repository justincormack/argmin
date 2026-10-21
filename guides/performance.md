<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Performance Guide

While some performance work has taken place, this has not been a high priority.
Much of the performance however is due to necessary work, such as writing disks,
fsync, and the erasure coding, encryption and checksum paths documented below.
The persistence barriers are correctness boundaries, not optional tuning knobs;
their normative contract is in the [durable-storage guide](durable-storage.md).

There are not yet optimisations for small objects, either on the read side
or batched fsync on the write side.

There is currently no support for prefix based partitioning, so write performance
is partly serialized per bucket.

The rest of this performance document mostly covers low level components such as
erasure coding that may need per platform optimisations, and may be limiting factors
on slower hardware,

## Benchmarks

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

## End-to-end payload reads

Run the current payload-read baseline with:

```bash
./scripts/bench-payload-read
```

Use `./scripts/bench-payload-read --openssl` for the compile-time OpenSSL
provider. Unlike the focused component benchmarks below, this starts the local
release S3 server and an AWS SDK client and exercises HTTP, SigV4,
authorization, object metadata, 4+2 EC shard reads, physical CRC validation,
SSE-S3 decryption, response streaming, and SDK body consumption. It is intended
to compare payload-codec changes on the same machine, not to represent a
networked deployment's absolute throughput.

The following environment variables adjust the workload:

| Variable | Default | Meaning |
|---|---:|---|
| `OBJECT_MIB` | `16` | Size of the object; must exceed the 8 MiB segment size |
| `RANGE_BYTES` | `4096` | Size of the range selected from inside a full segment |
| `WARMUP_ITERS` | `4` | Untimed requests before each operation |
| `FULL_ITERS` | `16` | Full GET requests in each sample |
| `RANGE_ITERS` | `64` | Range GET requests in each sample |
| `SAMPLES` | `5` | Number of full and range samples |

The benchmark verifies with `HeadObject` that its object uses the AWS-visible
SSE-S3 AES256 encryption mode. It deliberately does not infer the internal
payload codec from that response: a future chunked codec will expose the same
S3 encryption mode. It also does not create a plaintext S3 object: an absent or
deleted bucket encryption configuration has AWS's automatic SSE-S3 default, so
there is no production plaintext S3 writer. Each sample reports elapsed
throughput, per-request header latency, body time to first byte, completion
latency, and deltas for the `payload_read_*` work counters. Body TTFB is measured
from immediately before the SDK sends the request until the first non-empty SDK
body chunk becomes visible; it therefore measures payload availability rather
than merely receipt of HTTP response headers. Latencies use the nearest-rank
p50/p95/p99 definition. The work counters distinguish response bytes from
stored and shard bytes, EC reconstruction, decrypted bytes, AEAD records, CRC
work, buffer-pool activity, and repair fallback. The timed reader consumes SDK
`Bytes` chunks directly and sums their lengths, so it does not assemble or copy
the whole response in the benchmark client. One untimed request per case is
still collected and compared byte-for-byte with the expected payload as a
correctness canary.

The payload-codec comparison therefore has no plaintext benchmark axis. Compare
the current whole-segment AES-GCM codec and future encrypted codecs under the
same SSE-S3 or SSE-C key mode; former tag-0 bytes are immutable rejection
evidence, not a supported performance profile.

Reference runs require a clean worktree, making the printed source revision a
complete identifier for the measured source. The runner refuses a dirty tree
by default. `ALLOW_DIRTY_BENCHMARK=1` permits exploratory development runs, but
marks them `reference_run=false`; their results must not be recorded as a
reference baseline. The output records the CPU architecture and model, selected
CRC64, SHA-256, and EC runtime backends, and compile-time crypto provider.

### Whole-segment encrypted-read baseline

The throughput/work Phase 0 reference run was recorded on 2026-10-21 from clean source revision
`964de9ca4e90f5bb94ad9929a0c3a3933dce6082` with the default benchmark settings:

- Rust `1.98.1`, Linux `6.12.107+deb13-amd64`, x86-64;
- AMD Ryzen 7 255 with Radeon 780M Graphics;
- Ring crypto provider and SHA-256 backend;
- `x86_64-avx512-vpclmulqdq` CRC64 backend and `x86_64-avx512` EC backend; and
- HTTP, 4+2 EC, verified AWS-visible SSE-S3 AES256.

| Operation | Response bytes | Median req/s | Worst req/s | Best req/s |
|---|---:|---:|---:|---:|
| Full GET | 16,777,216 | 109.335 | 108.033 | 110.684 |
| Range GET | 4,096 | 342.586 | 337.801 | 356.570 |

The five full-GET samples were 108.033, 109.335, 109.616, 108.035, and
110.684 requests/s. The five range samples were 356.570, 353.193, 342.586,
337.961, and 337.801 requests/s. The range began at byte 8,389,632, inside the
second 8 MiB segment.

The cumulative counters were identical across all five samples. Dividing each
sample by its iteration count gives the actual work per request:

| Operation | Logical bytes | Stored/shard bytes | Decrypted bytes | AEAD records | CRC bytes | Buffer checkouts |
|---|---:|---:|---:|---:|---:|---:|
| Full GET | 16,777,216 | 16,777,248 | 16,777,216 | 2 | 33,554,464 | 2 |
| Range GET | 4,096 | 8,388,624 | 8,388,608 | 1 | 8,388,624 | 1 |

Neither case reconstructed bytes, allocated new pool backing buffers, or
entered repair fallback. The range row is the key Phase 0 result: returning
4 KiB currently reads and authenticates one complete 8 MiB segment. Future
codec measurements must retain these exact workload and environment fields and
compare both throughput and work counters, not throughput alone.

That revision used benchmark protocol `payload_read_v1`, which measured only
aggregate completion throughput. Protocol `payload_read_v2` adds header,
first-body-byte, and completion latency without adding timed whole-body
collection. Its clean-revision reference values must be recorded after the v2
harness itself is committed; dirty-tree exploratory values are not reference
evidence.

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
| `SIZE_MIB` | `8` | Segment/payload size for SSE, SHA-256, and established TLS records |
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
| `summary_sse_encrypt_gib_s` | SSE segment encryption through the selected AES-256-GCM provider |
| `summary_tls13_seal_gib_s` | Encrypting application data through an established peer TLS connection |
| `summary_tls13_open_gib_s` | Decrypting application data through an established peer TLS connection |
| `summary_tls13_peer_limit_gib_s` | The lower of seal and open throughput |

The TLS measurements use the TLS 1.3 profile and ALPN used by internal storage
RPC, but operate in memory. They exclude the handshake, sockets, network
latency, framing outside rustls, and simultaneous bidirectional traffic. The
SSE measurement mirrors the managed SSE-S3 per-segment write path, including
AES key setup and the allocation and copy into the encrypted segment buffer. It
excludes DEK generation and wrapping, checksums, erasure coding, and storage.
SSE-C uses the same segment operation with different fixed associated data. The
benchmark does not individually measure every provider primitive, such as MD5,
HKDF, or random-number generation.

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
