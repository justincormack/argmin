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
PAYLOAD_PROFILE=segment-aes-256-gcm-v1 EC_SHAPE=4+2 ./scripts/bench-payload-read
PAYLOAD_PROFILE=cobblestone-256-segment-v1 EC_SHAPE=4+2 ./scripts/bench-payload-read
```

Use the same commands with `--openssl` for the compile-time OpenSSL provider.
`PAYLOAD_PROFILE` and `EC_SHAPE` are required so a reference run cannot silently
benchmark the production default or an unintended topology. The supported local
shapes are `1+0` and `4+2`. Unlike the focused component benchmarks below, this
starts the local release S3 server and an AWS SDK client and exercises HTTP,
SigV4, authorization, object metadata, EC shard reads, physical CRC validation,
SSE-S3 decryption, response streaming, and SDK body consumption. It is intended
to compare payload-codec changes on the same machine, not to represent a
networked deployment's absolute throughput.

The following environment variables adjust the workload:

| Variable | Default | Meaning |
|---|---:|---|
| `PAYLOAD_PROFILE` | required | `segment-aes-256-gcm-v1` or `cobblestone-256-segment-v1` |
| `EC_SHAPE` | required | Local `1+0` or `4+2` placement |
| `OBJECT_MIB` | `16` | Size of the object; must exceed the 8 MiB segment size |
| `RANGE_BYTES` | `4096` | Size of the range selected from inside a full segment |
| `WARMUP_ITERS` | `4` | Untimed requests before each operation |
| `FULL_ITERS` | `16` | Full GET requests in each sample |
| `RANGE_ITERS` | `64` | Range GET requests in each sample |
| `SAMPLES` | `5` | Number of full and range samples |

Benchmark protocol `payload_read_v3` selects the requested writer profile on
every pooled frontend before any frontend is shared. After the PUT, a narrow
server-owned observation loads the current live object through the ordinary
admitted metadata-read route and requires its self-describing durable codec to
equal `PAYLOAD_PROFILE`. The output records both requested and persisted
profiles. `HeadObject` independently verifies the AWS-visible SSE-S3 AES256
mode; that header alone is not codec evidence because both profiles expose the
same S3 encryption mode. The benchmark also does not create a plaintext S3
object: an absent or deleted bucket encryption configuration has AWS's
automatic SSE-S3 default, so there is no production plaintext S3 writer. Each
sample reports elapsed throughput, per-request header latency, body time to
first byte, completion latency, and deltas for the `payload_read_*` work
counters. Body TTFB is measured from immediately before the SDK sends the
request until the first non-empty SDK
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

Reference runs require a clean worktree and runner-built current-source release
server and benchmark-client binaries, making the printed source revision a
complete identifier for the measured source. The runner refuses a dirty tree
by default. `ALLOW_DIRTY_BENCHMARK=1` permits exploratory development runs, and
debug or unverified prebuilt binary paths remain usable, but either condition
marks the result `reference_run=false`; those results must not be recorded as a
reference baseline. The output records binary provenance and digests, the CPU architecture and model, selected
CRC64, SHA-256, and EC runtime backends, compile-time crypto provider, explicit
local EC shape, and the authoritatively observed durable payload profile. The
selection seam is benchmark-only; ordinary test-server and production
constructors retain the production writer default.

### Multihost and degraded payload reads

Use the dedicated multihost mode to compare healthy and degraded reads over the production
TLS/TCP storage path. Run each profile from the same clean revision and host set:

```bash
./scripts/uat-multihost-raft \
  --hosts grey0,grey1,grey2,grey3 \
  --addresses 192.168.1.124,192.168.1.155,192.168.1.116,192.168.1.154 \
  --release \
  --pg-count 16 \
  --ec-data-shards 2 \
  --ec-parity-shards 1 \
  --payload-read-benchmark segment-aes-256-gcm-v1

./scripts/uat-multihost-raft \
  --hosts grey0,grey1,grey2,grey3 \
  --addresses 192.168.1.124,192.168.1.155,192.168.1.116,192.168.1.154 \
  --release \
  --pg-count 16 \
  --ec-data-shards 2 \
  --ec-parity-shards 1 \
  --payload-read-benchmark cobblestone-256-segment-v1
```

This mode requires at least one spare storage host beyond the `k+m` EC width. It builds the server
with the benchmark-only immutable writer selector; ordinary builds remain pinned to the production
default. It creates one object, then uses a storage-owned
diagnostic to verify the durable codec and resolve the exact metadata and data placement routes,
including the storage-computed shard-index-to-node mapping. The degraded case stops a systematic
shard's exact storage node, rather than treating acting-set order as shard order. It then waits for
the heartbeat lease and unavailable-placement grace to elapse, and for every current PG route to
exclude the unavailable node and become fully serving through the spare host. This exact condition
also excludes momentarily healthy maps between serialized per-PG replacements. Before sampling, it
uses a bounded retry while the finalized map is installed by storage nodes, then re-resolves the
object and proves that the persisted segment still places the selected shard on the stopped node.
The same object and byte
range therefore exercise retained-placement EC reconstruction after control-plane recovery, rather
than only the transient interval before route expiry. The selected 4 KiB range lies in the middle
of that systematic shard's plaintext span in the first full segment. The runner restarts the stopped
node and requires the route map and every service to recover before succeeding.

The workload can be reduced for exploratory validation with
`PAYLOAD_READ_BENCHMARK_OBJECT_MIB`, `PAYLOAD_READ_BENCHMARK_WARMUP_ITERS`,
`PAYLOAD_READ_BENCHMARK_FULL_ITERS`, `PAYLOAD_READ_BENCHMARK_RANGE_ITERS`, and
`PAYLOAD_READ_BENCHMARK_SAMPLES`. Reference runs require a clean worktree; setting
`ALLOW_DIRTY_BENCHMARK=1` permits a run but records `reference_run=false`. Supplying an arbitrary
prebuilt or non-release binary also forces that value to false. The output includes the source
revision, build provenance, binary digests, Rust/kernel/CPU identity, selected codec, exact
placement, and the healthy and degraded client measurements. External multihost clients report
`work_metrics_available=false`: the `payload_read_*` counters are process-local and cannot be
truthfully inferred from one distributed process. The clean local matrix owns exact work-counter
comparisons; this matrix owns network-visible throughput, latency, TTFB, and degraded-read cost.

### Clean local payload-codec comparison

The `payload_read_v3` local reference matrix was recorded on 2026-10-27 from clean source revision
`51e1aeb26ad2ec6a57584b9652b145670dcd2723` with the default workload settings. The environment
was Rust `1.98.1`, Linux `6.12.113+deb13-amd64`, an AMD Ryzen 7 255 with Radeon 780M Graphics,
Ring, the `x86_64-avx512-vpclmulqdq` CRC64 backend, the Ring SHA-256 backend, and the
`x86_64-avx512` EC backend. Every run independently observed that the durable codec matched the
requested profile and that the AWS-visible encryption mode was SSE-S3 AES256.

Each shape was measured twice in opposite profile order. The cells below show the median from the
legacy-then-Cobblestone pass followed by the median from the Cobblestone-then-legacy pass. The
sample range covers all ten samples across both passes.

| EC | Operation | Whole-segment median req/s (A / B) | Sample range | Cobblestone median req/s (A / B) | Sample range |
|---|---|---:|---:|---:|---:|
| 1+0 | Full GET | 112.407 / 110.267 | 109.553-117.953 | 176.534 / 183.901 | 170.214-186.995 |
| 1+0 | 4 KiB Range GET | 395.094 / 381.320 | 374.257-399.048 | 5,592.835 / 5,517.068 | 4,985.086-6,321.061 |
| 4+2 | Full GET | 96.556 / 104.870 | 93.796-109.465 | 164.862 / 165.418 | 162.875-167.822 |
| 4+2 | 4 KiB Range GET | 283.236 / 318.713 | 246.548-339.706 | 1,224.011 / 1,248.799 | 1,108.153-1,322.803 |

| EC | Operation | Whole-segment body TTFB p50 (A / B) | Cobblestone body TTFB p50 (A / B) | Whole-segment completion p50 (A / B) | Cobblestone completion p50 (A / B) |
|---|---|---:|---:|---:|---:|
| 1+0 | Full GET | 2.985 / 3.040 ms | 0.431 / 0.435 ms | 8.760 / 9.071 ms | 5.680 / 5.334 ms |
| 1+0 | 4 KiB Range GET | 2.524 / 2.594 ms | 0.158 / 0.166 ms | 2.525 / 2.595 ms | 0.158 / 0.166 ms |
| 4+2 | Full GET | 3.702 / 3.511 ms | 1.057 / 1.064 ms | 10.187 / 9.380 ms | 6.005 / 6.002 ms |
| 4+2 | 4 KiB Range GET | 3.053 / 3.031 ms | 0.761 / 0.755 ms | 3.054 / 3.033 ms | 0.762 / 0.756 ms |

The work counters were shape-independent and stable across both passes. Values below are per
request; neither profile reconstructed data or entered repair fallback.

| Profile | Operation | Logical bytes | Stored/shard bytes | Decrypted bytes | AEAD records | CRC bytes | Buffer checkouts |
|---|---|---:|---:|---:|---:|---:|---:|
| Whole-segment | Full GET | 16,777,216 | 16,777,248 | 16,777,216 | 2 | 33,554,464 | 2 |
| Whole-segment | 4 KiB Range GET | 4,096 | 8,388,624 | 8,388,608 | 1 | 8,388,624 | 1 |
| Cobblestone | Full GET | 16,777,216 | 16,793,744 | 16,777,216 | 1,026 | 33,570,960 | 0 |
| Cobblestone | 4 KiB Range GET | 4,096 | 16,456 | 16,384 | 1 | 0 | 0 |

Across the two orderings, Cobblestone increased full-read throughput by 57-67% on 1+0 and 58-71%
on 4+2. Full-read body TTFB fell by about 86% on 1+0 and 70-71% on 4+2, while completion p50 fell
by 35-41% and 36-41%, respectively. The 4 KiB range improved by 14.2-14.5x on 1+0 and 3.9-4.3x on
4+2, while reading roughly 16 KiB instead of a complete 8 MiB segment. These clean local results
remove the earlier sequential-throughput concern, but they do not replace the remaining multihost
and degraded-read measurements required before changing the production writer default.

### Whole-segment encrypted-read baseline

The `payload_read_v2` Phase 0 reference run was recorded on 2026-10-21 from clean source revision
`6062b37ac5ae652b7ad6c065b14fb804695d83ce` with the default benchmark settings:

- Rust `1.98.1`, Linux `6.12.107+deb13-amd64`, x86-64;
- AMD Ryzen 7 255 with Radeon 780M Graphics;
- Ring crypto provider and SHA-256 backend;
- `x86_64-avx512-vpclmulqdq` CRC64 backend and `x86_64-avx512` EC backend; and
- HTTP, 4+2 EC, verified AWS-visible SSE-S3 AES256.

| Operation | Response bytes | Median req/s | Worst req/s | Best req/s |
|---|---:|---:|---:|---:|
| Full GET | 16,777,216 | 135.833 | 130.725 | 139.345 |
| Range GET | 4,096 | 348.836 | 345.095 | 368.385 |

The five full-GET samples were 135.833, 130.725, 135.854, 139.345, and
134.148 requests/s. The five range samples were 348.836, 367.661, 368.385,
345.095, and 348.610 requests/s. The range began at byte 8,389,632, inside the
second 8 MiB segment.

| Operation | Headers p50/p95/p99 | Body TTFB p50/p95/p99 | Completion p50/p95/p99 |
|---|---:|---:|---:|
| Full GET | 0.522 / 0.646 / 0.763 ms | 3.288 / 3.678 / 4.534 ms | 7.320 / 8.143 / 8.831 ms |
| Range GET | 0.473 / 0.650 / 0.787 ms | 2.750 / 3.062 / 3.193 ms | 2.751 / 3.064 / 3.194 ms |

The 4 KiB range's first body bytes and completion are effectively simultaneous:
the current profile withholds the requested bytes until it has read, CRC-checked,
and authenticated the complete 8 MiB segment. The full GET exposes its first
authenticated segment after about 3.3 ms at p50 and completes both segments
after about 7.3 ms. These are the direct first-byte baselines that the chunked
reader is expected to improve.

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

The completed Cobblestone reader changes the expected work shape when that
profile is selected. A healthy full GET can expose its first 16 KiB plaintext
record after reading only the fixed header and first encrypted record; it does
not check out or fill the 8 MiB segment buffer. It calculates each systematic
shard CRC incrementally from the same ciphertext ranges and reads only the
small final EC-padding suffix needed to finish the physical witness. The final
plaintext-bearing record is retained until that check completes. Phase 5 must
record a clean-worktree benchmark after the production writer decision and
compare its TTFB, buffer, CRC, stored-byte, and completion counters against the
historical table above; the implementation canary is not a substitute for that
reference run.

For historical context, benchmark protocol `payload_read_v1` on clean revision
`964de9ca4e90f5bb94ad9929a0c3a3933dce6082` reported 109.335 full GET requests/s
and 342.586 range GET requests/s. It measured only aggregate completion
throughput and used SDK whole-body collection, so it is not the comparison
baseline for the chunked reader.

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
