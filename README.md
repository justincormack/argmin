<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# argmin

S3-compatible multihost object storage written in Rust.

Current status: not yet suited for production use, fine for local use cases like CI. There is
still work to do to get this production ready.

Single (currently 34MB) binary with minimal dependencies.

AI notice: this code was written with AI, with detailed care, attention and supervision.

Development has focused on correctness, security and data safety over raw performance. 

What is needed to be production ready
- Implement version upgrades and backwards compatibility for format changes.
- Production observability.
- Further failure case testing.
- Operational tooling.
- Operational recipes eg for Kubernetes.

Currently working on distributed failure testing to improve robustness and correctness.

## Prerequisites

- Rust toolchain (2021 edition), 1.95.0 or later
- Linux/Unix runtime platform
- amd64, aarch64 or riscv64 architecture for accelerated crc and EC

## Build

```bash
cargo build -p argmin-s3 --release
```

This builds the production server binary without pulling in the full workspace
test harness dependency set. The binary is at `target/release/argmin-s3`.

Do not use `--all-features` for production artifacts. The normal production
release build uses the default feature set. There is a single alternative
production feature, `openssl`, which dynamically links every cryptographic
primitive and TLS connection against the system OpenSSL library rather than
the default ring-based implementation. This alternative requires OpenSSL 3.0
or later:

```bash
cargo build -p argmin-s3 --release --no-default-features --features openssl
```

The `--no-default-features` flag prevents the default ring implementation from
also being linked into the OpenSSL binary. OpenSSL is the recommended provider
on RISC-V because ring does not currently provide comparable acceleration
there.

## Run

`argmin-s3` must run as a dedicated non-root user. The binary exits at startup
if its effective uid is `0`; configure the service manager or container image
to set an ordinary service user before launching the process.

Configuration is documented in the
[configuration guide](guides/configuration.md). It covers the environment-only
standalone and split-process paths, versioned TOML cluster manifests,
validation and initialization commands, internal TLS and authentication, and
durable identity constraints.

## Usage with AWS CLI

```bash
# Configure credentials
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=useasecuresecretkey
export AWS_DEFAULT_REGION=us-east-1

# Create a bucket
aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket my-bucket

# Upload a file
echo "Hello, argmin!" > /tmp/hello.txt
aws --endpoint-url http://127.0.0.1:9000 s3 cp /tmp/hello.txt s3://my-bucket/hello.txt

# List objects
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://my-bucket/

# Download the file
aws --endpoint-url http://127.0.0.1:9000 s3 cp s3://my-bucket/hello.txt /tmp/downloaded.txt
cat /tmp/downloaded.txt

# Head object
aws --endpoint-url http://127.0.0.1:9000 s3api head-object --bucket my-bucket --key hello.txt

# Delete object and bucket
aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://my-bucket/hello.txt
aws --endpoint-url http://127.0.0.1:9000 s3api delete-bucket --bucket my-bucket
```

All requests must use `--endpoint-url`. The AWS CLI uses path-style addressing
by default for custom endpoints.

## AWS S3 compatibility

In general there is a very high degree of compatibility to S3, with a very comprehensive test suite.

See [AWS compatibility guide](guides/aws-compatibility.md) for details of known incompatibilities.

Key compatibility notes
- Implements standard S3 buckets, not directory buckets and other types.
- Does not yet support SSE-KMS, only SSE-S3 and SSE-C as does not have key management yet.
- Minimal user management and support for dynamic credentials, only early stage work on this.
- Bucket policies based on user details, tags etc are also limited due to this.

## Operations

Currently operational observability and tooling remains weak. Functions such as adding storage are not
yet implemented, hence the "do not use in production" notice.

## Performance

Focused checksum, erasure-coding, SigV4, and peer TLS benchmarks are documented
in the [performance guide](guides/performance.md). The guide covers comparable
benchmark environments, ring and OpenSSL builds, backend-selection checks, and
architecture-specific notes for amd64, arm64, and riscv64.

## Testing

```bash
cargo nextest run
```

To run `s3-tests`, the S3 oracle test against AWS:

```bash
./scripts/aws-tests
```

To run `s3-tests` as a UAT acceptance suite against the standalone
`argmin-s3` binary:

```bash
./scripts/uat-s3-tests
```

To run the same suite against an already-built binary:

```bash
./scripts/uat-s3-tests --binary ./target/debug/argmin-s3
```

Full AWS environment setup, including the committed IAM policy, required
account-level S3 Block Public Access settings, the separate HTTP-only
`s3-http-tests` crate, the local-only `s3-local-tests` crate, and local
deep-tracing instructions are documented in
[`guides/testing.md`](guides/testing.md).

For contributing changes, see [CONTRIBUTING.md](CONTRIBUTING.md).
