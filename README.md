# argmin2

S3-compatible object storage written in Rust. Single-node, synchronous IO,
erasure-coded with ISA-L.

This is the v1-minimal implementation: path-style addressing, AWS SigV4
authentication, per-PG SQLite metadata, and CRC64-NVME integrity checking.

## Prerequisites

- **Rust** toolchain (2021 edition)
- **ISA-L** development library
- **pkg-config**

Install ISA-L:

```bash
# Debian/Ubuntu
apt install libisal-dev

# macOS
brew install isa-l
```

## Build

```bash
cargo build --release
```

The binary is at `target/release/argmin-s3`.

## Run

The server is configured via environment variables:

| Variable | Default | Description |
|---|---|---|
| `ARGMIN_ACCESS_KEY_ID` | *(required)* | S3 access key |
| `ARGMIN_SECRET_ACCESS_KEY` | *(required)* | S3 secret key |
| `ARGMIN_SSE_C_VALIDATOR_KEY` | *(unset)* | Base64-encoded 32-byte SSE-C validator secret; required to use SSE-C |
| `ARGMIN_LISTEN_ADDR` | `127.0.0.1:9000` | Listen address |
| `ARGMIN_TLS_CERT_PATH` | *(unset)* | PEM certificate path for direct HTTPS |
| `ARGMIN_TLS_KEY_PATH` | *(unset)* | PEM private key path for direct HTTPS |
| `ARGMIN_DATA_DIR` | `./data` | Data directory |
| `ARGMIN_PG_COUNT` | `16` | Number of placement groups |
| `ARGMIN_EC_K` | `4` | Erasure coding data shards |
| `ARGMIN_EC_M` | `2` | Erasure coding parity shards |
| `ARGMIN_REGION` | `us-east-1` | AWS region for auth |
| `ARGMIN_WORKERS` | `4` | Number of frontend workers |
| `ARGMIN_MAX_CONNECTIONS` | `512` | Max concurrent TCP connections |
| `ARGMIN_MAX_INFLIGHT_REQUESTS` | `32` | Max concurrent in-flight requests |
| `ARGMIN_STREAM_READ_CHUNK_SIZE` | `8388608` | HTTP streaming read chunk size |
| `ARGMIN_TRACE` | `0` | Enable local tracing when truthy |
| `ARGMIN_TRACE_FILTER` | *(all targets)* | Comma-separated trace target filter |
| `ARGMIN_TRACE_FILE` | *(stderr)* | Write trace lines to a file instead of stderr |

If both `ARGMIN_TLS_CERT_PATH` and `ARGMIN_TLS_KEY_PATH` are set, the server
accepts direct HTTPS on `ARGMIN_LISTEN_ADDR`. Both variables must be set
together.

`ARGMIN_SSE_C_VALIDATOR_KEY` has no built-in default. If it is unset, SSE-C
requests are rejected. This is intentional; we do not want a shared implicit
validator secret. For now it is configured directly via environment variable.
Once secret storage exists for the broader encryption work (`SSE-S3` / KMS),
this should move there as well.

If you set `ARGMIN_SSE_C_VALIDATOR_KEY`, keep it stable for the lifetime of
existing SSE-C objects. It is a base64-encoded 32-byte secret.

Start the server:

```bash
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
  ./target/release/argmin-s3
```

Enable tracing:

```bash
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
ARGMIN_TRACE=1 \
ARGMIN_TRACE_FILTER=server_http,auth,server_core,storage,ec \
ARGMIN_TRACE_FILE=/tmp/argmin.trace \
  ./target/release/argmin-s3
```

Start the server with direct HTTPS:

```bash
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
ARGMIN_TLS_CERT_PATH=/path/to/cert.pem \
ARGMIN_TLS_KEY_PATH=/path/to/key.pem \
  ./target/release/argmin-s3
```

If you want to enable SSE-C, set a stable validator secret as well:

```bash
ARGMIN_SSE_C_VALIDATOR_KEY='<base64-encoded-32-byte-secret>'
```

## Usage with AWS CLI

```bash
# Configure credentials
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=useasecuresecretkey
export AWS_DEFAULT_REGION=us-east-1

# Create a bucket
aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket my-bucket

# Upload a file
echo "Hello, argmin2!" > /tmp/hello.txt
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

## Supported S3 operations

| Operation | Method | Path |
|---|---|---|
| ListBuckets | `GET` | `/` |
| CreateBucket | `PUT` | `/<bucket>` |
| DeleteBucket | `DELETE` | `/<bucket>` |
| HeadBucket | `HEAD` | `/<bucket>` |
| ListObjectsV2 | `GET` | `/<bucket>` |
| PutObject | `PUT` | `/<bucket>/<key>` |
| GetObject | `GET` | `/<bucket>/<key>` |
| DeleteObject | `DELETE` | `/<bucket>/<key>` |
| HeadObject | `HEAD` | `/<bucket>/<key>` |

## Architecture

- **Erasure coding**: (4,2) Reed-Solomon via ISA-L
- **Storage**: per-placement-group SQLite metadata + shard files
- **Integrity**: CRC64-NVME on every read; mismatches quarantine the shard
- **Auth**: AWS Signature Version 4
- **ETag**: CRC64-NVME (not MD5)

## Limits

| Limit | Value |
|---|---|
| Max object size | 256 MB |
| Bucket name length | 3-63 characters |
| Object key length | 1-1024 bytes |
| Addressing | Path-style only |

Bucket names must be lowercase letters, digits, hyphens, or periods. No leading
or trailing hyphens, no consecutive periods, and not formatted as an IP address.

## Tests

```bash
cargo test --workspace
```

For local `s3-tests`, the embedded test server also supports trace helpers:

| Variable | Description |
|---|---|
| `S3_TEST_TRACE` | Enables tracing for the local embedded test server |
| `S3_TEST_TRACE_FILTER` | Sets `ARGMIN_TRACE_FILTER` for the local embedded test server |
| `S3_TEST_TRACE_FILE` | Sets `ARGMIN_TRACE_FILE` directly |
| `S3_TEST_TRACE_DIR` | Writes one trace file per test binary as `<dir>/<binary>.trace` |

Example:

```bash
S3_TEST_TRACE=1 \
S3_TEST_TRACE_FILTER=server_http,auth,server_core,storage,ec \
S3_TEST_TRACE_DIR=/tmp/s3-test-traces \
cargo test -p s3-tests --no-fail-fast
```
