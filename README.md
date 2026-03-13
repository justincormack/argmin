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
| `ARGMIN_LISTEN_ADDR` | `127.0.0.1:9000` | Listen address |
| `ARGMIN_DATA_DIR` | `./data` | Data directory |
| `ARGMIN_PG_COUNT` | `16` | Number of placement groups |
| `ARGMIN_EC_K` | `4` | Erasure coding data shards |
| `ARGMIN_EC_M` | `2` | Erasure coding parity shards |
| `ARGMIN_REGION` | `us-east-1` | AWS region for auth |
| `ARGMIN_WORKERS` | `4` | Number of frontend workers |
| `ARGMIN_MAX_CONNECTIONS` | `512` | Max concurrent TCP connections |
| `ARGMIN_MAX_INFLIGHT_REQUESTS` | `32` | Max concurrent in-flight requests |

Start the server:

```bash
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
  ./target/release/argmin-s3
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
