# argmin

S3-compatible object storage written in Rust. Single-process local cluster,
synchronous IO, erasure-coded with native Rust backends.

This is the v1-minimal implementation: path-style addressing, AWS SigV4
authentication, per-PG SQLite metadata, and CRC64-NVME integrity checking.

## Prerequisites

- **Rust** toolchain (2021 edition)
- **Unix/Linux** runtime platform. The server uses Unix domain sockets, POSIX
  file permissions, and Unix process ownership checks. Windows support would be
  a separate port with dedicated CI coverage.

## Build

```bash
cargo build -p argmin-s3 --release
```

This builds the production server binary without pulling in the full workspace
test harness dependency set. The binary is at `target/release/argmin-s3`.

## Run

`argmin-s3` must run as a dedicated non-root user. The binary exits at startup
if its effective uid is `0`; configure the service manager or container image
to set an ordinary service user before launching the process.

The server is configured via environment variables:

| Variable | Default | Description |
|---|---|---|
| `ARGMIN_ACCOUNT_ID` | *(required)* | 12-digit bucket-owner account ID used for ownership and `x-amz-expected-bucket-owner` checks |
| `ARGMIN_ACCESS_KEY_ID` | *(required)* | S3 access key |
| `ARGMIN_SECRET_ACCESS_KEY` | *(required)* | S3 secret key |
| `ARGMIN_HOST_ID` | *(random at startup)* | Stable S3 `HostId` / `x-amz-id-2` value to emit in responses; set this explicitly if you want it to persist across restarts |
| `ARGMIN_SSE_S3_WRAPPING_KEY` | *(required)* | Base64-encoded 32-byte SSE-S3 wrapping key; required because new buckets default to `AES256` / SSE-S3 |
| `ARGMIN_SSE_C_VALIDATOR_KEY` | *(unset)* | Base64-encoded 32-byte SSE-C validator secret; required to use SSE-C |
| `ARGMIN_LISTEN_ADDR` | `127.0.0.1:9000` | Listen address |
| `ARGMIN_TLS_CERT_PATH` | *(unset)* | PEM certificate path for direct HTTPS |
| `ARGMIN_TLS_KEY_PATH` | *(unset)* | PEM private key path for direct HTTPS |
| `ARGMIN_DATA_DIR` | `./data` | Data directory |
| `ARGMIN_PG_COUNT` | `16` | Number of placement groups |
| `ARGMIN_PROCESS_ROLE` | `legacy-local` | Process role: `legacy-local` or Phase 10.3 `storage-node`; `frontend` and `combined` are parsed but intentionally unsupported until remote storage routing is wired |
| `ARGMIN_STORAGE_NODE_ID` | *(required for `storage-node`)* | Storage-node identity to serve |
| `ARGMIN_STORAGE_NODE_DATA_DIR` | `ARGMIN_DATA_DIR/node-NNNN` | Data directory for the storage-node identity |
| `ARGMIN_STORAGE_NODE_SOCKET_PATH` | *(required for `storage-node`)* | Absolute Unix socket path for the storage-node process |
| `ARGMIN_STORAGE_CLUSTER_EPOCH` | `1` | Static storage-node topology epoch advertised over the Unix socket |
| `ARGMIN_STORAGE_PG_IDS` | all PGs in `0..ARGMIN_PG_COUNT` | Comma-separated PG ids opened by this storage-node process |
| `ARGMIN_EC_K` | `4` | Erasure coding data shards |
| `ARGMIN_EC_M` | `2` | Erasure coding parity shards |
| `ARGMIN_LOCAL_NODE_COUNT` | `ARGMIN_EC_K + ARGMIN_EC_M` (`6` with defaults) | In-progress local multihost harness node count |
| `ARGMIN_REGION` | `us-east-1` | AWS region for auth |
| `ARGMIN_WORKERS` | `4` | Number of frontend workers |
| `ARGMIN_MAX_CONNECTIONS` | `512` | Max concurrent TCP connections |
| `ARGMIN_MAX_INFLIGHT_REQUESTS` | `32` | Max concurrent in-flight requests |
| `ARGMIN_STREAM_READ_CHUNK_SIZE` | `8388608` | HTTP streaming read chunk size |

The following variables are only for user-acceptance testing the standalone
binary with `s3-tests`. They are not a production account-management API.

| Variable | Description |
|---|---|
| `ARGMIN_UAT_ALT_ACCOUNT_ID` | Alternate 12-digit account ID for cross-account tests; must differ from `ARGMIN_ACCOUNT_ID` |
| `ARGMIN_UAT_ALT_ACCESS_KEY_ID` / `ARGMIN_UAT_ALT_SECRET_ACCESS_KEY` | Alternate-account owner/admin credential pair |
| `ARGMIN_UAT_SECOND_ACCESS_KEY_ID` / `ARGMIN_UAT_SECOND_SECRET_ACCESS_KEY` | Same-account constrained credential pair |
| `ARGMIN_UAT_OWNER_ROOT_ACCESS_KEY_ID` / `ARGMIN_UAT_OWNER_ROOT_SECRET_ACCESS_KEY` | Same-account root/admin credential pair |

If both `ARGMIN_TLS_CERT_PATH` and `ARGMIN_TLS_KEY_PATH` are set, the server
accepts direct HTTPS on `ARGMIN_LISTEN_ADDR`. Both variables must be set
together.

`ARGMIN_SSE_S3_WRAPPING_KEY` has no built-in default. It is required at
startup because fresh buckets now default to SSE-S3 (`AES256`), so ordinary
`PutObject` and multipart writes need a managed wrapping key from the start.
It must be a base64-encoded 32-byte secret and should be kept stable for the
lifetime of existing SSE-S3 objects.

`ARGMIN_SSE_C_VALIDATOR_KEY` has no built-in default. If it is unset, SSE-C
requests are rejected. This is intentional; we do not want a shared implicit
validator secret. For now it is configured directly via environment variable.
Once secret storage exists for the broader encryption work (`SSE-S3` / KMS),
this should move there as well.

`ARGMIN_HOST_ID` is optional. If it is unset, `argmin-s3` generates a random
host ID once at startup and reuses it for all responses from that process. If
you want `HostId` / `x-amz-id-2` to stay stable across restarts, set
`ARGMIN_HOST_ID` explicitly and keep it unchanged.

If you set `ARGMIN_SSE_C_VALIDATOR_KEY`, keep it stable for the lifetime of
existing SSE-C objects. It is a base64-encoded 32-byte secret.

For an existing data directory, treat these settings as stable:

- `ARGMIN_PG_COUNT` is part of placement. Changing it without migration will
  route buckets and objects to different PGs.
- `ARGMIN_LOCAL_NODE_COUNT` is part of placement and must be at least
  `ARGMIN_EC_K + ARGMIN_EC_M`.
- If you set `ARGMIN_HOST_ID`, keep it stable if you want `HostId` /
  `x-amz-id-2` to remain stable across restarts.
- `ARGMIN_SSE_S3_WRAPPING_KEY` must remain stable for existing SSE-S3 objects.
- `ARGMIN_SSE_C_VALIDATOR_KEY` must remain stable for existing SSE-C objects.
- `ARGMIN_EC_K` and `ARGMIN_EC_M` are stored per object, but reconfiguration is
  not currently a supported operational workflow, so they should also be
  treated as cluster-creation settings for now.

Example: generate a new wrapping key

```bash
openssl rand -base64 32
```

Start the server:

```bash
ARGMIN_ACCOUNT_ID=111122223333 \
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
ARGMIN_SSE_S3_WRAPPING_KEY='<base64-encoded-32-byte-secret>' \
  ./target/release/argmin-s3
```

Start the server with direct HTTPS:

```bash
ARGMIN_ACCOUNT_ID=111122223333 \
ARGMIN_ACCESS_KEY_ID=admin \
ARGMIN_SECRET_ACCESS_KEY=useasecuresecretkey \
ARGMIN_SSE_S3_WRAPPING_KEY='<base64-encoded-32-byte-secret>' \
ARGMIN_TLS_CERT_PATH=/path/to/cert.pem \
ARGMIN_TLS_KEY_PATH=/path/to/key.pem \
  ./target/release/argmin-s3
```

If you want to enable SSE-C, set a stable validator secret as well:

```bash
ARGMIN_SSE_C_VALIDATOR_KEY='<base64-encoded-32-byte-secret>'
```

## In-Progress Local Multihost Harness

`ARGMIN_LOCAL_NODE_COUNT` is part of the multihost transition work. The default
is derived from the configured EC shape, so the default `4+2` shape starts `6`
local storage nodes. Each node gets its own data directory below
`ARGMIN_DATA_DIR`, using stable numeric node IDs:

```text
ARGMIN_DATA_DIR/
  node-0000/
  node-0001/
  node-0002/
```

Local multihost mode requires enough local nodes to place the configured EC
shape on distinct nodes. `ARGMIN_LOCAL_NODE_COUNT` must be at least
`ARGMIN_EC_K + ARGMIN_EC_M`.

This mode is intended for local development and tests while the distributed
storage path is being built. It does not yet add RPC, internal auth, failure
detection, or distributed shard placement. Request routing still uses the
current metadata-primary/local-node forwarding behavior for this phase.

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

- **Erasure coding**: (4,2) Reed-Solomon via native Rust backends
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

To run `s3-tests` against an external endpoint such as AWS S3:

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

The UAT wrapper starts `argmin-s3` with a temporary data directory, repository
test TLS certificate, and the UAT-only credentials listed above, then runs
`s3-tests` against that process as an external endpoint.

External `s3-tests` runs now fail fast if the alternate credentials, account
IDs, or bucket prefix are missing. For AWS S3, the alternate credentials must
belong to a different AWS account with a different S3 canonical owner ID. A
second IAM user in the same AWS account is not sufficient.

When `S3_TEST_ENDPOINT` is set, `s3-tests` now defaults to a 30 second client
timeout and disables the AWS SDK stalled-stream watchdog to avoid false
positive throughput failures on slower remote endpoints.

Full AWS environment setup, including the committed IAM policy, required
account-level S3 Block Public Access settings, the separate HTTP-only
`s3-http-tests` crate, the local-only `s3-local-tests` crate, and local
deep-tracing instructions are documented in
[`guides/testing.md`](guides/testing.md).
