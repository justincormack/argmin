# argmin

S3-compatible object storage written in Rust.

Current status: not yet suited for production use, fine for local use cases like CI. There is
still work to do to get this production ready, see [plans](plans/] for details.

AI notice: this code was written with AI, with detailed care, attention and supervision.

Development has focused on correctness, security and data safety over raw performance. 

## Prerequisites

- Rust toolchain (2021 edition)
- Unix/Linux runtime platform
- amd64 or aarch64 architecture

## Build

```bash
cargo build -p argmin-s3 --release
```

This builds the production server binary without pulling in the full workspace
test harness dependency set. The binary is at `target/release/argmin-s3`.

Do not use `--all-features` for production artifacts. The normal production
release build uses the default feature set. Optional features are diagnostic:

| Feature | Production use |
|---|---|
| `deep-tracing` | Diagnostic tracing only; not part of the normal production build |
| `local-debug-endpoints` | UAT/debug-only local diagnostics; release builds with this feature are rejected |

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
| `ARGMIN_PROCESS_ROLE` | `all-in-one` | Process topology: `all-in-one`, `frontend`, `storage-node`, `combined`, or `control-plane`; standalone versus replicated deployment is configured separately |
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
