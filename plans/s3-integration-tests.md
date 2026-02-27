# Rust S3 Integration Test Framework

## Context

The Ceph s3-tests (Python/boto3) have been useful for S3 compatibility validation but have
significant drawbacks: external to the codebase, fiddly virtualenv setup, can't run via
`cargo test`, poorly maintained, and we can't modify tests when our semantics differ.

We need to port these tests into the codebase as Rust integration tests that:
1. Replicate all ~800 Ceph tests (test_s3.py + test_headers.py) — these find real edge cases
2. Use an **independent S3 client** (not our own auth code) so tests validate the real wire protocol
3. Can run against **external endpoints** (e.g. real AWS S3) to validate test correctness
4. Run via `cargo test`

## Client Choice: aws-sdk-s3

Use the official AWS SDK for Rust as the test client. This gives completely independent
validation — AWS's own SigV4 implementation, XML parsing, and response handling. If our
server produces wrong output, the AWS SDK will reject it.

Requires tokio (async), but this is test-only — no runtime impact on the server.

```toml
# Test-only dependencies
aws-sdk-s3 = "1"
aws-config = "1"
aws-credential-types = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## External Endpoint Support

Tests detect whether to start a local server or connect to an external endpoint via
environment variables:

```bash
# Run against local server (default — starts TestServer automatically)
cargo test -p s3-tests

# Run against real AWS S3
S3_TEST_ENDPOINT=https://s3.us-east-1.amazonaws.com \
S3_TEST_ACCESS_KEY=AKIA... \
S3_TEST_SECRET_KEY=... \
S3_TEST_REGION=us-east-1 \
cargo test -p s3-tests

# Run against another S3-compatible server
S3_TEST_ENDPOINT=http://localhost:9000 \
S3_TEST_ACCESS_KEY=testkey \
S3_TEST_SECRET_KEY=testsecret \
cargo test -p s3-tests
```

The harness abstracts this: `TestContext::setup()` either starts a local server or
connects to the external endpoint, returning the same `aws_sdk_s3::Client` either way.

## Crate Structure

```
crates/s3-tests/
├── Cargo.toml
├── src/
│   ├── lib.rs              # TestContext, re-exports
│   ├── server.rs           # TestServer: local server on random port
│   └── helpers.rs          # Bucket name generation, common assertions
└── tests/
    ├── bucket_list.rs      # GROUP 1: 79 tests — ListObjects V1/V2
    ├── bucket_crud.rs      # GROUP 2: 19 tests — Bucket create/delete/head
    ├── object_delete.rs    # GROUP 3: 12 tests — Single & bulk delete
    ├── object_crud.rs      # GROUP 4: 24 tests — Read/write/metadata
    ├── post_object.rs      # GROUP 5: 44 tests — Form-based POST upload
    ├── conditional.rs      # GROUP 6: 28 tests — If-Match/If-None-Match/etc.
    ├── presigned.rs        # GROUP 7: 13 tests — Presigned URLs & expiry
    ├── bucket_naming.rs    # GROUP 8: 23 tests — DNS compliance, special names
    ├── acl_bucket.rs       # GROUP 9: 32 tests — Bucket ACLs
    ├── acl_object.rs       # GROUP 10: 23 tests — Object ACLs
    ├── acl_access.rs       # GROUP 11: 12 tests — Access control combinations
    ├── bucket_anon.rs      # GROUP 12: 10 tests — Anonymous/public listing
    ├── copy_object.rs      # GROUP 13: 19 tests — Copy operations
    ├── multipart.rs        # GROUP 14: 39 tests — Multipart upload
    ├── cors.rs             # GROUP 15: 14 tests — CORS
    ├── tagging.rs          # GROUP 16: 24 tests — Object/bucket tags
    ├── range_requests.rs   # GROUP 17:  6 tests — Byte range GET
    ├── versioning.rs       # GROUP 18: 24 tests — Versioning
    ├── lifecycle.rs        # GROUP 19: 93 tests — Lifecycle policies
    ├── encryption_sse_c.rs # GROUP 20: 20 tests — SSE-C
    ├── encryption_kms.rs   # GROUP 21: 14 tests — SSE-KMS
    ├── encryption_s3.rs    # GROUP 22: 30 tests — SSE-S3 / default encryption
    ├── encryption_kms_default.rs # GROUP 23: 8 tests — KMS default
    ├── bucket_policy.rs    # GROUP 24: 26 tests — Bucket policies
    ├── policy_encryption.rs# GROUP 25: 12 tests — Encryption enforcement
    ├── object_lock.rs      # GROUPS 26-27: 39 tests — Object lock & retention
    ├── public_access.rs    # GROUP 28: 20 tests — Policy status & public block
    ├── checksums.rs        # GROUP 29: 13 tests — SHA256, CRC checksums
    ├── object_attributes.rs# GROUP 30: 8 tests — GetObjectAttributes
    ├── logging.rs          # GROUP 31: 92 tests — Bucket logging
    ├── atomic.rs           # GROUP 32: 14 tests — Atomic read/write
    ├── ownership.rs        # GROUP 33: 8 tests — Object ownership controls
    └── headers.rs          # test_headers.py: 48 tests — Header validation
```

**Not ported** (separate services, not S3 API): test_iam.py (101), test_sts.py (36),
test_sns.py (4), test_s3select.py (34)

**Total: ~799 tests** across 34 test files matching the 33 groups from test_s3.py + headers.

## TestContext (Harness)

```rust
pub struct TestContext {
    client: aws_sdk_s3::Client,
    server: Option<TestServer>,  // None when using external endpoint
}

impl TestContext {
    /// Set up test context — either local server or external endpoint.
    pub async fn setup() -> Self { ... }

    /// The S3 client.
    pub fn client(&self) -> &aws_sdk_s3::Client { &self.client }
}
```

When `S3_TEST_ENDPOINT` is set, connects to external endpoint with provided credentials.
Otherwise, starts a `TestServer` on a random port with well-known test credentials.

Each test file uses `OnceLock<TestContext>` for a shared server per binary:
```rust
use std::sync::OnceLock;
use s3_tests::{TestContext, unique_bucket};

fn ctx() -> &'static TestContext {
    static CTX: OnceLock<TestContext> = OnceLock::new();
    CTX.get_or_init(|| tokio::runtime::Runtime::new().unwrap()
        .block_on(TestContext::setup()))
}

#[tokio::test]
async fn test_bucket_list_empty() {
    let client = ctx().client();
    let bucket = unique_bucket();
    client.create_bucket().bucket(&bucket).send().await.unwrap();
    let resp = client.list_objects_v2().bucket(&bucket).send().await.unwrap();
    assert_eq!(resp.key_count(), Some(0));
}
```

## TestServer (Local Server)

Same as main.rs: programmatically assembles Coordinator + HttpFrontend + tiny_http on port 0
in a background thread, with temp directory storage and in-memory bucket DB. Cleaned up on
drop.

Key: `TestServer` is sync (std::thread), tests are async (tokio). The server runs in its
own thread; the async test client talks to it over HTTP. No conflict.

## Test Naming Convention

Each Rust test matches the original Python test name exactly:
- Python: `def test_bucket_list_empty():`
- Rust: `async fn test_bucket_list_empty()`

This allows direct cross-referencing between the Python and Rust versions. Tests that
we've verified pass against AWS get a comment noting that.

## Unimplemented Feature Handling

Tests for features we haven't implemented yet should exist but be marked:

```rust
#[tokio::test]
#[ignore = "not implemented: lifecycle expiration"]
async fn test_lifecycle_expiration() { ... }
```

This way:
- `cargo test -p s3-tests` skips them by default
- `cargo test -p s3-tests -- --ignored` runs them to check progress
- The test count tracks our implementation completeness
- Tests are already written when we implement the feature

## Helpers (`src/helpers.rs`)

```rust
/// Generate a unique bucket name per test.
pub fn unique_bucket() -> String { ... }  // atomic counter

/// Assert an S3 error matches expected code.
pub async fn assert_s3_error(result: Result<T, SdkError<E>>, code: &str) { ... }

/// Create bucket + populate with N objects. Returns (bucket_name, keys).
pub async fn create_objects(client: &Client, n: usize) -> (String, Vec<String>) { ... }

/// Create bucket with hierarchical keys for listing tests.
pub async fn create_hierarchical(client: &Client) -> String { ... }
```

Some Ceph tests (presigned URLs, raw header inspection, anonymous requests, POST Object)
need raw HTTP access beyond what the SDK provides. For these, use `ureq` alongside the SDK:
- `ureq` for: anonymous requests, raw header checks, POST multipart form, presigned URL
  fetch, malformed requests
- `aws-sdk-s3` for: everything else (the vast majority)

## Dependencies

```toml
[dependencies]
# Server (for local TestServer)
server = { path = "../server" }
auth = { path = "../auth" }
ec = { path = "../ec" }
storage = { path = "../storage" }

# AWS SDK (independent S3 client for validation)
aws-sdk-s3 = "1"
aws-config = "1"
aws-credential-types = "1"

# Async runtime (required by AWS SDK)
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

# For raw HTTP requests (presigned URLs, POST, anonymous, header tests)
ureq = "3"

# Test infrastructure
tempfile = "3"
```

## Implementation Order

### Phase 1: Harness + Core (~3 files, validates the framework)
1. `src/lib.rs`, `src/server.rs`, `src/helpers.rs` — TestContext, TestServer
2. `tests/bucket_crud.rs` — 19 tests (bucket create/delete/head)
3. `tests/object_crud.rs` — 24 tests (put/get/head/delete/metadata)

### Phase 2: Listing + Bulk (~3 files, heavily exercised paths)
4. `tests/bucket_list.rs` — 79 tests (ListObjects V1/V2)
5. `tests/object_delete.rs` — 12 tests (single + bulk delete)
6. `tests/bucket_naming.rs` — 23 tests (DNS compliance)

### Phase 3: Advanced Operations (~5 files)
7. `tests/copy_object.rs` — 19 tests
8. `tests/range_requests.rs` — 6 tests
9. `tests/versioning.rs` — 24 tests
10. `tests/conditional.rs` — 28 tests
11. `tests/multipart.rs` — 39 tests

### Phase 4: Auth + POST + Headers (~3 files)
12. `tests/presigned.rs` — 13 tests (needs ureq for raw fetch)
13. `tests/post_object.rs` — 44 tests (needs ureq for multipart form)
14. `tests/headers.rs` — 48 tests (needs ureq for malformed requests)

### Phase 5: ACL + Policy (~5 files, many may be #[ignore])
15. `tests/acl_bucket.rs` — 32 tests
16. `tests/acl_object.rs` — 23 tests
17. `tests/acl_access.rs` — 12 tests
18. `tests/bucket_anon.rs` — 10 tests
19. `tests/bucket_policy.rs` — 26 tests

### Phase 6: Encryption + Remaining (~10 files, mostly #[ignore] initially)
20-34. Encryption, lifecycle, CORS, tagging, object lock, logging, atomic,
       checksums, object attributes, ownership, public access, policy encryption

## Verification

1. `cargo test -p s3-tests` — runs all non-ignored tests against local server
2. `cargo test -p s3-tests -- --ignored` — runs ignored tests (shows what's unimplemented)
3. `cargo test --workspace` — no regressions in other crates
4. `S3_TEST_ENDPOINT=... cargo test -p s3-tests` — validates tests match real AWS behavior
5. `cargo clippy --workspace`
