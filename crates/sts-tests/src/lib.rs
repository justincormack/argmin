// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

use s3_tests::{SignedRequestCredentials, TestServer};

/// Shared STS test context for one integration-test binary.
///
/// The same test source runs against the embedded server by default and an
/// external STS endpoint when `STS_TEST_ENDPOINT` is set.
pub static CTX: LazyLock<TestContext> =
    LazyLock::new(|| s3_tests::RT.block_on(TestContext::setup()));

pub struct TestContext {
    endpoint: String,
    access_key: String,
    secret_key: String,
    account_id: String,
    role_arn: String,
    role_name: String,
    denied_role_arn: String,
    region: String,
    tls_ca_pem: Option<Vec<u8>>,
    _server: Option<TestServer>,
}

impl TestContext {
    async fn setup() -> Self {
        if let Ok(endpoint) = std::env::var("STS_TEST_ENDPOINT") {
            assert!(
                endpoint.starts_with("https://"),
                "STS_TEST_ENDPOINT must use https://; got {endpoint}"
            );
            let tls_ca_pem = match std::env::var("STS_TEST_TLS_CA_CERT_PATH") {
                Ok(path) => Some(std::fs::read(&path).unwrap_or_else(|error| {
                    panic!("read STS_TEST_TLS_CA_CERT_PATH {path}: {error}");
                })),
                Err(std::env::VarError::NotPresent) => None,
                Err(error) => panic!("read STS_TEST_TLS_CA_CERT_PATH: {error}"),
            };
            return Self {
                endpoint,
                access_key: required_env("AWS_TEST_ACCESS_KEY"),
                secret_key: required_env("AWS_TEST_SECRET_KEY"),
                account_id: required_env("AWS_TEST_ACCOUNT_ID"),
                role_arn: required_env("STS_TEST_ROLE_ARN"),
                role_name: required_env("STS_TEST_ROLE_NAME"),
                denied_role_arn: required_env("STS_TEST_DENIED_ROLE_ARN"),
                region: std::env::var("AWS_TEST_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                tls_ca_pem,
                _server: None,
            };
        }

        let server = TestServer::start().await;
        let endpoint = server
            .sts_endpoint()
            .expect("HTTPS test server exposes an STS listener")
            .to_string();
        Self {
            endpoint,
            access_key: s3_tests::server::TEST_STS_ACCESS_KEY.to_string(),
            secret_key: s3_tests::server::TEST_STS_SECRET_KEY.to_string(),
            account_id: s3_tests::server::TEST_ACCOUNT_ID.to_string(),
            role_arn: s3_tests::server::TEST_STS_ROLE_ARN.to_string(),
            role_name: s3_tests::server::TEST_STS_ROLE_NAME.to_string(),
            denied_role_arn: s3_tests::server::TEST_STS_DENIED_ROLE_ARN.to_string(),
            region: s3_tests::server::TEST_REGION.to_string(),
            tls_ca_pem: server.tls_ca_pem().map(<[u8]>::to_vec),
            _server: Some(server),
        }
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn role_arn(&self) -> &str {
        &self.role_arn
    }

    #[must_use]
    pub fn role_name(&self) -> &str {
        &self.role_name
    }

    #[must_use]
    pub fn denied_role_arn(&self) -> &str {
        &self.denied_role_arn
    }

    #[must_use]
    pub fn credentials(&self) -> SignedRequestCredentials<'_> {
        SignedRequestCredentials {
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            region: &self.region,
            tls_ca_pem: self.tls_ca_pem.as_deref(),
        }
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} required with STS_TEST_ENDPOINT"))
}
