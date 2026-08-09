// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::error::Error as _;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use aws_sdk_s3::config::HttpClient;
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rustls::pki_types::{pem::PemObject, CertificateDer};
use rustls::{ClientConfig, RootCertStore};

type HyperHttpsConnector = HttpsConnector<HyperHttpConnector>;
type SmithyHyperClient = HyperClient<HyperHttpsConnector, SdkBody>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ConnectorSettingsKey {
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct TestHttpClient {
    tls_ca_pem: Option<Vec<u8>>,
    connectors: Mutex<HashMap<ConnectorSettingsKey, SharedHttpConnector>>,
}

impl TestHttpClient {
    pub(crate) fn new(tls_ca_pem: Option<&[u8]>) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Self {
            tls_ca_pem: tls_ca_pem.map(|pem| pem.to_vec()),
            connectors: Mutex::new(HashMap::new()),
        }
    }

    fn build_connector(&self, settings: &HttpConnectorSettings) -> SharedHttpConnector {
        SharedHttpConnector::new(TestHyperConnector {
            client: build_hyper_client(self.tls_ca_pem.as_deref(), settings.connect_timeout()),
            read_timeout: settings.read_timeout(),
        })
    }
}

impl HttpClient for TestHttpClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let key = ConnectorSettingsKey {
            connect_timeout: settings.connect_timeout(),
            read_timeout: settings.read_timeout(),
        };
        let mut connectors = self
            .connectors
            .lock()
            .expect("lock test http connector cache");
        connectors
            .entry(key)
            .or_insert_with(|| self.build_connector(settings))
            .clone()
    }
}

struct TestHyperConnector {
    client: SmithyHyperClient,
    read_timeout: Option<Duration>,
}

impl fmt::Debug for TestHyperConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestHyperConnector")
            .field("read_timeout", &self.read_timeout)
            .finish_non_exhaustive()
    }
}

impl HttpConnector for TestHyperConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let client = self.client.clone();
        let read_timeout = self.read_timeout;
        HttpConnectorFuture::new(async move {
            let request = request
                .try_into_http1x()
                .map_err(|err| ConnectorError::user(Box::new(err)))?;
            let response = match read_timeout {
                Some(timeout) => tokio::time::timeout(timeout, client.request(request))
                    .await
                    .map_err(|err| ConnectorError::timeout(Box::new(err)))?,
                None => client.request(request).await,
            }
            .map_err(classify_hyper_error)?;
            let response = response.map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response)
                .map_err(|err| ConnectorError::other(Box::new(err), None))
        })
    }
}

fn build_hyper_client(
    tls_ca_pem: Option<&[u8]>,
    connect_timeout: Option<Duration>,
) -> SmithyHyperClient {
    let mut http = HyperHttpConnector::new();
    http.enforce_http(false);
    http.set_connect_timeout(connect_timeout);

    let https = match tls_ca_pem {
        Some(pem) => HttpsConnectorBuilder::new()
            .with_tls_config(build_tls_config_with_custom_ca(pem))
            .https_or_http()
            .enable_http1()
            .wrap_connector(http),
        None => HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("load native trust roots for AWS test client")
            .https_or_http()
            .enable_http1()
            .wrap_connector(http),
    };

    HyperClient::builder(TokioExecutor::new())
        .pool_timer(TokioTimer::new())
        .build(https)
}

fn build_tls_config_with_custom_ca(tls_ca_pem: &[u8]) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(tls_ca_pem)
        .collect::<Result<_, _>>()
        .expect("valid custom CA PEM");
    let (valid, invalid) = roots.add_parsable_certificates(certs);
    assert!(
        valid > 0,
        "custom trust store must include at least one CA certificate"
    );
    assert_eq!(
        invalid, 0,
        "custom trust store contains invalid CA certificates"
    );
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

fn classify_hyper_error(err: hyper_util::client::legacy::Error) -> ConnectorError {
    let is_connect = err.is_connect();
    let is_transient_http = err
        .source()
        .and_then(|source| source.downcast_ref::<hyper::Error>())
        .is_some_and(|source| source.is_closed() || source.is_incomplete_message());
    let boxed = Box::new(err);
    if is_connect {
        ConnectorError::io(boxed).never_connected()
    } else if is_transient_http {
        ConnectorError::io(boxed)
    } else {
        ConnectorError::other(boxed, None)
    }
}
