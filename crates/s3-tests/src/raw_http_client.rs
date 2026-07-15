use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use hyper::header::{HeaderName, HeaderValue, CONTENT_LENGTH};
use hyper::{Method, StatusCode, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rustls::pki_types::{pem::PemObject, CertificateDer};
use rustls::{ClientConfig, RootCertStore};

type HyperHttpsConnector = HttpsConnector<HyperHttpConnector>;
type RawHyperClient = HyperClient<HyperHttpsConnector, SdkBody>;

#[derive(Clone, Debug)]
pub struct Agent {
    inner: Arc<AgentInner>,
}

#[derive(Debug)]
struct AgentInner {
    client: RawHyperClient,
    timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct Error {
    message: String,
}

#[derive(Debug, Clone)]
pub struct Response {
    status: StatusCode,
    headers: hyper::HeaderMap,
    body: Body,
    body_read_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Body {
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct RequestBuilder {
    agent: Agent,
    method: Method,
    uri: String,
    headers: Vec<(String, String)>,
    auto_content_length: bool,
    allow_response_body_error: bool,
}

#[derive(Clone, Copy, Debug)]
struct RequestOptions {
    auto_content_length: bool,
    allow_response_body_error: bool,
}

impl Agent {
    pub fn new(endpoint: &str, tls_ca_pem: Option<&[u8]>, timeout: Duration) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = build_hyper_client(
            endpoint,
            tls_ca_pem.filter(|_| endpoint.starts_with("https://")),
        );
        Self {
            inner: Arc::new(AgentInner { client, timeout }),
        }
    }

    pub fn get(&self, uri: &str) -> RequestBuilder {
        self.request_method(Method::GET, uri)
    }

    pub fn post(&self, uri: &str) -> RequestBuilder {
        self.request_method(Method::POST, uri)
    }

    pub fn put(&self, uri: &str) -> RequestBuilder {
        self.request_method(Method::PUT, uri)
    }

    pub fn delete(&self, uri: &str) -> RequestBuilder {
        self.request_method(Method::DELETE, uri)
    }

    pub fn head(&self, uri: &str) -> RequestBuilder {
        self.request_method(Method::HEAD, uri)
    }

    pub fn options(&self, uri: &str) -> RequestBuilder {
        self.request_method(Method::OPTIONS, uri)
    }

    /// Build a request for a standard or extension HTTP method token.
    pub fn request(&self, method: &str, uri: &str) -> RequestBuilder {
        let method = Method::from_bytes(method.as_bytes())
            .unwrap_or_else(|error| panic!("invalid raw HTTP method {method:?}: {error}"));
        self.request_method(method, uri)
    }

    fn request_method(&self, method: Method, uri: &str) -> RequestBuilder {
        RequestBuilder {
            agent: self.clone(),
            method,
            uri: uri.to_string(),
            headers: Vec::new(),
            auto_content_length: true,
            allow_response_body_error: false,
        }
    }

    fn execute(
        &self,
        method: Method,
        uri: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        options: RequestOptions,
    ) -> Result<Response, Error> {
        let client = self.inner.client.clone();
        let timeout = self.inner.timeout;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        crate::RT.spawn(async move {
            let result =
                execute_request(client, timeout, method, uri, headers, body, options).await;
            let _ = tx.send(result);
        });
        rx.recv()
            .expect("raw HTTP request task unexpectedly terminated")
    }
}

impl RequestBuilder {
    pub fn header(mut self, name: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.headers
            .push((name.as_ref().to_string(), value.as_ref().to_string()));
        self
    }

    pub fn call(self) -> Result<Response, Error> {
        self.send([])
    }

    pub fn send(self, body: impl AsRef<[u8]>) -> Result<Response, Error> {
        self.agent.execute(
            self.method,
            self.uri,
            self.headers,
            body.as_ref().to_vec(),
            RequestOptions {
                auto_content_length: self.auto_content_length,
                allow_response_body_error: self.allow_response_body_error,
            },
        )
    }

    pub fn send_allow_response_body_error(self, body: impl AsRef<[u8]>) -> Result<Response, Error> {
        Self {
            allow_response_body_error: true,
            ..self
        }
        .send(body)
    }

    pub fn send_without_content_length(self, body: impl AsRef<[u8]>) -> Result<Response, Error> {
        Self {
            auto_content_length: false,
            ..self
        }
        .send(body)
    }

    pub fn send_empty(self) -> Result<Response, Error> {
        self.send([])
    }
}

impl Response {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &hyper::HeaderMap {
        &self.headers
    }

    pub fn body_mut(&mut self) -> &mut Body {
        &mut self.body
    }

    pub fn body_read_error(&self) -> Option<&str> {
        self.body_read_error.as_deref()
    }
}

impl Body {
    pub fn read_to_string(&mut self) -> Result<String, Error> {
        let bytes = std::mem::take(&mut self.bytes);
        String::from_utf8(bytes).map_err(|err| Error::new(err.to_string()))
    }

    pub fn read_to_vec(&mut self) -> Result<Vec<u8>, Error> {
        Ok(std::mem::take(&mut self.bytes))
    }
}

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

async fn execute_request(
    client: RawHyperClient,
    timeout: Duration,
    method: Method,
    uri: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    options: RequestOptions,
) -> Result<Response, Error> {
    tokio::time::timeout(timeout, async move {
        let uri: Uri = uri
            .parse()
            .map_err(|err| Error::new(format!("invalid request URI: {err}")))?;
        let add_content_length = options.auto_content_length
            && matches!(method, Method::PUT | Method::POST)
            && !headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("content-length")
                    || name.eq_ignore_ascii_case("transfer-encoding")
            });
        let content_length = body.len();
        let mut request = hyper::Request::builder()
            .method(method)
            .uri(uri)
            .body(SdkBody::from(body))
            .map_err(|err| Error::new(format!("build raw HTTP request: {err}")))?;
        if add_content_length {
            request
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from(content_length));
        }
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|err| Error::new(format!("invalid header name: {err}")))?;
            let value = HeaderValue::from_str(&value)
                .map_err(|err| Error::new(format!("invalid header value: {err}")))?;
            request.headers_mut().append(name, value);
        }
        let response = client
            .request(request)
            .await
            .map_err(|err| Error::new(format!("raw HTTP transport error: {err}")))?;
        let (parts, body) = response.into_parts();
        let (body, body_read_error) = match ByteStream::from_body_1_x(body).collect().await {
            Ok(collected) => (collected.into_bytes().to_vec(), None),
            Err(err) if options.allow_response_body_error => (
                Vec::new(),
                Some(format!("read raw HTTP response body: {err}")),
            ),
            Err(err) => {
                return Err(Error::new(format!("read raw HTTP response body: {err}")));
            }
        };
        Ok(Response {
            status: parts.status,
            headers: parts.headers,
            body: Body { bytes: body },
            body_read_error,
        })
    })
    .await
    .map_err(|err| Error::new(format!("raw HTTP request timed out: {err}")))?
}

fn build_hyper_client(endpoint: &str, tls_ca_pem: Option<&[u8]>) -> RawHyperClient {
    let mut http = HyperHttpConnector::new();
    http.enforce_http(false);

    let https = match tls_ca_pem {
        Some(pem) => HttpsConnectorBuilder::new()
            .with_tls_config(build_tls_config_with_custom_ca(pem))
            .https_or_http()
            .enable_http1()
            .wrap_connector(http),
        None if endpoint.starts_with("https://") => HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("load native trust roots for raw HTTP test client")
            .https_or_http()
            .enable_http1()
            .wrap_connector(http),
        None => HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("load native trust roots for raw HTTP test client")
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

#[cfg(test)]
mod tests {
    use super::Agent;
    use std::time::Duration;

    #[test]
    fn request_accepts_extension_method_tokens() {
        let agent = Agent::new("http://127.0.0.1", None, Duration::from_secs(1));
        let request = agent.request("X-ARGMIN-PROBE", "http://127.0.0.1/");

        assert_eq!(request.method.as_str(), "X-ARGMIN-PROBE");
    }
}
