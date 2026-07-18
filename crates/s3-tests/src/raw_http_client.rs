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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;

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

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin {}

pub struct FlushedPartialRequest {
    stream: Box<dyn AsyncReadWrite + Send>,
    declared_content_length: usize,
    written: usize,
    response_bytes: Vec<u8>,
    deadline: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlushedResponse {
    status: u16,
    body: Vec<u8>,
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

impl FlushedResponse {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

/// Write a deliberately incomplete HTTP request body onto the connection and
/// disconnect only after the partial body has been flushed.
pub async fn write_partial_request_and_disconnect(
    method: &str,
    uri: &str,
    declared_content_length: usize,
    partial_body: &[u8],
    headers: &[(&str, &str)],
    tls_ca_pem: Option<&[u8]>,
) -> Result<(), Error> {
    open_flushed_partial_request(
        method,
        uri,
        declared_content_length,
        partial_body,
        headers,
        tls_ca_pem,
    )
    .await
    .map(drop)
}

/// Open a raw HTTP/1.1 request, write its headers and body prefix, and return
/// only after the underlying TCP/TLS stream has acknowledged a flush.
///
/// The caller retains the connection and may mutate external state before
/// writing the rest of the declared body. This is deliberately lower-level
/// than a Hyper body: successful return proves the prefix reached the
/// transport, rather than merely that a body producer was polled.
pub async fn open_flushed_partial_request(
    method: &str,
    uri: &str,
    declared_content_length: usize,
    partial_body: &[u8],
    headers: &[(&str, &str)],
    tls_ca_pem: Option<&[u8]>,
) -> Result<FlushedPartialRequest, Error> {
    open_flushed_partial_request_with_timeout(
        method,
        uri,
        declared_content_length,
        partial_body,
        headers,
        tls_ca_pem,
        crate::configured_test_timeout(),
    )
    .await
}

async fn open_flushed_partial_request_with_timeout(
    method: &str,
    uri: &str,
    declared_content_length: usize,
    partial_body: &[u8],
    headers: &[(&str, &str)],
    tls_ca_pem: Option<&[u8]>,
    timeout: Duration,
) -> Result<FlushedPartialRequest, Error> {
    if partial_body.len() >= declared_content_length {
        return Err(Error::new(
            "partial body must be shorter than the declared Content-Length",
        ));
    }
    let parsed = url::Url::parse(uri).map_err(|err| Error::new(format!("parse URI: {err}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| Error::new("request URI has no host"))?;
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| Error::new("request URI has no known port"))?;
    let uri_host_header = match parsed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    let mut supplied_host_headers = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("host"));
    let supplied_host_header = supplied_host_headers.next().map(|(_, value)| *value);
    if supplied_host_headers.next().is_some() {
        return Err(Error::new("raw request contains duplicate Host headers"));
    }
    let host_header = match supplied_host_header {
        Some(value) if value == uri_host_header => value,
        Some(_) => {
            return Err(Error::new(
                "raw request Host header does not match the request URI",
            ));
        }
        None => uri_host_header.as_str(),
    };
    let request_target = match parsed.query() {
        Some(query) => format!("{}?{query}", parsed.path()),
        None => parsed.path().to_string(),
    };
    let mut head = format!(
        "{method} {request_target} HTTP/1.1\r\nHost: {host_header}\r\nContent-Length: {declared_content_length}\r\nConnection: close\r\n"
    );
    for (name, value) in headers {
        if name.contains(['\r', '\n']) || value.contains(['\r', '\n']) {
            return Err(Error::new("raw request header contains a newline"));
        }
        if name.eq_ignore_ascii_case("host") {
            continue;
        }
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");

    let deadline = Instant::now() + timeout;
    tokio::time::timeout_at(deadline, async {
        let stream = TcpStream::connect((host, port))
            .await
            .map_err(|err| Error::new(format!("connect raw request: {err}")))?;
        let mut stream: Box<dyn AsyncReadWrite + Send> = match parsed.scheme() {
            "http" => Box::new(stream),
            "https" => {
                let _ = rustls::crypto::ring::default_provider().install_default();
                let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
                    .map_err(|err| Error::new(format!("invalid TLS server name: {err}")))?;
                let connector = TlsConnector::from(Arc::new(build_tls_client_config(tls_ca_pem)));
                let stream = connector
                    .connect(server_name, stream)
                    .await
                    .map_err(|err| Error::new(format!("connect raw request TLS: {err}")))?;
                Box::new(stream)
            }
            scheme => {
                return Err(Error::new(format!(
                    "unsupported raw request URI scheme: {scheme}"
                )));
            }
        };
        write_partial_request(&mut stream, head.as_bytes(), partial_body).await?;
        Ok(FlushedPartialRequest {
            stream,
            declared_content_length,
            written: partial_body.len(),
            response_bytes: Vec::new(),
            deadline,
        })
    })
    .await
    .map_err(|_| Error::new("partial raw request deadline exceeded"))?
}

async fn write_partial_request<S: AsyncWrite + Unpin>(
    mut stream: S,
    head: &[u8],
    partial_body: &[u8],
) -> Result<(), Error> {
    stream
        .write_all(head)
        .await
        .map_err(|err| Error::new(format!("write raw request head: {err}")))?;
    stream
        .write_all(partial_body)
        .await
        .map_err(|err| Error::new(format!("write raw request partial body: {err}")))?;
    stream
        .flush()
        .await
        .map_err(|err| Error::new(format!("flush raw request partial body: {err}")))
}

impl FlushedPartialRequest {
    pub async fn write_and_flush(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let new_written = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| Error::new("raw request body length overflow"))?;
        if new_written > self.declared_content_length {
            return Err(Error::new(
                "raw request body exceeds its declared Content-Length",
            ));
        }
        tokio::time::timeout_at(self.deadline, async {
            let mut remaining = bytes;
            while !remaining.is_empty() {
                match self.stream.write(remaining).await {
                    Ok(0) => {
                        return Err(Error::new("write raw request body returned zero bytes"));
                    }
                    Ok(written) => {
                        self.written += written;
                        remaining = &remaining[written..];
                    }
                    Err(err) => {
                        return Err(Error::new(format!("write raw request body: {err}")));
                    }
                }
            }
            self.stream
                .flush()
                .await
                .map_err(|err| Error::new(format!("flush raw request body: {err}")))
        })
        .await
        .map_err(|_| {
            Error::new("raw request deadline exceeded while writing or flushing body")
        })??;
        debug_assert_eq!(self.written, new_written);
        Ok(())
    }

    pub fn written_body_bytes(&self) -> usize {
        self.written
    }

    /// Return a response status if one becomes visible before `duration`.
    ///
    /// The request uses `Connection: close`, so a complete response normally
    /// reaches EOF. A parsed status line is also sufficient when a timeout
    /// races with response-body delivery.
    pub async fn response_status_within(
        &mut self,
        duration: Duration,
    ) -> Result<Option<u16>, Error> {
        let probe_deadline = Instant::now() + duration;
        let read_deadline = std::cmp::min(probe_deadline, self.deadline);
        match tokio::time::timeout_at(
            read_deadline,
            self.stream.read_to_end(&mut self.response_bytes),
        )
        .await
        {
            Ok(Ok(_)) => parse_raw_response_status(&self.response_bytes).map(Some),
            Ok(Err(err)) => Err(Error::new(format!("read raw HTTP response: {err}"))),
            Err(_) if read_deadline == self.deadline => Err(Error::new(
                "raw request deadline exceeded while reading response",
            )),
            Err(_) => match parse_raw_response_status(&self.response_bytes) {
                Ok(status) => Ok(Some(status)),
                Err(_) if self.response_bytes.is_empty() => Ok(None),
                Err(err) => Err(err),
            },
        }
    }

    pub async fn finish_and_read_response(mut self) -> Result<FlushedResponse, Error> {
        if self.written != self.declared_content_length {
            return Err(Error::new(format!(
                "raw request body is incomplete: wrote {} of {} bytes",
                self.written, self.declared_content_length
            )));
        }
        self.read_to_end_until_deadline().await?;
        parse_raw_response(&self.response_bytes)
    }

    pub async fn read_response(mut self) -> Result<FlushedResponse, Error> {
        self.read_to_end_until_deadline().await?;
        parse_raw_response(&self.response_bytes)
    }

    async fn read_to_end_until_deadline(&mut self) -> Result<(), Error> {
        tokio::time::timeout_at(
            self.deadline,
            self.stream.read_to_end(&mut self.response_bytes),
        )
        .await
        .map_err(|_| Error::new("raw request deadline exceeded while reading response"))?
        .map(|_| ())
        .map_err(|err| Error::new(format!("read raw HTTP response: {err}")))
    }
}

fn parse_raw_response_status(response: &[u8]) -> Result<u16, Error> {
    let line_end = response
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| Error::new("raw HTTP response has no complete status line"))?;
    let status_line = std::str::from_utf8(&response[..line_end])
        .map_err(|err| Error::new(format!("raw HTTP response status is not UTF-8: {err}")))?;
    let mut fields = status_line.split_ascii_whitespace();
    let version = fields
        .next()
        .ok_or_else(|| Error::new("raw HTTP response status line is empty"))?;
    if !version.starts_with("HTTP/") {
        return Err(Error::new(
            "raw HTTP response status line has no HTTP version",
        ));
    }
    fields
        .next()
        .ok_or_else(|| Error::new("raw HTTP response status line has no status code"))?
        .parse()
        .map_err(|err: std::num::ParseIntError| {
            Error::new(format!("raw HTTP response status code is invalid: {err}"))
        })
}

fn parse_raw_response(response: &[u8]) -> Result<FlushedResponse, Error> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| Error::new("raw HTTP response has no complete header section"))?;
    let status = parse_raw_response_status(response)?;
    let header_text = std::str::from_utf8(&response[..header_end])
        .map_err(|err| Error::new(format!("raw HTTP response headers are not UTF-8: {err}")))?;
    let mut chunked = false;
    let mut content_length = None;
    for line in header_text.split("\r\n").skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            return Err(Error::new("raw HTTP response contains a malformed header"));
        };
        if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
        } else if name.eq_ignore_ascii_case("content-length") {
            let parsed =
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|err: std::num::ParseIntError| {
                        Error::new(format!(
                            "raw HTTP response Content-Length is invalid: {err}"
                        ))
                    })?;
            if content_length.replace(parsed).is_some() {
                return Err(Error::new(
                    "raw HTTP response contains duplicate Content-Length headers",
                ));
            }
        }
    }

    let encoded_body = &response[header_end + 4..];
    let body = if chunked {
        decode_chunked_response_body(encoded_body)?
    } else if let Some(content_length) = content_length {
        if encoded_body.len() != content_length {
            return Err(Error::new(format!(
                "raw HTTP response body length {} does not match Content-Length {content_length}",
                encoded_body.len()
            )));
        }
        encoded_body.to_vec()
    } else {
        encoded_body.to_vec()
    };
    Ok(FlushedResponse { status, body })
}

fn decode_chunked_response_body(mut encoded: &[u8]) -> Result<Vec<u8>, Error> {
    let mut decoded = Vec::new();
    loop {
        let line_end = encoded
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::new("chunked raw HTTP response has no complete chunk size"))?;
        let size_text = std::str::from_utf8(&encoded[..line_end]).map_err(|err| {
            Error::new(format!(
                "chunked raw HTTP response size is not UTF-8: {err}"
            ))
        })?;
        let size_text = size_text.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_text, 16).map_err(|err| {
            Error::new(format!("chunked raw HTTP response size is invalid: {err}"))
        })?;
        encoded = &encoded[line_end + 2..];
        if size == 0 {
            validate_chunked_response_trailers(encoded)?;
            return Ok(decoded);
        }
        let framed_len = size
            .checked_add(2)
            .ok_or_else(|| Error::new("chunked raw HTTP response size overflow"))?;
        if encoded.len() < framed_len {
            return Err(Error::new("chunked raw HTTP response body is incomplete"));
        }
        if &encoded[size..framed_len] != b"\r\n" {
            return Err(Error::new(
                "chunked raw HTTP response chunk has no terminator",
            ));
        }
        decoded.extend_from_slice(&encoded[..size]);
        encoded = &encoded[framed_len..];
    }
}

fn validate_chunked_response_trailers(mut encoded: &[u8]) -> Result<(), Error> {
    loop {
        let line_end = encoded
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| Error::new("chunked raw HTTP response trailer section is incomplete"))?;
        let line = &encoded[..line_end];
        encoded = &encoded[line_end + 2..];
        if line.is_empty() {
            if encoded.is_empty() {
                return Ok(());
            }
            return Err(Error::new(
                "chunked raw HTTP response has data after its trailer terminator",
            ));
        }
        let colon = line
            .iter()
            .position(|byte| *byte == b':')
            .ok_or_else(|| Error::new("chunked raw HTTP response contains a malformed trailer"))?;
        HeaderName::from_bytes(&line[..colon]).map_err(|err| {
            Error::new(format!(
                "chunked raw HTTP response trailer name is invalid: {err}"
            ))
        })?;
        let value = line[colon + 1..]
            .strip_prefix(b" ")
            .unwrap_or(&line[colon + 1..]);
        HeaderValue::from_bytes(value).map_err(|err| {
            Error::new(format!(
                "chunked raw HTTP response trailer value is invalid: {err}"
            ))
        })?;
    }
}

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

fn build_tls_client_config(tls_ca_pem: Option<&[u8]>) -> ClientConfig {
    if let Some(tls_ca_pem) = tls_ca_pem {
        return build_tls_config_with_custom_ca(tls_ca_pem);
    }

    let mut roots = RootCertStore::empty();
    let certs = rustls_native_certs::load_native_certs()
        .expect("load native root certificates for partial raw request");
    let (valid, invalid) = roots.add_parsable_certificates(certs);
    assert!(
        valid > 0,
        "native trust store must include at least one CA certificate"
    );
    assert_eq!(
        invalid, 0,
        "native trust store contains invalid CA certificates"
    );
    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

#[cfg(test)]
mod tests {
    use super::{open_flushed_partial_request_with_timeout, parse_raw_response, Agent};
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[test]
    fn request_accepts_extension_method_tokens() {
        let agent = Agent::new("http://127.0.0.1", None, Duration::from_secs(1));
        let request = agent.request("X-ARGMIN-PROBE", "http://127.0.0.1/");

        assert_eq!(request.method.as_str(), "X-ARGMIN-PROBE");
    }

    #[test]
    fn parses_chunked_response_body() {
        let response = parse_raw_response(
            b"HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n\
              7\r\n<Error>\r\n\
              19\r\n<Code>AccessDenied</Code>\r\n\
              8\r\n</Error>\r\n\
              0\r\n\r\n",
        )
        .unwrap();

        assert_eq!(response.status(), 403);
        assert_eq!(response.body(), b"<Error><Code>AccessDenied</Code></Error>");
    }

    #[test]
    fn rejects_chunked_response_with_truncated_trailer_section() {
        let error = parse_raw_response(
            b"HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n\
              0\r\n",
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("trailer section is incomplete"),
            "unexpected parser error: {error}"
        );
    }

    #[test]
    fn rejects_chunked_response_with_data_after_trailer_terminator() {
        let error = parse_raw_response(
            b"HTTP/1.1 403 Forbidden\r\nTransfer-Encoding: chunked\r\n\r\n\
              0\r\n\r\ntrailing",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("data after its trailer terminator"),
            "unexpected parser error: {error}"
        );
    }

    #[test]
    fn partial_request_response_read_honors_retained_deadline() {
        crate::RT.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (_stream, _) = listener.accept().await.unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
            });
            let request = open_flushed_partial_request_with_timeout(
                "PUT",
                &format!("http://{address}/object"),
                2,
                b"x",
                &[],
                None,
                Duration::from_millis(250),
            )
            .await
            .unwrap();

            let result = tokio::time::timeout(Duration::from_secs(1), request.read_response())
                .await
                .expect("retained raw response deadline must prevent a hung read")
                .unwrap_err();
            assert!(
                result.to_string().contains("deadline exceeded"),
                "unexpected read error: {result}"
            );
            server.abort();
        });
    }

    #[test]
    fn partial_request_body_write_honors_retained_deadline_under_backpressure() {
        crate::RT.block_on(async {
            const REMAINING_BYTES: usize = 64 * 1024 * 1024;

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (_stream, _) = listener.accept().await.unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
            });
            let mut request = open_flushed_partial_request_with_timeout(
                "PUT",
                &format!("http://{address}/object"),
                REMAINING_BYTES + 1,
                b"x",
                &[],
                None,
                Duration::from_millis(250),
            )
            .await
            .unwrap();
            let body = vec![b'x'; REMAINING_BYTES];

            let result =
                tokio::time::timeout(Duration::from_secs(1), request.write_and_flush(&body))
                    .await
                    .expect("retained raw request deadline must prevent a hung write")
                    .unwrap_err();
            assert!(
                result.to_string().contains("deadline exceeded"),
                "unexpected write error: {result}"
            );
            server.abort();
        });
    }
}
