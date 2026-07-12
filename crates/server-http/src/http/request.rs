/// Parse HTTP requests into structured S3 request data.
use std::{borrow::Cow, net::IpAddr};

use crate::error::ServerError;
use auth::HeaderSource;
use storage::ObjectKey;

/// Maximum body size for the buffered request path.
///
/// The buffered path is used for control-plane style requests (primarily XML
/// payloads such as `DeleteObjects`, versioning/CORS config, and
/// `CompleteMultipartUpload`). Data-plane writes (`PutObject`, `UploadPart`,
/// and POST Object file uploads) are routed through streaming handlers and are
/// not limited by this constant.
///
/// AWS's largest measured XML control-plane body limit is
/// `CompleteMultipartUpload` at 2,621,440 bytes, so the generic fallback cap
/// should not exceed that.
pub(crate) const MAX_BUFFERED_CONTROL_BODY_SIZE: usize = 2_621_440;

/// Validate a Content-Length header value. Rejects negative and non-numeric values.
fn validate_content_length(value: &str) -> Result<u64, ServerError> {
    value
        .parse::<u64>()
        .map_err(|_| ServerError::InvalidRequest {
            reason: format!("invalid Content-Length: {value}"),
        })
}

/// Parsed S3 request data extracted from an HTTP request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportSecurity {
    InsecureHttp,
    Tls,
}

impl TransportSecurity {
    #[must_use]
    pub fn is_secure(self) -> bool {
        matches!(self, Self::Tls)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsProtocolVersion {
    Tls12,
    Tls13,
}

impl TlsProtocolVersion {
    #[must_use]
    pub const fn policy_value(self) -> &'static str {
        match self {
            Self::Tls12 => "1.2",
            Self::Tls13 => "1.3",
        }
    }
}

pub struct S3Request {
    pub method: http::Method,
    pub uri: http::Uri,
    pub headers: http::HeaderMap,
    pub body: Vec<u8>,
    pub transport_security: TransportSecurity,
    pub tls_version: Option<TlsProtocolVersion>,
    pub source_ip: Option<IpAddr>,
    pub request_epoch_seconds: u64,
}

pub(crate) struct RequestHeaderSource<'a>(&'a http::HeaderMap);

impl HeaderSource for RequestHeaderSource<'_> {
    fn first_value<'a>(&'a self, name: &str) -> Option<&'a str> {
        self.0.get(name).map(|value| {
            std::str::from_utf8(value.as_bytes()).expect("S3Request stores only validated UTF-8")
        })
    }

    fn visit<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a str, &'a str),
    {
        for (name, value) in self.0 {
            let value = std::str::from_utf8(value.as_bytes())
                .expect("S3Request stores only validated UTF-8");
            f(name.as_str(), value);
        }
    }
}

pub(crate) fn query_pairs(query_string: &str) -> impl Iterator<Item = (&str, &str)> {
    query_string
        .split('&')
        .filter(|segment| !segment.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((key, value)) => (key, value),
            None => (pair, ""),
        })
}

#[must_use]
pub(crate) fn query_has_key(query_string: &str, name: &str) -> bool {
    query_pairs(query_string).any(|(key, _)| key == name)
}

#[must_use]
pub(crate) fn query_has_param(query_string: &str, name: &str, value: &str) -> bool {
    query_pairs(query_string).any(|(key, candidate)| key == name && candidate == value)
}

#[must_use]
pub(crate) fn query_param_raw<'a>(query_string: &'a str, name: &str) -> Option<&'a str> {
    query_pairs(query_string).find_map(|(key, value)| (key == name).then_some(value))
}

impl S3Request {
    /// Parse hyper request parts and collected body into an `S3Request`.
    ///
    /// Body size limiting is done by the caller (serve layer) via `http_body_util::Limited`.
    pub fn from_hyper(
        parts: http::request::Parts,
        body: bytes::Bytes,
        transport_security: TransportSecurity,
        request_epoch_seconds: u64,
    ) -> Result<Self, ServerError> {
        Self::from_hyper_with_source_ip(
            parts,
            body,
            transport_security,
            None,
            request_epoch_seconds,
        )
    }

    pub fn from_hyper_with_source_ip(
        parts: http::request::Parts,
        body: bytes::Bytes,
        transport_security: TransportSecurity,
        source_ip: Option<IpAddr>,
        request_epoch_seconds: u64,
    ) -> Result<Self, ServerError> {
        let method = parts.method;
        let uri = parts.uri;

        // Validate that all header values are UTF-8. We keep the original
        // HeaderMap so later stages can borrow from it directly without first
        // materializing String pairs.
        for (name, value) in &parts.headers {
            std::str::from_utf8(value.as_bytes()).map_err(|_| ServerError::InvalidRequest {
                reason: format!("invalid UTF-8 in header value for {name}"),
            })?;
        }
        let headers = parts.headers;

        // Validate Content-Length header if present (reject negative/non-numeric)
        if let Some(cl_value) = headers.get("content-length").map(|value| {
            std::str::from_utf8(value.as_bytes()).expect("S3Request stores only validated UTF-8")
        }) {
            validate_content_length(cl_value)?;
        }

        let body = body.to_vec();

        Ok(S3Request {
            method,
            uri,
            headers,
            body,
            transport_security,
            tls_version: None,
            source_ip,
            request_epoch_seconds,
        })
    }

    /// Parse hyper request parts into an `S3Request` with an empty body.
    ///
    /// Used by the streaming write path where the body is consumed frame-by-frame
    /// rather than collected upfront. Auth works because the `x-amz-content-sha256`
    /// header provides the body hash (typically `UNSIGNED-PAYLOAD`).
    pub fn from_hyper_headers(
        parts: http::request::Parts,
        transport_security: TransportSecurity,
        request_epoch_seconds: u64,
    ) -> Result<Self, ServerError> {
        Self::from_hyper(
            parts,
            bytes::Bytes::new(),
            transport_security,
            request_epoch_seconds,
        )
    }

    pub fn from_hyper_headers_with_source_ip(
        parts: http::request::Parts,
        transport_security: TransportSecurity,
        source_ip: Option<IpAddr>,
        request_epoch_seconds: u64,
    ) -> Result<Self, ServerError> {
        Self::from_hyper_with_source_ip(
            parts,
            bytes::Bytes::new(),
            transport_security,
            source_ip,
            request_epoch_seconds,
        )
    }

    #[must_use]
    pub const fn with_tls_version(mut self, tls_version: Option<TlsProtocolVersion>) -> Self {
        self.tls_version = tls_version;
        self
    }

    /// Get a header value by lowercase name.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|value| {
            std::str::from_utf8(value.as_bytes()).expect("S3Request stores only validated UTF-8")
        })
    }

    #[must_use]
    pub fn header_count(&self, name: &str) -> usize {
        self.headers.get_all(name).iter().count()
    }

    #[must_use]
    pub fn path(&self) -> &str {
        self.uri.path()
    }

    #[must_use]
    pub fn query_string(&self) -> &str {
        self.uri.query().unwrap_or("")
    }

    #[must_use]
    pub fn source_ip(&self) -> Option<IpAddr> {
        self.source_ip
    }

    #[must_use]
    pub fn request_epoch_seconds(&self) -> u64 {
        self.request_epoch_seconds
    }

    pub(crate) fn header_source(&self) -> RequestHeaderSource<'_> {
        RequestHeaderSource(&self.headers)
    }

    pub(crate) fn header_iter(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.headers.iter().map(|(name, value)| {
            (
                name.as_str(),
                std::str::from_utf8(value.as_bytes())
                    .expect("S3Request stores only validated UTF-8"),
            )
        })
    }

    /// Create a new `S3Request` with a decoded body and trailer headers,
    /// stripping `aws-chunked` from Content-Encoding.
    ///
    /// Only used by the `#[cfg(test)]` batch chunked decoder path.
    /// Production streaming writes don't reassemble into S3Request.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_decoded_body(&self, body: Vec<u8>, trailers: Vec<(String, String)>) -> Self {
        let mut headers = self.headers.clone();
        if let Some(content_encoding) = self.header("content-encoding") {
            let filtered: Vec<&str> = content_encoding
                .split(',')
                .map(str::trim)
                .filter(|s| !s.eq_ignore_ascii_case("aws-chunked"))
                .collect();
            if filtered.is_empty() {
                headers.remove("content-encoding");
            } else {
                headers.insert(
                    "content-encoding",
                    http::HeaderValue::from_str(&filtered.join(", "))
                        .expect("filtered content-encoding is valid"),
                );
            }
        }
        headers.insert(
            "content-length",
            http::HeaderValue::from_str(&body.len().to_string())
                .expect("content-length is always valid"),
        );

        // Merge trailer headers: replace existing headers with same name,
        // or append if not present. This avoids duplicate checksum headers
        // when the trailer provides a value that was also in the initial headers.
        for (tk, tv) in trailers {
            headers.insert(
                http::header::HeaderName::from_bytes(tk.as_bytes())
                    .expect("decoder only yields valid trailer names"),
                http::HeaderValue::from_str(&tv).expect("decoder only yields valid trailer values"),
            );
        }

        S3Request {
            method: self.method.clone(),
            uri: self.uri.clone(),
            headers,
            body,
            transport_security: self.transport_security,
            tls_version: self.tls_version,
            source_ip: self.source_ip,
            request_epoch_seconds: self.request_epoch_seconds,
        }
    }

    /// Get headers as borrowed pairs for auth verification.
    #[cfg(test)]
    #[must_use]
    fn header_pairs(&self) -> Vec<(&str, &str)> {
        self.header_iter().collect()
    }

    /// Get query parameter by name.
    #[must_use]
    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query_param_lossy(name).map(Cow::into_owned)
    }

    #[must_use]
    pub fn query_param_lossy(&self, name: &str) -> Option<Cow<'_, str>> {
        query_param_lossy(self.query_string(), name)
    }

    #[must_use]
    pub fn query_params_lossy(&self, name: &str) -> Vec<Cow<'_, str>> {
        query_params_lossy(self.query_string(), name)
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn new_for_test(
        method: http::Method,
        path: &str,
        query_string: &str,
        headers: http::HeaderMap,
        body: Vec<u8>,
        request_epoch_seconds: u64,
    ) -> Self {
        Self::new_for_test_with_transport(
            method,
            path,
            query_string,
            headers,
            body,
            TransportSecurity::Tls,
            request_epoch_seconds,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_with_transport(
        method: http::Method,
        path: &str,
        query_string: &str,
        headers: http::HeaderMap,
        body: Vec<u8>,
        transport_security: TransportSecurity,
        request_epoch_seconds: u64,
    ) -> Self {
        let path = if path.is_empty() { "/" } else { path };
        let uri = if query_string.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query_string}")
        };
        Self {
            method,
            uri: uri.parse().expect("test URI should be valid"),
            headers,
            body,
            transport_security,
            tls_version: None,
            source_ip: None,
            request_epoch_seconds,
        }
    }
}

#[must_use]
pub(crate) fn query_param_lossy<'a>(query_string: &'a str, name: &str) -> Option<Cow<'a, str>> {
    query_param_raw(query_string, name).map(percent_decode_lossy)
}

#[must_use]
pub(crate) fn query_params_lossy<'a>(query_string: &'a str, name: &str) -> Vec<Cow<'a, str>> {
    query_pairs(query_string)
        .filter(|(key, _)| *key == name)
        .map(|(_, value)| percent_decode_lossy(value))
        .collect()
}

pub(crate) fn parse_part_number(value: &str) -> Result<u32, ServerError> {
    let part_number = value.parse().map_err(|_| ServerError::InvalidArgument {
        reason: "partNumber must be a positive integer".to_string(),
    })?;
    if part_number == 0 {
        return Err(ServerError::InvalidArgument {
            reason: "partNumber must be >= 1".to_string(),
        });
    }
    Ok(part_number)
}

pub(crate) fn parse_upload_part_query(query_string: &str) -> Result<(String, u32), ServerError> {
    let upload_id =
        query_param_lossy(query_string, "uploadId").ok_or_else(|| ServerError::InvalidRequest {
            reason: "missing uploadId query parameter".to_string(),
        })?;
    let part_number = query_param_lossy(query_string, "partNumber").ok_or_else(|| {
        ServerError::InvalidRequest {
            reason: "missing partNumber query parameter".to_string(),
        }
    })?;

    Ok((
        upload_id.into_owned(),
        parse_part_number(part_number.as_ref())?,
    ))
}

/// Percent-decode a string (RFC 3986) into raw bytes. Does NOT treat + as space.
pub(crate) fn percent_decode_bytes(s: &str) -> Vec<u8> {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                result.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    result
}

/// Percent-decode a string (RFC 3986) into UTF-8, rejecting invalid sequences.
pub(crate) fn percent_decode_strict(s: &str) -> Result<String, ServerError> {
    let bytes = percent_decode_bytes(s);
    String::from_utf8(bytes).map_err(|_| ServerError::InvalidURI {
        reason: "Couldn't parse the specified URI.".to_string(),
    })
}

/// Percent-decode a string (RFC 3986) into UTF-8, borrowing when unchanged.
pub(crate) fn percent_decode_lossy(s: &str) -> Cow<'_, str> {
    if !s.as_bytes().contains(&b'%') {
        return Cow::Borrowed(s);
    }
    Cow::Owned(String::from_utf8_lossy(&percent_decode_bytes(s)).into_owned())
}

/// Parse the `x-amz-copy-source` header value into `(bucket, key, version_id)`.
///
/// Accepts `[/]bucket/key[?versionId=...]`. Strips optional leading `/`.
/// Bucket, key, and `versionId` query-parameter value are percent-decoded.
pub(crate) fn parse_copy_source(
    header: &str,
) -> Result<(String, ObjectKey, Option<String>), ServerError> {
    let invalid_copy_source = || ServerError::InvalidArgument {
        reason: "Invalid copy source object key".to_string(),
    };

    // Strip optional leading slash
    let s = header.strip_prefix('/').unwrap_or(header);

    // Extract ?versionId=... query string
    let (s, version_id) = match s.find('?') {
        Some(pos) => {
            let query = &s[pos + 1..];
            let vid = query_param_raw(query, "versionId")
                .map(percent_decode_strict)
                .transpose()
                .map_err(|_| invalid_copy_source())?;
            (&s[..pos], vid)
        }
        None => (s, None),
    };

    // Split into bucket/key at first '/'
    let slash_pos = s.find('/').ok_or_else(invalid_copy_source)?;

    let bucket = percent_decode_strict(&s[..slash_pos]).map_err(|_| invalid_copy_source())?;
    let key = percent_decode_strict(&s[slash_pos + 1..]).map_err(|_| invalid_copy_source())?;

    let key = ObjectKey::try_from(key).map_err(|_| invalid_copy_source())?;

    Ok((bucket, key, version_id))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn header_map_from_owned(headers: Vec<(String, String)>) -> http::HeaderMap {
    let mut map = http::HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        map.append(
            http::header::HeaderName::from_bytes(name.as_bytes())
                .expect("test headers must use valid names"),
            http::HeaderValue::from_str(&value).expect("test headers must use valid values"),
        );
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_headers(headers: Vec<(String, String)>) -> http::HeaderMap {
        header_map_from_owned(headers)
    }

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode_lossy("hello%20world"), "hello world");
        assert_eq!(percent_decode_lossy("a%2Fb"), "a/b");
        assert_eq!(percent_decode_lossy("no+encoding"), "no+encoding"); // + is NOT space
    }

    #[test]
    fn percent_decode_passthrough() {
        assert_eq!(percent_decode_lossy("plain"), "plain");
        assert_eq!(percent_decode_lossy(""), "");
    }

    #[test]
    fn query_param_lookup() {
        let req = S3Request::new_for_test(
            http::Method::GET,
            "/bucket",
            "list-type=2&prefix=photos%2F&max-keys=10",
            test_headers(vec![]),
            vec![],
            0,
        );
        assert_eq!(req.query_param("list-type"), Some("2".to_string()));
        assert_eq!(req.query_param("prefix"), Some("photos/".to_string()));
        assert_eq!(req.query_param("max-keys"), Some("10".to_string()));
        assert_eq!(req.query_param("missing"), None);
    }

    #[test]
    fn query_param_lossy_raw_lookup() {
        assert_eq!(
            query_param_lossy("list-type=2&prefix=photos%2F&max-keys=10", "prefix").as_deref(),
            Some("photos/")
        );
        assert_eq!(
            query_param_lossy("flag&other=1", "flag").as_deref(),
            Some("")
        );
        assert_eq!(query_param_lossy("a=1", "missing").as_deref(), None);
    }

    #[test]
    fn query_helpers_handle_flags_and_first_match() {
        assert!(query_has_key("flag&&other=1", "flag"));
        assert!(!query_has_key("flag&&other=1", "missing"));
        assert!(query_has_param("list-type=1&list-type=2", "list-type", "2"));
        assert!(!query_has_param(
            "list-type=1&list-type=3",
            "list-type",
            "2"
        ));
        assert_eq!(query_param_raw("flag&other=1&other=2", "flag"), Some(""));
        assert_eq!(query_param_raw("flag&other=1&other=2", "other"), Some("1"));
    }

    #[test]
    fn parse_part_number_rejects_zero_and_non_numeric() {
        assert!(matches!(
            parse_part_number("0"),
            Err(ServerError::InvalidArgument { reason }) if reason == "partNumber must be >= 1"
        ));
        assert!(matches!(
            parse_part_number("abc"),
            Err(ServerError::InvalidArgument { reason })
                if reason == "partNumber must be a positive integer"
        ));
    }

    #[test]
    fn parse_upload_part_query_validates_required_fields() {
        assert_eq!(
            parse_upload_part_query("partNumber=3&uploadId=abc123").unwrap(),
            ("abc123".to_string(), 3)
        );
        assert!(matches!(
            parse_upload_part_query("uploadId=abc123"),
            Err(ServerError::InvalidRequest { reason })
                if reason == "missing partNumber query parameter"
        ));
        assert!(matches!(
            parse_upload_part_query("partNumber=abc&uploadId=abc123"),
            Err(ServerError::InvalidArgument { reason })
                if reason == "partNumber must be a positive integer"
        ));
    }

    #[test]
    fn parse_copy_source_percent_decodes_version_id() {
        let (bucket, key, version_id) =
            parse_copy_source("/bucket/key?partNumber=1&versionId=abc%2Fdef").unwrap();
        assert_eq!(bucket.as_str(), "bucket");
        assert_eq!(key.as_str(), "key");
        assert_eq!(version_id.as_deref(), Some("abc/def"));
    }

    #[test]
    fn percent_decode_truncated_escape() {
        // % at end of string — not enough chars for a hex pair
        assert_eq!(percent_decode_lossy("abc%"), "abc%");
        assert_eq!(percent_decode_lossy("abc%2"), "abc%2");
    }

    #[test]
    fn percent_decode_non_hex_after_percent() {
        assert_eq!(percent_decode_lossy("%ZZ"), "%ZZ");
        assert_eq!(percent_decode_lossy("%GH"), "%GH");
    }

    #[test]
    fn percent_decode_uppercase_hex() {
        assert_eq!(percent_decode_lossy("%2F"), "/");
        assert_eq!(percent_decode_lossy("%2f"), "/");
        assert_eq!(percent_decode_lossy("%3A"), ":");
        assert_eq!(percent_decode_lossy("%3a"), ":");
    }

    #[test]
    fn hex_val_digits() {
        for d in b'0'..=b'9' {
            assert_eq!(hex_val(d), Some(d - b'0'));
        }
    }

    #[test]
    fn hex_val_lower_alpha() {
        for c in b'a'..=b'f' {
            assert_eq!(hex_val(c), Some(c - b'a' + 10));
        }
    }

    #[test]
    fn hex_val_upper_alpha() {
        for c in b'A'..=b'F' {
            assert_eq!(hex_val(c), Some(c - b'A' + 10));
        }
    }

    #[test]
    fn hex_val_invalid() {
        assert_eq!(hex_val(b'g'), None);
        assert_eq!(hex_val(b'G'), None);
        assert_eq!(hex_val(b' '), None);
        assert_eq!(hex_val(b'/'), None);
        assert_eq!(hex_val(b':'), None);
    }

    #[test]
    fn query_param_empty_query_string() {
        let req =
            S3Request::new_for_test(http::Method::GET, "/", "", test_headers(vec![]), vec![], 0);
        assert_eq!(req.query_param("anything"), None);
    }

    #[test]
    fn query_param_no_equals() {
        let req = S3Request::new_for_test(
            http::Method::GET,
            "/",
            "flagonly&key=val",
            test_headers(vec![]),
            vec![],
            0,
        );
        // "flagonly" with no = has empty value
        assert_eq!(req.query_param("flagonly"), Some(String::new()));
        assert_eq!(req.query_param("key"), Some("val".to_string()));
    }

    #[test]
    fn query_param_match_not_first() {
        let req = S3Request::new_for_test(
            http::Method::GET,
            "/",
            "a=1&b=2&c=3",
            test_headers(vec![]),
            vec![],
            0,
        );
        assert_eq!(req.query_param("c"), Some("3".to_string()));
    }

    #[test]
    fn header_pairs_output() {
        let req = S3Request::new_for_test(
            http::Method::GET,
            "/",
            "",
            test_headers(vec![
                ("host".to_string(), "example.com".to_string()),
                ("content-type".to_string(), "text/plain".to_string()),
            ]),
            vec![],
            0,
        );
        let pairs = req.header_pairs();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("host", "example.com"));
        assert_eq!(pairs[1], ("content-type", "text/plain"));
    }

    #[test]
    fn validate_content_length_valid() {
        assert_eq!(validate_content_length("0").unwrap(), 0);
        assert_eq!(validate_content_length("42").unwrap(), 42);
        assert_eq!(validate_content_length("1000000").unwrap(), 1000000);
    }

    #[test]
    fn validate_content_length_negative() {
        assert!(validate_content_length("-1").is_err());
        assert!(validate_content_length("-100").is_err());
    }

    #[test]
    fn validate_content_length_non_numeric() {
        assert!(validate_content_length("abc").is_err());
        assert!(validate_content_length("12.5").is_err());
        assert!(validate_content_length("").is_err());
    }

    #[test]
    fn header_returns_none_for_missing() {
        let req = S3Request::new_for_test(
            http::Method::GET,
            "/",
            "",
            test_headers(vec![("host".to_string(), "example.com".to_string())]),
            vec![],
            0,
        );
        assert_eq!(req.header("content-type"), None);
        assert_eq!(req.header("host"), Some("example.com"));
    }

    // ── parse_copy_source ────────────────────────────────────────────

    #[test]
    fn parse_copy_source_basic() {
        let (bucket, key, vid) = parse_copy_source("/bucket/key").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_str(), "key");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_no_leading_slash() {
        let (bucket, key, vid) = parse_copy_source("bucket/key").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_str(), "key");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_encoded() {
        let (bucket, key, vid) = parse_copy_source("/bucket/key%20name").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_str(), "key name");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_nested_key() {
        let (bucket, key, vid) = parse_copy_source("/bucket/a/b/c").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_str(), "a/b/c");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_version_id() {
        let (bucket, key, vid) = parse_copy_source("/bucket/key?versionId=xyz").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key.as_str(), "key");
        assert_eq!(vid.as_deref(), Some("xyz"));
    }

    #[test]
    fn parse_copy_source_missing_key() {
        match parse_copy_source("/bucket") {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid copy source object key");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        match parse_copy_source("bucket") {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid copy source object key");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_copy_source_empty_key() {
        match parse_copy_source("/bucket/") {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid copy source object key");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_copy_source_invalid_utf8() {
        // %80 is not valid UTF-8
        match parse_copy_source("/bucket/key%80name") {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid copy source object key");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_copy_source_preserves_unvalidated_bucket_name() {
        let (bucket, key, vid) = parse_copy_source("/BadBucket/key").unwrap();
        assert_eq!(bucket, "BadBucket");
        assert_eq!(key.as_str(), "key");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_preserves_oversized_bucket_name() {
        let header = format!("/{}/key", "a".repeat(64));
        let (bucket, key, vid) = parse_copy_source(&header).unwrap();
        assert_eq!(bucket, "a".repeat(64));
        assert_eq!(key.as_str(), "key");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_rejects_oversized_key() {
        let header = format!("/bucket/{}", "x".repeat(1025));
        match parse_copy_source(&header) {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid copy source object key");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_copy_source_rejects_percent_encoded_nul_in_key() {
        match parse_copy_source("/bucket/key%00name") {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid copy source object key");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn parse_copy_source_rejects_invalid_percent_decoding_in_bucket() {
        match parse_copy_source("/bucket%80/key") {
            Err(ServerError::InvalidArgument { reason }) => {
                assert_eq!(reason, "Invalid copy source object key");
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn from_hyper_accepts_utf8_non_ascii_headers() {
        let mut headers = http::HeaderMap::new();
        // "café" in UTF-8: 0x63 0x61 0x66 0xC3 0xA9
        headers.insert(
            "x-amz-meta-tag",
            http::HeaderValue::from_bytes(b"caf\xc3\xa9").unwrap(),
        );
        let uri = http::Uri::from_static("/bucket/key");
        let (mut parts, ()) = http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(())
            .unwrap()
            .into_parts();
        parts.headers = headers;
        let req =
            S3Request::from_hyper(parts, bytes::Bytes::new(), TransportSecurity::Tls, 0).unwrap();
        assert_eq!(req.header("x-amz-meta-tag"), Some("caf\u{e9}"));
    }

    #[test]
    fn from_hyper_rejects_non_utf8_obs_text() {
        // Raw 0x80 is valid obs-text but not valid UTF-8.
        // Rejected because SigV4 canonicalization operates on Strings;
        // supporting raw obs-text would require a byte-level header
        // representation through the auth layer.
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-amz-meta-raw",
            http::HeaderValue::from_bytes(b"val\x80").unwrap(),
        );
        let uri = http::Uri::from_static("/bucket/key");
        let (mut parts, ()) = http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(())
            .unwrap()
            .into_parts();
        parts.headers = headers;
        match S3Request::from_hyper(parts, bytes::Bytes::new(), TransportSecurity::Tls, 0) {
            Err(ServerError::InvalidRequest { reason }) => {
                assert!(
                    reason.contains("invalid UTF-8") && reason.contains("x-amz-meta-raw"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("expected InvalidRequest, got {:?}", other.err()),
        }
    }
}
