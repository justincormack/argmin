/// Parse HTTP requests into structured S3 request data.
use std::borrow::Cow;

use crate::error::ServerError;
use auth::HeaderSource;

/// Maximum body size for the buffered request path.
///
/// The buffered path is used for control-plane style requests (primarily XML
/// payloads such as `DeleteObjects`, versioning/CORS config, and
/// `CompleteMultipartUpload`). Data-plane writes (`PutObject`, `UploadPart`,
/// and POST Object file uploads) are routed through streaming handlers and are
/// not limited by this constant.
pub(crate) const MAX_BUFFERED_CONTROL_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Validate a Content-Length header value. Rejects negative and non-numeric values.
fn validate_content_length(value: &str) -> Result<u64, ServerError> {
    value
        .parse::<u64>()
        .map_err(|_| ServerError::InvalidRequest {
            reason: format!("invalid Content-Length: {value}"),
        })
}

/// Parsed S3 request data extracted from an HTTP request.
pub struct S3Request {
    pub method: http::Method,
    pub uri: http::Uri,
    pub headers: http::HeaderMap,
    pub body: Vec<u8>,
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

impl S3Request {
    /// Parse hyper request parts and collected body into an `S3Request`.
    ///
    /// Body size limiting is done by the caller (serve layer) via `http_body_util::Limited`.
    pub fn from_hyper(
        parts: http::request::Parts,
        body: bytes::Bytes,
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
        })
    }

    /// Parse hyper request parts into an `S3Request` with an empty body.
    ///
    /// Used by the streaming write path where the body is consumed frame-by-frame
    /// rather than collected upfront. Auth works because the `x-amz-content-sha256`
    /// header provides the body hash (typically `UNSIGNED-PAYLOAD`).
    pub fn from_hyper_headers(parts: http::request::Parts) -> Result<Self, ServerError> {
        Self::from_hyper(parts, bytes::Bytes::new())
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
        }
    }

    /// Get headers as borrowed pairs for auth verification.
    #[cfg(test)]
    #[must_use]
    pub fn header_pairs(&self) -> Vec<(&str, &str)> {
        self.header_iter().collect()
    }

    /// Get query parameter by name.
    #[must_use]
    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query_param_lossy(name).map(Cow::into_owned)
    }

    #[must_use]
    pub fn query_param_lossy(&self, name: &str) -> Option<Cow<'_, str>> {
        self.query_string()
            .split('&')
            .filter(|s| !s.is_empty())
            .find_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next()?;
                let val = parts.next().unwrap_or("");
                if key == name {
                    Some(percent_decode_lossy(val))
                } else {
                    None
                }
            })
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn new_for_test(
        method: http::Method,
        path: &str,
        query_string: &str,
        headers: http::HeaderMap,
        body: Vec<u8>,
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
        }
    }
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
/// Both bucket and key are percent-decoded. Returns the raw `versionId`
/// query-parameter value (if present) as-is.
pub(crate) fn parse_copy_source(
    header: &str,
) -> Result<(String, String, Option<String>), ServerError> {
    // Strip optional leading slash
    let s = header.strip_prefix('/').unwrap_or(header);

    // Extract ?versionId=... query string
    let (s, version_id) = match s.find('?') {
        Some(pos) => {
            let query = &s[pos + 1..];
            let vid = query
                .split('&')
                .find_map(|param| param.strip_prefix("versionId="))
                .map(std::string::ToString::to_string);
            (&s[..pos], vid)
        }
        None => (s, None),
    };

    // Split into bucket/key at first '/'
    let slash_pos = s.find('/').ok_or_else(|| ServerError::InvalidArgument {
        reason: "Invalid copy source object key".to_string(),
    })?;

    let bucket =
        percent_decode_strict(&s[..slash_pos]).map_err(|_| ServerError::InvalidArgument {
            reason: "Invalid copy source object key".to_string(),
        })?;
    let key =
        percent_decode_strict(&s[slash_pos + 1..]).map_err(|_| ServerError::InvalidArgument {
            reason: "Invalid copy source object key".to_string(),
        })?;

    if key.is_empty() {
        return Err(ServerError::InvalidArgument {
            reason: "Invalid copy source object key".to_string(),
        });
    }

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
        );
        assert_eq!(req.query_param("list-type"), Some("2".to_string()));
        assert_eq!(req.query_param("prefix"), Some("photos/".to_string()));
        assert_eq!(req.query_param("max-keys"), Some("10".to_string()));
        assert_eq!(req.query_param("missing"), None);
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
        let req = S3Request::new_for_test(http::Method::GET, "/", "", test_headers(vec![]), vec![]);
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
        );
        assert_eq!(req.header("content-type"), None);
        assert_eq!(req.header("host"), Some("example.com"));
    }

    // ── parse_copy_source ────────────────────────────────────────────

    #[test]
    fn parse_copy_source_basic() {
        let (bucket, key, vid) = parse_copy_source("/bucket/key").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "key");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_no_leading_slash() {
        let (bucket, key, vid) = parse_copy_source("bucket/key").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "key");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_encoded() {
        let (bucket, key, vid) = parse_copy_source("/bucket/key%20name").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "key name");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_nested_key() {
        let (bucket, key, vid) = parse_copy_source("/bucket/a/b/c").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "a/b/c");
        assert_eq!(vid, None);
    }

    #[test]
    fn parse_copy_source_version_id() {
        let (bucket, key, vid) = parse_copy_source("/bucket/key?versionId=xyz").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "key");
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
        let req = S3Request::from_hyper(parts, bytes::Bytes::new()).unwrap();
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
        match S3Request::from_hyper(parts, bytes::Bytes::new()) {
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
