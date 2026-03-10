/// Parse HTTP requests into structured S3 request data.
use crate::error::ServerError;

/// Maximum request body size (256 MB + headroom for metadata blob).
/// Used by the serve layer for body size limiting.
pub(crate) const MAX_BODY_SIZE: usize = 256 * 1024 * 1024 + 64 * 1024;

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
    pub method: String,
    pub path: String,
    pub query_string: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl S3Request {
    /// Parse hyper request parts and collected body into an `S3Request`.
    ///
    /// Body size limiting is done by the caller (serve layer) via `http_body_util::Limited`.
    pub fn from_hyper(
        parts: &http::request::Parts,
        body: bytes::Bytes,
    ) -> Result<Self, ServerError> {
        let method = parts.method.as_str().to_string();
        let path = parts.uri.path().to_string();
        let query_string = parts.uri.query().unwrap_or("").to_string();

        // Extract headers as lowercase name/value pairs.
        // to_str() only accepts visible ASCII. For non-ASCII bytes (obs-text),
        // fall back to strict UTF-8. Non-UTF8 obs-text bytes (e.g. raw 0x80)
        // are rejected here — supporting them would require carrying raw bytes
        // through SigV4 canonicalization, which currently operates on Strings.
        // In practice the AWS SDK always sends valid UTF-8. Latin-1
        // reinterpretation for x-amz-meta-* storage happens later in
        // MetadataBlob::from_headers().
        let mut headers = Vec::with_capacity(parts.headers.len());
        for (name, value) in &parts.headers {
            let val_str = match value.to_str() {
                Ok(s) => s.to_string(),
                Err(_) => std::str::from_utf8(value.as_bytes())
                    .map_err(|_| ServerError::InvalidRequest {
                        reason: format!("invalid UTF-8 in header value for {name}"),
                    })?
                    .to_string(),
            };
            headers.push((name.as_str().to_string(), val_str));
        }

        // Validate Content-Length header if present (reject negative/non-numeric)
        if let Some((_, cl_value)) = headers.iter().find(|(k, _)| k == "content-length") {
            validate_content_length(cl_value)?;
        }

        let body = body.to_vec();

        Ok(S3Request {
            method,
            path,
            query_string,
            headers,
            body,
        })
    }

    /// Parse hyper request parts into an `S3Request` with an empty body.
    ///
    /// Used by the streaming write path where the body is consumed frame-by-frame
    /// rather than collected upfront. Auth works because the `x-amz-content-sha256`
    /// header provides the body hash (typically `UNSIGNED-PAYLOAD`).
    pub fn from_hyper_headers(parts: &http::request::Parts) -> Result<Self, ServerError> {
        Self::from_hyper(parts, bytes::Bytes::new())
    }

    /// Get a header value by lowercase name.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Get the body hash for `SigV4` canonical request.
    ///
    /// If the client sends `x-amz-content-sha256: UNSIGNED-PAYLOAD`, the literal
    /// string "UNSIGNED-PAYLOAD" is used in the canonical request (not the actual
    /// body hash). Otherwise, use the provided hash or compute from body.
    #[must_use]
    pub fn body_hash(&self) -> String {
        match self.header("x-amz-content-sha256") {
            Some("UNSIGNED-PAYLOAD") => "UNSIGNED-PAYLOAD".to_string(),
            Some(hash) => hash.to_string(),
            None => auth::canonical::sha256_hex(&self.body),
        }
    }

    /// Create a new `S3Request` with a decoded body and trailer headers,
    /// stripping `aws-chunked` from Content-Encoding.
    #[must_use]
    pub fn with_decoded_body(&self, body: Vec<u8>, trailers: Vec<(String, String)>) -> Self {
        let mut headers: Vec<(String, String)> = self
            .headers
            .iter()
            .map(|(k, v)| {
                if k == "content-encoding" {
                    // Strip "aws-chunked" from Content-Encoding.
                    let filtered: Vec<&str> = v
                        .split(',')
                        .map(str::trim)
                        .filter(|s| !s.eq_ignore_ascii_case("aws-chunked"))
                        .collect();
                    if filtered.is_empty() {
                        // Drop the header entirely if nothing remains.
                        return (k.clone(), String::new());
                    }
                    (k.clone(), filtered.join(", "))
                } else if k == "content-length" {
                    // Update to decoded length.
                    (k.clone(), body.len().to_string())
                } else {
                    (k.clone(), v.clone())
                }
            })
            .filter(|(k, v)| !(k == "content-encoding" && v.is_empty()))
            .collect();

        // Merge trailer headers: replace existing headers with same name,
        // or append if not present. This avoids duplicate checksum headers
        // when the trailer provides a value that was also in the initial headers.
        for (tk, tv) in trailers {
            if let Some(existing) = headers.iter_mut().find(|(k, _)| *k == tk) {
                existing.1 = tv;
            } else {
                headers.push((tk, tv));
            }
        }

        S3Request {
            method: self.method.clone(),
            path: self.path.clone(),
            query_string: self.query_string.clone(),
            headers,
            body,
        }
    }

    /// Get headers as borrowed pairs for auth verification.
    #[must_use]
    pub fn header_pairs(&self) -> Vec<(&str, &str)> {
        self.headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Get query parameter by name.
    #[must_use]
    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query_string
            .split('&')
            .filter(|s| !s.is_empty())
            .find_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next()?;
                let val = parts.next().unwrap_or("");
                if key == name {
                    Some(percent_decode(val))
                } else {
                    None
                }
            })
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
    String::from_utf8(bytes).map_err(|_| ServerError::InvalidRequest {
        reason: "Couldn't parse the specified URI.".to_string(),
    })
}

/// Percent-decode a string (RFC 3986). Does NOT treat + as space.
pub(crate) fn percent_decode(s: &str) -> String {
    String::from_utf8_lossy(&percent_decode_bytes(s)).to_string()
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
    let slash_pos = s.find('/').ok_or_else(|| ServerError::InvalidRequest {
        reason: "x-amz-copy-source must contain bucket/key".to_string(),
    })?;

    let bucket = percent_decode(&s[..slash_pos]);
    let key = percent_decode(&s[slash_pos + 1..]);

    if key.is_empty() {
        return Err(ServerError::InvalidRequest {
            reason: "x-amz-copy-source key must not be empty".to_string(),
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
mod tests {
    use super::*;

    #[test]
    fn body_hash_unsigned_payload() {
        let req = S3Request {
            method: "PUT".to_string(),
            path: "/bucket/key".to_string(),
            query_string: String::new(),
            headers: vec![(
                "x-amz-content-sha256".to_string(),
                "UNSIGNED-PAYLOAD".to_string(),
            )],
            body: b"some data".to_vec(),
        };
        assert_eq!(req.body_hash(), "UNSIGNED-PAYLOAD");
    }

    #[test]
    fn body_hash_with_explicit_hash() {
        let req = S3Request {
            method: "PUT".to_string(),
            path: "/bucket/key".to_string(),
            query_string: String::new(),
            headers: vec![("x-amz-content-sha256".to_string(), "abc123".to_string())],
            body: b"data".to_vec(),
        };
        assert_eq!(req.body_hash(), "abc123");
    }

    #[test]
    fn body_hash_computed_when_no_header() {
        let req = S3Request {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: vec![],
            body: vec![],
        };
        // SHA-256 of empty string
        assert_eq!(
            req.body_hash(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("a%2Fb"), "a/b");
        assert_eq!(percent_decode("no+encoding"), "no+encoding"); // + is NOT space
    }

    #[test]
    fn percent_decode_passthrough() {
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode(""), "");
    }

    #[test]
    fn query_param_lookup() {
        let req = S3Request {
            method: "GET".to_string(),
            path: "/bucket".to_string(),
            query_string: "list-type=2&prefix=photos%2F&max-keys=10".to_string(),
            headers: vec![],
            body: vec![],
        };
        assert_eq!(req.query_param("list-type"), Some("2".to_string()));
        assert_eq!(req.query_param("prefix"), Some("photos/".to_string()));
        assert_eq!(req.query_param("max-keys"), Some("10".to_string()));
        assert_eq!(req.query_param("missing"), None);
    }

    #[test]
    fn percent_decode_truncated_escape() {
        // % at end of string — not enough chars for a hex pair
        assert_eq!(percent_decode("abc%"), "abc%");
        assert_eq!(percent_decode("abc%2"), "abc%2");
    }

    #[test]
    fn percent_decode_non_hex_after_percent() {
        assert_eq!(percent_decode("%ZZ"), "%ZZ");
        assert_eq!(percent_decode("%GH"), "%GH");
    }

    #[test]
    fn percent_decode_uppercase_hex() {
        assert_eq!(percent_decode("%2F"), "/");
        assert_eq!(percent_decode("%2f"), "/");
        assert_eq!(percent_decode("%3A"), ":");
        assert_eq!(percent_decode("%3a"), ":");
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
        let req = S3Request {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: vec![],
            body: vec![],
        };
        assert_eq!(req.query_param("anything"), None);
    }

    #[test]
    fn query_param_no_equals() {
        let req = S3Request {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: "flagonly&key=val".to_string(),
            headers: vec![],
            body: vec![],
        };
        // "flagonly" with no = has empty value
        assert_eq!(req.query_param("flagonly"), Some(String::new()));
        assert_eq!(req.query_param("key"), Some("val".to_string()));
    }

    #[test]
    fn query_param_match_not_first() {
        let req = S3Request {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: "a=1&b=2&c=3".to_string(),
            headers: vec![],
            body: vec![],
        };
        assert_eq!(req.query_param("c"), Some("3".to_string()));
    }

    #[test]
    fn header_pairs_output() {
        let req = S3Request {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: vec![
                ("host".to_string(), "example.com".to_string()),
                ("content-type".to_string(), "text/plain".to_string()),
            ],
            body: vec![],
        };
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
        let req = S3Request {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: vec![("host".to_string(), "example.com".to_string())],
            body: vec![],
        };
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
        assert!(parse_copy_source("/bucket").is_err());
        assert!(parse_copy_source("bucket").is_err());
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
        let req = S3Request::from_hyper(&parts, bytes::Bytes::new()).unwrap();
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
        match S3Request::from_hyper(&parts, bytes::Bytes::new()) {
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
