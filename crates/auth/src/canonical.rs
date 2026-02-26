/// Canonical request construction per AWS SigV4 spec.
use ring::digest;

/// SHA-256 hash as lowercase hex string.
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = digest::digest(&digest::SHA256, data);
    hex_encode(hash.as_ref())
}

/// Percent-encode a value per SigV4 rules (RFC 3986 unreserved chars only).
/// Encodes everything except A-Z, a-z, 0-9, '-', '_', '.', '~'.
pub fn uri_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push(HEX_UPPER[(byte >> 4) as usize] as char);
                encoded.push(HEX_UPPER[(byte & 0x0f) as usize] as char);
            }
        }
    }
    encoded
}

/// Percent-encode a URI path, preserving '/' separators.
pub fn uri_encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| uri_encode(segment))
        .collect::<Vec<_>>()
        .join("/")
}

/// Build the canonical request string per SigV4 spec.
///
/// Parameters:
/// - `method`: HTTP method (e.g. "GET", "PUT")
/// - `uri`: URI path (e.g. "/mybucket/mykey")
/// - `query`: Canonical query string (sorted key=value pairs joined by &)
/// - `headers`: Canonical headers string (lowercase key:trimmed-value\n for each)
/// - `signed_headers`: Semicolon-separated list of signed header names
/// - `body_hash`: SHA-256 hex hash of the request body
pub fn canonical_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &str,
    signed_headers: &str,
    body_hash: &str,
) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        uri_encode_path(uri),
        query,
        headers,
        signed_headers,
        body_hash
    )
}

/// Build the canonical headers string from a list of (name, value) pairs.
/// Sorts by header name, combines duplicate headers with comma-separated values,
/// and collapses interior whitespace per the SigV4 spec.
pub fn canonical_headers(headers: &[(&str, &str)]) -> String {
    // Sort by header name. sort_by_key is a STABLE sort in Rust, so
    // duplicate header values retain their original request order as
    // required by SigV4.
    let mut sorted: Vec<(&str, &str)> = headers.to_vec();
    sorted.sort_by_key(|(name, _)| *name);

    let mut result = String::new();
    let mut i = 0;
    while i < sorted.len() {
        let name = sorted[i].0;
        result.push_str(name);
        result.push(':');
        // Collect all values for this header name
        result.push_str(&normalize_header_value(sorted[i].1));
        i += 1;
        while i < sorted.len() && sorted[i].0 == name {
            result.push(',');
            result.push_str(&normalize_header_value(sorted[i].1));
            i += 1;
        }
        result.push('\n');
    }
    result
}

/// Trim leading/trailing whitespace and collapse interior runs of whitespace
/// to a single space, per SigV4 canonical header value rules.
fn normalize_header_value(value: &str) -> String {
    let trimmed = value.trim();
    let mut result = String::with_capacity(trimmed.len());
    let mut prev_was_space = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_whitespace() {
            if !prev_was_space {
                result.push(' ');
                prev_was_space = true;
            }
        } else {
            result.push(ch);
            prev_was_space = false;
        }
    }
    result
}

/// Build the canonical query string from raw query string.
/// Per SigV4: percent-decode raw pairs first, then re-encode with SigV4 rules.
/// This avoids double-encoding when the incoming URL already has %XX sequences.
pub fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let val = parts.next().unwrap_or("");
            (uri_encode(&percent_decode(key)), uri_encode(&percent_decode(val)))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-decode a string (RFC 3986). Does NOT treat + as space.
fn percent_decode(s: &str) -> String {
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
    String::from_utf8_lossy(&result).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Build the string-to-sign per SigV4 spec.
///
/// Parameters:
/// - `timestamp`: ISO 8601 timestamp (e.g. "20130524T000000Z")
/// - `scope`: Credential scope string (e.g. "20130524/us-east-1/s3/aws4_request")
/// - `canonical_request_hash`: SHA-256 hex hash of the canonical request
pub fn string_to_sign(timestamp: &str, scope: &str, canonical_request_hash: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp, scope, canonical_request_hash
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX_LOWER[(b >> 4) as usize] as char);
        s.push(HEX_LOWER[(b & 0x0f) as usize] as char);
    }
    s
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";
const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_empty() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn uri_encode_simple() {
        assert_eq!(uri_encode("hello"), "hello");
        assert_eq!(uri_encode("hello world"), "hello%20world");
        assert_eq!(uri_encode("a+b=c"), "a%2Bb%3Dc");
    }

    #[test]
    fn uri_encode_preserves_unreserved() {
        assert_eq!(uri_encode("test-file_name.txt~"), "test-file_name.txt~");
    }

    #[test]
    fn uri_encode_unicode() {
        // UTF-8 bytes get percent-encoded
        assert_eq!(uri_encode("\u{00e9}"), "%C3%A9");
    }

    #[test]
    fn uri_encode_path_preserves_slashes() {
        assert_eq!(uri_encode_path("/bucket/my key"), "/bucket/my%20key");
    }

    #[test]
    fn canonical_query_string_sorts() {
        assert_eq!(
            canonical_query_string("b=2&a=1"),
            "a=1&b=2"
        );
    }

    #[test]
    fn canonical_query_string_empty() {
        assert_eq!(canonical_query_string(""), "");
    }

    #[test]
    fn canonical_query_string_encodes() {
        assert_eq!(
            canonical_query_string("key=val ue"),
            "key=val%20ue"
        );
    }

    #[test]
    fn canonical_headers_sorts_by_name() {
        let headers = [
            ("x-amz-date", "20130524T000000Z"),
            ("host", "example.com"),
            ("content-type", "text/plain"),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(
            result,
            "content-type:text/plain\nhost:example.com\nx-amz-date:20130524T000000Z\n"
        );
    }

    #[test]
    fn canonical_headers_combines_duplicates() {
        let headers = [
            ("host", "example.com"),
            ("x-amz-meta-tag", "alpha"),
            ("x-amz-meta-tag", "beta"),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(
            result,
            "host:example.com\nx-amz-meta-tag:alpha,beta\n"
        );
    }

    #[test]
    fn canonical_headers_trims_whitespace() {
        let headers = [
            ("host", "  example.com  "),
            ("content-type", " text/plain "),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(
            result,
            "content-type:text/plain\nhost:example.com\n"
        );
    }

    #[test]
    fn canonical_headers_collapses_interior_whitespace() {
        let headers = [
            ("host", "example.com"),
            ("x-amz-meta-desc", "  hello   world  foo  "),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(
            result,
            "host:example.com\nx-amz-meta-desc:hello world foo\n"
        );
    }

    #[test]
    fn canonical_query_string_no_double_encode() {
        // prefix=photos%2F should NOT become prefix=photos%252F
        assert_eq!(
            canonical_query_string("prefix=photos%2F"),
            "prefix=photos%2F"
        );
    }

    #[test]
    fn canonical_query_string_pre_encoded_mixed() {
        // Mix of encoded and unencoded values
        assert_eq!(
            canonical_query_string("key=hello%20world&b=2&a=1"),
            "a=1&b=2&key=hello%20world"
        );
    }
}
