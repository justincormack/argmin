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
/// Headers must already be lowercase and sorted by name.
pub fn canonical_headers(headers: &[(&str, &str)]) -> String {
    let mut result = String::new();
    for (name, value) in headers {
        result.push_str(name);
        result.push(':');
        result.push_str(value.trim());
        result.push('\n');
    }
    result
}

/// Build the canonical query string from raw query string.
/// Parses, sorts by key then value, and re-encodes.
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
            (uri_encode(key), uri_encode(val))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
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
}
