/// Parse tiny_http::Request into structured S3 request data.
use std::io::Read;

use crate::error::ServerError;

/// Maximum request body size (256 MB + headroom for metadata blob).
/// Checked before reading the body to prevent OOM.
const MAX_BODY_SIZE: usize = 256 * 1024 * 1024 + 64 * 1024;

/// Parsed S3 request data extracted from an HTTP request.
pub struct S3Request {
    pub method: String,
    pub path: String,
    pub query_string: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl S3Request {
    /// Parse a tiny_http::Request into an S3Request.
    ///
    /// Checks Content-Length before reading the body to prevent OOM.
    /// Returns an error if the body exceeds the size limit or cannot be read.
    pub fn from_http(
        mut request: tiny_http::Request,
    ) -> Result<(Self, tiny_http::Request), (ServerError, tiny_http::Request)> {
        let method = request.method().as_str().to_string();
        let url = request.url().to_string();

        // Split URL into path and query
        let (path, query_string) = match url.find('?') {
            Some(pos) => (url[..pos].to_string(), url[pos + 1..].to_string()),
            None => (url, String::new()),
        };

        // Extract headers as lowercase name/value pairs
        let headers: Vec<(String, String)> = request
            .headers()
            .iter()
            .map(|h| {
                (
                    h.field.as_str().as_str().to_ascii_lowercase(),
                    h.value.as_str().to_string(),
                )
            })
            .collect();

        // Check Content-Length before reading body
        let content_length = request.body_length().unwrap_or(0);
        if content_length > MAX_BODY_SIZE {
            return Err((
                ServerError::ObjectTooLarge {
                    size: content_length as u64,
                    max: MAX_BODY_SIZE as u64,
                },
                request,
            ));
        }

        // Read body with bounded size
        let mut body = Vec::with_capacity(content_length);
        let mut reader = request.as_reader().take(MAX_BODY_SIZE as u64 + 1);
        if let Err(_) = reader.read_to_end(&mut body) {
            return Err((
                ServerError::InvalidRequest {
                    reason: "failed to read request body".to_string(),
                },
                request,
            ));
        }

        // Double-check actual bytes read (handles chunked transfer without Content-Length)
        if body.len() > MAX_BODY_SIZE {
            return Err((
                ServerError::ObjectTooLarge {
                    size: body.len() as u64,
                    max: MAX_BODY_SIZE as u64,
                },
                request,
            ));
        }

        let s3req = S3Request {
            method,
            path,
            query_string,
            headers,
            body,
        };

        Ok((s3req, request))
    }

    /// Get a header value by lowercase name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Get the body hash for SigV4 canonical request.
    ///
    /// If the client sends `x-amz-content-sha256: UNSIGNED-PAYLOAD`, the literal
    /// string "UNSIGNED-PAYLOAD" is used in the canonical request (not the actual
    /// body hash). Otherwise, use the provided hash or compute from body.
    pub fn body_hash(&self) -> String {
        match self.header("x-amz-content-sha256") {
            Some("UNSIGNED-PAYLOAD") => "UNSIGNED-PAYLOAD".to_string(),
            Some(hash) => hash.to_string(),
            None => auth::canonical::sha256_hex(&self.body),
        }
    }

    /// Get headers as borrowed pairs for auth verification.
    pub fn header_pairs(&self) -> Vec<(&str, &str)> {
        self.headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Get query parameter by name.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_hash_unsigned_payload() {
        let req = S3Request {
            method: "PUT".to_string(),
            path: "/bucket/key".to_string(),
            query_string: String::new(),
            headers: vec![
                (
                    "x-amz-content-sha256".to_string(),
                    "UNSIGNED-PAYLOAD".to_string(),
                ),
            ],
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
            headers: vec![(
                "x-amz-content-sha256".to_string(),
                "abc123".to_string(),
            )],
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
}
