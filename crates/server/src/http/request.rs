/// Parse tiny_http::Request into structured S3 request data.

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
    pub fn from_http(mut request: tiny_http::Request) -> (Self, tiny_http::Request) {
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

        // Read body
        let content_length = request
            .body_length()
            .unwrap_or(0);
        let mut body = Vec::with_capacity(content_length);
        let _ = request.as_reader().read_to_end(&mut body);

        let s3req = S3Request {
            method,
            path,
            query_string,
            headers,
            body,
        };

        // We need to return a way to respond. Since we consumed the reader,
        // the request object is still usable for respond().
        // However, tiny_http::Request consumes self in respond().
        // We'll restructure: parse first, then pass request back for responding.
        // Actually, we can't easily return the request since we read the body.
        // Let's restructure to not own the request.
        (s3req, request)
    }

    /// Get a header value by lowercase name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Get the body hash (x-amz-content-sha256 header, or compute from body).
    pub fn body_hash(&self) -> String {
        if let Some(hash) = self.header("x-amz-content-sha256") {
            if hash != "UNSIGNED-PAYLOAD" {
                return hash.to_string();
            }
        }
        auth::canonical::sha256_hex(&self.body)
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
                    Some(url_decode(val))
                } else {
                    None
                }
            })
    }
}

/// Basic URL decoding (percent-decode).
fn url_decode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (
                hex_val(bytes[i + 1]),
                hex_val(bytes[i + 2]),
            ) {
                result.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            result.push(b' ');
        } else {
            result.push(bytes[i]);
        }
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
