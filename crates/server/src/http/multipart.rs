/// Multipart form-data parser for S3 POST Object.
use crate::error::ServerError;

/// Parsed multipart form data from a POST Object request.
pub struct PostFormData {
    /// Non-file form fields in order (name, value).
    pub fields: Vec<(String, String)>,
    /// The file content bytes.
    pub file_data: Vec<u8>,
    /// The filename from the file field's Content-Disposition, if present.
    pub file_name: Option<String>,
}

/// Extract the multipart boundary string from a Content-Type header value.
pub fn extract_boundary(content_type: &str) -> Option<&str> {
    if !content_type
        .split(';')
        .next()?
        .trim()
        .eq_ignore_ascii_case("multipart/form-data")
    {
        return None;
    }
    for part in content_type.split(';').skip(1) {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("boundary=") {
            // Strip optional quotes
            let val = val.trim_matches('"');
            if !val.is_empty() {
                return Some(val);
            }
        }
    }
    None
}

/// Parse a multipart/form-data body into form fields and file data.
pub fn parse_multipart(body: &[u8], boundary: &str) -> Result<PostFormData, ServerError> {
    let dash_boundary = format!("--{boundary}");
    let db = dash_boundary.as_bytes();

    // Find the first boundary
    let start = find_bytes(body, db).ok_or_else(|| ServerError::InvalidRequest {
        reason: "missing multipart boundary".to_string(),
    })?;
    let mut pos = start + db.len();

    // After first boundary, expect \r\n or --
    if body.get(pos..pos + 2) == Some(b"--") {
        // Empty form
        return Err(ServerError::InvalidRequest {
            reason: "empty multipart form".to_string(),
        });
    }
    if body.get(pos..pos + 2) == Some(b"\r\n") {
        pos += 2;
    }

    let mut fields = Vec::new();
    let mut file_data = Vec::new();
    let mut file_name = None;
    let mut found_file = false;

    loop {
        // Find the next boundary: \r\n--boundary
        let search = format!("\r\n{dash_boundary}");
        let search_bytes = search.as_bytes();
        let end = find_bytes(&body[pos..], search_bytes);

        match end {
            None => {
                // No more boundaries — malformed
                if !found_file {
                    return Err(ServerError::InvalidRequest {
                        reason: "missing file field in multipart form".to_string(),
                    });
                }
                break;
            }
            Some(end_offset) => {
                let part = &body[pos..pos + end_offset];
                parse_part(
                    part,
                    &mut fields,
                    &mut file_data,
                    &mut file_name,
                    &mut found_file,
                )?;

                pos = pos + end_offset + search_bytes.len();
                // Check if it's the final boundary (--)
                if body.get(pos..pos + 2) == Some(b"--") {
                    break;
                }
                // Skip \r\n after boundary
                if body.get(pos..pos + 2) == Some(b"\r\n") {
                    pos += 2;
                }
            }
        }
    }

    if !found_file {
        return Err(ServerError::InvalidRequest {
            reason: "missing file field in multipart form".to_string(),
        });
    }

    Ok(PostFormData {
        fields,
        file_data,
        file_name,
    })
}

/// Parse a single multipart part (headers + body).
fn parse_part(
    part: &[u8],
    fields: &mut Vec<(String, String)>,
    file_data: &mut Vec<u8>,
    file_name: &mut Option<String>,
    found_file: &mut bool,
) -> Result<(), ServerError> {
    // Find the header/body separator \r\n\r\n
    let header_end = find_bytes(part, b"\r\n\r\n").ok_or_else(|| ServerError::InvalidRequest {
        reason: "malformed multipart part: missing header separator".to_string(),
    })?;

    let header_bytes = &part[..header_end];
    let body = &part[header_end + 4..];

    // Parse Content-Disposition to get name and optional filename
    let header_str = std::str::from_utf8(header_bytes).map_err(|_| ServerError::InvalidRequest {
        reason: "invalid UTF-8 in multipart headers".to_string(),
    })?;

    let (name, fname) = parse_content_disposition(header_str)?;

    if name.eq_ignore_ascii_case("file") {
        *found_file = true;
        *file_data = body.to_vec();
        *file_name = fname;
    } else {
        let value =
            std::str::from_utf8(body).map_err(|_| ServerError::InvalidRequest {
                reason: format!("invalid UTF-8 in form field '{name}'"),
            })?;
        fields.push((name, value.to_string()));
    }

    Ok(())
}

/// Parse the Content-Disposition header to extract the field name and optional filename.
fn parse_content_disposition(headers: &str) -> Result<(String, Option<String>), ServerError> {
    let mut name = None;
    let mut filename = None;

    for line in headers.split("\r\n") {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Content-Disposition:") {
            let rest = rest.trim();
            // Parse params: form-data; name="key"; filename="photo.jpg"
            for param in rest.split(';') {
                let param = param.trim();
                if let Some(val) = param.strip_prefix("name=") {
                    name = Some(unquote(val));
                } else if let Some(val) = param.strip_prefix("filename=") {
                    filename = Some(unquote(val));
                }
            }
        }
    }

    let name = name.ok_or_else(|| ServerError::InvalidRequest {
        reason: "multipart part missing name in Content-Disposition".to_string(),
    })?;

    Ok((name, filename))
}

/// Remove surrounding double quotes from a value.
fn unquote(s: &str) -> String {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Find a byte pattern in a byte slice. Returns the offset of the first occurrence.
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

impl PostFormData {
    /// Get a form field value by name (case-insensitive).
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Resolve the object key, performing ${filename} substitution.
    pub fn resolve_key(&self) -> Result<String, ServerError> {
        let key = self
            .field("key")
            .ok_or_else(|| ServerError::InvalidRequest {
                reason: "POST form missing 'key' field".to_string(),
            })?;

        if key.contains("${filename}") {
            let fname = self.file_name.as_deref().unwrap_or("");
            // Use only the filename portion (strip directory components)
            let basename = fname
                .rsplit_once('/')
                .map(|(_, f)| f)
                .or_else(|| fname.rsplit_once('\\').map(|(_, f)| f))
                .unwrap_or(fname);
            Ok(key.replace("${filename}", basename))
        } else {
            Ok(key.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_boundary_basic() {
        assert_eq!(
            extract_boundary("multipart/form-data; boundary=abc123"),
            Some("abc123")
        );
    }

    #[test]
    fn extract_boundary_quoted() {
        assert_eq!(
            extract_boundary("multipart/form-data; boundary=\"abc123\""),
            Some("abc123")
        );
    }

    #[test]
    fn extract_boundary_case_insensitive() {
        assert_eq!(
            extract_boundary("Multipart/Form-Data; boundary=xyz"),
            Some("xyz")
        );
    }

    #[test]
    fn extract_boundary_no_boundary() {
        assert_eq!(extract_boundary("multipart/form-data"), None);
    }

    #[test]
    fn extract_boundary_wrong_type() {
        assert_eq!(
            extract_boundary("text/plain; boundary=abc"),
            None
        );
    }

    #[test]
    fn parse_simple_form() {
        let boundary = "----WebKitBoundary";
        let body =
            "------WebKitBoundary\r\n\
             Content-Disposition: form-data; name=\"key\"\r\n\
             \r\n\
             test.txt\r\n\
             ------WebKitBoundary\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n\
             Content-Type: text/plain\r\n\
             \r\n\
             hello world\r\n\
             ------WebKitBoundary--\r\n"
                .to_string();
        let result = parse_multipart(body.as_bytes(), boundary).unwrap();
        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].0, "key");
        assert_eq!(result.fields[0].1, "test.txt");
        assert_eq!(result.file_data, b"hello world");
        assert_eq!(result.file_name.as_deref(), Some("test.txt"));
    }

    #[test]
    fn parse_multiple_fields() {
        let boundary = "boundary";
        let body =
            "--boundary\r\n\
             Content-Disposition: form-data; name=\"key\"\r\n\
             \r\n\
             foo.txt\r\n\
             --boundary\r\n\
             Content-Disposition: form-data; name=\"acl\"\r\n\
             \r\n\
             private\r\n\
             --boundary\r\n\
             Content-Disposition: form-data; name=\"Content-Type\"\r\n\
             \r\n\
             text/plain\r\n\
             --boundary\r\n\
             Content-Disposition: form-data; name=\"file\"\r\n\
             \r\n\
             bar\r\n\
             --boundary--\r\n"
                .to_string();
        let result = parse_multipart(body.as_bytes(), boundary).unwrap();
        assert_eq!(result.fields.len(), 3);
        assert_eq!(result.field("key"), Some("foo.txt"));
        assert_eq!(result.field("acl"), Some("private"));
        assert_eq!(result.field("Content-Type"), Some("text/plain"));
        assert_eq!(result.file_data, b"bar");
    }

    #[test]
    fn resolve_key_filename_substitution() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "uploads/${filename}".to_string())],
            file_data: vec![],
            file_name: Some("photo.jpg".to_string()),
        };
        assert_eq!(data.resolve_key().unwrap(), "uploads/photo.jpg");
    }

    #[test]
    fn resolve_key_no_substitution() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "mykey.txt".to_string())],
            file_data: vec![],
            file_name: None,
        };
        assert_eq!(data.resolve_key().unwrap(), "mykey.txt");
    }

    #[test]
    fn resolve_key_strips_directory() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "${filename}".to_string())],
            file_data: vec![],
            file_name: Some("C:\\Users\\alice\\photo.jpg".to_string()),
        };
        assert_eq!(data.resolve_key().unwrap(), "photo.jpg");
    }

    #[test]
    fn missing_file_field() {
        let boundary = "boundary";
        let body = "--boundary\r\n\
                     Content-Disposition: form-data; name=\"key\"\r\n\
                     \r\n\
                     test\r\n\
                     --boundary--\r\n";
        let result = parse_multipart(body.as_bytes(), boundary);
        assert!(result.is_err());
    }

    #[test]
    fn binary_file_data() {
        let boundary = "boundary";
        let file_bytes: Vec<u8> = vec![0x00, 0x01, 0xFF, 0xFE, 0x80];
        let mut body = b"--boundary\r\nContent-Disposition: form-data; name=\"key\"\r\n\r\nk\r\n--boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"bin\"\r\n\r\n".to_vec();
        body.extend_from_slice(&file_bytes);
        body.extend_from_slice(b"\r\n--boundary--\r\n");
        let result = parse_multipart(&body, boundary).unwrap();
        assert_eq!(result.file_data, file_bytes);
    }
}
