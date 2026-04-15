/// Multipart form-data parser for S3 POST Object.
use crate::error::ServerError;
use storage::ObjectKey;

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
#[must_use]
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

/// Parse the Content-Disposition header to extract the field name and optional filename.
pub(crate) fn parse_content_disposition(
    headers: &str,
) -> Result<(String, Option<String>), ServerError> {
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

    let name = name.ok_or_else(|| ServerError::MalformedPOSTRequest {
        reason: "The body of your POST request is not well-formed multipart/form-data.".to_string(),
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

impl PostFormData {
    /// Get a form field value by name (case-insensitive).
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Resolve the object key, performing ${filename} substitution.
    pub fn resolve_key(&self) -> Result<ObjectKey, ServerError> {
        let key = self
            .field("key")
            .ok_or_else(|| ServerError::InvalidRequest {
                reason: "POST form missing 'key' field".to_string(),
            })?;

        let resolved = if key.contains("${filename}") {
            let fname = self.file_name.as_deref().unwrap_or("");
            // Use only the filename portion (strip directory components)
            let basename = fname
                .rsplit_once('/')
                .map(|(_, f)| f)
                .or_else(|| fname.rsplit_once('\\').map(|(_, f)| f))
                .unwrap_or(fname);
            key.replace("${filename}", basename)
        } else {
            key.to_string()
        };

        ObjectKey::try_from(resolved).map_err(|error| match error {
            storage::ObjectKeyError::InvalidLength { length } if length > 1024 => {
                ServerError::KeyTooLongError {
                    size: length,
                    max_size_allowed: 1024,
                }
            }
            storage::ObjectKeyError::InvalidLength { .. }
            | storage::ObjectKeyError::ContainsNullByte => ServerError::InvalidRequest {
                reason: error.to_string(),
            },
        })
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
        assert_eq!(extract_boundary("text/plain; boundary=abc"), None);
    }

    #[test]
    fn resolve_key_filename_substitution() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "uploads/${filename}".to_string())],
            file_data: vec![],
            file_name: Some("photo.jpg".to_string()),
        };
        assert_eq!(data.resolve_key().unwrap().as_str(), "uploads/photo.jpg");
    }

    #[test]
    fn resolve_key_no_substitution() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "mykey.txt".to_string())],
            file_data: vec![],
            file_name: None,
        };
        assert_eq!(data.resolve_key().unwrap().as_str(), "mykey.txt");
    }

    #[test]
    fn resolve_key_strips_directory() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "${filename}".to_string())],
            file_data: vec![],
            file_name: Some("C:\\Users\\alice\\photo.jpg".to_string()),
        };
        assert_eq!(data.resolve_key().unwrap().as_str(), "photo.jpg");
    }

    #[test]
    fn resolve_key_empty_filename_is_rejected() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "${filename}".to_string())],
            file_data: vec![],
            file_name: None,
        };
        match data.resolve_key() {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(reason, "object key must be 1-1024 bytes, got 0");
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn resolve_key_rejects_oversized_key() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "x".repeat(1025))],
            file_data: vec![],
            file_name: None,
        };
        match data.resolve_key() {
            Err(ServerError::KeyTooLongError {
                size,
                max_size_allowed,
            }) => {
                assert_eq!(size, 1025);
                assert_eq!(max_size_allowed, 1024);
            }
            other => panic!("expected KeyTooLongError, got {other:?}"),
        }
    }

    #[test]
    fn resolve_key_rejects_nul() {
        let data = PostFormData {
            fields: vec![("key".to_string(), "bad\0key".to_string())],
            file_data: vec![],
            file_name: None,
        };
        match data.resolve_key() {
            Err(ServerError::InvalidRequest { reason }) => {
                assert_eq!(reason, "object key must not contain null bytes");
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }
}
