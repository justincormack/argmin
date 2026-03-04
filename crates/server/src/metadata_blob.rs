/// C2 metadata blob: prepended to object data before EC encoding.
///
/// Wire format (V1):
/// ```text
/// metadata_len: u32 LE    — total blob length INCLUDING this 4-byte field
/// format_version: u8      — 1
/// entry_count: u16 LE     — number of key-value pairs
/// for each entry:
///   key_len: u16 LE
///   key: [u8; key_len]    — UTF-8
///   val_len: u16 LE
///   val: [u8; val_len]    — UTF-8
/// ```
use crate::error::ServerError;

const FORMAT_VERSION: u8 = 1;
/// Minimum blob size: 4 (len) + 1 (version) + 2 (count) = 7 bytes
const MIN_BLOB_SIZE: usize = 7;

/// A single metadata key-value entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

/// Metadata blob containing user-specified headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBlob {
    pub entries: Vec<MetadataEntry>,
}

/// Standard S3 headers that get stored in the metadata blob.
const STORED_HEADERS: &[&str] = &[
    "content-type",
    "content-encoding",
    "cache-control",
    "content-disposition",
    "content-language",
    "expires",
    // Checksum headers (stored so they can be returned with ChecksumMode=ENABLED)
    "x-amz-checksum-sha256",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-sha1",
    "x-amz-checksum-algorithm",
];

/// Check if a string contains bytes invalid in HTTP headers:
/// ASCII control characters (0x00-0x1F) or non-ASCII bytes (>= 0x7F).
fn has_invalid_header_bytes(s: &str) -> bool {
    s.bytes().any(|b| !(0x20..0x7f).contains(&b))
}

/// Strip `aws-chunked` from a comma-separated Content-Encoding value.
/// Returns `None` if nothing remains after stripping.
fn strip_aws_chunked(value: &str) -> Option<String> {
    let filtered: Vec<&str> = value
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    if filtered.is_empty() {
        None
    } else {
        Some(filtered.join(", "))
    }
}

impl MetadataBlob {
    /// Create an empty metadata blob.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Build a metadata blob from request headers.
    /// Extracts content-type, content-encoding, cache-control, content-disposition,
    /// content-language, expires, and all x-amz-meta-* headers.
    /// Rejects values containing control characters to prevent header injection.
    pub fn from_headers(headers: &[(&str, &str)]) -> Result<Self, ServerError> {
        let mut entries = Vec::new();
        for &(name, value) in headers {
            let lower = name.to_ascii_lowercase();
            if STORED_HEADERS.contains(&lower.as_str()) || lower.starts_with("x-amz-meta-") {
                if has_invalid_header_bytes(value) {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "metadata value for '{}' contains invalid header bytes",
                            lower
                        ),
                    });
                }
                if lower == "content-encoding" {
                    if let Some(filtered) = strip_aws_chunked(value) {
                        entries.push(MetadataEntry {
                            key: lower,
                            value: filtered,
                        });
                    }
                } else {
                    entries.push(MetadataEntry {
                        key: lower,
                        value: value.to_string(),
                    });
                }
            }
        }
        Ok(Self { entries })
    }

    /// Serialize the blob to bytes.
    pub fn serialize(&self) -> Result<Vec<u8>, ServerError> {
        // Validate field widths before serializing
        if self.entries.len() > u16::MAX as usize {
            return Err(ServerError::MetadataBlobError {
                reason: format!(
                    "too many metadata entries: {} (max {})",
                    self.entries.len(),
                    u16::MAX
                ),
            });
        }
        for entry in &self.entries {
            if entry.key.len() > u16::MAX as usize {
                return Err(ServerError::MetadataBlobError {
                    reason: format!(
                        "metadata key too long: {} bytes (max {})",
                        entry.key.len(),
                        u16::MAX
                    ),
                });
            }
            if entry.value.len() > u16::MAX as usize {
                return Err(ServerError::MetadataBlobError {
                    reason: format!(
                        "metadata value too long: {} bytes (max {})",
                        entry.value.len(),
                        u16::MAX
                    ),
                });
            }
        }

        // Calculate total size
        let mut body_size = 1 + 2; // version + entry_count
        for entry in &self.entries {
            body_size += 2 + entry.key.len() + 2 + entry.value.len();
        }
        let total_size = 4 + body_size; // include the length field itself

        if total_size > u32::MAX as usize {
            return Err(ServerError::MetadataBlobError {
                reason: "metadata blob too large".to_string(),
            });
        }

        let mut buf = Vec::with_capacity(total_size);

        // metadata_len (u32 LE)
        buf.extend_from_slice(&(total_size as u32).to_le_bytes());
        // format_version (u8)
        buf.push(FORMAT_VERSION);
        // entry_count (u16 LE)
        buf.extend_from_slice(&(self.entries.len() as u16).to_le_bytes());

        for entry in &self.entries {
            // key_len (u16 LE)
            buf.extend_from_slice(&(entry.key.len() as u16).to_le_bytes());
            // key bytes
            buf.extend_from_slice(entry.key.as_bytes());
            // val_len (u16 LE)
            buf.extend_from_slice(&(entry.value.len() as u16).to_le_bytes());
            // val bytes
            buf.extend_from_slice(entry.value.as_bytes());
        }

        debug_assert_eq!(buf.len(), total_size);
        Ok(buf)
    }

    /// Deserialize a blob from the front of a data buffer.
    /// Returns the parsed blob and the total number of bytes consumed.
    pub fn deserialize(data: &[u8]) -> Result<(MetadataBlob, usize), ServerError> {
        if data.len() < MIN_BLOB_SIZE {
            return Err(ServerError::MetadataBlobError {
                reason: "data too short for metadata blob".to_string(),
            });
        }

        // Read total length
        let total_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if total_len < MIN_BLOB_SIZE || total_len > data.len() {
            return Err(ServerError::MetadataBlobError {
                reason: format!(
                    "invalid metadata blob length: {} (data len: {})",
                    total_len,
                    data.len()
                ),
            });
        }

        let blob_data = &data[..total_len];
        let mut pos = 4;

        // format_version
        let version = blob_data[pos];
        pos += 1;
        if version != FORMAT_VERSION {
            return Err(ServerError::MetadataBlobError {
                reason: format!("unknown metadata blob version: {}", version),
            });
        }

        // entry_count
        let entry_count = u16::from_le_bytes([blob_data[pos], blob_data[pos + 1]]) as usize;
        pos += 2;

        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            if pos + 2 > total_len {
                return Err(ServerError::MetadataBlobError {
                    reason: "truncated metadata blob (key_len)".to_string(),
                });
            }
            let key_len = u16::from_le_bytes([blob_data[pos], blob_data[pos + 1]]) as usize;
            pos += 2;

            if pos + key_len > total_len {
                return Err(ServerError::MetadataBlobError {
                    reason: "truncated metadata blob (key)".to_string(),
                });
            }
            let key = std::str::from_utf8(&blob_data[pos..pos + key_len])
                .map_err(|_| ServerError::MetadataBlobError {
                    reason: "invalid UTF-8 in metadata key".to_string(),
                })?
                .to_string();
            pos += key_len;

            if pos + 2 > total_len {
                return Err(ServerError::MetadataBlobError {
                    reason: "truncated metadata blob (val_len)".to_string(),
                });
            }
            let val_len = u16::from_le_bytes([blob_data[pos], blob_data[pos + 1]]) as usize;
            pos += 2;

            if pos + val_len > total_len {
                return Err(ServerError::MetadataBlobError {
                    reason: "truncated metadata blob (value)".to_string(),
                });
            }
            let value = std::str::from_utf8(&blob_data[pos..pos + val_len])
                .map_err(|_| ServerError::MetadataBlobError {
                    reason: "invalid UTF-8 in metadata value".to_string(),
                })?
                .to_string();
            pos += val_len;

            entries.push(MetadataEntry { key, value });
        }

        Ok((MetadataBlob { entries }, total_len))
    }

    /// Get a metadata value by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.key == key)
            .map(|e| e.value.as_str())
    }

    /// Set a metadata key-value pair, replacing any existing entry with the same key.
    pub fn set(&mut self, key: &str, value: &str) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.key == key) {
            entry.value = value.to_string();
        } else {
            self.entries.push(MetadataEntry {
                key: key.to_string(),
                value: value.to_string(),
            });
        }
    }

    /// Return checksum value entries (x-amz-checksum-crc32, etc.) stored in the blob.
    /// Excludes x-amz-checksum-algorithm and x-amz-checksum-type which are
    /// surfaced as separate headers/elements.
    pub fn checksum_entries(&self) -> impl Iterator<Item = &MetadataEntry> {
        self.entries.iter().filter(|e| {
            e.key.starts_with("x-amz-checksum-")
                && e.key != "x-amz-checksum-algorithm"
                && e.key != "x-amz-checksum-type"
        })
    }

    /// Return all checksum-related entries including x-amz-checksum-type.
    /// Used for GetObjectAttributes where ChecksumType appears inside <Checksum>.
    pub fn checksum_entries_with_type(&self) -> impl Iterator<Item = &MetadataEntry> {
        self.entries
            .iter()
            .filter(|e| e.key.starts_with("x-amz-checksum-") && e.key != "x-amz-checksum-algorithm")
    }
}

impl Default for MetadataBlob {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_empty() {
        let blob = MetadataBlob::new();
        let data = blob.serialize().unwrap();
        let (decoded, consumed) = MetadataBlob::deserialize(&data).unwrap();
        assert_eq!(consumed, data.len());
        assert_eq!(decoded, blob);
        assert!(decoded.entries.is_empty());
    }

    #[test]
    fn round_trip_single_entry() {
        let blob = MetadataBlob {
            entries: vec![MetadataEntry {
                key: "content-type".to_string(),
                value: "application/json".to_string(),
            }],
        };
        let data = blob.serialize().unwrap();
        let (decoded, consumed) = MetadataBlob::deserialize(&data).unwrap();
        assert_eq!(consumed, data.len());
        assert_eq!(decoded, blob);
    }

    #[test]
    fn round_trip_multiple_entries() {
        let blob = MetadataBlob {
            entries: vec![
                MetadataEntry {
                    key: "content-type".to_string(),
                    value: "text/plain".to_string(),
                },
                MetadataEntry {
                    key: "x-amz-meta-author".to_string(),
                    value: "test-user".to_string(),
                },
                MetadataEntry {
                    key: "cache-control".to_string(),
                    value: "max-age=3600".to_string(),
                },
            ],
        };
        let data = blob.serialize().unwrap();
        let (decoded, consumed) = MetadataBlob::deserialize(&data).unwrap();
        assert_eq!(consumed, data.len());
        assert_eq!(decoded, blob);
    }

    #[test]
    fn deserialize_with_trailing_data() {
        let blob = MetadataBlob {
            entries: vec![MetadataEntry {
                key: "k".to_string(),
                value: "v".to_string(),
            }],
        };
        let mut data = blob.serialize().unwrap();
        data.extend_from_slice(b"trailing user data here");
        let (decoded, consumed) = MetadataBlob::deserialize(&data).unwrap();
        assert_eq!(decoded, blob);
        assert!(consumed < data.len());
    }

    #[test]
    fn deserialize_truncated() {
        let blob = MetadataBlob {
            entries: vec![MetadataEntry {
                key: "content-type".to_string(),
                value: "text/plain".to_string(),
            }],
        };
        let data = blob.serialize().unwrap();
        // Truncate
        assert!(MetadataBlob::deserialize(&data[..5]).is_err());
    }

    #[test]
    fn deserialize_bad_version() {
        let mut data = MetadataBlob::new().serialize().unwrap();
        data[4] = 99; // bad version
        assert!(MetadataBlob::deserialize(&data).is_err());
    }

    #[test]
    fn deserialize_bad_length() {
        let mut data = MetadataBlob::new().serialize().unwrap();
        // Set length to something larger than data
        let bad_len = (data.len() as u32 + 100).to_le_bytes();
        data[0..4].copy_from_slice(&bad_len);
        assert!(MetadataBlob::deserialize(&data).is_err());
    }

    #[test]
    fn from_headers_filters_correctly() {
        let headers = [
            ("Content-Type", "text/html"),
            ("Content-Length", "42"),          // not stored
            ("Authorization", "AWS4-HMAC..."), // not stored
            ("X-Amz-Meta-Author", "alice"),
            ("Cache-Control", "no-cache"),
            ("X-Amz-Meta-Version", "1"),
        ];
        let blob = MetadataBlob::from_headers(&headers).unwrap();
        assert_eq!(blob.entries.len(), 4);
        assert_eq!(blob.get("content-type"), Some("text/html"));
        assert_eq!(blob.get("x-amz-meta-author"), Some("alice"));
        assert_eq!(blob.get("cache-control"), Some("no-cache"));
        assert_eq!(blob.get("x-amz-meta-version"), Some("1"));
    }

    #[test]
    fn get_missing_key() {
        let blob = MetadataBlob::new();
        assert_eq!(blob.get("content-type"), None);
    }

    #[test]
    fn serialize_rejects_oversized_value() {
        let blob = MetadataBlob {
            entries: vec![MetadataEntry {
                key: "k".to_string(),
                value: "x".repeat(u16::MAX as usize + 1),
            }],
        };
        assert!(blob.serialize().is_err());
    }

    #[test]
    fn serialize_rejects_oversized_key() {
        let blob = MetadataBlob {
            entries: vec![MetadataEntry {
                key: "k".repeat(u16::MAX as usize + 1),
                value: "v".to_string(),
            }],
        };
        assert!(blob.serialize().is_err());
    }

    #[test]
    fn serialize_accepts_max_u16_value() {
        let blob = MetadataBlob {
            entries: vec![MetadataEntry {
                key: "k".to_string(),
                value: "x".repeat(u16::MAX as usize),
            }],
        };
        let data = blob.serialize().unwrap();
        let (decoded, _) = MetadataBlob::deserialize(&data).unwrap();
        assert_eq!(decoded.entries[0].value.len(), u16::MAX as usize);
    }

    #[test]
    fn from_headers_rejects_control_chars() {
        let headers = [("X-Amz-Meta-Evil", "value\r\nInjected: header")];
        assert!(MetadataBlob::from_headers(&headers).is_err());

        let headers = [("X-Amz-Meta-Null", "value\x00here")];
        assert!(MetadataBlob::from_headers(&headers).is_err());

        let headers = [("Content-Type", "text/plain\n")];
        assert!(MetadataBlob::from_headers(&headers).is_err());
    }

    #[test]
    fn from_headers_rejects_non_ascii() {
        let headers = [("X-Amz-Meta-Name", "caf\u{00e9}")]; // "café"
        assert!(MetadataBlob::from_headers(&headers).is_err());

        let headers = [("Content-Type", "text/plain; charset=\u{00fc}")];
        assert!(MetadataBlob::from_headers(&headers).is_err());

        // DEL (0x7F) should also be rejected
        let headers = [("X-Amz-Meta-Del", "val\x7f")];
        assert!(MetadataBlob::from_headers(&headers).is_err());
    }

    #[test]
    fn from_headers_accepts_clean_values() {
        let headers = [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("X-Amz-Meta-Tag", "hello world 123 !@#$%"),
        ];
        assert!(MetadataBlob::from_headers(&headers).is_ok());
    }

    #[test]
    fn strip_aws_chunked_removes_trailing() {
        assert_eq!(
            strip_aws_chunked("gzip, aws-chunked"),
            Some("gzip".to_string())
        );
    }

    #[test]
    fn strip_aws_chunked_removes_leading() {
        assert_eq!(
            strip_aws_chunked("aws-chunked, gzip"),
            Some("gzip".to_string())
        );
    }

    #[test]
    fn strip_aws_chunked_only() {
        assert_eq!(strip_aws_chunked("aws-chunked"), None);
    }

    #[test]
    fn strip_aws_chunked_duplicates() {
        assert_eq!(strip_aws_chunked("aws-chunked, aws-chunked"), None);
    }

    #[test]
    fn strip_aws_chunked_no_match() {
        assert_eq!(
            strip_aws_chunked("deflate, gzip"),
            Some("deflate, gzip".to_string())
        );
    }

    #[test]
    fn strip_aws_chunked_single_no_match() {
        assert_eq!(strip_aws_chunked("gzip"), Some("gzip".to_string()));
    }

    #[test]
    fn from_headers_strips_aws_chunked_from_content_encoding() {
        let headers = [("Content-Encoding", "gzip, aws-chunked")];
        let blob = MetadataBlob::from_headers(&headers).unwrap();
        assert_eq!(blob.get("content-encoding"), Some("gzip"));
    }

    #[test]
    fn from_headers_drops_content_encoding_when_only_aws_chunked() {
        let headers = [("Content-Encoding", "aws-chunked")];
        let blob = MetadataBlob::from_headers(&headers).unwrap();
        assert_eq!(blob.get("content-encoding"), None);
        assert!(blob.entries.is_empty());
    }
}
