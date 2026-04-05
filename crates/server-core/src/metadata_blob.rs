/// C2 metadata blob: stored in the object metadata DB row.
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
const USER_METADATA_SIZE_LIMIT: usize = 2 * 1024;

/// A single metadata key-value entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

/// Metadata blob containing user-specified `x-amz-meta-*` headers only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataBlob {
    entries: Vec<MetadataEntry>,
}

/// Check if a string contains bytes invalid in HTTP headers:
/// ASCII control characters (0x00-0x1F) and DEL (0x7F). Non-ASCII
/// printable bytes (>= 0x80) are allowed (AWS accepts unicode metadata).
fn has_invalid_header_bytes(s: &str) -> bool {
    s.bytes().any(|b| b < 0x20 || b == 0x7f)
}

impl MetadataBlob {
    /// Create an empty metadata blob.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Build a metadata blob from request headers.
    /// Extracts only `x-amz-meta-*` headers.
    /// Rejects values containing control characters to prevent header injection.
    ///
    /// For `x-amz-meta-*` headers with non-ASCII values, bytes are reinterpreted
    /// as Latin-1 (ISO 8859-1) code points before storage. This matches AWS S3's
    /// documented behavior: non-US-ASCII metadata is stored with each HTTP octet
    /// mapped to its corresponding Unicode code point, and returned RFC 2047
    /// Q-encoded. This reinterpretation must happen after SigV4 verification,
    /// which operates on the raw header bytes as received.
    ///
    /// References:
    /// - <https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html>
    /// - RFC 9110 §5.5 (obs-text in field values)
    /// - RFC 2047 (MIME encoded-words in headers)
    pub fn from_headers(headers: &[(&str, &str)]) -> Result<Self, ServerError> {
        Self::from_header_iter(headers.iter().copied())
    }

    pub fn from_header_iter<'a, I>(headers: I) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut entries = Vec::new();
        let mut total_metadata_size = 0usize;
        for (name, value) in headers {
            let lower = name.to_ascii_lowercase();
            if lower.starts_with("x-amz-meta-") {
                if has_invalid_header_bytes(value) {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "metadata value for '{lower}' contains invalid header bytes"
                        ),
                    });
                }
                let stored_value = if !value.is_ascii() {
                    // AWS compatibility: reinterpret non-ASCII bytes as
                    // Latin-1 code points for user metadata only.
                    value.bytes().map(|b| b as char).collect()
                } else {
                    value.to_string()
                };
                total_metadata_size = total_metadata_size
                    .checked_add(lower.len())
                    .and_then(|size| size.checked_add(value.len()))
                    .ok_or(ServerError::MetadataTooLarge)?;
                if total_metadata_size > USER_METADATA_SIZE_LIMIT {
                    return Err(ServerError::MetadataTooLarge);
                }
                entries.push(MetadataEntry {
                    key: lower,
                    value: stored_value,
                });
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
                reason: format!("unknown metadata blob version: {version}"),
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
        let lower = key.to_ascii_lowercase();
        assert!(
            lower.starts_with("x-amz-meta-"),
            "MetadataBlob only accepts x-amz-meta-* keys"
        );
        if let Some(entry) = self.entries.iter_mut().find(|e| e.key == lower) {
            entry.value = value.to_string();
        } else {
            self.entries.push(MetadataEntry {
                key: lower,
                value: value.to_string(),
            });
        }
    }

    /// Build a metadata blob from raw key-value pairs without header filtering.
    /// Unlike `from_headers`, this does not filter by STORED_HEADERS or
    /// lowercase keys — pairs are stored exactly as given.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        Self {
            entries: pairs
                .iter()
                .map(|(k, v)| {
                    let lower = k.to_ascii_lowercase();
                    assert!(
                        lower.starts_with("x-amz-meta-"),
                        "MetadataBlob only accepts x-amz-meta-* keys"
                    );
                    MetadataEntry {
                        key: lower,
                        value: v.to_string(),
                    }
                })
                .collect(),
        }
    }

    /// Return the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return whether the blob is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = &MetadataEntry> {
        self.entries.iter()
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
                key: "x-amz-meta-type".to_string(),
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
                    key: "x-amz-meta-type".to_string(),
                    value: "text/plain".to_string(),
                },
                MetadataEntry {
                    key: "x-amz-meta-author".to_string(),
                    value: "test-user".to_string(),
                },
                MetadataEntry {
                    key: "x-amz-meta-cache".to_string(),
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
                key: "x-amz-meta-type".to_string(),
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
        assert_eq!(blob.entries.len(), 2);
        assert_eq!(blob.get("x-amz-meta-author"), Some("alice"));
        assert_eq!(blob.get("x-amz-meta-version"), Some("1"));
    }

    #[test]
    fn get_missing_key() {
        let blob = MetadataBlob::new();
        assert_eq!(blob.get("x-amz-meta-missing"), None);
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
        assert!(MetadataBlob::from_headers(&headers).is_ok());
    }

    #[test]
    fn from_headers_accepts_non_ascii() {
        // AWS accepts unicode metadata values
        let headers = [("X-Amz-Meta-Name", "caf\u{00e9}")]; // "café"
        assert!(MetadataBlob::from_headers(&headers).is_ok());

        // DEL (0x7F) is a control character and should be rejected
        let headers = [("X-Amz-Meta-Del", "val\x7f")];
        assert!(MetadataBlob::from_headers(&headers).is_err());
    }

    #[test]
    fn from_headers_accepts_clean_values() {
        let headers = [("X-Amz-Meta-Tag", "hello world 123 !@#$%")];
        assert!(MetadataBlob::from_headers(&headers).is_ok());
    }

    #[test]
    fn from_headers_accepts_metadata_at_limit() {
        let key = "X-Amz-Meta-Limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT - key.len());
        let headers = [(key, value.as_str())];
        let blob = MetadataBlob::from_headers(&headers).unwrap();
        assert_eq!(blob.get("x-amz-meta-limit"), Some(value.as_str()));
    }

    #[test]
    fn from_headers_rejects_metadata_over_limit() {
        let key = "X-Amz-Meta-Limit";
        let value = "m".repeat(USER_METADATA_SIZE_LIMIT + 1 - key.len());
        let headers = [(key, value.as_str())];
        let err = MetadataBlob::from_headers(&headers).unwrap_err();
        assert!(matches!(err, ServerError::MetadataTooLarge));
    }
}
