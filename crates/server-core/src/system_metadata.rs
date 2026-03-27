use checksum::{ChecksumAlgorithm, ChecksumType};

use crate::error::ServerError;

const FORMAT_VERSION: u8 = 1;

const CONTENT_TYPE_BIT: u16 = 1 << 0;
const CONTENT_ENCODING_BIT: u16 = 1 << 1;
const CACHE_CONTROL_BIT: u16 = 1 << 2;
const CONTENT_DISPOSITION_BIT: u16 = 1 << 3;
const CONTENT_LANGUAGE_BIT: u16 = 1 << 4;
const EXPIRES_BIT: u16 = 1 << 5;
const CHECKSUM_BIT: u16 = 1 << 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectChecksumMetadata {
    algorithm: ChecksumAlgorithm,
    checksum_type: Option<ChecksumType>,
    value: String,
}

impl ObjectChecksumMetadata {
    #[must_use]
    pub fn new(
        algorithm: ChecksumAlgorithm,
        checksum_type: Option<ChecksumType>,
        value: String,
    ) -> Self {
        Self {
            algorithm,
            checksum_type,
            value,
        }
    }

    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    #[must_use]
    pub fn checksum_type(&self) -> Option<ChecksumType> {
        self.checksum_type
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub fn header_name(&self) -> &'static str {
        self.algorithm.header_name()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SystemMetadata {
    content_type: Option<String>,
    content_encoding: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_language: Option<String>,
    expires: Option<String>,
    checksum: Option<ObjectChecksumMetadata>,
}

fn has_invalid_header_bytes(s: &str) -> bool {
    s.bytes().any(|b| b < 0x20 || b == 0x7f)
}

fn checksum_algorithm_from_header_name(name: &str) -> Option<ChecksumAlgorithm> {
    match name {
        "x-amz-checksum-sha256" => Some(ChecksumAlgorithm::Sha256),
        "x-amz-checksum-sha1" => Some(ChecksumAlgorithm::Sha1),
        "x-amz-checksum-crc32" => Some(ChecksumAlgorithm::Crc32),
        "x-amz-checksum-crc32c" => Some(ChecksumAlgorithm::Crc32c),
        "x-amz-checksum-crc64nvme" => Some(ChecksumAlgorithm::Crc64nvme),
        _ => None,
    }
}

impl SystemMetadata {
    pub const EMPTY: Self = Self {
        content_type: None,
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        checksum: None,
    };

    #[must_use]
    pub const fn new() -> Self {
        Self::EMPTY
    }

    pub fn from_headers(headers: &[(&str, &str)]) -> Result<Self, ServerError> {
        Self::from_header_iter(headers.iter().copied())
    }

    pub fn from_header_iter<'a, I>(headers: I) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut out = Self::new();
        let mut checksum_algorithm = None;
        let mut checksum_type = None;
        let mut checksum_value = None;

        for (name, value) in headers {
            let lower = name.to_ascii_lowercase();
            if has_invalid_header_bytes(value) {
                return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "system metadata value for '{lower}' contains invalid header bytes"
                    ),
                });
            }
            match lower.as_str() {
                "content-type" => out.content_type = Some(value.to_string()),
                "content-encoding" => out.content_encoding = Some(value.to_string()),
                "cache-control" => out.cache_control = Some(value.to_string()),
                "content-disposition" => out.content_disposition = Some(value.to_string()),
                "content-language" => out.content_language = Some(value.to_string()),
                "expires" => out.expires = Some(value.to_string()),
                "x-amz-checksum-algorithm" => {
                    checksum_algorithm = ChecksumAlgorithm::parse(value);
                }
                "x-amz-checksum-type" => {
                    checksum_type = ChecksumType::parse(value);
                }
                _ if lower.starts_with("x-amz-checksum-") => {
                    if let Some(algo) = checksum_algorithm_from_header_name(&lower) {
                        checksum_algorithm = Some(algo);
                        checksum_value = Some(value.to_string());
                    }
                }
                _ => {}
            }
        }

        if let (Some(algorithm), Some(value)) = (checksum_algorithm, checksum_value) {
            out.checksum = Some(ObjectChecksumMetadata::new(algorithm, checksum_type, value));
        }

        Ok(out)
    }

    #[must_use]
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut out = Self::new();
        for (k, v) in pairs {
            match *k {
                "content-type" => out.content_type = Some((*v).to_string()),
                "content-encoding" => out.content_encoding = Some((*v).to_string()),
                "cache-control" => out.cache_control = Some((*v).to_string()),
                "content-disposition" => out.content_disposition = Some((*v).to_string()),
                "content-language" => out.content_language = Some((*v).to_string()),
                "expires" => out.expires = Some((*v).to_string()),
                "x-amz-checksum-type" => {
                    if let Some(ref mut checksum) = out.checksum {
                        checksum.checksum_type = ChecksumType::parse(v);
                    }
                }
                key if key.starts_with("x-amz-checksum-") => {
                    if let Some(algo) = checksum_algorithm_from_header_name(key) {
                        out.checksum = Some(ObjectChecksumMetadata::new(
                            algo,
                            out.checksum
                                .as_ref()
                                .and_then(ObjectChecksumMetadata::checksum_type),
                            (*v).to_string(),
                        ));
                    }
                }
                _ => {}
            }
        }
        out
    }

    #[must_use]
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    #[must_use]
    pub fn content_encoding(&self) -> Option<&str> {
        self.content_encoding.as_deref()
    }

    #[must_use]
    pub fn cache_control(&self) -> Option<&str> {
        self.cache_control.as_deref()
    }

    #[must_use]
    pub fn content_disposition(&self) -> Option<&str> {
        self.content_disposition.as_deref()
    }

    #[must_use]
    pub fn content_language(&self) -> Option<&str> {
        self.content_language.as_deref()
    }

    #[must_use]
    pub fn expires(&self) -> Option<&str> {
        self.expires.as_deref()
    }

    #[must_use]
    pub fn checksum(&self) -> Option<&ObjectChecksumMetadata> {
        self.checksum.as_ref()
    }

    pub fn set_checksum(
        &mut self,
        algorithm: ChecksumAlgorithm,
        checksum_type: Option<ChecksumType>,
        value: impl Into<String>,
    ) {
        self.checksum = Some(ObjectChecksumMetadata::new(
            algorithm,
            checksum_type,
            value.into(),
        ));
    }

    pub fn clear_checksum(&mut self) {
        self.checksum = None;
    }

    #[must_use]
    pub fn take_checksum(&mut self) -> Option<ObjectChecksumMetadata> {
        self.checksum.take()
    }

    pub fn strip_checksum_values(&mut self) {
        self.checksum = None;
    }

    pub fn strip_aws_chunked_content_encoding(&mut self) {
        let Some(encoding) = self.content_encoding.as_ref() else {
            return;
        };
        let filtered: Vec<&str> = encoding
            .split(',')
            .map(str::trim)
            .filter(|part| !part.eq_ignore_ascii_case("aws-chunked"))
            .collect();
        if filtered.is_empty() {
            self.content_encoding = None;
        } else {
            self.content_encoding = Some(filtered.join(", "));
        }
    }

    pub fn merge_from(&mut self, other: &Self) {
        if other.content_type.is_some() {
            self.content_type = other.content_type.clone();
        }
        if other.content_encoding.is_some() {
            self.content_encoding = other.content_encoding.clone();
        }
        if other.cache_control.is_some() {
            self.cache_control = other.cache_control.clone();
        }
        if other.content_disposition.is_some() {
            self.content_disposition = other.content_disposition.clone();
        }
        if other.content_language.is_some() {
            self.content_language = other.content_language.clone();
        }
        if other.expires.is_some() {
            self.expires = other.expires.clone();
        }
        if other.checksum.is_some() {
            self.checksum = other.checksum.clone();
        }
    }

    #[must_use]
    pub fn checksum_algorithm(&self) -> Option<ChecksumAlgorithm> {
        self.checksum
            .as_ref()
            .map(ObjectChecksumMetadata::algorithm)
    }

    #[must_use]
    pub fn checksum_type(&self) -> Option<ChecksumType> {
        self.checksum
            .as_ref()
            .and_then(ObjectChecksumMetadata::checksum_type)
    }

    #[must_use]
    pub fn checksum_header_value(&self) -> Option<(&'static str, &str)> {
        self.checksum
            .as_ref()
            .map(|checksum| (checksum.header_name(), checksum.value()))
    }

    #[must_use]
    pub fn checksum_header_pairs(&self) -> Vec<(&'static str, &str)> {
        let mut headers = Vec::new();
        if let Some((name, value)) = self.checksum_header_value() {
            headers.push((name, value));
        }
        if let Some(checksum_type) = self.checksum_type() {
            headers.push(("x-amz-checksum-type", checksum_type.as_str()));
        }
        headers
    }

    pub fn serialize(&self) -> Result<Vec<u8>, ServerError> {
        let mut out = Vec::new();
        out.push(FORMAT_VERSION);
        let mut flags = 0u16;
        if self.content_type.is_some() {
            flags |= CONTENT_TYPE_BIT;
        }
        if self.content_encoding.is_some() {
            flags |= CONTENT_ENCODING_BIT;
        }
        if self.cache_control.is_some() {
            flags |= CACHE_CONTROL_BIT;
        }
        if self.content_disposition.is_some() {
            flags |= CONTENT_DISPOSITION_BIT;
        }
        if self.content_language.is_some() {
            flags |= CONTENT_LANGUAGE_BIT;
        }
        if self.expires.is_some() {
            flags |= EXPIRES_BIT;
        }
        if self.checksum.is_some() {
            flags |= CHECKSUM_BIT;
        }
        out.extend_from_slice(&flags.to_le_bytes());

        for value in [
            self.content_type.as_deref(),
            self.content_encoding.as_deref(),
            self.cache_control.as_deref(),
            self.content_disposition.as_deref(),
            self.content_language.as_deref(),
            self.expires.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            let len = u16::try_from(value.len()).map_err(|_| ServerError::MetadataBlobError {
                reason: "system metadata value too long".to_string(),
            })?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(value.as_bytes());
        }

        if let Some(checksum) = &self.checksum {
            out.push(checksum.algorithm as u8);
            out.push(checksum.checksum_type.map_or(u8::MAX, |v| v as u8));
            let len = u16::try_from(checksum.value.len()).map_err(|_| {
                ServerError::MetadataBlobError {
                    reason: "checksum metadata value too long".to_string(),
                }
            })?;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(checksum.value.as_bytes());
        }

        Ok(out)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, ServerError> {
        if data.is_empty() {
            return Ok(Self::new());
        }
        if data.len() < 3 {
            return Err(ServerError::MetadataBlobError {
                reason: "system metadata blob too short".to_string(),
            });
        }
        if data[0] != FORMAT_VERSION {
            return Err(ServerError::MetadataBlobError {
                reason: format!("unknown system metadata blob version: {}", data[0]),
            });
        }
        let flags = u16::from_le_bytes([data[1], data[2]]);
        let mut pos = 3usize;
        fn read_system_string(
            data: &[u8],
            pos: &mut usize,
            field: &str,
        ) -> Result<String, ServerError> {
            if *pos + 2 > data.len() {
                return Err(ServerError::MetadataBlobError {
                    reason: format!("truncated system metadata ({field} length)"),
                });
            }
            let len = u16::from_le_bytes([data[*pos], data[*pos + 1]]) as usize;
            *pos += 2;
            if *pos + len > data.len() {
                return Err(ServerError::MetadataBlobError {
                    reason: format!("truncated system metadata ({field} value)"),
                });
            }
            let value = std::str::from_utf8(&data[*pos..*pos + len])
                .map_err(|_| ServerError::MetadataBlobError {
                    reason: format!("invalid UTF-8 in system metadata {field}"),
                })?
                .to_string();
            *pos += len;
            Ok(value)
        }

        let content_type = if flags & CONTENT_TYPE_BIT != 0 {
            Some(read_system_string(data, &mut pos, "content-type")?)
        } else {
            None
        };
        let content_encoding = if flags & CONTENT_ENCODING_BIT != 0 {
            Some(read_system_string(data, &mut pos, "content-encoding")?)
        } else {
            None
        };
        let cache_control = if flags & CACHE_CONTROL_BIT != 0 {
            Some(read_system_string(data, &mut pos, "cache-control")?)
        } else {
            None
        };
        let content_disposition = if flags & CONTENT_DISPOSITION_BIT != 0 {
            Some(read_system_string(data, &mut pos, "content-disposition")?)
        } else {
            None
        };
        let content_language = if flags & CONTENT_LANGUAGE_BIT != 0 {
            Some(read_system_string(data, &mut pos, "content-language")?)
        } else {
            None
        };
        let expires = if flags & EXPIRES_BIT != 0 {
            Some(read_system_string(data, &mut pos, "expires")?)
        } else {
            None
        };
        let checksum = if flags & CHECKSUM_BIT != 0 {
            if pos + 4 > data.len() {
                return Err(ServerError::MetadataBlobError {
                    reason: "truncated system metadata checksum".to_string(),
                });
            }
            let algorithm = ChecksumAlgorithm::from_u8(data[pos]).ok_or_else(|| {
                ServerError::MetadataBlobError {
                    reason: format!("invalid checksum algorithm {}", data[pos]),
                }
            })?;
            pos += 1;
            let checksum_type_raw = data[pos];
            pos += 1;
            let checksum_type = if checksum_type_raw == u8::MAX {
                None
            } else {
                Some(ChecksumType::from_u8(checksum_type_raw).ok_or_else(|| {
                    ServerError::MetadataBlobError {
                        reason: format!("invalid checksum type {checksum_type_raw}"),
                    }
                })?)
            };
            let value = read_system_string(data, &mut pos, "checksum")?;
            Some(ObjectChecksumMetadata::new(algorithm, checksum_type, value))
        } else {
            None
        };

        if pos != data.len() {
            return Err(ServerError::MetadataBlobError {
                reason: "trailing bytes in system metadata blob".to_string(),
            });
        }

        Ok(Self {
            content_type,
            content_encoding,
            cache_control,
            content_disposition,
            content_language,
            expires,
            checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_headers_extracts_only_system_headers() {
        let metadata = SystemMetadata::from_headers(&[
            ("Content-Type", "text/plain"),
            ("X-Amz-Meta-Author", "alice"),
            ("Cache-Control", "no-cache"),
            ("X-Amz-Checksum-Sha256", "abc"),
            ("X-Amz-Checksum-Type", "FULL_OBJECT"),
        ])
        .unwrap();
        assert_eq!(metadata.content_type(), Some("text/plain"));
        assert_eq!(metadata.cache_control(), Some("no-cache"));
        let checksum = metadata.checksum().unwrap();
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Sha256);
        assert_eq!(checksum.checksum_type(), Some(ChecksumType::FullObject));
        assert_eq!(checksum.value(), "abc");
    }

    #[test]
    fn round_trip() {
        let mut metadata = SystemMetadata::new();
        metadata.content_type = Some("text/plain".to_string());
        metadata.content_encoding = Some("gzip".to_string());
        metadata.set_checksum(
            ChecksumAlgorithm::Crc32,
            Some(ChecksumType::FullObject),
            "abcd",
        );
        let bytes = metadata.serialize().unwrap();
        let decoded = SystemMetadata::deserialize(&bytes).unwrap();
        assert_eq!(decoded, metadata);
    }

    #[test]
    fn strip_aws_chunked_content_encoding_removes_transport_token() {
        let mut metadata =
            SystemMetadata::from_headers(&[("Content-Encoding", "gzip, aws-chunked")]).unwrap();
        metadata.strip_aws_chunked_content_encoding();
        assert_eq!(metadata.content_encoding(), Some("gzip"));
    }
}
