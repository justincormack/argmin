/// Checksum algorithm for multipart uploads.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    Crc32 = 0,
    Crc32c = 1,
    Sha1 = 2,
    Sha256 = 3,
    Crc64nvme = 4,
}

impl ChecksumAlgorithm {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Crc32),
            1 => Some(Self::Crc32c),
            2 => Some(Self::Sha1),
            3 => Some(Self::Sha256),
            4 => Some(Self::Crc64nvme),
            _ => None,
        }
    }

    /// Parse from an S3 API header value. Only accepts the canonical
    /// uppercase form (`SHA256`, `CRC32`, `CRC32C`, `SHA1`, `CRC64NVME`).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "CRC32" => Some(Self::Crc32),
            "CRC32C" => Some(Self::Crc32c),
            "SHA1" => Some(Self::Sha1),
            "SHA256" => Some(Self::Sha256),
            "CRC64NVME" => Some(Self::Crc64nvme),
            _ => None,
        }
    }

    /// S3 API canonical name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Crc64nvme => "CRC64NVME",
        }
    }

    /// The `x-amz-checksum-*` header suffix for this algorithm.
    #[must_use]
    pub fn header_name(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
            Self::Crc64nvme => "x-amz-checksum-crc64nvme",
        }
    }

    /// XML element name for this checksum (e.g. `ChecksumCRC32`).
    #[must_use]
    pub fn xml_element_name(self) -> &'static str {
        match self {
            Self::Crc32 => "ChecksumCRC32",
            Self::Crc32c => "ChecksumCRC32C",
            Self::Sha1 => "ChecksumSHA1",
            Self::Sha256 => "ChecksumSHA256",
            Self::Crc64nvme => "ChecksumCRC64NVME",
        }
    }

    /// Expected raw byte length for this algorithm's checksum value.
    #[must_use]
    pub fn expected_byte_length(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64nvme => 8,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }
}

/// Checksum type for multipart uploads: COMPOSITE (SHA) or FULL_OBJECT (CRC).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumType {
    Composite = 0,
    FullObject = 1,
}

impl ChecksumType {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Composite),
            1 => Some(Self::FullObject),
            _ => None,
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "COMPOSITE" => Some(Self::Composite),
            "FULL_OBJECT" => Some(Self::FullObject),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Composite => "COMPOSITE",
            Self::FullObject => "FULL_OBJECT",
        }
    }

    /// Return the default checksum type for a given algorithm.
    #[must_use]
    pub fn default_for(algo: ChecksumAlgorithm) -> Self {
        match algo {
            ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => Self::Composite,
            ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32c => Self::Composite,
            ChecksumAlgorithm::Crc64nvme => Self::FullObject,
        }
    }
}

/// Error returned when an invalid algorithm + checksum-type combination is
/// requested (e.g. SHA256 + FULL_OBJECT, CRC64NVME + COMPOSITE).
#[derive(Debug, Clone)]
pub struct InvalidChecksumConfig {
    pub reason: String,
}

impl std::fmt::Display for InvalidChecksumConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for InvalidChecksumConfig {}

/// Validated checksum configuration for a multipart upload.
///
/// Encodes the S3 combination rules:
/// - SHA1/SHA256 only support COMPOSITE
/// - CRC64NVME only supports FULL_OBJECT
/// - CRC32/CRC32C support both (default COMPOSITE)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartChecksumConfig {
    algorithm: ChecksumAlgorithm,
    checksum_type: ChecksumType,
}

impl MultipartChecksumConfig {
    /// Create a validated checksum configuration.
    ///
    /// If `checksum_type` is `None`, the default for the algorithm is used.
    /// Returns an error for invalid combinations.
    pub fn new(
        algorithm: ChecksumAlgorithm,
        checksum_type: Option<ChecksumType>,
    ) -> Result<Self, InvalidChecksumConfig> {
        let checksum_type = checksum_type.unwrap_or_else(|| ChecksumType::default_for(algorithm));

        match (algorithm, checksum_type) {
            (ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256, ChecksumType::FullObject) => {
                Err(InvalidChecksumConfig {
                    reason: format!(
                        "FULL_OBJECT checksum type is not supported for {}",
                        algorithm.as_str()
                    ),
                })
            }
            (ChecksumAlgorithm::Crc64nvme, ChecksumType::Composite) => Err(InvalidChecksumConfig {
                reason: "COMPOSITE checksum type is not supported for CRC64NVME".to_string(),
            }),
            _ => Ok(Self {
                algorithm,
                checksum_type,
            }),
        }
    }

    #[must_use]
    pub fn algorithm(self) -> ChecksumAlgorithm {
        self.algorithm
    }

    #[must_use]
    pub fn checksum_type(self) -> ChecksumType {
        self.checksum_type
    }
}

/// A checksum with its algorithm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawChecksum {
    algorithm: ChecksumAlgorithm,
    bytes: Vec<u8>,
}

impl RawChecksum {
    /// Construct a `RawChecksum`, validating that the byte length matches the algorithm.
    pub fn new(algorithm: ChecksumAlgorithm, bytes: Vec<u8>) -> Result<Self, &'static str> {
        let expected = algorithm.expected_byte_length();
        if bytes.len() != expected {
            return Err("checksum byte length does not match algorithm");
        }
        Ok(Self { algorithm, bytes })
    }

    /// The checksum algorithm.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// The raw checksum bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_algorithm_from_u8_round_trip() {
        for v in 0..=4u8 {
            let algo = ChecksumAlgorithm::from_u8(v).unwrap();
            assert_eq!(algo as u8, v);
        }
        assert_eq!(ChecksumAlgorithm::from_u8(5), None);
        assert_eq!(ChecksumAlgorithm::from_u8(255), None);
    }

    #[test]
    fn checksum_algorithm_from_str() {
        assert_eq!(
            ChecksumAlgorithm::parse("SHA256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::parse("CRC64NVME"),
            Some(ChecksumAlgorithm::Crc64nvme)
        );
        assert_eq!(ChecksumAlgorithm::parse("bogus"), None);
    }

    #[test]
    fn checksum_algorithm_as_str_round_trip() {
        for v in 0..=4u8 {
            let algo = ChecksumAlgorithm::from_u8(v).unwrap();
            assert_eq!(ChecksumAlgorithm::parse(algo.as_str()), Some(algo));
        }
    }

    #[test]
    fn checksum_type_from_u8_round_trip() {
        assert_eq!(ChecksumType::from_u8(0), Some(ChecksumType::Composite));
        assert_eq!(ChecksumType::from_u8(1), Some(ChecksumType::FullObject));
        assert_eq!(ChecksumType::from_u8(2), None);
    }

    #[test]
    fn checksum_type_from_str() {
        assert_eq!(
            ChecksumType::parse("COMPOSITE"),
            Some(ChecksumType::Composite)
        );
        assert_eq!(
            ChecksumType::parse("FULL_OBJECT"),
            Some(ChecksumType::FullObject)
        );
        assert_eq!(ChecksumType::parse("bogus"), None);
    }

    #[test]
    fn checksum_type_default_for_algorithm() {
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Sha256),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Sha1),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Crc32),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Crc32c),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Crc64nvme),
            ChecksumType::FullObject
        );
    }
}
