/// Checksum algorithm for multipart uploads.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    Crc32 = 0,
    Crc32c = 1,
    Sha1 = 2,
    Sha256 = 3,
    Crc64nvme = 4,
    Md5 = 5,
    XxHash64 = 6,
    XxHash3 = 7,
    XxHash128 = 8,
    Sha512 = 9,
}

impl ChecksumAlgorithm {
    pub const ALL: [Self; 10] = [
        Self::Crc32,
        Self::Crc32c,
        Self::Sha1,
        Self::Sha256,
        Self::Crc64nvme,
        Self::Md5,
        Self::XxHash64,
        Self::XxHash3,
        Self::XxHash128,
        Self::Sha512,
    ];

    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Crc32),
            1 => Some(Self::Crc32c),
            2 => Some(Self::Sha1),
            3 => Some(Self::Sha256),
            4 => Some(Self::Crc64nvme),
            5 => Some(Self::Md5),
            6 => Some(Self::XxHash64),
            7 => Some(Self::XxHash3),
            8 => Some(Self::XxHash128),
            9 => Some(Self::Sha512),
            _ => None,
        }
    }

    /// Parse from an S3 API header value. Only accepts the canonical
    /// uppercase form (`SHA256`, `CRC32`, `CRC32C`, etc.).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "CRC32" => Some(Self::Crc32),
            "CRC32C" => Some(Self::Crc32c),
            "SHA1" => Some(Self::Sha1),
            "SHA256" => Some(Self::Sha256),
            "CRC64NVME" => Some(Self::Crc64nvme),
            "MD5" => Some(Self::Md5),
            "XXHASH64" => Some(Self::XxHash64),
            "XXHASH3" => Some(Self::XxHash3),
            "XXHASH128" => Some(Self::XxHash128),
            "SHA512" => Some(Self::Sha512),
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
            Self::Md5 => "MD5",
            Self::XxHash64 => "XXHASH64",
            Self::XxHash3 => "XXHASH3",
            Self::XxHash128 => "XXHASH128",
            Self::Sha512 => "SHA512",
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
            Self::Md5 => "x-amz-checksum-md5",
            Self::XxHash64 => "x-amz-checksum-xxhash64",
            Self::XxHash3 => "x-amz-checksum-xxhash3",
            Self::XxHash128 => "x-amz-checksum-xxhash128",
            Self::Sha512 => "x-amz-checksum-sha512",
        }
    }

    /// Parse an `x-amz-checksum-*` header name.
    #[must_use]
    pub fn from_header_name(name: &str) -> Option<Self> {
        let lower = name.to_ascii_lowercase();
        Self::ALL
            .into_iter()
            .find(|algorithm| algorithm.header_name() == lower)
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
            Self::Md5 => "ChecksumMD5",
            Self::XxHash64 => "ChecksumXXHASH64",
            Self::XxHash3 => "ChecksumXXHASH3",
            Self::XxHash128 => "ChecksumXXHASH128",
            Self::Sha512 => "ChecksumSHA512",
        }
    }

    /// Expected raw byte length for this algorithm's checksum value.
    #[must_use]
    pub fn expected_byte_length(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64nvme | Self::XxHash64 | Self::XxHash3 => 8,
            Self::Md5 | Self::XxHash128 => 16,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    /// Algorithms whose CompleteMultipartUpload checksum header is accepted
    /// but ignored when the multipart upload was not created with an
    /// algorithm.
    #[must_use]
    pub fn accepts_unconfigured_complete_multipart_header(self) -> bool {
        matches!(self, Self::Crc32 | Self::Crc32c | Self::Sha1 | Self::Sha256)
    }

    /// Algorithms whose CompleteMultipartUpload checksum header is accepted
    /// without a CreateMultipartUpload algorithm and causes S3 to compute and
    /// store a full-object checksum.
    #[must_use]
    pub fn stores_unconfigured_complete_multipart_header(self) -> bool {
        matches!(self, Self::Crc64nvme)
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
            ChecksumAlgorithm::Crc64nvme => Self::FullObject,
            ChecksumAlgorithm::Crc32
            | ChecksumAlgorithm::Crc32c
            | ChecksumAlgorithm::Sha1
            | ChecksumAlgorithm::Sha256
            | ChecksumAlgorithm::Md5
            | ChecksumAlgorithm::XxHash64
            | ChecksumAlgorithm::XxHash3
            | ChecksumAlgorithm::XxHash128
            | ChecksumAlgorithm::Sha512 => Self::Composite,
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
/// - SHA1/SHA256/MD5/XXHash/SHA512 only support COMPOSITE
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
            (
                ChecksumAlgorithm::Sha1
                | ChecksumAlgorithm::Sha256
                | ChecksumAlgorithm::Md5
                | ChecksumAlgorithm::XxHash64
                | ChecksumAlgorithm::XxHash3
                | ChecksumAlgorithm::XxHash128
                | ChecksumAlgorithm::Sha512,
                ChecksumType::FullObject,
            ) => Err(InvalidChecksumConfig {
                reason: format!(
                    "FULL_OBJECT checksum type is not supported for {}",
                    algorithm.as_str()
                ),
            }),
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
    bytes: [u8; Self::MAX_LEN],
}

impl RawChecksum {
    const MAX_LEN: usize = 64;

    /// Construct a `RawChecksum`, validating that the byte length matches the algorithm.
    pub fn new(
        algorithm: ChecksumAlgorithm,
        bytes: impl AsRef<[u8]>,
    ) -> Result<Self, &'static str> {
        let bytes = bytes.as_ref();
        let expected = algorithm.expected_byte_length();
        if bytes.len() != expected {
            return Err("checksum byte length does not match algorithm");
        }

        let mut stored = [0u8; Self::MAX_LEN];
        stored[..expected].copy_from_slice(bytes);
        Ok(Self {
            algorithm,
            bytes: stored,
        })
    }

    /// The checksum algorithm.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// The raw checksum bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes[..self.algorithm.expected_byte_length()]
    }
}

/// Raw checksum bytes stored without algorithm context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumBytes {
    len: u8,
    bytes: [u8; Self::MAX_LEN],
}

impl ChecksumBytes {
    pub const MAX_LEN: usize = 64;

    /// Construct inline checksum bytes, validating only the bounded size.
    pub fn new(bytes: impl AsRef<[u8]>) -> Result<Self, &'static str> {
        let bytes = bytes.as_ref();
        if bytes.is_empty() || bytes.len() > Self::MAX_LEN {
            return Err("checksum byte length must be between 1 and 64");
        }

        let mut stored = [0u8; Self::MAX_LEN];
        stored[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            len: bytes.len() as u8,
            bytes: stored,
        })
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl AsRef<[u8]> for ChecksumBytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<RawChecksum> for ChecksumBytes {
    fn from(value: RawChecksum) -> Self {
        Self::new(value.bytes()).expect("raw checksum bytes are bounded by construction")
    }
}

impl From<&RawChecksum> for ChecksumBytes {
    fn from(value: &RawChecksum) -> Self {
        Self::new(value.bytes()).expect("raw checksum bytes are bounded by construction")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_algorithm_from_u8_round_trip() {
        for algorithm in ChecksumAlgorithm::ALL {
            assert_eq!(ChecksumAlgorithm::from_u8(algorithm as u8), Some(algorithm));
        }
        assert_eq!(ChecksumAlgorithm::from_u8(10), None);
        assert_eq!(ChecksumAlgorithm::from_u8(255), None);
    }

    #[test]
    fn checksum_algorithm_from_str() {
        for algorithm in ChecksumAlgorithm::ALL {
            assert_eq!(
                ChecksumAlgorithm::parse(algorithm.as_str()),
                Some(algorithm)
            );
        }
        assert_eq!(ChecksumAlgorithm::parse("bogus"), None);
    }

    #[test]
    fn checksum_algorithm_as_str_round_trip() {
        for algorithm in ChecksumAlgorithm::ALL {
            assert_eq!(
                ChecksumAlgorithm::parse(algorithm.as_str()),
                Some(algorithm)
            );
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
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Md5),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::XxHash64),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::XxHash3),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::XxHash128),
            ChecksumType::Composite
        );
        assert_eq!(
            ChecksumType::default_for(ChecksumAlgorithm::Sha512),
            ChecksumType::Composite
        );
    }

    #[test]
    fn checksum_algorithm_header_name() {
        let expected = [
            (ChecksumAlgorithm::Crc32, "x-amz-checksum-crc32"),
            (ChecksumAlgorithm::Crc32c, "x-amz-checksum-crc32c"),
            (ChecksumAlgorithm::Sha1, "x-amz-checksum-sha1"),
            (ChecksumAlgorithm::Sha256, "x-amz-checksum-sha256"),
            (ChecksumAlgorithm::Crc64nvme, "x-amz-checksum-crc64nvme"),
            (ChecksumAlgorithm::Md5, "x-amz-checksum-md5"),
            (ChecksumAlgorithm::XxHash64, "x-amz-checksum-xxhash64"),
            (ChecksumAlgorithm::XxHash3, "x-amz-checksum-xxhash3"),
            (ChecksumAlgorithm::XxHash128, "x-amz-checksum-xxhash128"),
            (ChecksumAlgorithm::Sha512, "x-amz-checksum-sha512"),
        ];
        for (algorithm, header) in expected {
            assert_eq!(algorithm.header_name(), header);
            assert_eq!(ChecksumAlgorithm::from_header_name(header), Some(algorithm));
        }
    }

    #[test]
    fn checksum_algorithm_xml_element_name() {
        let expected = [
            (ChecksumAlgorithm::Crc32, "ChecksumCRC32"),
            (ChecksumAlgorithm::Crc32c, "ChecksumCRC32C"),
            (ChecksumAlgorithm::Sha1, "ChecksumSHA1"),
            (ChecksumAlgorithm::Sha256, "ChecksumSHA256"),
            (ChecksumAlgorithm::Crc64nvme, "ChecksumCRC64NVME"),
            (ChecksumAlgorithm::Md5, "ChecksumMD5"),
            (ChecksumAlgorithm::XxHash64, "ChecksumXXHASH64"),
            (ChecksumAlgorithm::XxHash3, "ChecksumXXHASH3"),
            (ChecksumAlgorithm::XxHash128, "ChecksumXXHASH128"),
            (ChecksumAlgorithm::Sha512, "ChecksumSHA512"),
        ];
        for (algorithm, element) in expected {
            assert_eq!(algorithm.xml_element_name(), element);
        }
    }

    #[test]
    fn checksum_algorithm_expected_byte_length() {
        assert_eq!(ChecksumAlgorithm::Crc32.expected_byte_length(), 4);
        assert_eq!(ChecksumAlgorithm::Crc32c.expected_byte_length(), 4);
        assert_eq!(ChecksumAlgorithm::Sha1.expected_byte_length(), 20);
        assert_eq!(ChecksumAlgorithm::Sha256.expected_byte_length(), 32);
        assert_eq!(ChecksumAlgorithm::Crc64nvme.expected_byte_length(), 8);
        assert_eq!(ChecksumAlgorithm::Md5.expected_byte_length(), 16);
        assert_eq!(ChecksumAlgorithm::XxHash64.expected_byte_length(), 8);
        assert_eq!(ChecksumAlgorithm::XxHash3.expected_byte_length(), 8);
        assert_eq!(ChecksumAlgorithm::XxHash128.expected_byte_length(), 16);
        assert_eq!(ChecksumAlgorithm::Sha512.expected_byte_length(), 64);
    }

    #[test]
    fn checksum_type_as_str() {
        assert_eq!(ChecksumType::Composite.as_str(), "COMPOSITE");
        assert_eq!(ChecksumType::FullObject.as_str(), "FULL_OBJECT");
    }

    #[test]
    fn invalid_checksum_config_display() {
        let err = InvalidChecksumConfig {
            reason: "test error".to_string(),
        };
        assert_eq!(format!("{}", err), "test error");
    }

    #[test]
    fn multipart_checksum_config_valid_combinations() {
        // SHA256 + COMPOSITE (default)
        let config = MultipartChecksumConfig::new(ChecksumAlgorithm::Sha256, None).unwrap();
        assert_eq!(config.algorithm(), ChecksumAlgorithm::Sha256);
        assert_eq!(config.checksum_type(), ChecksumType::Composite);

        // SHA1 + COMPOSITE (explicit)
        let config =
            MultipartChecksumConfig::new(ChecksumAlgorithm::Sha1, Some(ChecksumType::Composite))
                .unwrap();
        assert_eq!(config.algorithm(), ChecksumAlgorithm::Sha1);
        assert_eq!(config.checksum_type(), ChecksumType::Composite);

        // CRC32 + COMPOSITE (default)
        let config = MultipartChecksumConfig::new(ChecksumAlgorithm::Crc32, None).unwrap();
        assert_eq!(config.algorithm(), ChecksumAlgorithm::Crc32);
        assert_eq!(config.checksum_type(), ChecksumType::Composite);

        // CRC32C + FULL_OBJECT (explicit)
        let config =
            MultipartChecksumConfig::new(ChecksumAlgorithm::Crc32c, Some(ChecksumType::FullObject))
                .unwrap();
        assert_eq!(config.algorithm(), ChecksumAlgorithm::Crc32c);
        assert_eq!(config.checksum_type(), ChecksumType::FullObject);

        // CRC64NVME + FULL_OBJECT (default)
        let config = MultipartChecksumConfig::new(ChecksumAlgorithm::Crc64nvme, None).unwrap();
        assert_eq!(config.algorithm(), ChecksumAlgorithm::Crc64nvme);
        assert_eq!(config.checksum_type(), ChecksumType::FullObject);

        for algorithm in [
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::XxHash64,
            ChecksumAlgorithm::XxHash3,
            ChecksumAlgorithm::XxHash128,
            ChecksumAlgorithm::Sha512,
        ] {
            let config = MultipartChecksumConfig::new(algorithm, None).unwrap();
            assert_eq!(config.algorithm(), algorithm);
            assert_eq!(config.checksum_type(), ChecksumType::Composite);
        }
    }

    #[test]
    fn multipart_checksum_config_invalid_combinations() {
        // SHA256 + FULL_OBJECT is invalid
        let result =
            MultipartChecksumConfig::new(ChecksumAlgorithm::Sha256, Some(ChecksumType::FullObject));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.reason.contains("FULL_OBJECT"));
        assert!(err.reason.contains("SHA256"));

        // SHA1 + FULL_OBJECT is invalid
        let result =
            MultipartChecksumConfig::new(ChecksumAlgorithm::Sha1, Some(ChecksumType::FullObject));
        assert!(result.is_err());

        for algorithm in [
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::XxHash64,
            ChecksumAlgorithm::XxHash3,
            ChecksumAlgorithm::XxHash128,
            ChecksumAlgorithm::Sha512,
        ] {
            let result = MultipartChecksumConfig::new(algorithm, Some(ChecksumType::FullObject));
            assert!(result.is_err());
        }

        // CRC64NVME + COMPOSITE is invalid
        let result = MultipartChecksumConfig::new(
            ChecksumAlgorithm::Crc64nvme,
            Some(ChecksumType::Composite),
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.reason.contains("COMPOSITE"));
        assert!(err.reason.contains("CRC64NVME"));
    }

    #[test]
    fn raw_checksum_valid() {
        // CRC32: 4 bytes
        let checksum = RawChecksum::new(ChecksumAlgorithm::Crc32, vec![1, 2, 3, 4]).unwrap();
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc32);
        assert_eq!(checksum.bytes(), &[1, 2, 3, 4]);

        // CRC64NVME: 8 bytes
        let checksum =
            RawChecksum::new(ChecksumAlgorithm::Crc64nvme, vec![1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Crc64nvme);
        assert_eq!(checksum.bytes(), &[1, 2, 3, 4, 5, 6, 7, 8]);

        // SHA1: 20 bytes
        let checksum = RawChecksum::new(ChecksumAlgorithm::Sha1, vec![0; 20]).unwrap();
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Sha1);
        assert_eq!(checksum.bytes().len(), 20);

        // SHA256: 32 bytes
        let checksum = RawChecksum::new(ChecksumAlgorithm::Sha256, vec![0; 32]).unwrap();
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Sha256);
        assert_eq!(checksum.bytes().len(), 32);

        let checksum = RawChecksum::new(ChecksumAlgorithm::Md5, vec![0; 16]).unwrap();
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Md5);
        assert_eq!(checksum.bytes().len(), 16);

        let checksum = RawChecksum::new(ChecksumAlgorithm::Sha512, vec![0; 64]).unwrap();
        assert_eq!(checksum.algorithm(), ChecksumAlgorithm::Sha512);
        assert_eq!(checksum.bytes().len(), 64);
    }

    #[test]
    fn raw_checksum_invalid_length() {
        // CRC32 with wrong length
        let result = RawChecksum::new(ChecksumAlgorithm::Crc32, vec![1, 2, 3]);
        assert!(result.is_err());

        // SHA256 with wrong length
        let result = RawChecksum::new(ChecksumAlgorithm::Sha256, vec![0; 16]);
        assert!(result.is_err());
    }

    #[test]
    fn checksum_bytes_valid() {
        let bytes = ChecksumBytes::new([1u8, 2, 3, 4]).unwrap();
        assert_eq!(bytes.as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn checksum_bytes_invalid_length() {
        assert!(ChecksumBytes::new([]).is_err());
        assert!(ChecksumBytes::new([0u8; 65]).is_err());
    }
}
