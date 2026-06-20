use checksum::{ChecksumAlgorithm, RawChecksum};

use crate::error::ServerError;

/// A checksum claim parsed from HTTP headers or trailers.
///
/// Base64 decoding and length validation happen at construction time,
/// so the coordinator receives already-decoded, validated bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumClaim {
    expected: RawChecksum,
}

impl ChecksumClaim {
    pub(crate) fn from_raw(expected: RawChecksum) -> Self {
        Self { expected }
    }

    /// Parse a base64-encoded checksum value, validating format and length.
    pub fn from_base64(algorithm: ChecksumAlgorithm, b64: &str) -> Result<Self, ServerError> {
        use base64::Engine;

        let mut bytes = [0u8; checksum::ChecksumBytes::MAX_LEN];
        let decoded_len = base64::engine::general_purpose::STANDARD
            .decode_slice(b64, &mut bytes)
            .map_err(|_| ServerError::InvalidRequest {
                reason: "invalid base64 in checksum value".to_string(),
            })?;
        let expected_len = algorithm.expected_byte_length();
        if decoded_len != expected_len {
            return Err(ServerError::InvalidRequest {
                reason: format!(
                    "checksum length {} does not match {} (expected {})",
                    decoded_len,
                    algorithm.as_str(),
                    expected_len,
                ),
            });
        }
        Ok(Self::from_raw(
            RawChecksum::new(algorithm, &bytes[..decoded_len])
                .expect("decoded checksum bytes were length-validated against the algorithm"),
        ))
    }

    /// The checksum algorithm.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.expected.algorithm()
    }

    /// The decoded checksum bytes.
    #[must_use]
    pub fn expected_bytes(&self) -> &[u8] {
        self.expected.bytes()
    }

    /// The expected checksum value as canonical base64.
    #[must_use]
    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(self.expected.bytes())
    }
}

/// A typed encoded checksum claim whose serialized form is preserved as-is.
///
/// Used for multipart-complete object-level checksum claims, where some valid
/// values are composite forms such as `base64-N`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedChecksumClaim {
    algorithm: ChecksumAlgorithm,
    encoded_value: String,
}

impl EncodedChecksumClaim {
    #[must_use]
    pub fn new(algorithm: ChecksumAlgorithm, encoded_value: String) -> Self {
        Self {
            algorithm,
            encoded_value,
        }
    }

    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    #[must_use]
    pub fn encoded_value(&self) -> &str {
        &self.encoded_value
    }

    /// Validate the CompleteMultipartUpload header value shape.
    ///
    /// AWS accepts both a plain full-object checksum value and the multipart
    /// composite form (`base64-N`) in the object-level checksum header. In both
    /// cases the checksum bytes before the optional suffix must be valid for
    /// the selected algorithm.
    pub fn validate_complete_multipart_header_value(&self) -> Result<(), ServerError> {
        let value = self.encoded_value.as_str();
        if let Some((checksum_value, part_count)) = value.split_once('-') {
            if part_count.is_empty()
                || part_count.starts_with('0')
                || part_count.parse::<u32>().is_err()
            {
                return Err(ServerError::CompleteMultipartChecksumHeaderInvalid {
                    header_name: self.algorithm.header_name().to_string(),
                });
            }
            ChecksumClaim::from_base64(self.algorithm, checksum_value).map_err(|_| {
                ServerError::CompleteMultipartChecksumHeaderInvalid {
                    header_name: self.algorithm.header_name().to_string(),
                }
            })?;
        } else {
            ChecksumClaim::from_base64(self.algorithm, value).map_err(|_| {
                ServerError::CompleteMultipartChecksumHeaderInvalid {
                    header_name: self.algorithm.header_name().to_string(),
                }
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha256_b64(bytes: [u8; 32]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn encoded_complete_multipart_checksum_accepts_plain_and_composite_values() {
        let plain = EncodedChecksumClaim::new(ChecksumAlgorithm::Sha256, sha256_b64([0; 32]));
        plain.validate_complete_multipart_header_value().unwrap();

        let composite = EncodedChecksumClaim::new(
            ChecksumAlgorithm::Sha256,
            format!("{}-1", sha256_b64([1; 32])),
        );
        composite
            .validate_complete_multipart_header_value()
            .unwrap();
    }

    #[test]
    fn encoded_complete_multipart_checksum_rejects_malformed_values() {
        for value in ["bad", "not-valid-base64!!!", "", "abcd-", "abcd-0"] {
            let claim = EncodedChecksumClaim::new(ChecksumAlgorithm::Sha256, value.to_string());
            let err = claim
                .validate_complete_multipart_header_value()
                .unwrap_err();
            assert!(
                matches!(
                    err,
                    ServerError::CompleteMultipartChecksumHeaderInvalid { ref header_name }
                        if header_name == "x-amz-checksum-sha256"
                ),
                "expected CompleteMultipartChecksumHeaderInvalid for {value:?}, got {err:?}"
            );
        }
    }
}
