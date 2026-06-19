use std::hash::Hasher as _;

use md5_legacy::Digest as _;

use crate::{ChecksumAlgorithm, RawChecksum};

/// Incremental checksum hasher for S3 object checksum algorithms.
pub enum ChecksumHasher {
    Crc32(crate::crc32::Hasher),
    Crc32c(crate::crc32c::Hasher),
    Crc64(crate::crc64::Hasher),
    Md5(md5_legacy::Md5),
    Sha1(ring::digest::Context),
    Sha256(ring::digest::Context),
    Sha512(ring::digest::Context),
    XxHash64(twox_hash::XxHash64),
    XxHash3(twox_hash::XxHash3_64),
    XxHash128(twox_hash::XxHash3_128),
}

impl ChecksumHasher {
    #[must_use]
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Crc32 => Self::Crc32(crate::crc32::Hasher::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(crate::crc32c::Hasher::new()),
            ChecksumAlgorithm::Crc64nvme => Self::Crc64(crate::crc64::Hasher::new()),
            ChecksumAlgorithm::Md5 => Self::Md5(md5_legacy::Md5::new()),
            ChecksumAlgorithm::Sha1 => Self::Sha1(ring::digest::Context::new(
                &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
            )),
            ChecksumAlgorithm::Sha256 => {
                Self::Sha256(ring::digest::Context::new(&ring::digest::SHA256))
            }
            ChecksumAlgorithm::Sha512 => {
                Self::Sha512(ring::digest::Context::new(&ring::digest::SHA512))
            }
            ChecksumAlgorithm::XxHash64 => Self::XxHash64(twox_hash::XxHash64::with_seed(0)),
            ChecksumAlgorithm::XxHash3 => Self::XxHash3(twox_hash::XxHash3_64::new()),
            ChecksumAlgorithm::XxHash128 => Self::XxHash128(twox_hash::XxHash3_128::new()),
        }
    }

    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32(_) => ChecksumAlgorithm::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            Self::Crc64(_) => ChecksumAlgorithm::Crc64nvme,
            Self::Md5(_) => ChecksumAlgorithm::Md5,
            Self::Sha1(_) => ChecksumAlgorithm::Sha1,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
            Self::Sha512(_) => ChecksumAlgorithm::Sha512,
            Self::XxHash64(_) => ChecksumAlgorithm::XxHash64,
            Self::XxHash3(_) => ChecksumAlgorithm::XxHash3,
            Self::XxHash128(_) => ChecksumAlgorithm::XxHash128,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(hasher) => hasher.update(data),
            Self::Crc32c(hasher) => hasher.update(data),
            Self::Crc64(hasher) => hasher.update(data),
            Self::Md5(hasher) => hasher.update(data),
            Self::Sha1(hasher) | Self::Sha256(hasher) | Self::Sha512(hasher) => {
                hasher.update(data);
            }
            Self::XxHash64(hasher) => hasher.write(data),
            Self::XxHash3(hasher) => hasher.write(data),
            Self::XxHash128(hasher) => hasher.write(data),
        }
    }

    #[must_use]
    pub fn finalize(self) -> RawChecksum {
        match self {
            Self::Crc32(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32, hasher.finalize().to_be_bytes())
            }
            Self::Crc32c(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32c, hasher.finalize().to_be_bytes())
            }
            Self::Crc64(hasher) => RawChecksum::new(
                ChecksumAlgorithm::Crc64nvme,
                hasher.finalize().to_be_bytes(),
            ),
            Self::Md5(hasher) => RawChecksum::new(ChecksumAlgorithm::Md5, hasher.finalize()),
            Self::Sha1(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha1, hasher.finish().as_ref())
            }
            Self::Sha256(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha256, hasher.finish().as_ref())
            }
            Self::Sha512(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha512, hasher.finish().as_ref())
            }
            Self::XxHash64(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::XxHash64, hasher.finish().to_be_bytes())
            }
            Self::XxHash3(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::XxHash3, hasher.finish().to_be_bytes())
            }
            Self::XxHash128(hasher) => RawChecksum::new(
                ChecksumAlgorithm::XxHash128,
                hasher.finish_128().to_be_bytes(),
            ),
        }
        .expect("checksum hasher produced bytes matching the algorithm")
    }
}

#[must_use]
pub fn compute_checksum(algorithm: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
    let mut hasher = ChecksumHasher::new(algorithm);
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_hash_matches_oneshot() {
        let data = b"123456789";
        for algorithm in ChecksumAlgorithm::ALL {
            let mut hasher = ChecksumHasher::new(algorithm);
            hasher.update(&data[..3]);
            hasher.update(&data[3..]);
            assert_eq!(hasher.finalize(), compute_checksum(algorithm, data));
        }
    }

    #[test]
    fn xxhash_variants_are_distinct() {
        let data = b"123456789";
        assert_ne!(
            compute_checksum(ChecksumAlgorithm::XxHash64, data).bytes(),
            compute_checksum(ChecksumAlgorithm::XxHash3, data).bytes()
        );
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::XxHash128, data)
                .bytes()
                .len(),
            16
        );
    }

    #[test]
    fn xxhash_empty_input_vectors_are_big_endian() {
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::XxHash64, b"").bytes(),
            [0xef, 0x46, 0xdb, 0x37, 0x51, 0xd8, 0xe9, 0x99]
        );
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::XxHash3, b"").bytes(),
            [0x2d, 0x06, 0x80, 0x05, 0x38, 0xd3, 0x94, 0xc2]
        );
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::XxHash128, b"").bytes(),
            [
                0x99, 0xaa, 0x06, 0xd3, 0x01, 0x47, 0x98, 0xd8, 0x60, 0x01, 0xc3, 0x24, 0x46, 0x8d,
                0x49, 0x7f,
            ]
        );
    }
}
