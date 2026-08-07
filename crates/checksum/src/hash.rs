use std::hash::Hasher as _;

use md5_legacy::Digest as _;

use crate::{ChecksumAlgorithm, RawChecksum};

/// Incremental checksum hasher for S3 object checksum algorithms.
pub enum ChecksumHasher {
    Crc32(crate::crc32::Hasher),
    Crc32c(crate::crc32c::Hasher),
    Crc64(crate::crc64::Hasher),
    Md5(md5_legacy::Md5),
    Sha1(argmin_crypto::digest::Sha1),
    Sha256(crate::sha256::Sha256),
    Sha512(argmin_crypto::digest::Sha512),
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
            ChecksumAlgorithm::Sha1 => Self::Sha1(argmin_crypto::digest::Sha1::new()),
            ChecksumAlgorithm::Sha256 => Self::Sha256(crate::sha256::Sha256::new()),
            ChecksumAlgorithm::Sha512 => Self::Sha512(argmin_crypto::digest::Sha512::new()),
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
            Self::Sha1(hasher) => hasher.update(data),
            Self::Sha256(hasher) => hasher.update(data),
            Self::Sha512(hasher) => hasher.update(data),
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
            Self::Sha1(hasher) => RawChecksum::new(ChecksumAlgorithm::Sha1, hasher.finalize()),
            Self::Sha256(hasher) => RawChecksum::new(ChecksumAlgorithm::Sha256, hasher.finalize()),
            Self::Sha512(hasher) => RawChecksum::new(ChecksumAlgorithm::Sha512, hasher.finalize()),
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
    fn incremental_hash_matches_oneshot_for_varied_chunk_boundaries() {
        let data = b"abcdefghijklmnopqrstuvwxyz0123456789checksum-boundary-test";
        for algorithm in ChecksumAlgorithm::ALL {
            let expected = compute_checksum(algorithm, data);
            for chunk_size in [1, 2, 3, 5, 8, 13, 31, data.len() + 1] {
                let mut hasher = ChecksumHasher::new(algorithm);
                for chunk in data.chunks(chunk_size) {
                    hasher.update(chunk);
                }
                assert_eq!(
                    hasher.finalize(),
                    expected,
                    "{algorithm:?} differed with chunk size {chunk_size}"
                );
            }
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

    #[test]
    fn non_empty_input_vectors_are_big_endian() {
        let data = b"123456789";
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::Md5, data).bytes(),
            [
                0x25, 0xf9, 0xe7, 0x94, 0x32, 0x3b, 0x45, 0x38, 0x85, 0xf5, 0x18, 0x1f, 0x1b, 0x62,
                0x4d, 0x0b,
            ]
        );
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::Sha512, data).bytes(),
            [
                0xd9, 0xe6, 0x76, 0x2d, 0xd1, 0xc8, 0xea, 0xf6, 0xd6, 0x1b, 0x3c, 0x61, 0x92, 0xfc,
                0x40, 0x8d, 0x4d, 0x6d, 0x5f, 0x11, 0x76, 0xd0, 0xc2, 0x91, 0x69, 0xbc, 0x24, 0xe7,
                0x1c, 0x3f, 0x27, 0x4a, 0xd2, 0x7f, 0xcd, 0x58, 0x11, 0xb3, 0x13, 0xd6, 0x81, 0xf7,
                0xe5, 0x5e, 0xc0, 0x2d, 0x73, 0xd4, 0x99, 0xc9, 0x54, 0x55, 0xb6, 0xb5, 0xbb, 0x50,
                0x3a, 0xcf, 0x57, 0x4f, 0xba, 0x8f, 0xfe, 0x85,
            ]
        );
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::XxHash64, data).bytes(),
            [0x8c, 0xb8, 0x41, 0xdb, 0x40, 0xe6, 0xae, 0x83]
        );
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::XxHash3, data).bytes(),
            [0x72, 0xdc, 0xb1, 0x8b, 0x67, 0xa1, 0x7d, 0xff]
        );
        assert_eq!(
            compute_checksum(ChecksumAlgorithm::XxHash128, data).bytes(),
            [
                0x33, 0x11, 0x94, 0x77, 0xed, 0xe5, 0xdc, 0xd5, 0xe9, 0x71, 0x64, 0x27, 0x68, 0x1d,
                0x58, 0x60,
            ]
        );
    }
}
