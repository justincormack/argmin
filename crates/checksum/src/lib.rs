#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::doc_markdown,
    clippy::match_same_arms,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::ptr_cast_constness,
    clippy::range_plus_one,
    clippy::unreadable_literal
)]

pub mod crc32;
pub mod crc32c;
pub mod crc64;
mod hash;
mod types;

pub use hash::{compute_checksum, ChecksumHasher};
pub use types::{
    ChecksumAlgorithm, ChecksumBytes, ChecksumBytesError, ChecksumType, InvalidChecksumConfig,
    MultipartChecksumConfig, RawChecksum, RawChecksumError,
};
