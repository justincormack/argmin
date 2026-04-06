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

#[cfg(not(any(feature = "isa-l", feature = "pure-rust")))]
compile_error!("enable either the `isa-l` or `pure-rust` feature for crate `checksum`");

pub mod crc32;
pub mod crc32c;
pub mod crc64;
mod types;

pub use types::{
    ChecksumAlgorithm, ChecksumBytes, ChecksumType, InvalidChecksumConfig, MultipartChecksumConfig,
    RawChecksum,
};
