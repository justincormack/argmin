// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Logical write bounds shared by ingestion and the storage mutation owner.
//! These do not limit the total size of an object assembled from MPU parts.

/// Maximum logical payload of a single PUT or one multipart part (5 GiB).
pub const MAX_SINGLE_WRITE_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Logical segment size used by payload ingestion (ciphertext includes overhead).
pub const INTERNAL_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

pub(crate) const MAX_STREAM_SEGMENTS: u32 =
    MAX_SINGLE_WRITE_SIZE.div_ceil(INTERNAL_SEGMENT_SIZE as u64) as u32;

// A future limit change must not truncate the derived segment-index bound.
const _: () = assert!(
    MAX_SINGLE_WRITE_SIZE > 0
        && MAX_SINGLE_WRITE_SIZE.div_ceil(INTERNAL_SEGMENT_SIZE as u64) <= u32::MAX as u64
);

/// Bounds a selected segment, not a speculative lookahead descriptor. The
/// terminal lookahead index is never available for payload allocation. This
/// deliberately does not prove ordering, contiguous prior segments, encryption,
/// reservation ownership or a durable staging window.
pub(crate) const fn stream_segment_extent_is_valid(index: u32, logical_size: u64) -> bool {
    index < MAX_STREAM_SEGMENTS
        && logical_size <= INTERNAL_SEGMENT_SIZE as u64
        && (index as u64) * (INTERNAL_SEGMENT_SIZE as u64) + logical_size <= MAX_SINGLE_WRITE_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_segment_extent_limits_pin_first_last_and_lookahead_boundaries() {
        assert_eq!(MAX_STREAM_SEGMENTS, 640);
        for index in [0, MAX_STREAM_SEGMENTS - 1] {
            for size in [0, 1, INTERNAL_SEGMENT_SIZE as u64] {
                assert!(stream_segment_extent_is_valid(index, size));
            }
            for size in [INTERNAL_SEGMENT_SIZE as u64 + 1, u64::MAX] {
                assert!(!stream_segment_extent_is_valid(index, size));
            }
        }
        for index in [MAX_STREAM_SEGMENTS, MAX_STREAM_SEGMENTS + 1, u32::MAX] {
            for size in [0, 1, INTERNAL_SEGMENT_SIZE as u64, u64::MAX] {
                assert!(!stream_segment_extent_is_valid(index, size));
            }
        }
    }
}
