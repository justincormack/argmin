//! CRC-32C (Castagnoli / iSCSI) checksum with hardware-accelerated combine.
//!
//! Wraps ISA-L's `crc32_iscsi` for one-shot computation and provides
//! GF(2) matrix exponentiation for combining independently computed checksums.
//!
//! # Examples
//!
//! ```
//! let crc = checksum::crc32c::checksum(b"123456789");
//! assert_eq!(crc, 0xE3069283);
//! ```
//!
//! Combining:
//! ```
//! let crc_a = checksum::crc32c::checksum(b"hello ");
//! let crc_b = checksum::crc32c::checksum(b"world!");
//! let combined = checksum::crc32c::combine(crc_a, crc_b, 6);
//! assert_eq!(combined, checksum::crc32c::checksum(b"hello world!"));
//! ```

/// CRC-32C (Castagnoli) reflected polynomial.
const POLY: u32 = 0x82F63B78;

/// Compute CRC-32C over the entire buffer.
#[inline]
pub fn checksum(data: &[u8]) -> u32 {
    // Unlike crc32_gzip_refl, crc32_iscsi does NOT handle
    // init=0xFFFFFFFF / final XOR internally, so we do it here.
    //
    // crc32_iscsi takes len as c_int, so we process in chunks to
    // avoid truncation for buffers larger than i32::MAX.
    const CHUNK: usize = std::ffi::c_int::MAX as usize;
    let mut crc = !0u32;
    let mut remaining = data;
    while !remaining.is_empty() {
        let n = remaining.len().min(CHUNK);
        // SAFETY: pointer is valid for n bytes. The cast to *mut is
        // safe because ISA-L does not mutate the buffer.
        crc =
            unsafe { ec_sys::crc32_iscsi(remaining.as_ptr() as *mut _, n as std::ffi::c_int, crc) };
        remaining = &remaining[n..];
    }
    crc ^ !0u32
}

/// Combine two independently computed CRC-32C checksums.
///
/// Given `crc_a = checksum(A)` and `crc_b = checksum(B)`, returns
/// `checksum(A || B)` without needing the original data. `len_b` is
/// the byte length of `B`.
pub fn combine(crc_a: u32, crc_b: u32, len_b: u64) -> u32 {
    if len_b == 0 {
        return crc_a;
    }

    let mut odd = [0u32; 32];
    let mut even = [0u32; 32];

    // odd = operator for 1 zero bit
    odd[0] = POLY;
    for (i, slot) in odd.iter_mut().enumerate().skip(1) {
        *slot = 1u32 << (i - 1);
    }

    // even = operator for 2 zero bits
    gf2_matrix_square(&mut even, &odd);
    // odd = operator for 4 zero bits
    gf2_matrix_square(&mut odd, &even);

    let mut crc = crc_a;
    let mut n = len_b;
    loop {
        gf2_matrix_square(&mut even, &odd);
        if n & 1 != 0 {
            crc = gf2_matrix_times(&even, crc);
        }
        n >>= 1;
        if n == 0 {
            break;
        }

        gf2_matrix_square(&mut odd, &even);
        if n & 1 != 0 {
            crc = gf2_matrix_times(&odd, crc);
        }
        n >>= 1;
        if n == 0 {
            break;
        }
    }

    crc ^ crc_b
}

/// Multiply a GF(2) matrix by a vector.
fn gf2_matrix_times(mat: &[u32; 32], mut vec: u32) -> u32 {
    let mut result = 0u32;
    let mut i = 0;
    while vec != 0 {
        if vec & 1 != 0 {
            result ^= mat[i];
        }
        vec >>= 1;
        i += 1;
    }
    result
}

/// Square a GF(2) matrix: square = mat * mat.
fn gf2_matrix_square(square: &mut [u32; 32], mat: &[u32; 32]) {
    for i in 0..32 {
        square[i] = gf2_matrix_times(mat, mat[i]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ── Standard test vectors ────────────────────────────────────────

    #[test]
    fn check_string_123456789() {
        assert_eq!(checksum(b"123456789"), 0xE3069283);
    }

    #[test]
    fn empty_input() {
        assert_eq!(checksum(b""), 0);
    }

    #[test]
    fn single_byte() {
        let a = checksum(&[0x00]);
        let b = checksum(&[0x00]);
        assert_eq!(a, b);
        assert_ne!(checksum(&[0x00]), checksum(&[0x01]));
    }

    #[test]
    fn zeros_32() {
        let zeros = [0u8; 32];
        let crc = checksum(&zeros);
        assert_ne!(crc, 0);
    }

    // ── Combine ──────────────────────────────────────────────────────

    #[test]
    fn combine_basic() {
        let crc_a = checksum(b"12345");
        let crc_b = checksum(b"6789");
        assert_eq!(combine(crc_a, crc_b, 4), checksum(b"123456789"));
    }

    #[test]
    fn combine_three_parts() {
        let c1 = checksum(b"123");
        let c2 = checksum(b"456");
        let c3 = checksum(b"789");
        let c12 = combine(c1, c2, 3);
        let c123 = combine(c12, c3, 3);
        assert_eq!(c123, checksum(b"123456789"));
    }

    #[test]
    fn combine_empty_b() {
        let crc_a = checksum(b"hello");
        assert_eq!(combine(crc_a, checksum(b""), 0), crc_a);
    }

    #[test]
    fn combine_empty_a() {
        let crc_b = checksum(b"world");
        assert_eq!(combine(checksum(b""), crc_b, 5), crc_b);
    }

    #[test]
    fn combine_one_mb_halves() {
        let data: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        let half = data.len() / 2;
        let crc_full = checksum(&data);
        let crc_h1 = checksum(&data[..half]);
        let crc_h2 = checksum(&data[half..]);
        assert_eq!(combine(crc_h1, crc_h2, half as u64), crc_full);
    }

    // ── Property-based tests ────────────────────────────────────────

    proptest! {
        #[test]
        fn prop_combine_matches_concat(
            data in proptest::collection::vec(any::<u8>(), 0..=2048),
            split in 0usize..=2048,
        ) {
            let len = data.len();
            let split = split.min(len);
            let (a, b) = data.split_at(split);
            let crc_a = checksum(a);
            let crc_b = checksum(b);
            let combined = combine(crc_a, crc_b, b.len() as u64);
            prop_assert_eq!(combined, checksum(&data));
        }

        #[test]
        fn prop_combine_associative(
            data in proptest::collection::vec(any::<u8>(), 0..=2048),
            a_end in 0usize..=2048,
            b_end in 0usize..=2048,
        ) {
            let len = data.len();
            let a_end = a_end.min(len);
            let b_end = b_end.min(len);
            let (a_end, b_end) = if a_end <= b_end { (a_end, b_end) } else { (b_end, a_end) };

            let (a, rest) = data.split_at(a_end);
            let (b, c) = rest.split_at(b_end - a_end);

            let crc_a = checksum(a);
            let crc_b = checksum(b);
            let crc_c = checksum(c);

            let left = combine(combine(crc_a, crc_b, b.len() as u64), crc_c, c.len() as u64);
            let right = combine(crc_a, combine(crc_b, crc_c, c.len() as u64), (b.len() + c.len()) as u64);
            prop_assert_eq!(left, right);
            prop_assert_eq!(left, checksum(&data));
        }
    }
}
