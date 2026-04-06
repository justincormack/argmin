//! CRC-32 (IEEE / gzip) checksum with combine support.
//!
//! Uses ISA-L when the `isa-l` feature is enabled and a pure-Rust table-based
//! implementation when the `pure-rust` feature is enabled. Also provides GF(2)
//! matrix exponentiation for combining independently computed checksums.
//!
//! # Examples
//!
//! ```
//! let crc = checksum::crc32::checksum(b"123456789");
//! assert_eq!(crc, 0xCBF43926);
//! ```
//!
//! Combining:
//! ```
//! let crc_a = checksum::crc32::checksum(b"hello ");
//! let crc_b = checksum::crc32::checksum(b"world!");
//! let combined = checksum::crc32::combine(crc_a, crc_b, 6);
//! assert_eq!(combined, checksum::crc32::checksum(b"hello world!"));
//! ```

/// CRC-32 reflected polynomial.
const POLY: u32 = 0xEDB88320;

#[cfg(feature = "pure-rust")]
const TABLE: [u32; 256] = build_table();

#[cfg(feature = "pure-rust")]
const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0u8;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

#[inline]
fn extend(crc: u32, data: &[u8]) -> u32 {
    #[cfg(feature = "pure-rust")]
    {
        let mut state = !crc;
        for &byte in data {
            let idx = ((state as u8) ^ byte) as usize;
            state = TABLE[idx] ^ (state >> 8);
        }
        !state
    }

    #[cfg(all(not(feature = "pure-rust"), feature = "isa-l"))]
    {
        // SAFETY: we pass a valid pointer and exact length. ISA-L reads
        // only within [buf, buf+len). For empty slices the pointer is
        // never dereferenced (len=0).
        unsafe { ec_sys::crc32_gzip_refl(crc, data.as_ptr(), data.len() as u64) }
    }
}

/// Compute CRC-32 (gzip/IEEE) over the entire buffer.
#[inline]
pub fn checksum(data: &[u8]) -> u32 {
    extend(0, data)
}

/// Streaming CRC-32 (gzip/IEEE) hasher.
///
/// Feed data in chunks via [`update`](Hasher::update), then call
/// [`finalize`](Hasher::finalize) to get the checksum.
#[derive(Clone, Debug)]
pub struct Hasher {
    crc: u32,
}

impl Hasher {
    /// Create a new hasher with initial state.
    #[inline]
    pub fn new() -> Self {
        Self { crc: 0 }
    }

    /// Feed more data into the hasher.
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        self.crc = extend(self.crc, data);
    }

    /// Return the CRC-32 checksum of all data fed so far.
    #[inline]
    pub fn finalize(&self) -> u32 {
        self.crc
    }

    /// Reset the hasher to its initial state.
    #[inline]
    pub fn reset(&mut self) {
        self.crc = 0;
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Combine two independently computed CRC-32 checksums.
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
        assert_eq!(checksum(b"123456789"), 0xCBF43926);
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

    #[test]
    fn streaming_matches_oneshot() {
        let data = b"hello world";
        let mut hasher = Hasher::new();
        hasher.update(b"hello ");
        hasher.update(b"world");
        assert_eq!(hasher.finalize(), checksum(data));
    }

    #[test]
    fn streaming_empty() {
        let hasher = Hasher::new();
        assert_eq!(hasher.finalize(), checksum(b""));
    }

    #[test]
    fn streaming_reset() {
        let mut hasher = Hasher::new();
        hasher.update(b"abc");
        hasher.reset();
        hasher.update(b"123456789");
        assert_eq!(hasher.finalize(), 0xCBF43926);
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
