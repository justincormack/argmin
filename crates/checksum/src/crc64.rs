//! CRC-64/NVME (= CRC-64/Rocksoft) checksum.
//!
//! Wraps ISA-L's `crc64_rocksoft_refl`, which auto-selects the fastest
//! implementation at runtime (table-based, CLMUL, or AVX-512).
//!
//! # Examples
//!
//! One-shot:
//! ```
//! let checksum = checksum::crc64::checksum(b"123456789");
//! assert_eq!(checksum, 0xAE8B14860A799888);
//! ```
//!
//! Streaming:
//! ```
//! let mut hasher = checksum::crc64::Hasher::new();
//! hasher.update(b"12345");
//! hasher.update(b"6789");
//! assert_eq!(hasher.finalize(), 0xAE8B14860A799888);
//! ```
//!
//! Combining independently computed checksums:
//! ```
//! let crc_a = checksum::crc64::checksum(b"hello ");
//! let crc_b = checksum::crc64::checksum(b"world!");
//! let combined = checksum::crc64::combine(crc_a, crc_b, 6);
//! assert_eq!(combined, checksum::crc64::checksum(b"hello world!"));
//! ```

/// CRC-64/NVME reflected polynomial (bit-reversal of 0xAD93D23594C93659).
const POLY: u64 = 0x9A6C9329AC4BC9B5;

/// Compute CRC-64/NVME over the entire buffer.
#[inline]
pub fn checksum(data: &[u8]) -> u64 {
    // SAFETY: we pass a valid pointer and exact length. ISA-L reads
    // only within [buf, buf+len). For empty slices the pointer is
    // never dereferenced (len=0).
    unsafe { ec_sys::crc64_rocksoft_refl(0, data.as_ptr(), data.len() as u64) }
}

/// Combine two independently computed CRC-64/NVME checksums.
///
/// Given `crc_a = checksum(A)` and `crc_b = checksum(B)`, returns
/// `checksum(A || B)` without needing the original data. `len_b` is
/// the byte length of `B`.
///
/// This enables parallel hashing: compute the CRC of each part
/// independently (potentially on different threads or machines), then
/// combine the results. Useful for multipart uploads.
pub fn combine(crc_a: u64, crc_b: u64, len_b: u64) -> u64 {
    if len_b == 0 {
        return crc_a;
    }

    // Use GF(2) matrix exponentiation (zlib approach).
    //
    // The CRC shift register for one zero bit is a linear operation
    // representable as a 64×64 matrix over GF(2). We use repeated
    // squaring to compute the matrix for len_b zero bytes, then apply
    // it to crc_a. Finally XOR with crc_b.

    let mut odd = [0u64; 64];
    let mut even = [0u64; 64];

    // odd = operator for 1 zero bit
    odd[0] = POLY;
    for (i, slot) in odd.iter_mut().enumerate().skip(1) {
        *slot = 1u64 << (i - 1);
    }

    // even = operator for 2 zero bits
    gf2_matrix_square(&mut even, &odd);
    // odd = operator for 4 zero bits
    gf2_matrix_square(&mut odd, &even);

    // Iterate over the bits of len_b. Each loop iteration squares
    // twice, so the matrices advance through 8, 16, 32, ... zero-bit
    // operators — exactly one byte, two bytes, four bytes, etc.
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
fn gf2_matrix_times(mat: &[u64; 64], mut vec: u64) -> u64 {
    let mut result = 0u64;
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

/// Square a GF(2) matrix: square = mat × mat.
fn gf2_matrix_square(square: &mut [u64; 64], mat: &[u64; 64]) {
    for i in 0..64 {
        square[i] = gf2_matrix_times(mat, mat[i]);
    }
}

/// Streaming CRC-64/NVME hasher.
///
/// Feed data in chunks via [`update`](Hasher::update), then call
/// [`finalize`](Hasher::finalize) to get the checksum.
#[derive(Clone, Debug)]
pub struct Hasher {
    crc: u64,
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
        // SAFETY: same as `checksum` — valid pointer and length.
        self.crc =
            unsafe { ec_sys::crc64_rocksoft_refl(self.crc, data.as_ptr(), data.len() as u64) };
    }

    /// Return the CRC-64/NVME checksum of all data fed so far.
    #[inline]
    pub fn finalize(&self) -> u64 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ── Standard test vectors ────────────────────────────────────────

    #[test]
    fn check_string_123456789() {
        // The canonical CRC-64/NVME check value from the NVMe spec.
        assert_eq!(checksum(b"123456789"), 0xAE8B14860A799888);
    }

    #[test]
    fn check_string_hello_world() {
        assert_eq!(checksum(b"hello world!"), 0xD9160D1FA8E418E3);
    }

    #[test]
    fn check_32_zero_bytes() {
        // AWS aws-checksums test vector.
        let zeros = [0u8; 32];
        assert_eq!(checksum(&zeros), 0xCF3473434D4ECF3B);
    }

    #[test]
    fn check_32_sequential_bytes() {
        // AWS aws-checksums test vector: bytes 0x00..0x1F.
        let seq: Vec<u8> = (0u8..32).collect();
        assert_eq!(checksum(&seq), 0xB9D9D4A8492CBD7F);
    }

    // ── Edge cases ───────────────────────────────────────────────────

    #[test]
    fn empty_input() {
        assert_eq!(checksum(b""), 0);
    }

    #[test]
    fn single_byte() {
        // Smoke test: single-byte inputs must not panic and must be
        // deterministic.
        let a = checksum(&[0x00]);
        let b = checksum(&[0x00]);
        assert_eq!(a, b);
        assert_ne!(checksum(&[0x00]), checksum(&[0x01]));
    }

    // ── Streaming hasher ─────────────────────────────────────────────

    #[test]
    fn streaming_matches_oneshot() {
        let mut hasher = Hasher::new();
        hasher.update(b"12345");
        hasher.update(b"6789");
        assert_eq!(hasher.finalize(), checksum(b"123456789"));
    }

    #[test]
    fn streaming_byte_at_a_time() {
        let data = b"hello world!";
        let mut hasher = Hasher::new();
        for &byte in data {
            hasher.update(std::slice::from_ref(&byte));
        }
        assert_eq!(hasher.finalize(), checksum(data));
    }

    #[test]
    fn streaming_reset() {
        let mut hasher = Hasher::new();
        hasher.update(b"garbage");
        hasher.reset();
        hasher.update(b"123456789");
        assert_eq!(hasher.finalize(), 0xAE8B14860A799888);
    }

    #[test]
    fn streaming_empty() {
        let hasher = Hasher::new();
        assert_eq!(hasher.finalize(), 0);
    }

    #[test]
    fn streaming_finalize_is_idempotent() {
        let mut hasher = Hasher::new();
        hasher.update(b"123456789");
        let first = hasher.finalize();
        let second = hasher.finalize();
        assert_eq!(first, second);
    }

    #[test]
    fn hasher_clone_is_independent() {
        let mut h1 = Hasher::new();
        h1.update(b"12345");
        let mut h2 = h1.clone();
        h1.update(b"6789");
        h2.update(b"ABCD");
        // They diverged after the clone.
        assert_ne!(h1.finalize(), h2.finalize());
        // h1 should still match the full "123456789" checksum.
        assert_eq!(h1.finalize(), checksum(b"123456789"));
    }

    // ── Larger data ──────────────────────────────────────────────────

    #[test]
    fn four_kb_zeros() {
        let data = vec![0u8; 4096];
        let oneshot = checksum(&data);
        // Streaming in 1KB chunks must match.
        let mut hasher = Hasher::new();
        for chunk in data.chunks(1024) {
            hasher.update(chunk);
        }
        assert_eq!(hasher.finalize(), oneshot);
    }

    #[test]
    fn one_mb_deterministic() {
        // 1MB of patterned data: CRC must be deterministic.
        let data: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        let a = checksum(&data);
        let b = checksum(&data);
        assert_eq!(a, b);
        assert_ne!(a, 0);
    }

    // ── Combine ──────────────────────────────────────────────────────

    #[test]
    fn combine_basic() {
        let crc_a = checksum(b"12345");
        let crc_b = checksum(b"6789");
        assert_eq!(combine(crc_a, crc_b, 4), checksum(b"123456789"));
    }

    #[test]
    fn combine_hello_world() {
        let crc_a = checksum(b"hello ");
        let crc_b = checksum(b"world!");
        assert_eq!(combine(crc_a, crc_b, 6), checksum(b"hello world!"));
    }

    #[test]
    fn combine_single_byte_prefix() {
        let crc_a = checksum(b"A");
        let crc_b = checksum(b"BCDEF");
        assert_eq!(combine(crc_a, crc_b, 5), checksum(b"ABCDEF"));
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
    fn combine_three_parts() {
        let c1 = checksum(b"123");
        let c2 = checksum(b"456");
        let c3 = checksum(b"789");
        let c12 = combine(c1, c2, 3);
        let c123 = combine(c12, c3, 3);
        assert_eq!(c123, checksum(b"123456789"));
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

    #[test]
    fn combine_unequal_parts() {
        let data = b"The quick brown fox jumps over the lazy dog";
        // Split at an arbitrary point.
        let split = 17;
        let crc_a = checksum(&data[..split]);
        let crc_b = checksum(&data[split..]);
        assert_eq!(
            combine(crc_a, crc_b, (data.len() - split) as u64),
            checksum(data)
        );
    }

    #[test]
    fn combine_many_small_parts() {
        // Combine 100 single-byte CRCs.
        let data: Vec<u8> = (0u8..100).collect();
        let mut running = checksum(&data[..1]);
        for i in 1..100 {
            running = combine(running, checksum(&data[i..i + 1]), 1);
        }
        assert_eq!(running, checksum(&data));
    }

    // ── Property-based tests ────────────────────────────────────────

    proptest! {
        #[test]
        fn prop_streaming_matches_oneshot(
            data in proptest::collection::vec(any::<u8>(), 0..=2048),
            mut splits in proptest::collection::vec(0usize..=2048, 0..=32),
        ) {
            let len = data.len();
            // Normalize split points into a sorted unique list in [0, len].
            splits.retain(|&i| i <= len);
            splits.sort_unstable();
            splits.dedup();
            // Ensure 0 and len are included.
            if splits.first().copied() != Some(0) {
                splits.insert(0, 0);
            }
            if splits.last().copied() != Some(len) {
                splits.push(len);
            }

            let mut hasher = Hasher::new();
            for w in splits.windows(2) {
                let start = w[0];
                let end = w[1];
                hasher.update(&data[start..end]);
            }
            prop_assert_eq!(hasher.finalize(), checksum(&data));
        }

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
        fn prop_combine_associative_three_parts(
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
