//! CRC-32C (Castagnoli / iSCSI) checksum with combine support.
//!
//! Uses ISA-L when the `isa-l` feature is enabled and a pure-Rust table-based
//! implementation when the `pure-rust` feature is enabled. Also provides GF(2)
//! matrix exponentiation for combining independently computed checksums.
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

#[cfg(feature = "pure-rust")]
const TABLES: [[u32; 256]; 16] = build_tables();

#[cfg(feature = "pure-rust")]
const fn build_tables() -> [[u32; 256]; 16] {
    let mut tables = [[0u32; 256]; 16];
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
        tables[0][i] = crc;
        i += 1;
    }

    let mut table_idx = 1usize;
    while table_idx < 16 {
        let mut byte = 0usize;
        while byte < 256 {
            let crc = tables[table_idx - 1][byte];
            tables[table_idx][byte] = tables[0][(crc & 0xFF) as usize] ^ (crc >> 8);
            byte += 1;
        }
        table_idx += 1;
    }

    tables
}

#[cfg(feature = "pure-rust")]
#[inline]
fn read_u32_le(ptr: *const u8) -> u32 {
    // SAFETY: callers only pass pointers proven to be valid for at least
    // 4 bytes. We use read_unaligned because the input buffer may not be
    // naturally aligned.
    unsafe { u32::from_le(ptr.cast::<u32>().read_unaligned()) }
}

#[cfg(feature = "pure-rust")]
#[inline]
fn read_u64_le(ptr: *const u8) -> u64 {
    // SAFETY: callers only pass pointers proven to be valid for at least
    // 8 bytes. We use read_unaligned because the input buffer may not be
    // naturally aligned.
    unsafe { u64::from_le(ptr.cast::<u64>().read_unaligned()) }
}

#[cfg(feature = "pure-rust")]
#[inline]
fn update_scalar(mut crc: u32, data: &[u8]) -> u32 {
    let mut remaining = data;

    while remaining.len() >= 16 {
        let first = read_u32_le(remaining.as_ptr());
        let second = read_u32_le(remaining.as_ptr().wrapping_add(4));
        let third = read_u32_le(remaining.as_ptr().wrapping_add(8));
        let fourth = read_u32_le(remaining.as_ptr().wrapping_add(12));
        let mixed = crc ^ first;
        crc = TABLES[15][(mixed & 0xFF) as usize]
            ^ TABLES[14][((mixed >> 8) & 0xFF) as usize]
            ^ TABLES[13][((mixed >> 16) & 0xFF) as usize]
            ^ TABLES[12][(mixed >> 24) as usize]
            ^ TABLES[11][(second & 0xFF) as usize]
            ^ TABLES[10][((second >> 8) & 0xFF) as usize]
            ^ TABLES[9][((second >> 16) & 0xFF) as usize]
            ^ TABLES[8][(second >> 24) as usize]
            ^ TABLES[7][(third & 0xFF) as usize]
            ^ TABLES[6][((third >> 8) & 0xFF) as usize]
            ^ TABLES[5][((third >> 16) & 0xFF) as usize]
            ^ TABLES[4][(third >> 24) as usize]
            ^ TABLES[3][(fourth & 0xFF) as usize]
            ^ TABLES[2][((fourth >> 8) & 0xFF) as usize]
            ^ TABLES[1][((fourth >> 16) & 0xFF) as usize]
            ^ TABLES[0][(fourth >> 24) as usize];
        remaining = &remaining[16..];
    }

    while remaining.len() >= 4 {
        let mixed = crc ^ read_u32_le(remaining.as_ptr());
        crc = TABLES[3][(mixed & 0xFF) as usize]
            ^ TABLES[2][((mixed >> 8) & 0xFF) as usize]
            ^ TABLES[1][((mixed >> 16) & 0xFF) as usize]
            ^ TABLES[0][(mixed >> 24) as usize];
        remaining = &remaining[4..];
    }

    for &byte in remaining {
        let idx = ((crc as u8) ^ byte) as usize;
        crc = TABLES[0][idx] ^ (crc >> 8);
    }

    crc
}

#[cfg(feature = "pure-rust")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PureRustBackend {
    Scalar,
    #[cfg(target_arch = "x86_64")]
    Sse42X86_64,
}

#[cfg(feature = "pure-rust")]
#[inline]
fn pure_rust_backend() -> PureRustBackend {
    #[cfg(target_arch = "x86_64")]
    {
        if has_sse42_x86_64() {
            return PureRustBackend::Sse42X86_64;
        }
    }

    PureRustBackend::Scalar
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
#[inline]
fn has_sse42_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("sse4.2")
}

#[inline]
fn update_internal(crc: u32, data: &[u8]) -> u32 {
    #[cfg(feature = "pure-rust")]
    {
        match pure_rust_backend() {
            PureRustBackend::Scalar => update_scalar(crc, data),
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::Sse42X86_64 => unsafe { update_sse42_x86_64(crc, data) },
        }
    }

    #[cfg(all(not(feature = "pure-rust"), feature = "isa-l"))]
    {
        let mut crc = crc;
        // Unlike crc32_gzip_refl, crc32_iscsi does NOT handle
        // init=0xFFFFFFFF / final XOR internally, so we do it here.
        //
        // crc32_iscsi takes len as c_int, so we process in chunks to
        // avoid truncation for buffers larger than i32::MAX.
        const CHUNK: usize = std::ffi::c_int::MAX as usize;
        let mut remaining = data;
        while !remaining.is_empty() {
            let n = remaining.len().min(CHUNK);
            // SAFETY: pointer is valid for n bytes. The cast to *mut is
            // safe because ISA-L does not mutate the buffer.
            crc = unsafe {
                ec_sys::crc32_iscsi(remaining.as_ptr() as *mut _, n as std::ffi::c_int, crc)
            };
            remaining = &remaining[n..];
        }
        crc
    }
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
#[target_feature(enable = "sse4.2")]
unsafe fn update_sse42_x86_64(mut crc: u32, data: &[u8]) -> u32 {
    use core::arch::x86_64::{_mm_crc32_u32, _mm_crc32_u64, _mm_crc32_u8};

    let mut remaining = data;

    while remaining.len() >= 8 {
        crc = _mm_crc32_u64(crc as u64, read_u64_le(remaining.as_ptr())) as u32;
        remaining = &remaining[8..];
    }

    while remaining.len() >= 4 {
        crc = _mm_crc32_u32(crc, read_u32_le(remaining.as_ptr()));
        remaining = &remaining[4..];
    }

    for &byte in remaining {
        crc = _mm_crc32_u8(crc, byte);
    }

    crc
}

/// Compute CRC-32C over the entire buffer.
#[inline]
pub fn checksum(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    crc = update_internal(crc, data);
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

/// Streaming CRC-32C hasher.
///
/// Handles the init=0xFFFFFFFF and final XOR=0xFFFFFFFF internally,
/// matching [`checksum()`]. Feed data in chunks via [`update`](Hasher::update),
/// then call [`finalize`](Hasher::finalize) to get the checksum.
///
/// # Examples
///
/// ```
/// let mut hasher = checksum::crc32c::Hasher::new();
/// hasher.update(b"12345");
/// hasher.update(b"6789");
/// assert_eq!(hasher.finalize(), 0xE3069283);
/// ```
#[derive(Clone, Debug)]
pub struct Hasher {
    crc: u32,
}

impl Hasher {
    /// Create a new hasher with initial state.
    #[inline]
    pub fn new() -> Self {
        Self { crc: !0u32 }
    }

    /// Feed more data into the hasher.
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        self.crc = update_internal(self.crc, data);
    }

    /// Return the CRC-32C checksum of all data fed so far.
    #[inline]
    pub fn finalize(&self) -> u32 {
        self.crc ^ !0u32
    }

    /// Reset the hasher to its initial state.
    #[inline]
    pub fn reset(&mut self) {
        self.crc = !0u32;
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

    #[cfg(feature = "pure-rust")]
    #[derive(Clone, Copy, Debug)]
    struct BackendCase {
        backend: PureRustBackend,
        name: &'static str,
    }

    #[cfg(feature = "pure-rust")]
    fn supported_backend_cases() -> Vec<BackendCase> {
        let mut cases = vec![BackendCase {
            backend: PureRustBackend::Scalar,
            name: "scalar",
        }];

        #[cfg(target_arch = "x86_64")]
        {
            if has_sse42_x86_64() {
                cases.push(BackendCase {
                    backend: PureRustBackend::Sse42X86_64,
                    name: "x86_64-sse4.2-crc32",
                });
            }
        }

        cases
    }

    #[cfg(feature = "pure-rust")]
    fn checksum_with_backend(backend: PureRustBackend, data: &[u8]) -> u32 {
        let mut crc = !0u32;
        crc = match backend {
            PureRustBackend::Scalar => update_scalar(crc, data),
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::Sse42X86_64 => unsafe { update_sse42_x86_64(crc, data) },
        };
        crc ^ !0u32
    }

    #[cfg(feature = "pure-rust")]
    fn checksum_streaming_with_backend(
        backend: PureRustBackend,
        data: &[u8],
        chunk_size: usize,
    ) -> u32 {
        let mut crc = !0u32;
        for chunk in data.chunks(chunk_size.max(1)) {
            crc = match backend {
                PureRustBackend::Scalar => update_scalar(crc, chunk),
                #[cfg(target_arch = "x86_64")]
                PureRustBackend::Sse42X86_64 => unsafe { update_sse42_x86_64(crc, chunk) },
            };
        }
        crc ^ !0u32
    }

    #[cfg(feature = "pure-rust")]
    #[test]
    fn supported_backends_match_scalar_across_boundaries() {
        let lengths = [
            0usize, 1, 2, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257,
            511, 512, 513, 1024, 4096,
        ];
        let chunk_sizes = [1usize, 2, 3, 7, 15, 16, 17, 31, 32, 63, 64, 127];
        let backend_cases = supported_backend_cases();

        for &len in &lengths {
            let data: Vec<u8> = (0u8..=255)
                .cycle()
                .take(len)
                .enumerate()
                .map(|(i, byte)| byte ^ ((i as u8).wrapping_mul(17)))
                .collect();
            let expected = checksum_with_backend(PureRustBackend::Scalar, &data);

            for case in &backend_cases {
                let actual = checksum_with_backend(case.backend, &data);
                assert_eq!(
                    actual, expected,
                    "backend {} mismatch at len {}",
                    case.name, len
                );

                for &chunk_size in &chunk_sizes {
                    let streaming =
                        checksum_streaming_with_backend(case.backend, &data, chunk_size);
                    assert_eq!(
                        streaming, expected,
                        "backend {} streaming mismatch at len {} chunk_size {}",
                        case.name, len, chunk_size
                    );
                }
            }
        }
    }

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
    fn streaming_empty() {
        let hasher = Hasher::new();
        assert_eq!(hasher.finalize(), checksum(b""));
    }

    #[test]
    fn streaming_default() {
        let hasher = Hasher::default();
        assert_eq!(hasher.finalize(), checksum(b""));
    }

    #[test]
    fn streaming_reset() {
        let mut hasher = Hasher::new();
        hasher.update(b"garbage");
        hasher.reset();
        hasher.update(b"123456789");
        assert_eq!(hasher.finalize(), 0xE3069283);
    }

    #[test]
    fn streaming_finalize_is_idempotent() {
        let mut hasher = Hasher::new();
        hasher.update(b"123456789");
        let first = hasher.finalize();
        let second = hasher.finalize();
        assert_eq!(first, second);
    }

    // ── Property-based tests ────────────────────────────────────────

    proptest! {
        #[test]
        fn prop_streaming_matches_oneshot(
            data in proptest::collection::vec(any::<u8>(), 0..=2048),
            mut splits in proptest::collection::vec(0usize..=2048, 0..=32),
        ) {
            let len = data.len();
            splits.retain(|&i| i <= len);
            splits.sort_unstable();
            splits.dedup();
            if splits.first().copied() != Some(0) {
                splits.insert(0, 0);
            }
            if splits.last().copied() != Some(len) {
                splits.push(len);
            }

            let mut hasher = Hasher::new();
            for w in splits.windows(2) {
                hasher.update(&data[w[0]..w[1]]);
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
