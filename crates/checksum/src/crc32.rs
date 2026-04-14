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

#[cfg(feature = "bench-select")]
use std::sync::OnceLock;

/// CRC-32 reflected polynomial.
const POLY: u32 = 0xEDB88320;

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
fn extend_scalar(crc: u32, data: &[u8]) -> u32 {
    let mut state = !crc;
    let mut remaining = data;

    while remaining.len() >= 16 {
        let first = read_u32_le(remaining.as_ptr());
        let second = read_u32_le(remaining.as_ptr().wrapping_add(4));
        let third = read_u32_le(remaining.as_ptr().wrapping_add(8));
        let fourth = read_u32_le(remaining.as_ptr().wrapping_add(12));
        let mixed = state ^ first;
        state = TABLES[15][(mixed & 0xFF) as usize]
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
        let mixed = state ^ read_u32_le(remaining.as_ptr());
        state = TABLES[3][(mixed & 0xFF) as usize]
            ^ TABLES[2][((mixed >> 8) & 0xFF) as usize]
            ^ TABLES[1][((mixed >> 16) & 0xFF) as usize]
            ^ TABLES[0][(mixed >> 24) as usize];
        remaining = &remaining[4..];
    }

    for &byte in remaining {
        let idx = ((state as u8) ^ byte) as usize;
        state = TABLES[0][idx] ^ (state >> 8);
    }

    !state
}

#[cfg(feature = "pure-rust")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PureRustBackend {
    Scalar,
    #[cfg(target_arch = "x86_64")]
    VpclmulX86_64,
    #[cfg(target_arch = "x86_64")]
    PclmulX86_64,
}

#[cfg(feature = "pure-rust")]
#[inline]
fn pure_rust_backend() -> PureRustBackend {
    #[cfg(feature = "bench-select")]
    if let Some(backend) = bench_override_backend() {
        return backend;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if has_vpclmul_x86_64() {
            return PureRustBackend::VpclmulX86_64;
        }

        if has_pclmul_x86_64() {
            return PureRustBackend::PclmulX86_64;
        }
    }

    PureRustBackend::Scalar
}

#[cfg(all(feature = "pure-rust", feature = "bench-select"))]
#[inline]
fn bench_override_backend() -> Option<PureRustBackend> {
    static OVERRIDE: OnceLock<Option<PureRustBackend>> = OnceLock::new();

    *OVERRIDE.get_or_init(|| {
        let override_name = std::env::var("ARGMIN_CRC32_BENCH_BACKEND")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase());

        match override_name.as_deref() {
            Some("scalar") => Some(PureRustBackend::Scalar),
            #[cfg(target_arch = "x86_64")]
            Some("vpclmul") if has_vpclmul_x86_64() => Some(PureRustBackend::VpclmulX86_64),
            #[cfg(target_arch = "x86_64")]
            Some("pclmul") if has_pclmul_x86_64() => Some(PureRustBackend::PclmulX86_64),
            _ => None,
        }
    })
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
#[inline]
fn has_pclmul_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("sse4.1")
        && std::arch::is_x86_feature_detected!("pclmulqdq")
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
#[inline]
fn has_vpclmul_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("sse4.1")
        && std::arch::is_x86_feature_detected!("pclmulqdq")
        && std::arch::is_x86_feature_detected!("vpclmulqdq")
        && std::arch::is_x86_feature_detected!("avx2")
}

#[inline]
fn extend(crc: u32, data: &[u8]) -> u32 {
    #[cfg(feature = "pure-rust")]
    {
        match pure_rust_backend() {
            PureRustBackend::Scalar => extend_scalar(crc, data),
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::VpclmulX86_64 => unsafe { extend_vpclmul_x86_64(crc, data) },
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::PclmulX86_64 => unsafe { extend_pclmul_x86_64(crc, data) },
        }
    }

    #[cfg(all(not(feature = "pure-rust"), feature = "isa-l"))]
    {
        // SAFETY: we pass a valid pointer and exact length. ISA-L reads
        // only within [buf, buf+len). For empty slices the pointer is
        // never dereferenced (len=0).
        unsafe { ec_sys::crc32_gzip_refl(crc, data.as_ptr(), data.len() as u64) }
    }
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
mod x86_64_pclmul {
    use core::arch::x86_64::{
        __m128i, __m256i, _mm256_castsi256_si128, _mm256_clmulepi64_epi128,
        _mm256_extracti128_si256, _mm256_load_si256, _mm256_loadu_si256, _mm256_set_epi64x,
        _mm256_xor_si256, _mm_and_si128, _mm_clmulepi64_si128, _mm_cvtsi128_si32,
        _mm_cvtsi32_si128, _mm_load_si128, _mm_loadu_si128, _mm_slli_si128, _mm_srli_si128,
        _mm_xor_si128,
    };

    #[repr(align(16))]
    struct Aligned([u64; 2]);

    #[repr(align(32))]
    struct Aligned256([u64; 4]);

    const fn aligned(lo: u64, hi: u64) -> Aligned {
        Aligned([lo, hi])
    }

    const fn aligned256(a0: u64, a1: u64, a2: u64, a3: u64) -> Aligned256 {
        Aligned256([a0, a1, a2, a3])
    }

    static FOLD_1: Aligned = aligned(0x00000000ccaa009e, 0x00000001751997d0);
    static FOLD_128_TO_64: Aligned = aligned(0x00000000ccaa009e, 0x0000000163cd6124);
    static BARRETT: Aligned = aligned(0x00000001f7011640, 0x00000001db710640);
    static LO32_CLR_MASK: Aligned = aligned(0xFFFFFFFF00000000, 0xFFFFFFFFFFFFFFFF);
    static HI64_MASK: Aligned = aligned(0xFFFFFFFFFFFFFFFF, 0x0000000000000000);
    static FOLD_8X2: Aligned256 = aligned256(
        0x000000014a7fe880,
        0x00000001e88ef372,
        0x000000014a7fe880,
        0x00000001e88ef372,
    );
    static FOLD_7_6: Aligned256 = aligned256(
        0x00000001d7cfc6ac,
        0x00000001ea89367e,
        0x000000018cb44e58,
        0x00000000df068dc2,
    );
    static FOLD_5_4: Aligned256 = aligned256(
        0x00000000ae0b5394,
        0x00000001c7569e54,
        0x00000001c6e41596,
        0x0000000154442bd4,
    );
    static FOLD_3_2: Aligned256 = aligned256(
        0x0000000174359406,
        0x000000003db1ecdc,
        0x000000015a546366,
        0x00000000f1da05aa,
    );

    #[inline]
    fn load_aligned(value: &Aligned) -> __m128i {
        unsafe { _mm_load_si128(value.0.as_ptr().cast::<__m128i>()) }
    }

    #[inline]
    fn load_block(ptr: *const u8) -> __m128i {
        unsafe { _mm_loadu_si128(ptr.cast::<__m128i>()) }
    }

    #[inline]
    fn load_aligned256(value: &Aligned256) -> __m256i {
        unsafe { _mm256_load_si256(value.0.as_ptr().cast::<__m256i>()) }
    }

    #[inline]
    fn load_block256(ptr: *const u8) -> __m256i {
        unsafe { _mm256_loadu_si256(ptr.cast::<__m256i>()) }
    }

    #[inline]
    fn xor_crc(block: __m128i, crc: u32) -> __m128i {
        unsafe { _mm_xor_si128(block, _mm_cvtsi32_si128(crc as i32)) }
    }

    #[inline]
    fn xor_crc256(block: __m256i, crc: u32) -> __m256i {
        unsafe { _mm256_xor_si256(block, _mm256_set_epi64x(0, 0, 0, crc as i64)) }
    }

    #[inline]
    fn fold_block(x: __m128i, next: __m128i, constant: __m128i) -> __m128i {
        unsafe {
            let lo = _mm_clmulepi64_si128::<0x01>(x, constant);
            let hi = _mm_clmulepi64_si128::<0x10>(x, constant);
            _mm_xor_si128(_mm_xor_si128(lo, hi), next)
        }
    }

    #[inline]
    fn fold_without_next(x: __m128i, constant: __m128i) -> __m128i {
        unsafe {
            let lo = _mm_clmulepi64_si128::<0x01>(x, constant);
            let hi = _mm_clmulepi64_si128::<0x10>(x, constant);
            _mm_xor_si128(lo, hi)
        }
    }

    #[inline]
    fn fold_block256(x: __m256i, next: __m256i, constant: __m256i) -> __m256i {
        unsafe {
            let lo = _mm256_clmulepi64_epi128::<0x01>(x, constant);
            let hi = _mm256_clmulepi64_epi128::<0x10>(x, constant);
            _mm256_xor_si256(_mm256_xor_si256(lo, hi), next)
        }
    }

    #[inline]
    fn fold_without_next256(x: __m256i, constant: __m256i) -> __m256i {
        unsafe {
            let lo = _mm256_clmulepi64_epi128::<0x01>(x, constant);
            let hi = _mm256_clmulepi64_epi128::<0x10>(x, constant);
            _mm256_xor_si256(lo, hi)
        }
    }

    #[inline]
    fn reduce_to_crc(x: __m128i) -> u32 {
        unsafe {
            let fold = load_aligned(&FOLD_128_TO_64);
            let hi = _mm_srli_si128::<8>(x);
            let folded = _mm_xor_si128(_mm_clmulepi64_si128::<0x00>(x, fold), hi);

            let tmp = _mm_slli_si128::<4>(folded);
            let folded32 = _mm_xor_si128(_mm_clmulepi64_si128::<0x10>(tmp, fold), folded);

            let masked = _mm_and_si128(folded32, load_aligned(&LO32_CLR_MASK));
            let y = _mm_xor_si128(
                _mm_clmulepi64_si128::<0x00>(masked, load_aligned(&BARRETT)),
                masked,
            );
            let y = _mm_and_si128(y, load_aligned(&HI64_MASK));
            let z = _mm_xor_si128(_mm_clmulepi64_si128::<0x10>(y, load_aligned(&BARRETT)), y);
            let reduced = _mm_xor_si128(z, masked);

            _mm_cvtsi128_si32(_mm_srli_si128::<8>(reduced)) as u32
        }
    }

    #[target_feature(enable = "sse4.1,pclmulqdq")]
    pub unsafe fn extend(crc: u32, data: &[u8]) -> u32 {
        let prefix_len = data.len() & !0x0F;
        if prefix_len < 16 {
            return super::extend_scalar(crc, data);
        }

        let (prefix, tail) = data.split_at(prefix_len);
        let prefix_crc = extend_blocks_only(crc, prefix);
        super::extend_scalar(prefix_crc, tail)
    }

    #[target_feature(enable = "sse4.1,pclmulqdq,vpclmulqdq,avx2")]
    pub unsafe fn extend_vpclmul(crc: u32, data: &[u8]) -> u32 {
        let prefix_len = data.len() & !0x0F;
        if prefix_len < 128 {
            return extend(crc, data);
        }

        let prefix_len = prefix_len & !0x7F;
        let (prefix, tail) = data.split_at(prefix_len);
        let prefix_crc = extend_blocks_only_vpclmul(crc, prefix);
        super::extend_scalar(prefix_crc, tail)
    }

    #[target_feature(enable = "sse4.1,pclmulqdq")]
    unsafe fn extend_blocks_only(crc: u32, data: &[u8]) -> u32 {
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());
        let fold_1 = load_aligned(&FOLD_1);

        let mut state = xor_crc(load_block(ptr), !crc);
        ptr = ptr.add(16);

        while ptr < end {
            state = fold_block(state, load_block(ptr), fold_1);
            ptr = ptr.add(16);
        }

        !reduce_to_crc(state)
    }

    #[target_feature(enable = "sse4.1,pclmulqdq,vpclmulqdq,avx2")]
    unsafe fn extend_blocks_only_vpclmul(crc: u32, data: &[u8]) -> u32 {
        let fold_8x2 = load_aligned256(&FOLD_8X2);
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        let mut x0 = xor_crc256(load_block256(ptr), !crc);
        let mut x1 = load_block256(ptr.add(32));
        let mut x2 = load_block256(ptr.add(64));
        let mut x3 = load_block256(ptr.add(96));
        ptr = ptr.add(128);

        while ptr < end {
            x0 = fold_block256(x0, load_block256(ptr), fold_8x2);
            x1 = fold_block256(x1, load_block256(ptr.add(32)), fold_8x2);
            x2 = fold_block256(x2, load_block256(ptr.add(64)), fold_8x2);
            x3 = fold_block256(x3, load_block256(ptr.add(96)), fold_8x2);
            ptr = ptr.add(128);
        }

        let accum_7_6 = fold_without_next256(x0, load_aligned256(&FOLD_7_6));
        let accum_5_4 = fold_without_next256(x1, load_aligned256(&FOLD_5_4));
        let accum_3_2 = fold_without_next256(x2, load_aligned256(&FOLD_3_2));
        let accum = _mm256_xor_si256(_mm256_xor_si256(accum_7_6, accum_5_4), accum_3_2);

        let x3_lo = _mm256_castsi256_si128(x3);
        let x3_hi = _mm256_extracti128_si256::<1>(x3);
        let x3_lo_folded = fold_without_next(x3_lo, load_aligned(&FOLD_1));
        let accum_lo = _mm_xor_si128(_mm256_castsi256_si128(accum), x3_lo_folded);
        let accum_hi = _mm256_extracti128_si256::<1>(accum);
        let state = _mm_xor_si128(_mm_xor_si128(x3_hi, accum_lo), accum_hi);

        !reduce_to_crc(state)
    }
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
#[inline]
unsafe fn extend_pclmul_x86_64(crc: u32, data: &[u8]) -> u32 {
    x86_64_pclmul::extend(crc, data)
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
#[inline]
unsafe fn extend_vpclmul_x86_64(crc: u32, data: &[u8]) -> u32 {
    x86_64_pclmul::extend_vpclmul(crc, data)
}

/// Return the active CRC-32 backend name for this build and process.
#[inline]
pub fn backend_name() -> &'static str {
    #[cfg(feature = "pure-rust")]
    {
        return match pure_rust_backend() {
            PureRustBackend::Scalar => "scalar",
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::VpclmulX86_64 => "x86_64-vpclmulqdq",
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::PclmulX86_64 => "x86_64-pclmulqdq",
        };
    }

    #[cfg(all(not(feature = "pure-rust"), feature = "isa-l"))]
    {
        "isa-l"
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
            if has_vpclmul_x86_64() {
                cases.push(BackendCase {
                    backend: PureRustBackend::VpclmulX86_64,
                    name: "x86_64-vpclmulqdq",
                });
            }

            if has_pclmul_x86_64() {
                cases.push(BackendCase {
                    backend: PureRustBackend::PclmulX86_64,
                    name: "x86_64-pclmulqdq",
                });
            }
        }

        cases
    }

    #[cfg(feature = "pure-rust")]
    fn checksum_with_backend(backend: PureRustBackend, data: &[u8]) -> u32 {
        match backend {
            PureRustBackend::Scalar => extend_scalar(0, data),
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::VpclmulX86_64 => unsafe { extend_vpclmul_x86_64(0, data) },
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::PclmulX86_64 => unsafe { extend_pclmul_x86_64(0, data) },
        }
    }

    #[cfg(feature = "pure-rust")]
    fn checksum_streaming_with_backend(
        backend: PureRustBackend,
        data: &[u8],
        chunk_size: usize,
    ) -> u32 {
        let mut crc = 0;
        for chunk in data.chunks(chunk_size.max(1)) {
            crc = match backend {
                PureRustBackend::Scalar => extend_scalar(crc, chunk),
                #[cfg(target_arch = "x86_64")]
                PureRustBackend::VpclmulX86_64 => unsafe { extend_vpclmul_x86_64(crc, chunk) },
                #[cfg(target_arch = "x86_64")]
                PureRustBackend::PclmulX86_64 => unsafe { extend_pclmul_x86_64(crc, chunk) },
            };
        }
        crc
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
