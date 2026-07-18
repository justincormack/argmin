//! CRC-32C (Castagnoli / iSCSI) checksum with combine support.
//!
//! Uses the native Rust implementation on all builds. On supported CPUs it
//! runtime-dispatches to hardware-accelerated backends and otherwise falls
//! back to a scalar slicing-by-16 implementation. Also provides GF(2) matrix
//! exponentiation for combining independently computed checksums.
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

#[cfg(feature = "bench-select")]
use std::sync::OnceLock;

/// CRC-32C (Castagnoli) reflected polynomial.
const POLY: u32 = 0x82F63B78;

const TABLES: [[u32; 256]; 16] = build_tables();

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

#[cfg(target_arch = "aarch64")]
#[inline]
fn read_u16_le(ptr: *const u8) -> u16 {
    // SAFETY: callers only pass pointers proven to be valid for at least
    // 2 bytes. We use read_unaligned because the input buffer may not be
    // naturally aligned.
    unsafe { u16::from_le(ptr.cast::<u16>().read_unaligned()) }
}

#[inline]
fn read_u32_le(ptr: *const u8) -> u32 {
    // SAFETY: callers only pass pointers proven to be valid for at least
    // 4 bytes. We use read_unaligned because the input buffer may not be
    // naturally aligned.
    unsafe { u32::from_le(ptr.cast::<u32>().read_unaligned()) }
}

#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
#[inline]
fn read_u64_le(ptr: *const u8) -> u64 {
    // SAFETY: callers only pass pointers proven to be valid for at least
    // 8 bytes. We use read_unaligned because the input buffer may not be
    // naturally aligned.
    unsafe { u64::from_le(ptr.cast::<u64>().read_unaligned()) }
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PureRustBackend {
    Scalar,
    #[cfg(target_arch = "aarch64")]
    CrcPmullAarch64,
    #[cfg(target_arch = "x86_64")]
    VpclmulX86_64,
    #[cfg(target_arch = "x86_64")]
    Sse42X86_64,
}

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

        if has_sse42_x86_64() {
            return PureRustBackend::Sse42X86_64;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if has_crc_pmull_aarch64() {
            return PureRustBackend::CrcPmullAarch64;
        }
    }

    PureRustBackend::Scalar
}

#[cfg(feature = "bench-select")]
#[inline]
fn bench_override_backend() -> Option<PureRustBackend> {
    static OVERRIDE: OnceLock<Option<PureRustBackend>> = OnceLock::new();

    *OVERRIDE.get_or_init(|| {
        let override_name = std::env::var("ARGMIN_CRC32C_BENCH_BACKEND")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase());

        match override_name.as_deref() {
            Some("scalar") => Some(PureRustBackend::Scalar),
            #[cfg(target_arch = "aarch64")]
            Some("3crc") if has_crc_pmull_aarch64() => Some(PureRustBackend::CrcPmullAarch64),
            #[cfg(target_arch = "x86_64")]
            Some("vpclmul") if has_vpclmul_x86_64() => Some(PureRustBackend::VpclmulX86_64),
            #[cfg(target_arch = "x86_64")]
            Some("sse42") if has_sse42_x86_64() => Some(PureRustBackend::Sse42X86_64),
            _ => None,
        }
    })
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn has_crc_pmull_aarch64() -> bool {
    std::arch::is_aarch64_feature_detected!("crc") && std::arch::is_aarch64_feature_detected!("aes")
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn has_sse42_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("sse4.2")
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn has_vpclmul_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("sse4.1")
        && std::arch::is_x86_feature_detected!("pclmulqdq")
        && std::arch::is_x86_feature_detected!("vpclmulqdq")
        && std::arch::is_x86_feature_detected!("avx2")
}

#[inline]
fn update_internal(crc: u32, data: &[u8]) -> u32 {
    match pure_rust_backend() {
        PureRustBackend::Scalar => update_scalar(crc, data),
        #[cfg(target_arch = "aarch64")]
        PureRustBackend::CrcPmullAarch64 => unsafe { update_crc_pmull_aarch64(crc, data) },
        #[cfg(target_arch = "x86_64")]
        PureRustBackend::VpclmulX86_64 => unsafe { update_vpclmul_x86_64(crc, data) },
        #[cfg(target_arch = "x86_64")]
        PureRustBackend::Sse42X86_64 => unsafe { update_sse42_x86_64(crc, data) },
    }
}

#[cfg(target_arch = "x86_64")]
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

#[cfg(target_arch = "x86_64")]
mod x86_64_vpclmul {
    use core::arch::x86_64::{
        __m128i, __m256i, _mm256_castsi256_si128, _mm256_clmulepi64_epi128,
        _mm256_extracti128_si256, _mm256_load_si256, _mm256_loadu_si256, _mm256_set_epi64x,
        _mm256_xor_si256, _mm_and_si128, _mm_clmulepi64_si128, _mm_cvtsi128_si32, _mm_load_si128,
        _mm_slli_si128, _mm_srli_si128, _mm_xor_si128,
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

    // Constants copied from ISA-L's crc32_iscsi_const block.
    static FOLD_1: Aligned = aligned(0x00000000493c7d27, 0x0000000ec1068c50);
    static FOLD_128_TO_64: Aligned = aligned(0x00000000493c7d27, 0x00000000dd45aab8);
    static BARRETT: Aligned = aligned(0x00000000dea713f0, 0x0000000105ec76f0);
    static LO32_CLR_MASK: Aligned = aligned(0xFFFFFFFF00000000, 0xFFFFFFFFFFFFFFFF);
    static HI64_MASK: Aligned = aligned(0xFFFFFFFFFFFFFFFF, 0x0000000000000000);
    static FOLD_8X2: Aligned256 = aligned256(
        0x0000000206e38d70,
        0x000000006992cea2,
        0x0000000206e38d70,
        0x000000006992cea2,
    );
    static FOLD_7_6: Aligned256 = aligned256(
        0x0000000047db8317,
        0x000000002ad91c30,
        0x000000000715ce53,
        0x00000000c49f4f67,
    );
    static FOLD_5_4: Aligned256 = aligned256(
        0x0000000039d3b296,
        0x00000000083a6eec,
        0x000000009e4addf8,
        0x00000000740eef02,
    );
    static FOLD_3_2: Aligned256 = aligned256(
        0x00000000ddc0152b,
        0x000000001c291d04,
        0x00000000ba4fc28e,
        0x000000003da6d0cb,
    );

    #[inline]
    fn load_aligned(value: &Aligned) -> __m128i {
        unsafe { _mm_load_si128(value.0.as_ptr().cast()) }
    }

    #[inline]
    fn load_aligned256(value: &Aligned256) -> __m256i {
        unsafe { _mm256_load_si256(value.0.as_ptr().cast()) }
    }

    #[inline]
    fn load_block256(ptr: *const u8) -> __m256i {
        unsafe { _mm256_loadu_si256(ptr.cast()) }
    }

    #[inline]
    fn xor_crc256(block: __m256i, crc: u32) -> __m256i {
        unsafe { _mm256_xor_si256(block, _mm256_set_epi64x(0, 0, 0, crc as i64)) }
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

    #[target_feature(enable = "sse4.1,pclmulqdq,vpclmulqdq,avx2")]
    pub unsafe fn update(crc: u32, data: &[u8]) -> u32 {
        let prefix_len = data.len() & !0x0F;
        if prefix_len < 128 {
            return super::update_scalar(crc, data);
        }

        let prefix_len = prefix_len & !0x7F;
        let (prefix, tail) = data.split_at(prefix_len);
        let prefix_crc = update_blocks_only(crc, prefix);
        super::update_scalar(prefix_crc, tail)
    }

    #[target_feature(enable = "sse4.1,pclmulqdq,vpclmulqdq,avx2")]
    unsafe fn update_blocks_only(crc: u32, data: &[u8]) -> u32 {
        let fold_8x2 = load_aligned256(&FOLD_8X2);
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        let mut x0 = xor_crc256(load_block256(ptr), crc);
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

        reduce_to_crc(state)
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn update_vpclmul_x86_64(crc: u32, data: &[u8]) -> u32 {
    x86_64_vpclmul::update(crc, data)
}

#[cfg(target_arch = "aarch64")]
mod aarch64_crc_pmull {
    use core::arch::aarch64::{
        __crc32cb, __crc32cd, __crc32ch, __crc32cw, vgetq_lane_u64, vmull_p64,
        vreinterpretq_u64_p128,
    };

    const BLOCK_SIZE: usize = 1024;
    const STRIPE_BYTES: usize = 336;
    const FOLD_CRC0: u64 = 0x0000_0000_E417_F38A;
    const FOLD_CRC1: u64 = 0x0000_0000_8F15_8014;

    #[inline]
    #[target_feature(enable = "crc,neon,aes")]
    unsafe fn fold_crc_word(crc: u32, constant: u64) -> u32 {
        let folded = vmull_p64(crc as u64, constant);
        let folded_words = vreinterpretq_u64_p128(folded);
        __crc32cd(0, vgetq_lane_u64::<0>(folded_words))
    }

    #[target_feature(enable = "crc,neon,aes")]
    #[inline]
    unsafe fn update_hw(mut crc: u32, data: &[u8]) -> u32 {
        let mut remaining = data;

        while remaining.len() >= 16 {
            crc = __crc32cd(crc, super::read_u64_le(remaining.as_ptr()));
            crc = __crc32cd(crc, super::read_u64_le(remaining.as_ptr().add(8)));
            remaining = &remaining[16..];
        }

        while remaining.len() >= 8 {
            crc = __crc32cd(crc, super::read_u64_le(remaining.as_ptr()));
            remaining = &remaining[8..];
        }

        if remaining.len() >= 4 {
            crc = __crc32cw(crc, super::read_u32_le(remaining.as_ptr()));
            remaining = &remaining[4..];
        }

        if remaining.len() >= 2 {
            crc = __crc32ch(crc, super::read_u16_le(remaining.as_ptr()));
            remaining = &remaining[2..];
        }

        if let Some(&byte) = remaining.first() {
            crc = __crc32cb(crc, byte);
        }

        crc
    }

    #[target_feature(enable = "crc,neon,aes")]
    #[inline]
    unsafe fn update_blocks_only(mut crc: u32, data: &[u8]) -> u32 {
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        while ptr < end {
            let mut crc0 = __crc32cd(crc, super::read_u64_le(ptr));
            let mut crc1 = 0u32;
            let mut crc2 = 0u32;
            let mut ptr0 = ptr.add(8);
            let mut ptr1 = ptr0.add(STRIPE_BYTES);
            let mut ptr2 = ptr1.add(STRIPE_BYTES);

            for _ in 0..(STRIPE_BYTES / 16) {
                crc0 = __crc32cd(crc0, super::read_u64_le(ptr0));
                crc0 = __crc32cd(crc0, super::read_u64_le(ptr0.add(8)));
                ptr0 = ptr0.add(16);

                crc1 = __crc32cd(crc1, super::read_u64_le(ptr1));
                crc1 = __crc32cd(crc1, super::read_u64_le(ptr1.add(8)));
                ptr1 = ptr1.add(16);

                crc2 = __crc32cd(crc2, super::read_u64_le(ptr2));
                crc2 = __crc32cd(crc2, super::read_u64_le(ptr2.add(8)));
                ptr2 = ptr2.add(16);
            }

            crc2 = __crc32cd(crc2, super::read_u64_le(ptr2));
            crc = fold_crc_word(crc0, FOLD_CRC0) ^ fold_crc_word(crc1, FOLD_CRC1) ^ crc2;
            ptr = ptr.add(BLOCK_SIZE);
        }

        crc
    }

    #[target_feature(enable = "crc,neon,aes")]
    pub unsafe fn update(crc: u32, data: &[u8]) -> u32 {
        let prefix_len = data.len() & !(BLOCK_SIZE - 1);
        let mut state = crc;

        if prefix_len != 0 {
            state = update_blocks_only(state, &data[..prefix_len]);
        }

        update_hw(state, &data[prefix_len..])
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn update_crc_pmull_aarch64(crc: u32, data: &[u8]) -> u32 {
    aarch64_crc_pmull::update(crc, data)
}

/// Return the active CRC-32C backend name for this build and process.
#[inline]
pub fn backend_name() -> &'static str {
    match pure_rust_backend() {
        PureRustBackend::Scalar => "scalar",
        #[cfg(target_arch = "aarch64")]
        PureRustBackend::CrcPmullAarch64 => "aarch64-3crc-fold",
        #[cfg(target_arch = "x86_64")]
        PureRustBackend::VpclmulX86_64 => "x86_64-vpclmulqdq",
        #[cfg(target_arch = "x86_64")]
        PureRustBackend::Sse42X86_64 => "x86_64-sse4.2-crc32",
    }
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

    #[derive(Clone, Copy, Debug)]
    struct BackendCase {
        backend: PureRustBackend,
        name: &'static str,
    }

    fn supported_backend_cases() -> Vec<BackendCase> {
        #[cfg_attr(
            not(any(target_arch = "aarch64", target_arch = "x86_64")),
            allow(unused_mut)
        )]
        let mut cases = vec![BackendCase {
            backend: PureRustBackend::Scalar,
            name: "scalar",
        }];

        #[cfg(target_arch = "aarch64")]
        {
            if has_crc_pmull_aarch64() {
                cases.push(BackendCase {
                    backend: PureRustBackend::CrcPmullAarch64,
                    name: "aarch64-3crc-fold",
                });
            }
        }

        #[cfg(target_arch = "x86_64")]
        {
            if has_vpclmul_x86_64() {
                cases.push(BackendCase {
                    backend: PureRustBackend::VpclmulX86_64,
                    name: "x86_64-vpclmulqdq",
                });
            }

            if has_sse42_x86_64() {
                cases.push(BackendCase {
                    backend: PureRustBackend::Sse42X86_64,
                    name: "x86_64-sse4.2-crc32",
                });
            }
        }

        cases
    }

    fn checksum_with_backend(backend: PureRustBackend, data: &[u8]) -> u32 {
        let mut crc = !0u32;
        crc = match backend {
            PureRustBackend::Scalar => update_scalar(crc, data),
            #[cfg(target_arch = "aarch64")]
            PureRustBackend::CrcPmullAarch64 => unsafe { update_crc_pmull_aarch64(crc, data) },
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::VpclmulX86_64 => unsafe { update_vpclmul_x86_64(crc, data) },
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::Sse42X86_64 => unsafe { update_sse42_x86_64(crc, data) },
        };
        crc ^ !0u32
    }

    fn checksum_streaming_with_backend(
        backend: PureRustBackend,
        data: &[u8],
        chunk_size: usize,
    ) -> u32 {
        let mut crc = !0u32;
        for chunk in data.chunks(chunk_size.max(1)) {
            crc = match backend {
                PureRustBackend::Scalar => update_scalar(crc, chunk),
                #[cfg(target_arch = "aarch64")]
                PureRustBackend::CrcPmullAarch64 => unsafe { update_crc_pmull_aarch64(crc, chunk) },
                #[cfg(target_arch = "x86_64")]
                PureRustBackend::VpclmulX86_64 => unsafe { update_vpclmul_x86_64(crc, chunk) },
                #[cfg(target_arch = "x86_64")]
                PureRustBackend::Sse42X86_64 => unsafe { update_sse42_x86_64(crc, chunk) },
            };
        }
        crc ^ !0u32
    }

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
