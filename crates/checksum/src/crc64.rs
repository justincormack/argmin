//! CRC-64/NVME (= CRC-64/Rocksoft) checksum.
//!
//! Uses ISA-L when the `isa-l` feature is enabled and a pure-Rust table-based
//! implementation when the `pure-rust` feature is enabled.
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

#[cfg(all(feature = "pure-rust", feature = "bench-select"))]
use std::sync::OnceLock;

/// CRC-64/NVME reflected polynomial (bit-reversal of 0xAD93D23594C93659).
const POLY: u64 = 0x9A6C9329AC4BC9B5;

#[cfg(feature = "pure-rust")]
static TABLES: [[u64; 256]; 16] = build_tables();

#[cfg(feature = "pure-rust")]
const fn build_tables() -> [[u64; 256]; 16] {
    let mut tables = [[0u64; 256]; 16];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
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
fn read_u64_le(ptr: *const u8) -> u64 {
    // SAFETY: callers only pass pointers proven to be valid for at least
    // 8 bytes. We use read_unaligned because the input buffer may not be
    // naturally aligned.
    unsafe { u64::from_le(ptr.cast::<u64>().read_unaligned()) }
}

#[inline]
fn extend(crc: u64, data: &[u8]) -> u64 {
    #[cfg(feature = "pure-rust")]
    {
        match pure_rust_backend() {
            PureRustBackend::Scalar => extend_scalar(crc, data),
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::PclmulX86_64 => unsafe { extend_pclmul_x86_64(crc, data) },
            #[cfg(target_arch = "aarch64")]
            PureRustBackend::PmullSha3Aarch64 => unsafe { extend_pmull_sha3_aarch64(crc, data) },
            #[cfg(target_arch = "aarch64")]
            PureRustBackend::PmullAarch64 => unsafe { extend_pmull_aarch64(crc, data) },
        }
    }

    #[cfg(all(not(feature = "pure-rust"), feature = "isa-l"))]
    {
        // SAFETY: we pass a valid pointer and exact length. ISA-L reads
        // only within [buf, buf+len). For empty slices the pointer is
        // never dereferenced (len=0).
        unsafe { ec_sys::crc64_rocksoft_refl(crc, data.as_ptr(), data.len() as u64) }
    }
}

#[cfg(feature = "pure-rust")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PureRustBackend {
    Scalar,
    #[cfg(target_arch = "x86_64")]
    PclmulX86_64,
    #[cfg(target_arch = "aarch64")]
    PmullSha3Aarch64,
    #[cfg(target_arch = "aarch64")]
    PmullAarch64,
}

#[cfg(feature = "pure-rust")]
#[inline]
fn pure_rust_backend() -> PureRustBackend {
    #[cfg(feature = "bench-select")]
    if bench_force_scalar() {
        return PureRustBackend::Scalar;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("pclmulqdq") {
            return PureRustBackend::PclmulX86_64;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("aes")
            && std::arch::is_aarch64_feature_detected!("sha3")
        {
            return PureRustBackend::PmullSha3Aarch64;
        }

        if std::arch::is_aarch64_feature_detected!("aes") {
            return PureRustBackend::PmullAarch64;
        }
    }

    PureRustBackend::Scalar
}

#[cfg(all(feature = "pure-rust", feature = "bench-select"))]
#[inline]
fn bench_force_scalar() -> bool {
    static FORCE_SCALAR: OnceLock<bool> = OnceLock::new();

    *FORCE_SCALAR.get_or_init(|| {
        matches!(
            std::env::var("ARGMIN_CRC64_BENCH_BACKEND")
                .ok()
                .as_deref()
                .map(str::trim)
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("scalar")
        )
    })
}

#[cfg(feature = "pure-rust")]
#[inline]
fn extend_scalar(crc: u64, data: &[u8]) -> u64 {
    let mut state = !crc;
    let mut remaining = data;

    while remaining.len() >= 16 {
        let first = read_u64_le(remaining.as_ptr());
        let second = read_u64_le(remaining.as_ptr().wrapping_add(8));
        let mixed = state ^ first;
        state = TABLES[15][(mixed & 0xFF) as usize]
            ^ TABLES[14][((mixed >> 8) & 0xFF) as usize]
            ^ TABLES[13][((mixed >> 16) & 0xFF) as usize]
            ^ TABLES[12][((mixed >> 24) & 0xFF) as usize]
            ^ TABLES[11][((mixed >> 32) & 0xFF) as usize]
            ^ TABLES[10][((mixed >> 40) & 0xFF) as usize]
            ^ TABLES[9][((mixed >> 48) & 0xFF) as usize]
            ^ TABLES[8][(mixed >> 56) as usize]
            ^ TABLES[7][(second & 0xFF) as usize]
            ^ TABLES[6][((second >> 8) & 0xFF) as usize]
            ^ TABLES[5][((second >> 16) & 0xFF) as usize]
            ^ TABLES[4][((second >> 24) & 0xFF) as usize]
            ^ TABLES[3][((second >> 32) & 0xFF) as usize]
            ^ TABLES[2][((second >> 40) & 0xFF) as usize]
            ^ TABLES[1][((second >> 48) & 0xFF) as usize]
            ^ TABLES[0][(second >> 56) as usize];
        remaining = &remaining[16..];
    }

    while remaining.len() >= 8 {
        let block = read_u64_le(remaining.as_ptr());
        let mixed = state ^ block;
        state = TABLES[7][(mixed & 0xFF) as usize]
            ^ TABLES[6][((mixed >> 8) & 0xFF) as usize]
            ^ TABLES[5][((mixed >> 16) & 0xFF) as usize]
            ^ TABLES[4][((mixed >> 24) & 0xFF) as usize]
            ^ TABLES[3][((mixed >> 32) & 0xFF) as usize]
            ^ TABLES[2][((mixed >> 40) & 0xFF) as usize]
            ^ TABLES[1][((mixed >> 48) & 0xFF) as usize]
            ^ TABLES[0][(mixed >> 56) as usize];
        remaining = &remaining[8..];
    }

    for &byte in remaining {
        let idx = ((state as u8) ^ byte) as usize;
        state = TABLES[0][idx] ^ (state >> 8);
    }
    !state
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
mod x86_64_pclmul {
    use core::arch::x86_64::{
        __m128i, _mm_clmulepi64_si128, _mm_load_si128, _mm_loadu_si128, _mm_set_epi64x,
        _mm_slli_si128, _mm_srli_si128, _mm_xor_si128,
    };

    #[repr(align(16))]
    struct Aligned([u64; 2]);

    const fn aligned(lo: u64, hi: u64) -> Aligned {
        Aligned([lo, hi])
    }

    // Constants copied from ISA-L's crc64_rocksoft_refl_const block.
    static FOLD_1: Aligned = aligned(0x21e9761e252621ac, 0xeadc41fd2ba3d420);
    static FOLD_8: Aligned = aligned(0x5f852fb61e8d92dc, 0xa1ca681e733f9c40);
    static FOLD_7: Aligned = aligned(0x946588403d4adcbc, 0xd083dd594d96319d);
    static FOLD_6: Aligned = aligned(0x34f5a24e22d66e90, 0x3c255f5ebc414423);
    static FOLD_5: Aligned = aligned(0x03363823e6e791e5, 0x7b0ab10dd0f809fe);
    static FOLD_4: Aligned = aligned(0x62242240ace5045a, 0x0c32cdb31e18a84a);
    static FOLD_3: Aligned = aligned(0xa3ffdc1fe8e82a8b, 0xbdd7ac0ee1a4a0f0);
    static FOLD_2: Aligned = aligned(0xe1e0bb9d45d7a44c, 0xb0bc2e589204f500);
    static FOLD_128_TO_64: Aligned = aligned(0x21e9761e252621ac, 0x0000000000000000);
    static BARRETT: Aligned = aligned(0x27ecfa329aef9f77, 0x34d926535897936a);

    #[inline]
    fn load_aligned(value: &Aligned) -> __m128i {
        // SAFETY: the wrapper guarantees 16-byte alignment and valid storage.
        unsafe { _mm_load_si128(value.0.as_ptr().cast()) }
    }

    #[inline]
    fn load_block(ptr: *const u8) -> __m128i {
        // SAFETY: callers only pass pointers valid for 16 readable bytes.
        unsafe { _mm_loadu_si128(ptr.cast()) }
    }

    #[inline]
    fn xor_crc(block: __m128i, crc: u64) -> __m128i {
        unsafe { _mm_xor_si128(block, _mm_set_epi64x(0, crc as i64)) }
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
    fn reduce_to_crc(x: __m128i) -> u64 {
        unsafe {
            let fold = load_aligned(&FOLD_128_TO_64);
            let hi = _mm_srli_si128::<8>(x);
            let folded = _mm_xor_si128(_mm_clmulepi64_si128::<0x00>(x, fold), hi);

            let barrett = load_aligned(&BARRETT);
            let y = _mm_clmulepi64_si128::<0x00>(folded, barrett);
            let y_shifted = _mm_slli_si128::<8>(y);
            let z = _mm_clmulepi64_si128::<0x10>(y, barrett);
            let reduced = _mm_xor_si128(_mm_xor_si128(z, y_shifted), folded);

            let words: [u64; 2] = core::mem::transmute(reduced);
            words[1]
        }
    }

    #[target_feature(enable = "pclmulqdq")]
    pub unsafe fn extend(crc: u64, data: &[u8]) -> u64 {
        let prefix_len = data.len() & !0x0F;
        if prefix_len < 32 {
            return super::extend_scalar(crc, data);
        }

        let (prefix, tail) = data.split_at(prefix_len);
        let prefix_crc = extend_blocks_only(crc, prefix);
        super::extend_scalar(prefix_crc, tail)
    }

    #[target_feature(enable = "pclmulqdq")]
    unsafe fn extend_blocks_only(crc: u64, data: &[u8]) -> u64 {
        let fold_1 = load_aligned(&FOLD_1);
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        let mut state = if data.len() >= 128 {
            let fold_8 = load_aligned(&FOLD_8);
            let mut x0 = xor_crc(load_block(ptr), !crc);
            let mut x1 = load_block(ptr.add(16));
            let mut x2 = load_block(ptr.add(32));
            let mut x3 = load_block(ptr.add(48));
            let mut x4 = load_block(ptr.add(64));
            let mut x5 = load_block(ptr.add(80));
            let mut x6 = load_block(ptr.add(96));
            let mut x7 = load_block(ptr.add(112));
            ptr = ptr.add(128);

            while ptr.add(128) <= end {
                x0 = fold_block(x0, load_block(ptr), fold_8);
                x1 = fold_block(x1, load_block(ptr.add(16)), fold_8);
                x2 = fold_block(x2, load_block(ptr.add(32)), fold_8);
                x3 = fold_block(x3, load_block(ptr.add(48)), fold_8);
                x4 = fold_block(x4, load_block(ptr.add(64)), fold_8);
                x5 = fold_block(x5, load_block(ptr.add(80)), fold_8);
                x6 = fold_block(x6, load_block(ptr.add(96)), fold_8);
                x7 = fold_block(x7, load_block(ptr.add(112)), fold_8);
                ptr = ptr.add(128);
            }

            let fold_7 = load_aligned(&FOLD_7);
            let fold_6 = load_aligned(&FOLD_6);
            let fold_5 = load_aligned(&FOLD_5);
            let fold_4 = load_aligned(&FOLD_4);
            let fold_3 = load_aligned(&FOLD_3);
            let fold_2 = load_aligned(&FOLD_2);

            x7 = _mm_xor_si128(x7, fold_without_next(x0, fold_7));
            x7 = _mm_xor_si128(x7, fold_without_next(x1, fold_6));
            x7 = _mm_xor_si128(x7, fold_without_next(x2, fold_5));
            x7 = _mm_xor_si128(x7, fold_without_next(x3, fold_4));
            x7 = _mm_xor_si128(x7, fold_without_next(x4, fold_3));
            x7 = _mm_xor_si128(x7, fold_without_next(x5, fold_2));
            x7 = _mm_xor_si128(x7, fold_without_next(x6, fold_1));
            x7
        } else {
            let first = xor_crc(load_block(ptr), !crc);
            ptr = ptr.add(16);
            first
        };

        while ptr < end {
            state = fold_block(state, load_block(ptr), fold_1);
            ptr = ptr.add(16);
        }

        !reduce_to_crc(state)
    }
}

#[cfg(all(feature = "pure-rust", target_arch = "x86_64"))]
#[inline]
unsafe fn extend_pclmul_x86_64(crc: u64, data: &[u8]) -> u64 {
    x86_64_pclmul::extend(crc, data)
}

#[cfg(all(feature = "pure-rust", target_arch = "aarch64"))]
mod aarch64_pmull {
    use core::arch::aarch64::{
        poly64x2_t, uint64x2_t, uint8x16_t, vdupq_n_u64, veorq_u8, vget_lane_p64, vget_low_p64,
        vgetq_lane_u64, vld1q_u8, vmull_high_p64, vmull_p64, vreinterpretq_p64_u64,
        vreinterpretq_p64_u8, vreinterpretq_u64_p128, vreinterpretq_u64_u8, vreinterpretq_u8_p128,
        vreinterpretq_u8_u64, vsetq_lane_u64,
    };

    const P4_LOW: u64 = 0x0C32_CDB3_1E18_A84A;
    const P4_HIGH: u64 = 0x6224_2240_ACE5_045A;
    const P1_LOW: u64 = 0xEADC_41FD_2BA3_D420;
    const P1_HIGH: u64 = 0x21E9_761E_2526_21AC;
    const P0_LOW: u64 = 0x21E9_761E_2526_21AC;
    const BR_LOW: u64 = 0x27EC_FA32_9AEF_9F77;
    const BR_HIGH: u64 = 0x34D9_2653_5897_936B;

    #[inline]
    unsafe fn u64x2(lo: u64, hi: u64) -> uint64x2_t {
        let value = vdupq_n_u64(0);
        let value = vsetq_lane_u64::<0>(lo, value);
        vsetq_lane_u64::<1>(hi, value)
    }

    #[inline]
    unsafe fn poly64x2(lo: u64, hi: u64) -> poly64x2_t {
        vreinterpretq_p64_u64(u64x2(lo, hi))
    }

    #[inline]
    unsafe fn low64_as_u8x16(value: u64) -> uint8x16_t {
        vreinterpretq_u8_u64(u64x2(value, 0))
    }

    #[inline]
    fn load_block(ptr: *const u8) -> uint8x16_t {
        unsafe { vld1q_u8(ptr) }
    }

    #[inline]
    fn xor_crc(block: uint8x16_t, crc: u64) -> uint8x16_t {
        unsafe { veorq_u8(block, low64_as_u8x16(crc)) }
    }

    #[inline]
    fn low_p64(x: poly64x2_t) -> u64 {
        unsafe { vget_lane_p64::<0>(vget_low_p64(x)) }
    }

    #[inline]
    fn clmul_low(x: poly64x2_t, y: poly64x2_t) -> uint8x16_t {
        unsafe { vreinterpretq_u8_p128(vmull_p64(low_p64(x), low_p64(y))) }
    }

    #[inline]
    fn clmul_high(x: poly64x2_t, y: poly64x2_t) -> uint8x16_t {
        unsafe { vreinterpretq_u8_p128(vmull_high_p64(x, y)) }
    }

    #[inline]
    fn fold_block(x: uint8x16_t, next: uint8x16_t, constant: poly64x2_t) -> uint8x16_t {
        unsafe {
            let poly = vreinterpretq_p64_u8(x);
            veorq_u8(
                veorq_u8(clmul_low(poly, constant), clmul_high(poly, constant)),
                next,
            )
        }
    }

    #[inline]
    fn fold_without_next(x: uint8x16_t, constant: poly64x2_t) -> uint8x16_t {
        unsafe {
            let poly = vreinterpretq_p64_u8(x);
            veorq_u8(clmul_low(poly, constant), clmul_high(poly, constant))
        }
    }

    #[inline]
    fn reduce_to_crc(x: uint8x16_t) -> u64 {
        unsafe {
            let folded = {
                let x_poly = vreinterpretq_p64_u8(x);
                let tmp_low = vreinterpretq_u8_p128(vmull_p64(low_p64(x_poly), P0_LOW));
                let x_words = vreinterpretq_u64_u8(x);
                let tmp_high = low64_as_u8x16(vgetq_lane_u64::<1>(x_words));
                veorq_u8(tmp_low, tmp_high)
            };

            let folded_poly = vreinterpretq_p64_u8(folded);
            let y = vmull_p64(low_p64(folded_poly), BR_LOW);
            let y_words = vreinterpretq_u64_p128(y);
            let y_low = vgetq_lane_u64::<0>(y_words);
            let y_shifted = vreinterpretq_u8_u64(u64x2(0, y_low));
            let z = vreinterpretq_u8_p128(vmull_p64(y_low, BR_HIGH));
            let reduced = veorq_u8(veorq_u8(z, y_shifted), folded);
            vgetq_lane_u64::<1>(vreinterpretq_u64_u8(reduced))
        }
    }

    #[target_feature(enable = "neon,aes")]
    pub unsafe fn extend(crc: u64, data: &[u8]) -> u64 {
        let prefix_len = data.len() & !0x3F;
        if prefix_len < 64 {
            return super::extend_scalar(crc, data);
        }

        let (prefix, tail) = data.split_at(prefix_len);
        let prefix_crc = extend_blocks_only(crc, prefix);
        super::extend_scalar(prefix_crc, tail)
    }

    #[target_feature(enable = "neon,aes")]
    unsafe fn extend_blocks_only(crc: u64, data: &[u8]) -> u64 {
        let p4 = poly64x2(P4_LOW, P4_HIGH);
        let p1 = poly64x2(P1_LOW, P1_HIGH);
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        let mut x0 = xor_crc(load_block(ptr), !crc);
        let mut x1 = load_block(ptr.add(16));
        let mut x2 = load_block(ptr.add(32));
        let mut x3 = load_block(ptr.add(48));
        ptr = ptr.add(64);

        while ptr < end {
            x0 = fold_block(x0, load_block(ptr), p4);
            x1 = fold_block(x1, load_block(ptr.add(16)), p4);
            x2 = fold_block(x2, load_block(ptr.add(32)), p4);
            x3 = fold_block(x3, load_block(ptr.add(48)), p4);
            ptr = ptr.add(64);
        }

        x1 = veorq_u8(x1, fold_without_next(x0, p1));
        x2 = veorq_u8(x2, fold_without_next(x1, p1));
        x3 = veorq_u8(x3, fold_without_next(x2, p1));

        !reduce_to_crc(x3)
    }

    #[target_feature(enable = "neon,aes,sha3")]
    pub unsafe fn extend_sha3(crc: u64, data: &[u8]) -> u64 {
        let prefix_len = data.len() & !0x3F;
        if prefix_len < 64 {
            return super::extend_scalar(crc, data);
        }

        let (prefix, tail) = data.split_at(prefix_len);
        let prefix_crc = extend_blocks_only_sha3(crc, prefix);
        super::extend_scalar(prefix_crc, tail)
    }

    #[target_feature(enable = "neon,aes,sha3")]
    unsafe fn extend_blocks_only_sha3(crc: u64, data: &[u8]) -> u64 {
        let p4 = poly64x2(P4_LOW, P4_HIGH);
        let p1 = poly64x2(P1_LOW, P1_HIGH);
        let mut ptr = data.as_ptr();
        let end = ptr.add(data.len());

        let mut x0 = xor_crc(load_block(ptr), !crc);
        let mut x1 = load_block(ptr.add(16));
        let mut x2 = load_block(ptr.add(32));
        let mut x3 = load_block(ptr.add(48));
        ptr = ptr.add(64);

        while ptr < end {
            x0 = fold_block(x0, load_block(ptr), p4);
            x1 = fold_block(x1, load_block(ptr.add(16)), p4);
            x2 = fold_block(x2, load_block(ptr.add(32)), p4);
            x3 = fold_block(x3, load_block(ptr.add(48)), p4);
            ptr = ptr.add(64);
        }

        x1 = veorq_u8(x1, fold_without_next(x0, p1));
        x2 = veorq_u8(x2, fold_without_next(x1, p1));
        x3 = veorq_u8(x3, fold_without_next(x2, p1));

        !reduce_to_crc(x3)
    }
}

#[cfg(all(feature = "pure-rust", target_arch = "aarch64"))]
#[inline]
unsafe fn extend_pmull_aarch64(crc: u64, data: &[u8]) -> u64 {
    aarch64_pmull::extend(crc, data)
}

#[cfg(all(feature = "pure-rust", target_arch = "aarch64"))]
#[inline]
unsafe fn extend_pmull_sha3_aarch64(crc: u64, data: &[u8]) -> u64 {
    aarch64_pmull::extend_sha3(crc, data)
}

/// Return the active CRC64 backend name for this build and process.
#[inline]
pub fn backend_name() -> &'static str {
    #[cfg(feature = "pure-rust")]
    {
        match pure_rust_backend() {
            PureRustBackend::Scalar => "scalar",
            #[cfg(target_arch = "x86_64")]
            PureRustBackend::PclmulX86_64 => "x86_64-pclmulqdq",
            #[cfg(target_arch = "aarch64")]
            PureRustBackend::PmullSha3Aarch64 => "aarch64-pmull+sha3",
            #[cfg(target_arch = "aarch64")]
            PureRustBackend::PmullAarch64 => "aarch64-pmull",
        }
    }

    #[cfg(all(not(feature = "pure-rust"), feature = "isa-l"))]
    {
        "isa-l"
    }
}

/// Compute CRC-64/NVME over the entire buffer.
#[inline]
pub fn checksum(data: &[u8]) -> u64 {
    extend(0, data)
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
        self.crc = extend(self.crc, data);
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
    fn streaming_default() {
        let hasher = Hasher::default();
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
