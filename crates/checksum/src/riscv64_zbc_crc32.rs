//! Shared RISC-V Zbc folding kernel for 32-bit reflected CRCs.

use core::arch::asm;

trait Parameters {
    const P4_LOW: u64;
    const P4_HIGH: u64;
    const P1_LOW: u64;
    const P1_HIGH: u64;
    const P0_LOW: u64;
    const P0_HIGH: u64;
    const BR_LOW: u64;
    const BR_HIGH: u64;
}

enum Ieee {}

// These are the IEEE folding and Barrett constants used by crc32's PCLMUL backends. Folding
// constants have their x86 vector lanes reversed because the scalar kernel names them by the
// input word they multiply.
impl Parameters for Ieee {
    const P4_LOW: u64 = 0x0000_0001_5444_2BD4;
    const P4_HIGH: u64 = 0x0000_0001_C6E4_1596;
    const P1_LOW: u64 = 0x0000_0001_7519_97D0;
    const P1_HIGH: u64 = 0x0000_0000_CCAA_009E;
    const P0_LOW: u64 = 0x0000_0000_CCAA_009E;
    const P0_HIGH: u64 = 0x0000_0001_63CD_6124;
    const BR_LOW: u64 = 0x0000_0001_F701_1640;
    const BR_HIGH: u64 = 0x0000_0001_DB71_0640;
}

enum Castagnoli {}

// These are the Castagnoli folding and Barrett constants used by crc32c's VPCLMUL backend, with
// the folding lanes represented in scalar multiplication order as above.
impl Parameters for Castagnoli {
    const P4_LOW: u64 = 0x0000_0000_740E_EF02;
    const P4_HIGH: u64 = 0x0000_0000_9E4A_DDF8;
    const P1_LOW: u64 = 0x0000_000E_C106_8C50;
    const P1_HIGH: u64 = 0x0000_0000_493C_7D27;
    const P0_LOW: u64 = 0x0000_0000_493C_7D27;
    const P0_HIGH: u64 = 0x0000_0000_DD45_AAB8;
    const BR_LOW: u64 = 0x0000_0000_DEA7_13F0;
    const BR_HIGH: u64 = 0x0000_0001_05EC_76F0;
}

#[derive(Clone, Copy)]
struct Polynomial {
    low: u64,
    high: u64,
}

impl Polynomial {
    #[inline]
    fn xor(self, other: Self) -> Self {
        Self {
            low: self.low ^ other.low,
            high: self.high ^ other.high,
        }
    }
}

#[inline]
#[target_feature(enable = "zbc")]
unsafe fn clmul_low(lhs: u64, rhs: u64) -> u64 {
    let result;
    // SAFETY: the caller guarantees Zbc support. CLMUL accesses no memory and has no other
    // architectural side effects.
    unsafe {
        asm!(
            "clmul {result}, {lhs}, {rhs}",
            result = out(reg) result,
            lhs = in(reg) lhs,
            rhs = in(reg) rhs,
            options(pure, nomem, nostack),
        );
    }
    result
}

#[inline]
#[target_feature(enable = "zbc")]
unsafe fn clmul_high(lhs: u64, rhs: u64) -> u64 {
    let result;
    // SAFETY: the caller guarantees Zbc support. CLMULH accesses no memory and has no other
    // architectural side effects.
    unsafe {
        asm!(
            "clmulh {result}, {lhs}, {rhs}",
            result = out(reg) result,
            lhs = in(reg) lhs,
            rhs = in(reg) rhs,
            options(pure, nomem, nostack),
        );
    }
    result
}

#[inline]
#[target_feature(enable = "zbc")]
unsafe fn multiply(lhs: u64, rhs: u64) -> Polynomial {
    Polynomial {
        low: clmul_low(lhs, rhs),
        high: clmul_high(lhs, rhs),
    }
}

#[inline]
unsafe fn load_block(ptr: *const u8) -> Polynomial {
    debug_assert_eq!(ptr.align_offset(core::mem::align_of::<u64>()), 0);
    // SAFETY: the block kernel asserts u64 alignment and callers keep both reads within the
    // complete 16-byte block.
    let low = unsafe { ptr.cast::<u64>().read() };
    // SAFETY: the second word has the same alignment and the block contains 16 readable bytes.
    let high = unsafe { ptr.wrapping_add(8).cast::<u64>().read() };
    Polynomial {
        low: u64::from_le(low),
        high: u64::from_le(high),
    }
}

#[inline]
#[target_feature(enable = "zbc")]
unsafe fn fold_without_next<P: Parameters>(value: Polynomial) -> Polynomial {
    multiply(value.low, P::P1_LOW).xor(multiply(value.high, P::P1_HIGH))
}

#[inline]
#[target_feature(enable = "zbc")]
unsafe fn fold_block<P: Parameters>(value: Polynomial, next: Polynomial) -> Polynomial {
    multiply(value.low, P::P4_LOW)
        .xor(multiply(value.high, P::P4_HIGH))
        .xor(next)
}

#[inline]
#[target_feature(enable = "zbc")]
unsafe fn reduce_to_crc<P: Parameters>(value: Polynomial) -> u32 {
    let folded = multiply(value.low, P::P0_LOW).xor(Polynomial {
        low: value.high,
        high: 0,
    });
    let folded32 = multiply(folded.low << 32, P::P0_HIGH).xor(folded);
    let masked = Polynomial {
        low: folded32.low & 0xFFFF_FFFF_0000_0000,
        high: folded32.high,
    };
    let quotient = multiply(masked.low, P::BR_LOW).xor(masked).low;
    let reduced = multiply(quotient, P::BR_HIGH)
        .xor(Polynomial {
            low: quotient,
            high: 0,
        })
        .xor(masked);
    reduced.high as u32
}

/// Updates a raw IEEE CRC state over aligned whole 64-byte blocks.
///
/// # Safety
///
/// The current CPU must support Zbc. `data` must be nonempty, its length must be a
/// multiple of 64, and its starting address must be aligned to `u64`. The structural
/// preconditions are asserted before any data is read.
#[target_feature(enable = "zbc")]
pub(crate) unsafe fn update_ieee_blocks(crc: u32, data: &[u8]) -> u32 {
    update_blocks::<Ieee>(crc, data)
}

/// Updates a raw Castagnoli CRC state over aligned whole 64-byte blocks.
///
/// # Safety
///
/// The current CPU must support Zbc. `data` must be nonempty, its length must be a
/// multiple of 64, and its starting address must be aligned to `u64`. The structural
/// preconditions are asserted before any data is read.
#[target_feature(enable = "zbc")]
pub(crate) unsafe fn update_castagnoli_blocks(crc: u32, data: &[u8]) -> u32 {
    update_blocks::<Castagnoli>(crc, data)
}

#[inline]
#[target_feature(enable = "zbc")]
unsafe fn update_blocks<P: Parameters>(crc: u32, data: &[u8]) -> u32 {
    assert!(!data.is_empty(), "Zbc block input must not be empty");
    assert_eq!(
        data.len() & 0x3f,
        0,
        "Zbc block input length must be a multiple of 64"
    );
    assert_eq!(
        data.as_ptr().align_offset(core::mem::align_of::<u64>()),
        0,
        "Zbc block input must be aligned to u64"
    );

    let mut ptr = data.as_ptr();
    let end = ptr.wrapping_add(data.len());
    let mut x0 = load_block(ptr).xor(Polynomial {
        low: crc as u64,
        high: 0,
    });
    let mut x1 = load_block(ptr.wrapping_add(16));
    let mut x2 = load_block(ptr.wrapping_add(32));
    let mut x3 = load_block(ptr.wrapping_add(48));
    ptr = ptr.wrapping_add(64);

    while ptr < end {
        x0 = fold_block::<P>(x0, load_block(ptr));
        x1 = fold_block::<P>(x1, load_block(ptr.wrapping_add(16)));
        x2 = fold_block::<P>(x2, load_block(ptr.wrapping_add(32)));
        x3 = fold_block::<P>(x3, load_block(ptr.wrapping_add(48)));
        ptr = ptr.wrapping_add(64);
    }

    x1 = x1.xor(fold_without_next::<P>(x0));
    x2 = x2.xor(fold_without_next::<P>(x1));
    x3 = x3.xor(fold_without_next::<P>(x2));

    reduce_to_crc::<P>(x3)
}
