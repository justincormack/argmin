// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0 AND MIT
//
// This file contains a Rust port of code from the CORE-MATH project:
// https://core-math.gitlabpages.inria.fr/
//
// Copyright (c) 2025 Maxence Ponsardin and Paul Zimmermann
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Restricted-domain Rust port of CORE-MATH binary64 log for placement scoring.
//
// Relevant upstream references:
// - project home: https://core-math.gitlabpages.inria.fr/
// - current upstream log.c: https://gitlab.inria.fr/core-math/core-math/-/blob/master/src/binary64/log/log.c
// - pinned source revision for this port:
//   https://gitlab.inria.fr/core-math/core-math/-/tree/782ad8f8831bc7b2676f2a97648881f80bf9d750/src/binary64/log
#[path = "deterministic_log_tables.rs"]
mod tables;

use tables::{
    FAST_ERR, INVERSE, INVERSE_2, LOG2, LOG2_H, LOG2_L, LOG_INV, LOG_INV_2, M_ONE, OFFSET, P, P_2,
    SQRT2_CUTOFF_M, ZERO,
};

/// Hash-derived unit-interval values used by placement scoring.
///
/// The placement score path does not accept arbitrary `f64` log inputs. Values
/// are always constructed from a 53-bit integer numerator `n` in `[1, 2^53]`
/// and represent `n / 2^53`, i.e. the closed interval `[2^-53, 1]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Unit53(u64);

impl Unit53 {
    const DENOMINATOR_BITS: u32 = 53;
    const DENOMINATOR: u64 = 1u64 << Self::DENOMINATOR_BITS;

    #[inline]
    pub(crate) fn from_hash(hash: u64) -> Self {
        // hash >> 11 is in [0, 2^53 - 1]; adding 1 gives [1, 2^53].
        Self((hash >> 11) + 1)
    }

    #[inline]
    fn numerator(self) -> u64 {
        debug_assert!((1..=Self::DENOMINATOR).contains(&self.0));
        self.0
    }

    #[inline]
    pub(crate) fn to_f64(self) -> f64 {
        self.numerator() as f64 / Self::DENOMINATOR as f64
    }
}

#[derive(Clone, Copy, Debug)]
struct DInt64 {
    hi: u64,
    lo: u64,
    ex: i64,
    sgn: u64,
}

#[derive(Clone, Copy)]
struct F64Bits {
    f: f64,
    u: u64,
}

impl F64Bits {
    #[inline]
    fn from_bits(bits: u64) -> Self {
        Self {
            f: f64::from_bits(bits),
            u: bits,
        }
    }
}

/// Deterministic natural logarithm for the placement score domain `[2^-53, 1]`.
///
/// This is a restricted Rust port of the CORE-MATH binary64 implementation for
/// the positive normal placement domain. The caller contract guarantees that
/// no NaN, infinity, zero, negative, or subnormal inputs can reach this path.
#[inline]
pub(crate) fn deterministic_log_u53(x: Unit53) -> f64 {
    cr_log_restricted(x.to_f64())
}

#[inline]
fn cr_log_restricted(x: f64) -> f64 {
    let bits = x.to_bits();
    let e = ((bits >> 52) as i32 & 0x7ff) - 0x3ff;
    let v = F64Bits::from_bits((0x3ff_u64 << 52) | (bits & 0x000f_ffff_ffff_ffff));

    if v.u == 0x3ff0_0000_0000_0000 && e == 0 {
        return 0.0;
    }

    let (h, l) = cr_log_fast(e, v);
    let left = h + (l - FAST_ERR);
    let right = h + (l + FAST_ERR);
    if left.to_bits() == right.to_bits() {
        left
    } else {
        cr_log_accurate(x)
    }
}

#[inline]
fn cr_log_accurate(x: f64) -> f64 {
    if x.to_bits() == 1.0f64.to_bits() {
        return 0.0;
    }

    let mut input = dint_from_f64(x);
    let output = log_2(&mut input);
    dint_to_f64(output)
}

#[inline]
fn cr_log_fast(e: i32, v: F64Bits) -> (f64, f64) {
    let m = 0x0010_0000_0000_0000_u64 + (v.u & 0x000f_ffff_ffff_ffff);
    let c = m >= SQRT2_CUTOFF_M;
    let e = e + i32::from(c);
    let (cy, shift) = if c { (0.5, 44_u32) } else { (1.0, 43_u32) };

    let i = (m >> shift) as usize;
    let y = v.f * cy;
    let index = i - OFFSET;
    let r = INVERSE[index];
    let (l1, l2) = LOG_INV[index];
    let z = r.mul_add(y, -1.0);
    let z2 = z * z;
    let p45 = P[5].mul_add(z, P[4]);
    let p23 = P[3].mul_add(z, P[2]);
    let mut ph = p45.mul_add(z2, p23);
    ph = ph.mul_add(z, P[1]);
    ph *= z2;

    let ee = e as f64;
    let (h, l0) = fast_two_sum(ee.mul_add(LOG2_H, l1), z);
    let l = ph + (l0 + l2);
    let l = ee.mul_add(LOG2_L, l);
    (h, l)
}

#[inline]
fn fast_two_sum(a: f64, b: f64) -> (f64, f64) {
    let hi = a + b;
    let e = hi - a;
    let lo = b - e;
    (hi, lo)
}

#[inline]
fn cmp_i64(a: i64, b: i64) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

#[inline]
fn cmp_u64(a: u64, b: u64) -> i32 {
    (a > b) as i32 - (a < b) as i32
}

#[inline]
fn cmp_dint(a: &DInt64, b: &DInt64) -> i32 {
    let c = cmp_i64(a.ex, b.ex);
    if c != 0 {
        return c;
    }
    let c = cmp_u64(a.hi, b.hi);
    if c != 0 {
        return c;
    }
    cmp_u64(a.lo, b.lo)
}

#[inline]
fn add_u128(a: u128, b: u128) -> (u128, bool) {
    a.overflowing_add(b)
}

#[inline]
fn sub_u128(a: u128, b: u128) -> (u128, bool) {
    a.overflowing_sub(b)
}

#[inline]
fn pack_u128(hi: u64, lo: u64) -> u128 {
    ((hi as u128) << 64) | lo as u128
}

#[inline]
fn unpack_u128(value: u128) -> (u64, u64) {
    ((value >> 64) as u64, value as u64)
}

fn add_dint(a: &DInt64, b: &DInt64) -> DInt64 {
    if (a.hi | a.lo) == 0 {
        return *b;
    }
    if (b.hi | b.lo) == 0 {
        return *a;
    }

    match cmp_dint(a, b) {
        0 => {
            if (a.sgn ^ b.sgn) != 0 {
                return ZERO;
            }
            let mut out = *a;
            out.ex += 1;
            return out;
        }
        -1 => return add_dint(b, a),
        _ => {}
    }

    let a_bits = pack_u128(a.hi, a.lo);
    let mut b_bits = pack_u128(b.hi, b.lo);
    let m_ex = a.ex;

    if a.ex > b.ex {
        let sh = (a.ex - b.ex) as u32;
        if sh <= 128 {
            b_bits = b_bits.wrapping_add((b_bits >> (sh - 1)) & 1);
        }
        b_bits = if sh < 128 { b_bits >> sh } else { 0 };
    }

    let (mut c_bits, _) = if (a.sgn ^ b.sgn) != 0 {
        sub_u128(a_bits, b_bits)
    } else {
        let (sum, overflow) = add_u128(a_bits, b_bits);
        if overflow {
            let rounded = sum.wrapping_add(sum & 1);
            (((1_u128 << 127) | (rounded >> 1)), true)
        } else {
            (sum, false)
        }
    };

    let mut out_ex = m_ex;
    if (a.sgn ^ b.sgn) == 0 {
        let (_, overflow) = add_u128(a_bits, b_bits);
        if overflow {
            out_ex += 1;
        }
    }

    let (c_hi, c_lo) = unpack_u128(c_bits);
    let shift = if c_hi != 0 {
        c_hi.leading_zeros() as i64
    } else if c_lo != 0 {
        64 + c_lo.leading_zeros() as i64
    } else {
        a.ex
    };
    c_bits <<= shift as u32;
    let (hi, lo) = unpack_u128(c_bits);
    DInt64 {
        hi,
        lo,
        ex: out_ex - shift,
        sgn: a.sgn,
    }
}

fn mul_dint(a: &DInt64, b: &DInt64) -> DInt64 {
    let mut t = (a.hi as u128) * (b.hi as u128);
    let m1 = (a.hi as u128) * (b.lo as u128);
    let m2 = (a.lo as u128) * (b.hi as u128);
    let (sum, overflow) = m1.overflowing_add(m2);
    let carry = (sum >> 64) + ((overflow as u128) << 64);
    t = t.wrapping_add(carry);

    let mut ex = 0_i64;
    if (t >> 127) == 0 {
        t <<= 1;
        ex = 1;
    }
    t = t.wrapping_add((sum >> 63) & 1);
    let (hi, lo) = unpack_u128(t);

    DInt64 {
        hi,
        lo,
        ex: a.ex + b.ex - ex + 1,
        sgn: a.sgn ^ b.sgn,
    }
}

fn mul_dint_i64(b: i64, a: &DInt64) -> DInt64 {
    if b == 0 {
        return ZERO;
    }

    let c = b.unsigned_abs();
    let sgn = if b < 0 { a.sgn ^ 1 } else { a.sgn };

    let mut t = (a.hi as u128) * (c as u128);
    let mut m = if (t >> 64) != 0 {
        ((t >> 64) as u64).leading_zeros()
    } else {
        64
    };
    t <<= m;

    let mut l = (a.lo as u128) * (c as u128);
    l = (l << (m - 1)) >> 63;

    let (sum, overflow) = l.overflowing_add(t);
    t = sum;
    if overflow {
        t = t.wrapping_add(t & 1);
        t = (1_u128 << 127) | (t >> 1);
        m -= 1;
    }

    let (hi, lo) = unpack_u128(t);
    DInt64 {
        hi,
        lo,
        ex: a.ex + 64 - i64::from(m),
        sgn,
    }
}

fn log_2(x: &mut DInt64) -> DInt64 {
    let mut exp = x.ex;
    let mut i = (x.hi >> 55) as usize;

    if x.hi > 0xb504_f333_f9de_6484 {
        exp += 1;
        i >>= 1;
    }

    x.ex -= exp;

    let mut z = mul_dint(x, &INVERSE_2[i - 128]);
    z = add_dint(&z, &M_ONE);

    let mut out = mul_dint_i64(exp, &LOG2);
    let mut p = p_2(&z);
    p = add_dint(&LOG_INV_2[i - 128], &p);
    out = add_dint(&p, &out);
    out
}

fn p_2(z: &DInt64) -> DInt64 {
    let mut out = P_2[0];
    for coeff in P_2.iter().skip(1) {
        out = mul_dint(z, &out);
        out = add_dint(coeff, &out);
    }
    mul_dint(z, &out)
}

fn fast_extract(x: f64) -> (i64, u64) {
    let bits = x.to_bits();
    let exp_bits = ((bits >> 52) & 0x7ff) as i64;
    let mantissa = (bits & ((1_u64 << 52) - 1)) + if exp_bits != 0 { 1_u64 << 52 } else { 0 };
    (exp_bits - 0x3ff, mantissa)
}

fn dint_from_f64(value: f64) -> DInt64 {
    let (mut ex, mut hi) = fast_extract(value);
    let t = hi.leading_zeros() as i64;
    hi <<= t as u32;
    ex -= if t > 11 { t - 12 } else { 0 };

    DInt64 {
        hi,
        lo: 0,
        ex,
        sgn: u64::from(value.is_sign_negative()),
    }
}

fn dint_to_f64(value: DInt64) -> f64 {
    let mut r = F64Bits::from_bits((value.hi >> 11) | (0x3ff_u64 << 52));
    let mut rd = 0.0;
    if ((value.hi >> 10) & 1) != 0 {
        rd += f64::from_bits(0x3ca0_0000_0000_0000);
    }
    if (value.hi & 0x3ff) != 0 || value.lo != 0 {
        rd += f64::from_bits(0x3c90_0000_0000_0000);
    }
    r.u |= value.sgn << 63;
    r.f = f64::from_bits(r.u);
    r.f += if value.sgn == 0 { rd } else { -rd };

    let e = F64Bits::from_bits((((value.ex + 1023) as u64) & 0x7ff) << 52);
    r.f * e.f
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_hash_maps_zero_to_smallest_value() {
        let x = Unit53::from_hash(0);
        assert_eq!(x.numerator(), 1);
        assert_eq!(x.to_f64().to_bits(), (2.0f64).powi(-53).to_bits());
    }

    #[test]
    fn from_hash_maps_max_to_one() {
        let x = Unit53::from_hash(u64::MAX);
        assert_eq!(x.numerator(), 1u64 << 53);
        assert_eq!(x.to_f64().to_bits(), 1.0f64.to_bits());
    }

    #[test]
    fn deterministic_log_respects_domain_endpoints() {
        let min = deterministic_log_u53(Unit53::from_hash(0));
        let max = deterministic_log_u53(Unit53::from_hash(u64::MAX));

        assert!(min.is_finite());
        assert!(min < 0.0);
        assert_eq!(max.to_bits(), 0.0f64.to_bits());
    }

    #[test]
    fn deterministic_log_is_monotonic_over_hash_domain() {
        let smaller = Unit53::from_hash(0);
        let larger = Unit53::from_hash(1u64 << 63);

        assert!(smaller.to_f64() < larger.to_f64());
        assert!(deterministic_log_u53(smaller) < deterministic_log_u53(larger));
    }

    #[test]
    fn deterministic_log_matches_reference_corpus() {
        for line in include_str!("../testdata/log_u53_reference.tsv").lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut fields = line.split('\t');
            let numerator = fields.next().unwrap().parse::<u64>().unwrap();
            let _input_bits = fields.next().unwrap();
            let expected_bits = u64::from_str_radix(fields.next().unwrap(), 16).unwrap();

            let unit = Unit53(numerator);
            assert_eq!(
                deterministic_log_u53(unit).to_bits(),
                expected_bits,
                "numerator={numerator}"
            );
        }
    }
}
