use crate::codec::Backend;
use crate::MAX_TOTAL_SHARDS;

const GF_POLY_LOW: u8 = 0x1d;
const GF_TABLE_SIZE: usize = 256 * 256;

const fn gf_mul_slow(mut a: u8, mut b: u8) -> u8 {
    let mut product = 0u8;
    while b != 0 {
        if b & 1 != 0 {
            product ^= a;
        }
        let carry = a & 0x80;
        a <<= 1;
        if carry != 0 {
            a ^= GF_POLY_LOW;
        }
        b >>= 1;
    }
    product
}

const fn build_mul_table() -> [u8; GF_TABLE_SIZE] {
    let mut table = [0u8; GF_TABLE_SIZE];
    let mut a = 0usize;
    while a < 256 {
        let mut b = 0usize;
        while b < 256 {
            table[a * 256 + b] = gf_mul_slow(a as u8, b as u8);
            b += 1;
        }
        a += 1;
    }
    table
}

const fn build_inv_table(mul_table: &[u8; GF_TABLE_SIZE]) -> [u8; 256] {
    let mut table = [0u8; 256];
    let mut value = 1usize;
    while value < 256 {
        let mut candidate = 1usize;
        while candidate < 256 {
            if mul_table[value * 256 + candidate] == 1 {
                table[value] = candidate as u8;
                break;
            }
            candidate += 1;
        }
        value += 1;
    }
    table
}

const fn build_all_nibble_tables() -> [[u8; 32]; 256] {
    let mut tables = [[0u8; 32]; 256];
    let mut coeff = 0usize;
    while coeff < 256 {
        let mut nibble = 0usize;
        while nibble < 16 {
            tables[coeff][nibble] = gf_mul_slow(coeff as u8, nibble as u8);
            tables[coeff][16 + nibble] = gf_mul_slow(coeff as u8, (nibble << 4) as u8);
            nibble += 1;
        }
        coeff += 1;
    }
    tables
}

static GF_MUL_TABLE: [u8; GF_TABLE_SIZE] = build_mul_table();
static GF_INV_TABLE: [u8; 256] = build_inv_table(&GF_MUL_TABLE);
#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
static GF_NIBBLE_TABLES: [[u8; 32]; 256] = build_all_nibble_tables();

#[inline(always)]
pub(crate) fn gf_mul(a: u8, b: u8) -> u8 {
    GF_MUL_TABLE[a as usize * 256 + b as usize]
}

#[inline(always)]
pub(crate) fn gf_inv(a: u8) -> u8 {
    GF_INV_TABLE[a as usize]
}

#[inline(always)]
pub(crate) fn gf_mul_table(coeff: u8) -> &'static [u8] {
    &GF_MUL_TABLE[coeff as usize * 256..(coeff as usize + 1) * 256]
}

pub(crate) fn gen_cauchy1_matrix(matrix: &mut [u8], rows: usize, k: usize) {
    matrix.fill(0);
    for row in 0..k {
        matrix[row * k + row] = 1;
    }
    let mut offset = k * k;
    for row in k..rows {
        for col in 0..k {
            matrix[offset] = gf_inv((row ^ col) as u8);
            offset += 1;
        }
    }
}

pub(crate) fn build_mul_tables(coefficients: &[u8]) -> Vec<u8> {
    let mut tables = vec![0u8; coefficients.len() * 256];
    for (index, &coefficient) in coefficients.iter().enumerate() {
        let start = index * 256;
        let end = start + 256;
        tables[start..end].copy_from_slice(gf_mul_table(coefficient));
    }
    tables
}

pub(crate) fn build_nibble_tables(coefficients: &[u8]) -> Vec<u8> {
    let mut tables = vec![0u8; coefficients.len() * 32];
    for (index, &coefficient) in coefficients.iter().enumerate() {
        let start = index * 32;
        let end = start + 32;
        tables[start..end].copy_from_slice(&build_nibble_table(coefficient));
    }
    tables
}

fn build_nibble_table(coeff: u8) -> [u8; 32] {
    let full = gf_mul_table(coeff);
    let mut table = [0u8; 32];
    let mut index = 0usize;
    while index < 16 {
        table[index] = full[index];
        table[16 + index] = full[index << 4];
        index += 1;
    }
    table
}

#[inline(always)]
fn xor_with_coeff(dest: &mut [u8], src: &[u8], coeff: u8) {
    match coeff {
        0 => {}
        1 => {
            xor_with_slice(dest, src);
        }
        _ => {
            xor_with_table(dest, src, gf_mul_table(coeff));
        }
    }
}

#[inline(always)]
fn write_with_coeff(dest: &mut [u8], src: &[u8], coeff: u8) {
    match coeff {
        0 => dest.fill(0),
        1 => dest.copy_from_slice(src),
        _ => write_with_table(dest, src, gf_mul_table(coeff)),
    }
}

#[inline(always)]
fn xor_with_slice(dest: &mut [u8], src: &[u8]) {
    debug_assert_eq!(dest.len(), src.len());

    let len = dest.len();
    let dest_ptr = dest.as_mut_ptr();
    let src_ptr = src.as_ptr();
    let mut index = 0usize;

    // SAFETY: `dest` and `src` have the same length, and every indexed access
    // stays within bounds of those slices.
    unsafe {
        while index + 8 <= len {
            *dest_ptr.add(index) ^= *src_ptr.add(index);
            *dest_ptr.add(index + 1) ^= *src_ptr.add(index + 1);
            *dest_ptr.add(index + 2) ^= *src_ptr.add(index + 2);
            *dest_ptr.add(index + 3) ^= *src_ptr.add(index + 3);
            *dest_ptr.add(index + 4) ^= *src_ptr.add(index + 4);
            *dest_ptr.add(index + 5) ^= *src_ptr.add(index + 5);
            *dest_ptr.add(index + 6) ^= *src_ptr.add(index + 6);
            *dest_ptr.add(index + 7) ^= *src_ptr.add(index + 7);
            index += 8;
        }
        while index < len {
            *dest_ptr.add(index) ^= *src_ptr.add(index);
            index += 1;
        }
    }
}

#[inline(always)]
fn xor_with_table(dest: &mut [u8], src: &[u8], table: &[u8]) {
    debug_assert_eq!(dest.len(), src.len());
    debug_assert_eq!(table.len(), 256);

    let len = dest.len();
    let dest_ptr = dest.as_mut_ptr();
    let src_ptr = src.as_ptr();
    let table_ptr = table.as_ptr();
    let mut index = 0usize;

    // SAFETY: `dest` and `src` have the same length, every table lookup stays
    // within the fixed 256-byte multiplication table, and every slice access is
    // within bounds.
    unsafe {
        while index + 8 <= len {
            *dest_ptr.add(index) ^= *table_ptr.add(*src_ptr.add(index) as usize);
            *dest_ptr.add(index + 1) ^= *table_ptr.add(*src_ptr.add(index + 1) as usize);
            *dest_ptr.add(index + 2) ^= *table_ptr.add(*src_ptr.add(index + 2) as usize);
            *dest_ptr.add(index + 3) ^= *table_ptr.add(*src_ptr.add(index + 3) as usize);
            *dest_ptr.add(index + 4) ^= *table_ptr.add(*src_ptr.add(index + 4) as usize);
            *dest_ptr.add(index + 5) ^= *table_ptr.add(*src_ptr.add(index + 5) as usize);
            *dest_ptr.add(index + 6) ^= *table_ptr.add(*src_ptr.add(index + 6) as usize);
            *dest_ptr.add(index + 7) ^= *table_ptr.add(*src_ptr.add(index + 7) as usize);
            index += 8;
        }
        while index < len {
            *dest_ptr.add(index) ^= *table_ptr.add(*src_ptr.add(index) as usize);
            index += 1;
        }
    }
}

#[inline(always)]
fn write_with_table(dest: &mut [u8], src: &[u8], table: &[u8]) {
    debug_assert_eq!(dest.len(), src.len());
    debug_assert_eq!(table.len(), 256);

    let len = dest.len();
    let dest_ptr = dest.as_mut_ptr();
    let src_ptr = src.as_ptr();
    let table_ptr = table.as_ptr();
    let mut index = 0usize;

    // SAFETY: `dest` and `src` have the same length, every table lookup stays
    // within the fixed 256-byte multiplication table, and every slice access is
    // within bounds.
    unsafe {
        while index + 8 <= len {
            *dest_ptr.add(index) = *table_ptr.add(*src_ptr.add(index) as usize);
            *dest_ptr.add(index + 1) = *table_ptr.add(*src_ptr.add(index + 1) as usize);
            *dest_ptr.add(index + 2) = *table_ptr.add(*src_ptr.add(index + 2) as usize);
            *dest_ptr.add(index + 3) = *table_ptr.add(*src_ptr.add(index + 3) as usize);
            *dest_ptr.add(index + 4) = *table_ptr.add(*src_ptr.add(index + 4) as usize);
            *dest_ptr.add(index + 5) = *table_ptr.add(*src_ptr.add(index + 5) as usize);
            *dest_ptr.add(index + 6) = *table_ptr.add(*src_ptr.add(index + 6) as usize);
            *dest_ptr.add(index + 7) = *table_ptr.add(*src_ptr.add(index + 7) as usize);
            index += 8;
        }
        while index < len {
            *dest_ptr.add(index) = *table_ptr.add(*src_ptr.add(index) as usize);
            index += 1;
        }
    }
}

fn encode_rows_scalar(k: usize, tables: &[u8], data: &[&[u8]], outputs: &mut [&mut [u8]]) {
    for (row, output) in outputs.iter_mut().enumerate() {
        let mut first_nonzero_col = None;
        for col in 0..k {
            let table_start = (row * k + col) * 256;
            let table_end = table_start + 256;
            if tables[table_start + 1] != 0 {
                first_nonzero_col = Some((col, &tables[table_start..table_end]));
                break;
            }
        }

        let Some((first_col, first_table)) = first_nonzero_col else {
            output.fill(0);
            continue;
        };

        write_with_table(output, data[first_col], first_table);
        for (col, source) in data.iter().enumerate().take(k).skip(first_col + 1) {
            let table_start = (row * k + col) * 256;
            if tables[table_start + 1] == 0 {
                continue;
            }
            let table_end = table_start + 256;
            xor_with_table(output, source, &tables[table_start..table_end]);
        }
    }
}

pub(crate) fn encode_rows(
    backend: Backend,
    k: usize,
    tables: &[u8],
    #[cfg(target_arch = "aarch64")] neon_tables: &[u8],
    #[cfg(target_arch = "x86_64")] x86_tables: &[u8],
    data: &[&[u8]],
    outputs: &mut [&mut [u8]],
) {
    match backend {
        Backend::Scalar => encode_rows_scalar(k, tables, data, outputs),
        #[cfg(target_arch = "aarch64")]
        Backend::NeonAarch64 => unsafe { aarch64_neon::encode_rows(k, neon_tables, data, outputs) },
        #[cfg(target_arch = "x86_64")]
        Backend::Avx512X86_64 => unsafe {
            x86_64_avx512::encode_rows(k, x86_tables, data, outputs)
        },
        #[cfg(target_arch = "x86_64")]
        Backend::Avx2X86_64 => unsafe { x86_64_avx2::encode_rows(k, x86_tables, data, outputs) },
    }
}

fn apply_matrix_rows_scalar(k: usize, rows: &[u8], inputs: &[&[u8]], outputs: &mut [&mut [u8]]) {
    debug_assert!(k <= MAX_TOTAL_SHARDS);
    for (row_index, output) in outputs.iter_mut().enumerate() {
        let row = &rows[row_index * k..(row_index + 1) * k];
        let mut first_nonzero = None;
        for (index, &coeff) in row.iter().enumerate() {
            if coeff != 0 {
                first_nonzero = Some((index, coeff));
                break;
            }
        }

        let Some((first_index, first_coeff)) = first_nonzero else {
            output.fill(0);
            continue;
        };

        write_with_coeff(output, inputs[first_index], first_coeff);
        for (coeff, source) in row
            .iter()
            .copied()
            .zip(inputs.iter().copied())
            .skip(first_index + 1)
        {
            xor_with_coeff(output, source, coeff);
        }
    }
}

pub(crate) fn apply_matrix_rows(
    backend: Backend,
    k: usize,
    rows: &[u8],
    inputs: &[&[u8]],
    outputs: &mut [&mut [u8]],
) {
    match backend {
        Backend::Scalar => apply_matrix_rows_scalar(k, rows, inputs, outputs),
        #[cfg(target_arch = "aarch64")]
        Backend::NeonAarch64 => unsafe {
            aarch64_neon::apply_matrix_rows(k, rows, inputs, outputs)
        },
        #[cfg(target_arch = "x86_64")]
        Backend::Avx512X86_64 => unsafe {
            x86_64_avx512::apply_matrix_rows(k, rows, inputs, outputs)
        },
        #[cfg(target_arch = "x86_64")]
        Backend::Avx2X86_64 => unsafe { x86_64_avx2::apply_matrix_rows(k, rows, inputs, outputs) },
    }
}

pub(crate) fn invert_matrix(input: &mut [u8], output: &mut [u8], n: usize) -> Result<(), ()> {
    output.fill(0);
    for i in 0..n {
        output[i * n + i] = 1;
    }

    for pivot in 0..n {
        if input[pivot * n + pivot] == 0 {
            let mut swap_row = pivot + 1;
            while swap_row < n && input[swap_row * n + pivot] == 0 {
                swap_row += 1;
            }
            if swap_row == n {
                return Err(());
            }
            for col in 0..n {
                input.swap(pivot * n + col, swap_row * n + col);
                output.swap(pivot * n + col, swap_row * n + col);
            }
        }

        let scale = gf_inv(input[pivot * n + pivot]);
        for col in 0..n {
            input[pivot * n + col] = gf_mul(input[pivot * n + col], scale);
            output[pivot * n + col] = gf_mul(output[pivot * n + col], scale);
        }

        for row in 0..n {
            if row == pivot {
                continue;
            }
            let factor = input[row * n + pivot];
            if factor == 0 {
                continue;
            }
            for col in 0..n {
                output[row * n + col] ^= gf_mul(factor, output[pivot * n + col]);
                input[row * n + col] ^= gf_mul(factor, input[pivot * n + col]);
            }
        }
    }

    Ok(())
}

#[cfg(target_arch = "aarch64")]
mod aarch64_neon {
    use super::{write_with_coeff, xor_with_coeff, GF_NIBBLE_TABLES, MAX_TOTAL_SHARDS};
    use core::arch::aarch64::{
        uint8x16_t, vandq_u8, vdupq_n_u8, veorq_u8, vld1q_u8, vqtbl1q_u8, vshrq_n_u8, vst1q_u8,
    };

    #[target_feature(enable = "neon")]
    pub(super) unsafe fn encode_rows(
        k: usize,
        tables: &[u8],
        data: &[&[u8]],
        outputs: &mut [&mut [u8]],
    ) {
        for (row, output) in outputs.iter_mut().enumerate() {
            dot_prod_table_row(k, row, tables, data, output);
        }
    }

    #[target_feature(enable = "neon")]
    unsafe fn dot_prod_table_row(
        k: usize,
        row_index: usize,
        tables: &[u8],
        data: &[&[u8]],
        output: &mut [u8],
    ) {
        let zero = vdupq_n_u8(0);
        let mut active_coeffs = [0u8; MAX_TOTAL_SHARDS];
        let mut active_inputs = [core::ptr::null::<u8>(); MAX_TOTAL_SHARDS];
        let mut low_tables = [zero; MAX_TOTAL_SHARDS];
        let mut high_tables = [zero; MAX_TOTAL_SHARDS];
        let mut active_len = 0usize;

        for (col, source) in data.iter().enumerate().take(k) {
            let table_start = (row_index * k + col) * 32;
            let coeff = tables[table_start + 1];
            if coeff == 0 {
                continue;
            }
            active_coeffs[active_len] = coeff;
            active_inputs[active_len] = source.as_ptr();
            if coeff != 1 {
                low_tables[active_len] = vld1q_u8(tables[table_start..].as_ptr());
                high_tables[active_len] = vld1q_u8(tables[table_start + 16..].as_ptr());
            }
            active_len += 1;
        }

        if active_len == 0 {
            output.fill(0);
            return;
        }

        let len = output.len();
        let out_ptr = output.as_mut_ptr();
        let mask = vdupq_n_u8(0x0f);
        let mut index = 0usize;

        while index + 64 <= len {
            let mut acc0 = zero;
            let mut acc1 = zero;
            let mut acc2 = zero;
            let mut acc3 = zero;

            for slot in 0..active_len {
                let src_ptr = active_inputs[slot].add(index);
                let src0 = vld1q_u8(src_ptr);
                let src1 = vld1q_u8(src_ptr.add(16));
                let src2 = vld1q_u8(src_ptr.add(32));
                let src3 = vld1q_u8(src_ptr.add(48));
                if active_coeffs[slot] == 1 {
                    acc0 = veorq_u8(acc0, src0);
                    acc1 = veorq_u8(acc1, src1);
                    acc2 = veorq_u8(acc2, src2);
                    acc3 = veorq_u8(acc3, src3);
                    continue;
                }

                let low_table = low_tables[slot];
                let high_table = high_tables[slot];
                acc0 = veorq_u8(acc0, mul_chunk(src0, low_table, high_table, mask));
                acc1 = veorq_u8(acc1, mul_chunk(src1, low_table, high_table, mask));
                acc2 = veorq_u8(acc2, mul_chunk(src2, low_table, high_table, mask));
                acc3 = veorq_u8(acc3, mul_chunk(src3, low_table, high_table, mask));
            }

            vst1q_u8(out_ptr.add(index), acc0);
            vst1q_u8(out_ptr.add(index + 16), acc1);
            vst1q_u8(out_ptr.add(index + 32), acc2);
            vst1q_u8(out_ptr.add(index + 48), acc3);
            index += 64;
        }

        while index + 16 <= len {
            let mut acc = zero;
            for slot in 0..active_len {
                let src = vld1q_u8(active_inputs[slot].add(index));
                let product = if active_coeffs[slot] == 1 {
                    src
                } else {
                    mul_chunk(src, low_tables[slot], high_tables[slot], mask)
                };
                acc = veorq_u8(acc, product);
            }
            vst1q_u8(out_ptr.add(index), acc);
            index += 16;
        }

        if index == len {
            return;
        }

        let mut first_index = None;
        for (col, source) in data.iter().enumerate().take(k) {
            let table_start = (row_index * k + col) * 32;
            let coeff = tables[table_start + 1];
            if coeff != 0 {
                first_index = Some((col, coeff, source));
                break;
            }
        }

        let (first_col, first_coeff, first_source) = first_index.unwrap();
        if first_coeff == 1 {
            output[index..].copy_from_slice(&first_source[index..]);
        } else {
            write_with_coeff(&mut output[index..], &first_source[index..], first_coeff);
        }

        for (col, source) in data.iter().enumerate().take(k) {
            let table_start = (row_index * k + col) * 32;
            let coeff = tables[table_start + 1];
            if coeff == 0 || col == first_col {
                continue;
            }
            xor_with_coeff(&mut output[index..], &source[index..], coeff);
        }
    }

    #[target_feature(enable = "neon")]
    pub(super) unsafe fn apply_matrix_rows(
        k: usize,
        rows: &[u8],
        inputs: &[&[u8]],
        outputs: &mut [&mut [u8]],
    ) {
        for (row_index, output) in outputs.iter_mut().enumerate() {
            let row = &rows[row_index * k..(row_index + 1) * k];
            dot_prod_row(k, row, inputs, output);
        }
    }

    #[target_feature(enable = "neon")]
    unsafe fn dot_prod_row(k: usize, row: &[u8], inputs: &[&[u8]], output: &mut [u8]) {
        let mut active_coeffs = [0u8; MAX_TOTAL_SHARDS];
        let mut active_inputs = [core::ptr::null::<u8>(); MAX_TOTAL_SHARDS];
        let mut active_len = 0usize;

        for col in 0..k {
            let coeff = row[col];
            if coeff == 0 {
                continue;
            }
            active_coeffs[active_len] = coeff;
            active_inputs[active_len] = inputs[col].as_ptr();
            active_len += 1;
        }

        if active_len == 0 {
            output.fill(0);
            return;
        }

        let len = output.len();
        let out_ptr = output.as_mut_ptr();
        let mask = vdupq_n_u8(0x0f);
        let mut index = 0usize;

        while index + 64 <= len {
            let mut acc0 = vdupq_n_u8(0);
            let mut acc1 = vdupq_n_u8(0);
            let mut acc2 = vdupq_n_u8(0);
            let mut acc3 = vdupq_n_u8(0);

            for slot in 0..active_len {
                let src_ptr = active_inputs[slot].add(index);
                let src0 = vld1q_u8(src_ptr);
                let src1 = vld1q_u8(src_ptr.add(16));
                let src2 = vld1q_u8(src_ptr.add(32));
                let src3 = vld1q_u8(src_ptr.add(48));
                if active_coeffs[slot] == 1 {
                    acc0 = veorq_u8(acc0, src0);
                    acc1 = veorq_u8(acc1, src1);
                    acc2 = veorq_u8(acc2, src2);
                    acc3 = veorq_u8(acc3, src3);
                    continue;
                }

                let table = &GF_NIBBLE_TABLES[active_coeffs[slot] as usize];
                let low_table = vld1q_u8(table.as_ptr());
                let high_table = vld1q_u8(table[16..].as_ptr());
                acc0 = veorq_u8(acc0, mul_chunk(src0, low_table, high_table, mask));
                acc1 = veorq_u8(acc1, mul_chunk(src1, low_table, high_table, mask));
                acc2 = veorq_u8(acc2, mul_chunk(src2, low_table, high_table, mask));
                acc3 = veorq_u8(acc3, mul_chunk(src3, low_table, high_table, mask));
            }

            vst1q_u8(out_ptr.add(index), acc0);
            vst1q_u8(out_ptr.add(index + 16), acc1);
            vst1q_u8(out_ptr.add(index + 32), acc2);
            vst1q_u8(out_ptr.add(index + 48), acc3);
            index += 64;
        }

        while index + 16 <= len {
            let mut acc = vdupq_n_u8(0);
            for slot in 0..active_len {
                let src = vld1q_u8(active_inputs[slot].add(index));
                let product = if active_coeffs[slot] == 1 {
                    src
                } else {
                    let table = &GF_NIBBLE_TABLES[active_coeffs[slot] as usize];
                    let low_table = vld1q_u8(table.as_ptr());
                    let high_table = vld1q_u8(table[16..].as_ptr());
                    mul_chunk(src, low_table, high_table, mask)
                };
                acc = veorq_u8(acc, product);
            }
            vst1q_u8(out_ptr.add(index), acc);
            index += 16;
        }

        if index == len {
            return;
        }

        let mut first_index = None;
        for (col, &coeff) in row.iter().enumerate().take(k) {
            if coeff != 0 {
                first_index = Some((col, coeff));
                break;
            }
        }
        let (first_col, first_coeff) = first_index.unwrap();
        if first_coeff == 1 {
            output[index..].copy_from_slice(&inputs[first_col][index..]);
        } else {
            write_with_coeff(
                &mut output[index..],
                &inputs[first_col][index..],
                first_coeff,
            );
        }
        for (&coeff, source) in row.iter().zip(inputs.iter().copied()).skip(first_col + 1) {
            xor_with_coeff(&mut output[index..], &source[index..], coeff);
        }
    }

    #[target_feature(enable = "neon")]
    unsafe fn mul_chunk(
        src_chunk: uint8x16_t,
        low_table: uint8x16_t,
        high_table: uint8x16_t,
        mask: uint8x16_t,
    ) -> uint8x16_t {
        let low = vandq_u8(src_chunk, mask);
        let high = vandq_u8(vshrq_n_u8::<4>(src_chunk), mask);
        let low_product = vqtbl1q_u8(low_table, low);
        let high_product = vqtbl1q_u8(high_table, high);
        veorq_u8(low_product, high_product)
    }
}

#[cfg(target_arch = "x86_64")]
mod x86_64_avx512 {
    use super::{
        write_with_coeff, xor_with_coeff, xor_with_slice, GF_NIBBLE_TABLES, MAX_TOTAL_SHARDS,
    };
    use core::arch::x86_64::{
        __m128i, __m512i, _mm512_and_si512, _mm512_broadcast_i32x4, _mm512_loadu_si512,
        _mm512_set1_epi8, _mm512_setzero_si512, _mm512_shuffle_epi8, _mm512_srli_epi16,
        _mm512_storeu_si512, _mm512_xor_si512, _mm_loadu_si128,
    };

    #[target_feature(enable = "avx512f,avx512bw")]
    pub(super) unsafe fn encode_rows(
        k: usize,
        tables: &[u8],
        data: &[&[u8]],
        outputs: &mut [&mut [u8]],
    ) {
        for (row, output) in outputs.iter_mut().enumerate() {
            let mut first_nonzero_col = None;
            for col in 0..k {
                let table_start = (row * k + col) * 32;
                let coeff = tables[table_start + 1];
                if coeff != 0 {
                    first_nonzero_col = Some((col, coeff, &tables[table_start..table_start + 32]));
                    break;
                }
            }

            let Some((first_col, first_coeff, first_table)) = first_nonzero_col else {
                output.fill(0);
                continue;
            };

            if first_coeff == 1 {
                output.copy_from_slice(data[first_col]);
            } else {
                write_with_nibble_table(output, data[first_col], first_table);
            }

            for (col, source) in data.iter().enumerate().take(k).skip(first_col + 1) {
                let table_start = (row * k + col) * 32;
                let coeff = tables[table_start + 1];
                if coeff == 0 {
                    continue;
                }
                if coeff == 1 {
                    xor_with_slice(output, source);
                } else {
                    xor_with_nibble_table(output, source, &tables[table_start..table_start + 32]);
                }
            }
        }
    }

    #[target_feature(enable = "avx512f,avx512bw")]
    pub(super) unsafe fn apply_matrix_rows(
        k: usize,
        rows: &[u8],
        inputs: &[&[u8]],
        outputs: &mut [&mut [u8]],
    ) {
        for (row_index, output) in outputs.iter_mut().enumerate() {
            let row = &rows[row_index * k..(row_index + 1) * k];
            dot_prod_row(k, row, inputs, output);
        }
    }

    #[target_feature(enable = "avx512f,avx512bw")]
    unsafe fn dot_prod_row(k: usize, row: &[u8], inputs: &[&[u8]], output: &mut [u8]) {
        let mut active_coeffs = [0u8; MAX_TOTAL_SHARDS];
        let mut active_inputs = [core::ptr::null::<u8>(); MAX_TOTAL_SHARDS];
        let mut active_len = 0usize;

        for col in 0..k {
            let coeff = row[col];
            if coeff == 0 {
                continue;
            }
            active_coeffs[active_len] = coeff;
            active_inputs[active_len] = inputs[col].as_ptr();
            active_len += 1;
        }

        if active_len == 0 {
            output.fill(0);
            return;
        }

        let len = output.len();
        let out_ptr = output.as_mut_ptr();
        let mask = _mm512_set1_epi8(0x0f);
        let mut index = 0usize;

        while index + 64 <= len {
            let mut acc = _mm512_setzero_si512();
            for slot in 0..active_len {
                let src_chunk =
                    _mm512_loadu_si512(active_inputs[slot].add(index) as *const __m512i);
                let product = if active_coeffs[slot] == 1 {
                    src_chunk
                } else {
                    let table = &GF_NIBBLE_TABLES[active_coeffs[slot] as usize];
                    let low_table =
                        _mm512_broadcast_i32x4(_mm_loadu_si128(table.as_ptr() as *const __m128i));
                    let high_table = _mm512_broadcast_i32x4(_mm_loadu_si128(
                        table[16..].as_ptr() as *const __m128i
                    ));
                    mul_chunk(src_chunk, low_table, high_table, mask)
                };
                acc = _mm512_xor_si512(acc, product);
            }
            _mm512_storeu_si512(out_ptr.add(index) as *mut __m512i, acc);
            index += 64;
        }

        if index == len {
            return;
        }

        let mut first_index = None;
        for (col, &coeff) in row.iter().enumerate().take(k) {
            if coeff != 0 {
                first_index = Some((col, coeff));
                break;
            }
        }
        let (first_col, first_coeff) = first_index.unwrap();
        if first_coeff == 1 {
            output[index..].copy_from_slice(&inputs[first_col][index..]);
        } else {
            write_with_coeff(
                &mut output[index..],
                &inputs[first_col][index..],
                first_coeff,
            );
        }
        for (&coeff, source) in row.iter().zip(inputs.iter().copied()).skip(first_col + 1) {
            xor_with_coeff(&mut output[index..], &source[index..], coeff);
        }
    }

    #[target_feature(enable = "avx512f,avx512bw")]
    unsafe fn write_with_nibble_table(dest: &mut [u8], src: &[u8], table: &[u8]) {
        let coeff = table[1];
        if coeff == 0 || coeff == 1 || dest.len() < 64 {
            write_with_coeff(dest, src, coeff);
            return;
        }

        let len = dest.len();
        let mut index = 0usize;
        let mask = _mm512_set1_epi8(0x0f);
        let low_table = _mm512_broadcast_i32x4(_mm_loadu_si128(table.as_ptr() as *const __m128i));
        let high_table =
            _mm512_broadcast_i32x4(_mm_loadu_si128(table[16..].as_ptr() as *const __m128i));

        while index + 64 <= len {
            let src_chunk = _mm512_loadu_si512(src.as_ptr().add(index) as *const __m512i);
            let product = mul_chunk(src_chunk, low_table, high_table, mask);
            _mm512_storeu_si512(dest.as_mut_ptr().add(index) as *mut __m512i, product);
            index += 64;
        }

        if index < len {
            write_with_coeff(&mut dest[index..], &src[index..], coeff);
        }
    }

    #[target_feature(enable = "avx512f,avx512bw")]
    unsafe fn xor_with_nibble_table(dest: &mut [u8], src: &[u8], table: &[u8]) {
        let coeff = table[1];
        if coeff == 0 || coeff == 1 || dest.len() < 64 {
            xor_with_coeff(dest, src, coeff);
            return;
        }

        let len = dest.len();
        let mut index = 0usize;
        let mask = _mm512_set1_epi8(0x0f);
        let low_table = _mm512_broadcast_i32x4(_mm_loadu_si128(table.as_ptr() as *const __m128i));
        let high_table =
            _mm512_broadcast_i32x4(_mm_loadu_si128(table[16..].as_ptr() as *const __m128i));

        while index + 64 <= len {
            let src_chunk = _mm512_loadu_si512(src.as_ptr().add(index) as *const __m512i);
            let dest_chunk = _mm512_loadu_si512(dest.as_ptr().add(index) as *const __m512i);
            let product = mul_chunk(src_chunk, low_table, high_table, mask);
            let combined = _mm512_xor_si512(dest_chunk, product);
            _mm512_storeu_si512(dest.as_mut_ptr().add(index) as *mut __m512i, combined);
            index += 64;
        }

        if index < len {
            xor_with_coeff(&mut dest[index..], &src[index..], coeff);
        }
    }

    #[target_feature(enable = "avx512f,avx512bw")]
    unsafe fn mul_chunk(
        src_chunk: __m512i,
        low_table: __m512i,
        high_table: __m512i,
        mask: __m512i,
    ) -> __m512i {
        let low = _mm512_and_si512(src_chunk, mask);
        let high = _mm512_and_si512(_mm512_srli_epi16::<4>(src_chunk), mask);
        let low_product = _mm512_shuffle_epi8(low_table, low);
        let high_product = _mm512_shuffle_epi8(high_table, high);
        _mm512_xor_si512(low_product, high_product)
    }
}

#[cfg(target_arch = "x86_64")]
#[cfg(target_arch = "x86_64")]
mod x86_64_avx2 {
    use super::{
        write_with_coeff, xor_with_coeff, xor_with_slice, GF_NIBBLE_TABLES, MAX_TOTAL_SHARDS,
    };
    use core::arch::x86_64::{
        __m128i, __m256i, _mm256_and_si256, _mm256_broadcastsi128_si256, _mm256_loadu_si256,
        _mm256_set1_epi8, _mm256_setzero_si256, _mm256_shuffle_epi8, _mm256_srli_epi16,
        _mm256_storeu_si256, _mm256_xor_si256, _mm_loadu_si128,
    };

    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn encode_rows(
        k: usize,
        tables: &[u8],
        data: &[&[u8]],
        outputs: &mut [&mut [u8]],
    ) {
        for (row, output) in outputs.iter_mut().enumerate() {
            let mut first_nonzero_col = None;
            for col in 0..k {
                let table_start = (row * k + col) * 32;
                let coeff = tables[table_start + 1];
                if coeff != 0 {
                    first_nonzero_col = Some((col, coeff, &tables[table_start..table_start + 32]));
                    break;
                }
            }

            let Some((first_col, first_coeff, first_table)) = first_nonzero_col else {
                output.fill(0);
                continue;
            };

            if first_coeff == 1 {
                output.copy_from_slice(data[first_col]);
            } else {
                write_with_nibble_table(output, data[first_col], first_table);
            }

            for (col, source) in data.iter().enumerate().take(k).skip(first_col + 1) {
                let table_start = (row * k + col) * 32;
                let coeff = tables[table_start + 1];
                if coeff == 0 {
                    continue;
                }
                if coeff == 1 {
                    xor_with_slice(output, source);
                } else {
                    xor_with_nibble_table(output, source, &tables[table_start..table_start + 32]);
                }
            }
        }
    }

    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn apply_matrix_rows(
        k: usize,
        rows: &[u8],
        inputs: &[&[u8]],
        outputs: &mut [&mut [u8]],
    ) {
        for (row_index, output) in outputs.iter_mut().enumerate() {
            let row = &rows[row_index * k..(row_index + 1) * k];
            dot_prod_row(k, row, inputs, output);
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn dot_prod_row(k: usize, row: &[u8], inputs: &[&[u8]], output: &mut [u8]) {
        let mut active_coeffs = [0u8; MAX_TOTAL_SHARDS];
        let mut active_inputs = [core::ptr::null::<u8>(); MAX_TOTAL_SHARDS];
        let mut active_len = 0usize;

        for col in 0..k {
            let coeff = row[col];
            if coeff == 0 {
                continue;
            }
            active_coeffs[active_len] = coeff;
            active_inputs[active_len] = inputs[col].as_ptr();
            active_len += 1;
        }

        if active_len == 0 {
            output.fill(0);
            return;
        }

        let len = output.len();
        let out_ptr = output.as_mut_ptr();
        let mask = _mm256_set1_epi8(0x0f);
        let mut index = 0usize;

        while index + 32 <= len {
            let mut acc = _mm256_setzero_si256();
            for slot in 0..active_len {
                let src_chunk =
                    _mm256_loadu_si256(active_inputs[slot].add(index) as *const __m256i);
                let product = if active_coeffs[slot] == 1 {
                    src_chunk
                } else {
                    let table = &GF_NIBBLE_TABLES[active_coeffs[slot] as usize];
                    let low_table = _mm256_broadcastsi128_si256(_mm_loadu_si128(
                        table.as_ptr() as *const __m128i
                    ));
                    let high_table = _mm256_broadcastsi128_si256(_mm_loadu_si128(
                        table[16..].as_ptr() as *const __m128i,
                    ));
                    mul_chunk(src_chunk, low_table, high_table, mask)
                };
                acc = _mm256_xor_si256(acc, product);
            }
            _mm256_storeu_si256(out_ptr.add(index) as *mut __m256i, acc);
            index += 32;
        }

        if index == len {
            return;
        }

        let mut first_index = None;
        for (col, &coeff) in row.iter().enumerate().take(k) {
            if coeff != 0 {
                first_index = Some((col, coeff));
                break;
            }
        }
        let (first_col, first_coeff) = first_index.unwrap();
        if first_coeff == 1 {
            output[index..].copy_from_slice(&inputs[first_col][index..]);
        } else {
            write_with_coeff(
                &mut output[index..],
                &inputs[first_col][index..],
                first_coeff,
            );
        }
        for (&coeff, source) in row.iter().zip(inputs.iter().copied()).skip(first_col + 1) {
            xor_with_coeff(&mut output[index..], &source[index..], coeff);
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn write_with_nibble_table(dest: &mut [u8], src: &[u8], table: &[u8]) {
        let coeff = table[1];
        if coeff == 0 || coeff == 1 || dest.len() < 32 {
            write_with_coeff(dest, src, coeff);
            return;
        }

        let len = dest.len();
        let mut index = 0usize;
        let mask = _mm256_set1_epi8(0x0f);
        let low_table =
            _mm256_broadcastsi128_si256(_mm_loadu_si128(table.as_ptr() as *const __m128i));
        let high_table =
            _mm256_broadcastsi128_si256(_mm_loadu_si128(table[16..].as_ptr() as *const __m128i));

        while index + 32 <= len {
            let src_chunk = _mm256_loadu_si256(src.as_ptr().add(index) as *const __m256i);
            let product = mul_chunk(src_chunk, low_table, high_table, mask);
            _mm256_storeu_si256(dest.as_mut_ptr().add(index) as *mut __m256i, product);
            index += 32;
        }

        if index < len {
            write_with_coeff(&mut dest[index..], &src[index..], coeff);
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn xor_with_nibble_table(dest: &mut [u8], src: &[u8], table: &[u8]) {
        let coeff = table[1];
        if coeff == 0 || coeff == 1 || dest.len() < 32 {
            xor_with_coeff(dest, src, coeff);
            return;
        }

        let len = dest.len();
        let mut index = 0usize;
        let mask = _mm256_set1_epi8(0x0f);
        let low_table =
            _mm256_broadcastsi128_si256(_mm_loadu_si128(table.as_ptr() as *const __m128i));
        let high_table =
            _mm256_broadcastsi128_si256(_mm_loadu_si128(table[16..].as_ptr() as *const __m128i));

        while index + 32 <= len {
            let src_chunk = _mm256_loadu_si256(src.as_ptr().add(index) as *const __m256i);
            let dest_chunk = _mm256_loadu_si256(dest.as_ptr().add(index) as *const __m256i);
            let product = mul_chunk(src_chunk, low_table, high_table, mask);
            let combined = _mm256_xor_si256(dest_chunk, product);
            _mm256_storeu_si256(dest.as_mut_ptr().add(index) as *mut __m256i, combined);
            index += 32;
        }

        if index < len {
            xor_with_coeff(&mut dest[index..], &src[index..], coeff);
        }
    }

    #[target_feature(enable = "avx2")]
    unsafe fn mul_chunk(
        src_chunk: __m256i,
        low_table: __m256i,
        high_table: __m256i,
        mask: __m256i,
    ) -> __m256i {
        let low = _mm256_and_si256(src_chunk, mask);
        let high = _mm256_and_si256(_mm256_srli_epi16(src_chunk, 4), mask);
        let low_product = _mm256_shuffle_epi8(low_table, low);
        let high_product = _mm256_shuffle_epi8(high_table, high);
        _mm256_xor_si256(low_product, high_product)
    }
}
