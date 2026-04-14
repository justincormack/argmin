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

static GF_MUL_TABLE: [u8; GF_TABLE_SIZE] = build_mul_table();
static GF_INV_TABLE: [u8; 256] = build_inv_table(&GF_MUL_TABLE);

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

#[inline(always)]
fn xor_with_coeff(dest: &mut [u8], src: &[u8], coeff: u8) {
    match coeff {
        0 => {}
        1 => {
            for (out, &value) in dest.iter_mut().zip(src.iter()) {
                *out ^= value;
            }
        }
        _ => {
            let table = gf_mul_table(coeff);
            for (out, &value) in dest.iter_mut().zip(src.iter()) {
                *out ^= table[value as usize];
            }
        }
    }
}

#[inline(always)]
fn xor_with_table(dest: &mut [u8], src: &[u8], table: &[u8]) {
    for (out, &value) in dest.iter_mut().zip(src.iter()) {
        *out ^= table[value as usize];
    }
}

pub(crate) fn encode_rows(k: usize, tables: &[u8], data: &[&[u8]], outputs: &mut [&mut [u8]]) {
    for (row, output) in outputs.iter_mut().enumerate() {
        output.fill(0);
        for (col, source) in data.iter().enumerate().take(k) {
            let table_start = (row * k + col) * 256;
            let table_end = table_start + 256;
            xor_with_table(output, source, &tables[table_start..table_end]);
        }
    }
}

pub(crate) fn apply_matrix_rows(
    k: usize,
    rows: &[u8],
    inputs: &[&[u8]],
    outputs: &mut [&mut [u8]],
) {
    debug_assert!(k <= MAX_TOTAL_SHARDS);
    for (row_index, output) in outputs.iter_mut().enumerate() {
        output.fill(0);
        let row = &rows[row_index * k..(row_index + 1) * k];
        for (coeff, source) in row.iter().copied().zip(inputs.iter().copied()) {
            xor_with_coeff(output, source, coeff);
        }
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
