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

pub(crate) fn encode_rows(k: usize, tables: &[u8], data: &[&[u8]], outputs: &mut [&mut [u8]]) {
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

pub(crate) fn apply_matrix_rows(
    k: usize,
    rows: &[u8],
    inputs: &[&[u8]],
    outputs: &mut [&mut [u8]],
) {
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
