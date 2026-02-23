//! Raw FFI bindings to Intel ISA-L erasure coding functions.
//!
//! Only the functions used by the `ec` crate are bound here. All other ISA-L
//! functionality (CRC, compression, etc.) is out of scope.
//!
//! # Safety
//! All functions here are `unsafe`. Callers must ensure buffer sizes match the
//! documented requirements. See the ISA-L header `isa-l/erasure_code.h` for
//! full documentation.

use std::os::raw::{c_int, c_uchar};

extern "C" {
    /// Generate the full systematic encoding matrix of size `m` × `k` in GF(2^8).
    ///
    /// Despite the name, this writes a *complete* `m × k` matrix where:
    /// - The top-left `(m-k) × k` block (rows 0..m-k) is the `k × k` identity
    ///   matrix (coefficients for the k data shards).
    /// - The remaining `(m - (m-k)) × k` rows are the Cauchy parity coefficients.
    ///
    /// In practice, call this as `gf_gen_cauchy1_matrix(a, k + p, k)` where `k`
    /// is the data shard count and `p` is the parity shard count.  The result is
    /// a `(k+p) × k` matrix; pass `&a[k*k..]` to `ec_init_tables` for encoding.
    ///
    /// `a` must point to a buffer of at least `m * k` bytes.
    pub fn gf_gen_cauchy1_matrix(a: *mut c_uchar, m: c_int, k: c_int);

    /// Invert an `n` × `n` matrix over GF(2^8).
    ///
    /// `in_` and `out` must each point to `n * n` bytes.
    /// Returns 0 on success, non-zero if the matrix is singular.
    pub fn gf_invert_matrix(in_: *mut c_uchar, out: *mut c_uchar, n: c_int) -> c_int;

    /// Pre-compute GF multiplication tables for encoding/decoding.
    ///
    /// `k`:      number of source (input) vectors
    /// `rows`:   number of output vectors (parity shards for encode; recovered shards for decode)
    /// `a`:      coefficient matrix, `rows * k` bytes
    /// `gftbls`: output table buffer, must be `32 * k * rows` bytes
    pub fn ec_init_tables(k: c_int, rows: c_int, a: *mut c_uchar, gftbls: *mut c_uchar);

    /// Multiply two bytes in GF(2^8).
    pub fn gf_mul(a: c_uchar, b: c_uchar) -> c_uchar;

    /// Encode or decode erasure codes over blocks of data.
    ///
    /// `len`:     byte length of each shard buffer (0 is valid, becomes a no-op)
    /// `k`:       number of source (input) vectors
    /// `rows`:    number of output (coding) vectors
    /// `gftbls`:  table buffer from `ec_init_tables`, `32 * k * rows` bytes
    /// `data`:    array of `k` pointers to source buffers, each `len` bytes
    /// `coding`:  array of `rows` pointers to output buffers, each `len` bytes
    pub fn ec_encode_data(
        len: c_int,
        k: c_int,
        rows: c_int,
        gftbls: *mut c_uchar,
        data: *mut *mut c_uchar,
        coding: *mut *mut c_uchar,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `gf_gen_cauchy1_matrix(a, k+m, k)` writes the k×k identity
    /// matrix into the first k rows. This guards against the parameter-order bug
    /// where `(a, m, k)` was called instead of `(a, k+m, k)`, which silently
    /// produced identity rows for parity (encode appeared to work but reconstruction
    /// was always singular when any parity shard was used).
    #[test]
    fn cauchy_matrix_has_identity_rows() {
        let k: i32 = 4;
        let m: i32 = 2;
        let total = k + m;
        let mut matrix = vec![0u8; (total * k) as usize];
        unsafe {
            gf_gen_cauchy1_matrix(matrix.as_mut_ptr(), total, k);
        }
        // The first k rows must form the k×k identity matrix.
        for row in 0..k as usize {
            for col in 0..k as usize {
                let expected = if row == col { 1u8 } else { 0u8 };
                assert_eq!(
                    matrix[row * k as usize + col],
                    expected,
                    "identity check failed at row={row}, col={col}"
                );
            }
        }
        // The remaining m rows must be non-zero (Cauchy coefficients; not identity).
        // We don't check exact values, just that they are not all-zero rows.
        for row in k as usize..total as usize {
            let row_slice = &matrix[row * k as usize..(row + 1) * k as usize];
            assert!(
                row_slice.iter().any(|&b| b != 0),
                "parity row {row} is all-zero (expected non-zero Cauchy coefficients)"
            );
        }
    }
}
