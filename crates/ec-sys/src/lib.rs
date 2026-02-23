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
    /// Generate a Cauchy matrix of size `m` × `k` in GF(2^8).
    ///
    /// `a` must point to a buffer of at least `m * k` bytes.
    /// The identity rows (data shards) are NOT included; this is the parity
    /// sub-matrix only.
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
