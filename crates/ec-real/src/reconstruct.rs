/// Reconstruction algorithm using ISA-L.
///
/// Algorithm:
/// 1. Use the pre-computed full (k+m)×k encoding matrix passed in as `encode_matrix`.
///    (Row i is the i-th row of the systematic encoding matrix; rows 0..k are identity,
///    rows k..k+m are the Cauchy parity coefficients.)
/// 2. Select the k rows corresponding to the k present shards → `sub_matrix` (k×k).
/// 3. Invert `sub_matrix` → `inv_matrix` (k×k).
///    This matrix maps present shards → original k data shards.
/// 4. For each shard to recover (index `r`):
///    - Get row `r` of the full encode matrix → `encode_row` (k elements).
///    - Multiply `encode_row` by `inv_matrix` over GF(2^8) → `recovery_row` (k elements).
///    - This expresses shard `r` as a linear combination of the k present shards.
/// 5. Bundle all recovery rows into `recovery_matrix` (n_recover × k).
/// 6. Call `ec_init_tables` + `ec_encode_data` to apply `recovery_matrix` to the
///    present shards and write into outputs.
///
/// All scratch is stack-allocated. Maximum stack usage:
///   full_matrix:     (k+m)×k  ≤ 32×32 = 1 KB
///   sub_matrix:      k×k      ≤ 32×32 = 1 KB
///   inv_matrix:      k×k      ≤ 32×32 = 1 KB
///   recovery_matrix: n×k      ≤ 32×32 = 1 KB
///   gf_tables:       32×k×n   ≤ 32×32×32 = 32 KB
///   pointer arrays:  (k+n)×8  ≤ 512 B
use crate::{EcError, MAX_TOTAL_SHARDS};

const GF_TABLE_ENTRY: usize = 32;

/// Multiply two bytes in GF(2^8) using the standard log/antilog tables.
/// Only used during matrix-vector multiply, not in the data hot path.
#[inline(always)]
fn gf_mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    // Use ISA-L's gf_mul for consistency with the encoding matrix.
    // This is only called k² times per reconstruct call (matrix multiply),
    // not per byte, so the non-SIMD cost is negligible.
    unsafe { ec_sys::gf_mul(a, b) }
}

/// XOR two bytes (addition in GF(2^8)).
#[inline(always)]
fn gf_add(a: u8, b: u8) -> u8 {
    a ^ b
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn reconstruct_shards(
    k: usize,
    _m: usize,
    encode_matrix: &[u8], // full (k+m)×k systematic encoding matrix, row-major
    present_indices: &[usize],
    present_data: &[&[u8]],
    recover_indices: &[usize],
    outputs: &mut [&mut [u8]],
    shard_size: usize,
) -> Result<(), EcError> {
    let n_recover = recover_indices.len();

    // We only need exactly k present shards. If more were provided, use the first k.
    let active_present = &present_indices[..k];
    let active_data = &present_data[..k];

    // ── Step 1: Build k×k sub-matrix from present rows of encode_matrix ───
    // encode_matrix is already the full (k+m)×k systematic matrix.
    let mut sub_matrix = [0u8; MAX_TOTAL_SHARDS * MAX_TOTAL_SHARDS];
    for (row, &shard_idx) in active_present.iter().enumerate() {
        let src = &encode_matrix[shard_idx * k..shard_idx * k + k];
        let dst = &mut sub_matrix[row * k..row * k + k];
        dst.copy_from_slice(src);
    }

    // ── Step 3: Invert sub-matrix ─────────────────────────────────────────
    let mut inv_matrix = [0u8; MAX_TOTAL_SHARDS * MAX_TOTAL_SHARDS];
    // SAFETY: ISA-L's gf_invert_matrix reads and writes exactly k*k bytes in
    // the provided buffers and does not access memory beyond those arrays.
    let rc = unsafe {
        ec_sys::gf_invert_matrix(sub_matrix.as_mut_ptr(), inv_matrix.as_mut_ptr(), k as i32)
    };
    if rc != 0 {
        return Err(EcError::SingularMatrix);
    }

    // ── Step 4 & 5: Build recovery matrix (n_recover × k) ─────────────────
    // For shard `r`:
    //   recovery_row[j] = sum_col( encode_matrix[r][col] * inv_matrix[col][j] )
    //   over col in 0..k
    // This is a standard matrix multiply: encode_row × inv_matrix.
    let mut recovery_matrix = [0u8; MAX_TOTAL_SHARDS * MAX_TOTAL_SHARDS];
    for (out_row, &r) in recover_indices.iter().enumerate() {
        for j in 0..k {
            let mut val = 0u8;
            for col in 0..k {
                val = gf_add(
                    val,
                    gf_mul(encode_matrix[r * k + col], inv_matrix[col * k + j]),
                );
            }
            recovery_matrix[out_row * k + j] = val;
        }
    }

    // ── Step 6: Apply recovery matrix via ISA-L ────────────────────────────
    let mut gf_tables = [0u8; GF_TABLE_ENTRY * MAX_TOTAL_SHARDS * MAX_TOTAL_SHARDS];
    // SAFETY: ISA-L treats the recovery matrix as read-only and writes exactly
    // 32*k*n_recover bytes into gf_tables.
    unsafe {
        ec_sys::ec_init_tables(
            k as i32,
            n_recover as i32,
            recovery_matrix.as_mut_ptr(),
            gf_tables.as_mut_ptr(),
        );
    }

    let mut data_ptrs = [std::ptr::null_mut::<u8>(); MAX_TOTAL_SHARDS];
    let mut out_ptrs = [std::ptr::null_mut::<u8>(); MAX_TOTAL_SHARDS];

    for (i, s) in active_data.iter().enumerate() {
        data_ptrs[i] = s.as_ptr() as *mut u8;
    }
    for (i, s) in outputs.iter_mut().enumerate() {
        out_ptrs[i] = s.as_mut_ptr();
    }

    // SAFETY: ISA-L treats gftbls and data inputs as read-only and writes
    // exactly shard_size bytes into each output buffer. The buffers are
    // non-overlapping per the API contract enforced by the caller.
    unsafe {
        ec_sys::ec_encode_data(
            shard_size as i32,
            k as i32,
            n_recover as i32,
            gf_tables.as_mut_ptr(),
            data_ptrs.as_mut_ptr(),
            out_ptrs.as_mut_ptr(),
        );
    }

    Ok(())
}
