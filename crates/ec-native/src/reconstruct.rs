use crate::gf::{apply_matrix_rows, gf_mul, invert_matrix};
use crate::{EcError, MAX_TOTAL_SHARDS};

#[allow(clippy::too_many_arguments)]
pub(crate) fn reconstruct_shards(
    k: usize,
    encode_matrix: &[u8],
    present_indices: &[usize],
    present_data: &[&[u8]],
    recover_indices: &[usize],
    outputs: &mut [&mut [u8]],
) -> Result<(), EcError> {
    let active_present = &present_indices[..k];
    let active_data = &present_data[..k];

    let mut sub_matrix = [0u8; MAX_TOTAL_SHARDS * MAX_TOTAL_SHARDS];
    for (row, &shard_idx) in active_present.iter().enumerate() {
        let src = &encode_matrix[shard_idx * k..shard_idx * k + k];
        let dst = &mut sub_matrix[row * k..row * k + k];
        dst.copy_from_slice(src);
    }

    let mut inv_matrix = [0u8; MAX_TOTAL_SHARDS * MAX_TOTAL_SHARDS];
    invert_matrix(&mut sub_matrix[..k * k], &mut inv_matrix[..k * k], k)
        .map_err(|_| EcError::SingularMatrix)?;

    let mut recovery_matrix = [0u8; MAX_TOTAL_SHARDS * MAX_TOTAL_SHARDS];
    for (out_row, &recover_index) in recover_indices.iter().enumerate() {
        for j in 0..k {
            let mut value = 0u8;
            for col in 0..k {
                value ^= gf_mul(
                    encode_matrix[recover_index * k + col],
                    inv_matrix[col * k + j],
                );
            }
            recovery_matrix[out_row * k + j] = value;
        }
    }

    apply_matrix_rows(k, &recovery_matrix, active_data, outputs);
    Ok(())
}
