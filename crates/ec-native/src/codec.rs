#[cfg(target_arch = "x86_64")]
use crate::gf::build_gfni_tables;
use crate::gf::{build_mul_tables, encode_rows, gen_cauchy1_matrix};
use crate::reconstruct::reconstruct_shards;
use std::sync::OnceLock;

const TRACE_TARGET: &str = "ec";

/// Maximum supported total shards (k + m).
pub const MAX_TOTAL_SHARDS: usize = 32;

/// Parameters for an erasure coding scheme.
/// Built once; cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcConfig {
    /// k: number of data shards.
    pub data_shards: u8,
    /// m: number of parity shards.
    pub parity_shards: u8,
}

impl EcConfig {
    /// Create and validate a config.
    pub fn new(data_shards: u8, parity_shards: u8) -> Result<Self, EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "EcConfig::new",
            "data_shards={} parity_shards={}",
            data_shards,
            parity_shards
        );
        if data_shards == 0 {
            return Err(EcError::InvalidConfig {
                reason: "data_shards must be >= 1",
            });
        }
        let total = data_shards as usize + parity_shards as usize;
        if total > MAX_TOTAL_SHARDS {
            return Err(EcError::InvalidConfig {
                reason: "data_shards + parity_shards exceeds MAX_TOTAL_SHARDS",
            });
        }
        Ok(Self {
            data_shards,
            parity_shards,
        })
    }

    /// Total number of shards (k + m).
    pub fn total_shards(self) -> usize {
        self.data_shards as usize + self.parity_shards as usize
    }

    /// Storage overhead multiplier, e.g. (4,2) → 1.5.
    pub fn overhead(self) -> f64 {
        self.total_shards() as f64 / self.data_shards as f64
    }
}

impl Default for EcConfig {
    fn default() -> Self {
        Self {
            data_shards: 4,
            parity_shards: 2,
        }
    }
}

/// Result of a parity verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    /// All parity shards matched.
    Ok,
    /// Parity shard at this index (k..k+m) did not match.
    Mismatch(usize),
}

/// Errors from the erasure coding engine.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EcError {
    #[error("invalid config: {reason}")]
    InvalidConfig { reason: &'static str },

    #[error("wrong shard count: expected {expected}, got {got}")]
    ShardCount { expected: usize, got: usize },

    #[error("shard size mismatch at shard {index}: expected {expected} bytes, got {got}")]
    ShardSizeMismatch {
        index: usize,
        expected: usize,
        got: usize,
    },

    #[error("insufficient shards for reconstruction: need {need}, have {have}")]
    InsufficientShards { need: usize, have: usize },

    #[error("shard index {index} out of range 0..{total}")]
    ShardIndexOutOfRange { index: usize, total: usize },

    #[error("duplicate shard index {index}")]
    DuplicateShardIndex { index: usize },

    #[error("present_indices and recover_indices overlap at shard index {index}")]
    OverlappingIndices { index: usize },

    #[error("present_indices is not sorted: element at position {index} is out of order")]
    UnsortedIndices { index: usize },

    #[error("scratch buffer too small: need {required} bytes, got {provided}")]
    ScratchTooSmall { required: usize, provided: usize },

    #[error("shard size {size} exceeds i32::MAX; split into smaller stripes")]
    ShardSizeTooLarge { size: usize },

    #[error("duplicate recover shard index {index}")]
    DuplicateRecoverIndex { index: usize },

    #[error("internal: matrix inversion failed (singular matrix)")]
    SingularMatrix,

    #[error("runtime smoke test failed: {reason}")]
    SmokeTestFailed { reason: String },
}

/// Validate that `shard_size` fits in ISA-L's `c_int` (`len`) parameter.
///
/// Extracted as a testable helper so tests can verify the boundary without
/// constructing fake large slices (which would be UB).
pub(crate) fn check_shard_size(size: usize) -> Result<(), EcError> {
    if size > i32::MAX as usize {
        Err(EcError::ShardSizeTooLarge { size })
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Backend {
    Scalar,
    #[cfg(target_arch = "aarch64")]
    NeonAarch64,
    #[cfg(target_arch = "x86_64")]
    Avx512GfniX86_64,
    #[cfg(target_arch = "x86_64")]
    Avx512X86_64,
    #[cfg(target_arch = "x86_64")]
    Avx2X86_64,
}

#[inline]
pub(crate) fn selected_backend() -> Backend {
    if let Some(backend) = bench_override_backend() {
        return backend;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if has_avx512_gfni_x86_64() {
            return Backend::Avx512GfniX86_64;
        }

        if has_avx512_x86_64() {
            return Backend::Avx512X86_64;
        }

        if has_avx2_x86_64() {
            return Backend::Avx2X86_64;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if has_neon_aarch64() {
            return Backend::NeonAarch64;
        }
    }

    Backend::Scalar
}

#[inline]
#[cfg(target_arch = "x86_64")]
pub(crate) fn has_avx512_gfni_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("avx512f") && std::arch::is_x86_feature_detected!("gfni")
}

#[inline]
#[cfg(target_arch = "x86_64")]
pub(crate) fn has_avx2_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("avx2")
}

#[inline]
#[cfg(target_arch = "x86_64")]
pub(crate) fn has_avx512_x86_64() -> bool {
    std::arch::is_x86_feature_detected!("avx512f")
        && std::arch::is_x86_feature_detected!("avx512bw")
}

#[inline]
#[cfg(target_arch = "aarch64")]
pub(crate) fn has_neon_aarch64() -> bool {
    std::arch::is_aarch64_feature_detected!("neon")
}

#[inline]
fn bench_override_backend() -> Option<Backend> {
    static OVERRIDE: OnceLock<Option<Backend>> = OnceLock::new();

    *OVERRIDE.get_or_init(|| {
        let override_name = std::env::var("ARGMIN_EC_BENCH_BACKEND")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase());

        match override_name.as_deref() {
            Some("scalar") => Some(Backend::Scalar),
            #[cfg(target_arch = "aarch64")]
            Some("neon") if has_neon_aarch64() => Some(Backend::NeonAarch64),
            #[cfg(target_arch = "x86_64")]
            Some("avx512-gfni" | "avx512_gfni" | "gfni") if has_avx512_gfni_x86_64() => {
                Some(Backend::Avx512GfniX86_64)
            }
            #[cfg(target_arch = "x86_64")]
            Some("avx512") if has_avx512_x86_64() => Some(Backend::Avx512X86_64),
            #[cfg(target_arch = "x86_64")]
            Some("avx2") if has_avx2_x86_64() => Some(Backend::Avx2X86_64),
            _ => None,
        }
    })
}

#[cfg(test)]
pub(crate) fn supported_backends() -> Vec<Backend> {
    let mut backends = vec![Backend::Scalar];
    #[cfg(target_arch = "aarch64")]
    if has_neon_aarch64() {
        backends.push(Backend::NeonAarch64);
    }
    #[cfg(target_arch = "x86_64")]
    if has_avx512_gfni_x86_64() {
        backends.push(Backend::Avx512GfniX86_64);
    }
    #[cfg(target_arch = "x86_64")]
    if has_avx512_x86_64() {
        backends.push(Backend::Avx512X86_64);
    }
    #[cfg(target_arch = "x86_64")]
    if has_avx2_x86_64() {
        backends.push(Backend::Avx2X86_64);
    }
    backends
}

/// Pre-computed erasure coding context. Build once; reuse across encode/reconstruct calls.
///
/// Holds the Cauchy encoding matrix and per-coefficient multiply tables for the
/// parity rows.
pub struct ErasureCodec {
    config: EcConfig,
    /// Full (k+m)×k systematic encoding matrix, row-major.
    encode_matrix: Vec<u8>,
    /// Per-coefficient multiply tables for parity rows: 256 * k * m bytes.
    encode_tables: Vec<u8>,
    #[cfg(target_arch = "aarch64")]
    /// Per-coefficient 32-byte nibble tables for NEON parity rows.
    encode_tables_neon: Vec<u8>,
    #[cfg(target_arch = "x86_64")]
    /// Per-coefficient 32-byte nibble tables for x86 shuffle backends.
    encode_tables_x86: Vec<u8>,
    #[cfg(target_arch = "x86_64")]
    /// Per-coefficient 64-bit affine matrices for x86 GFNI backends.
    encode_tables_x86_gfni: Vec<u64>,
}

impl ErasureCodec {
    /// Build a codec for the given config. Pre-computes encoding matrix and multiply tables.
    ///
    /// ZONE_INIT: allocates.
    pub fn new(config: EcConfig) -> Result<Self, EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::new",
            "k={} m={}",
            config.data_shards,
            config.parity_shards
        );
        let k = config.data_shards as usize;
        let m = config.parity_shards as usize;
        let total = k + m;

        let mut encode_matrix = vec![0u8; total * k];
        gen_cauchy1_matrix(&mut encode_matrix, total, k);

        let encode_tables = build_mul_tables(&encode_matrix[k * k..]);
        #[cfg(target_arch = "aarch64")]
        let encode_tables_neon = crate::gf::build_nibble_tables(&encode_matrix[k * k..]);
        #[cfg(target_arch = "x86_64")]
        let encode_tables_x86 = crate::gf::build_nibble_tables(&encode_matrix[k * k..]);
        #[cfg(target_arch = "x86_64")]
        let encode_tables_x86_gfni = build_gfni_tables(&encode_matrix[k * k..]);

        Ok(Self {
            config,
            encode_matrix,
            encode_tables,
            #[cfg(target_arch = "aarch64")]
            encode_tables_neon,
            #[cfg(target_arch = "x86_64")]
            encode_tables_x86,
            #[cfg(target_arch = "x86_64")]
            encode_tables_x86_gfni,
        })
    }

    /// Returns the config this codec was built for.
    pub fn config(&self) -> EcConfig {
        self.config
    }

    /// Required scratch buffer size (bytes) for `verify` at a given `shard_size`.
    pub fn verify_scratch_size(&self, shard_size: usize) -> usize {
        (self.config.parity_shards as usize).saturating_mul(shard_size)
    }

    /// Encode k data shards into m parity shards.
    ///
    /// ZONE_HOT: no heap allocation.
    pub fn encode(&self, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), EcError> {
        self.encode_with_backend(data, parity, selected_backend())
    }

    fn encode_with_backend(
        &self,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
        backend: Backend,
    ) -> Result<(), EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::encode",
            "data_shards={} parity_shards={}",
            data.len(),
            parity.len()
        );
        let k = self.config.data_shards as usize;
        let m = self.config.parity_shards as usize;

        if data.len() != k {
            return Err(EcError::ShardCount {
                expected: k,
                got: data.len(),
            });
        }
        if parity.len() != m {
            return Err(EcError::ShardCount {
                expected: m,
                got: parity.len(),
            });
        }

        let shard_size = data[0].len();
        check_shard_size(shard_size)?;
        for (i, s) in data.iter().enumerate().skip(1) {
            if s.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: i,
                    expected: shard_size,
                    got: s.len(),
                });
            }
        }
        for (i, s) in parity.iter().enumerate() {
            if s.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: k + i,
                    expected: shard_size,
                    got: s.len(),
                });
            }
        }

        if m == 0 || shard_size == 0 {
            return Ok(());
        }

        encode_rows(
            backend,
            k,
            &self.encode_tables,
            #[cfg(target_arch = "aarch64")]
            &self.encode_tables_neon,
            #[cfg(target_arch = "x86_64")]
            &self.encode_tables_x86,
            #[cfg(target_arch = "x86_64")]
            &self.encode_tables_x86_gfni,
            data,
            parity,
        );
        Ok(())
    }

    /// Verify that parity shards are consistent with data shards.
    ///
    /// Re-encodes `data` into `scratch` and compares against `parity`.
    ///
    /// ZONE_HOT: no heap allocation.
    pub fn verify(
        &self,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
    ) -> Result<VerifyResult, EcError> {
        self.verify_with_backend(data, parity, scratch, selected_backend())
    }

    fn verify_with_backend(
        &self,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
        backend: Backend,
    ) -> Result<VerifyResult, EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::verify",
            "data_shards={} parity_shards={} scratch={}",
            data.len(),
            parity.len(),
            scratch.len()
        );
        let k = self.config.data_shards as usize;
        let m = self.config.parity_shards as usize;

        if data.len() != k {
            return Err(EcError::ShardCount {
                expected: k,
                got: data.len(),
            });
        }
        if parity.len() != m {
            return Err(EcError::ShardCount {
                expected: m,
                got: parity.len(),
            });
        }

        let shard_size = data[0].len();
        check_shard_size(shard_size)?;
        for (i, s) in data.iter().enumerate().skip(1) {
            if s.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: i,
                    expected: shard_size,
                    got: s.len(),
                });
            }
        }
        for (i, s) in parity.iter().enumerate() {
            if s.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: k + i,
                    expected: shard_size,
                    got: s.len(),
                });
            }
        }

        let required = m.saturating_mul(shard_size);
        if scratch.len() < required {
            return Err(EcError::ScratchTooSmall {
                required,
                provided: scratch.len(),
            });
        }

        if m == 0 || shard_size == 0 {
            return Ok(VerifyResult::Ok);
        }

        for row in 0..m {
            {
                let scratch_row = &mut scratch[row * shard_size..(row + 1) * shard_size];
                let mut outputs = [scratch_row];
                let table_start = row * k * 256;
                let table_end = table_start + k * 256;
                #[cfg(target_arch = "x86_64")]
                let table_start_x86 = row * k * 32;
                #[cfg(target_arch = "x86_64")]
                let table_end_x86 = table_start_x86 + k * 32;
                #[cfg(target_arch = "x86_64")]
                let table_start_x86_gfni = row * k;
                #[cfg(target_arch = "x86_64")]
                let table_end_x86_gfni = table_start_x86_gfni + k;
                encode_rows(
                    backend,
                    k,
                    &self.encode_tables[table_start..table_end],
                    #[cfg(target_arch = "aarch64")]
                    &self.encode_tables_neon[row * k * 32..(row + 1) * k * 32],
                    #[cfg(target_arch = "x86_64")]
                    &self.encode_tables_x86[table_start_x86..table_end_x86],
                    #[cfg(target_arch = "x86_64")]
                    &self.encode_tables_x86_gfni[table_start_x86_gfni..table_end_x86_gfni],
                    data,
                    &mut outputs,
                );
            }
            let scratch_row = &scratch[row * shard_size..(row + 1) * shard_size];
            if scratch_row != parity[row] {
                return Ok(VerifyResult::Mismatch(k + row));
            }
        }

        Ok(VerifyResult::Ok)
    }

    /// Reconstruct missing shards from a subset of available shards.
    ///
    /// ZONE_HOT: no heap allocation. All scratch is stack-allocated and bounded
    /// by MAX_TOTAL_SHARDS.
    pub fn reconstruct(
        &self,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), EcError> {
        self.reconstruct_with_backend(
            present_indices,
            present_data,
            recover_indices,
            outputs,
            selected_backend(),
        )
    }

    fn reconstruct_with_backend(
        &self,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
        backend: Backend,
    ) -> Result<(), EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::reconstruct",
            "present={} recover={}",
            present_data.len(),
            outputs.len()
        );
        let k = self.config.data_shards as usize;
        let m = self.config.parity_shards as usize;
        let total = k + m;

        if present_indices.len() != present_data.len() {
            return Err(EcError::ShardCount {
                expected: present_indices.len(),
                got: present_data.len(),
            });
        }
        if recover_indices.len() != outputs.len() {
            return Err(EcError::ShardCount {
                expected: recover_indices.len(),
                got: outputs.len(),
            });
        }
        if present_indices.len() < k {
            return Err(EcError::InsufficientShards {
                need: k,
                have: present_indices.len(),
            });
        }

        for (i, &idx) in present_indices.iter().enumerate() {
            if idx >= total {
                return Err(EcError::ShardIndexOutOfRange { index: idx, total });
            }
            if i > 0 && idx <= present_indices[i - 1] {
                if idx == present_indices[i - 1] {
                    return Err(EcError::DuplicateShardIndex { index: idx });
                }
                return Err(EcError::UnsortedIndices { index: i });
            }
        }

        let mut present_set = 0u64;
        for &idx in present_indices {
            present_set |= 1u64 << idx;
        }
        let mut recover_set = 0u64;
        for &idx in recover_indices {
            if idx >= total {
                return Err(EcError::ShardIndexOutOfRange { index: idx, total });
            }
            if present_set & (1u64 << idx) != 0 {
                return Err(EcError::OverlappingIndices { index: idx });
            }
            if recover_set & (1u64 << idx) != 0 {
                return Err(EcError::DuplicateRecoverIndex { index: idx });
            }
            recover_set |= 1u64 << idx;
        }

        let shard_size = present_data[0].len();
        check_shard_size(shard_size)?;
        for (i, s) in present_data.iter().enumerate().skip(1) {
            if s.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: present_indices[i],
                    expected: shard_size,
                    got: s.len(),
                });
            }
        }
        for (i, s) in outputs.iter().enumerate() {
            if s.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: recover_indices[i],
                    expected: shard_size,
                    got: s.len(),
                });
            }
        }

        if recover_indices.is_empty() || shard_size == 0 {
            return Ok(());
        }

        reconstruct_shards(
            backend,
            k,
            &self.encode_matrix,
            present_indices,
            present_data,
            recover_indices,
            outputs,
        )
    }
}

#[cfg(test)]
impl ErasureCodec {
    pub(crate) fn encode_with_backend_for_test(
        &self,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
        backend: Backend,
    ) -> Result<(), EcError> {
        self.encode_with_backend(data, parity, backend)
    }

    pub(crate) fn verify_with_backend_for_test(
        &self,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
        backend: Backend,
    ) -> Result<VerifyResult, EcError> {
        self.verify_with_backend(data, parity, scratch, backend)
    }

    pub(crate) fn reconstruct_with_backend_for_test(
        &self,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
        backend: Backend,
    ) -> Result<(), EcError> {
        self.reconstruct_with_backend(
            present_indices,
            present_data,
            recover_indices,
            outputs,
            backend,
        )
    }
}

/// Runtime smoke test for erasure coding.
pub fn self_test() -> Result<(), EcError> {
    let config = EcConfig::new(4, 2)?;
    let codec = ErasureCodec::new(config)?;
    let k = config.data_shards as usize;
    let m = config.parity_shards as usize;
    let shard_size = 256usize;

    let data: Vec<Vec<u8>> = (0..k)
        .map(|i| {
            (0..shard_size)
                .map(|j| ((i * 37 + j * 13 + 7) & 0xFF) as u8)
                .collect()
        })
        .collect();

    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    codec.encode(&data_refs, &mut parity_refs)?;

    let mut scratch = vec![0u8; codec.verify_scratch_size(shard_size)];
    match codec.verify(
        &data_refs,
        &parity.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        &mut scratch,
    )? {
        VerifyResult::Ok => {}
        VerifyResult::Mismatch(idx) => {
            return Err(EcError::SmokeTestFailed {
                reason: format!("verify mismatch at shard index {}", idx),
            });
        }
    }

    let present_indices: Vec<usize> = vec![0, 2, 3, k];
    let present_data: Vec<&[u8]> = vec![
        data[0].as_slice(),
        data[2].as_slice(),
        data[3].as_slice(),
        parity[0].as_slice(),
    ];
    let recover_indices: Vec<usize> = vec![1, k + 1];
    let mut recovered_data = vec![vec![0u8; shard_size]; recover_indices.len()];
    let mut recovered_refs: Vec<&mut [u8]> = recovered_data
        .iter_mut()
        .map(|v| v.as_mut_slice())
        .collect();

    codec.reconstruct(
        &present_indices,
        &present_data,
        &recover_indices,
        &mut recovered_refs,
    )?;

    if recovered_data[0] != data[1] {
        return Err(EcError::SmokeTestFailed {
            reason: "reconstruction mismatch for missing data shard".to_string(),
        });
    }
    if recovered_data[1] != parity[1] {
        return Err(EcError::SmokeTestFailed {
            reason: "reconstruction mismatch for missing parity shard".to_string(),
        });
    }

    Ok(())
}
