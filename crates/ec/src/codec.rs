use crate::reconstruct::reconstruct_shards;

/// Maximum supported total shards (k + m).
pub const MAX_TOTAL_SHARDS: usize = 32;

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

// GF table size: 32 bytes per (source, output) pair.
const GF_TABLE_ENTRY: usize = 32;

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
        if data_shards == 0 {
            return Err(EcError::InvalidConfig { reason: "data_shards must be >= 1" });
        }
        if parity_shards == 0 {
            return Err(EcError::InvalidConfig { reason: "parity_shards must be >= 1" });
        }
        let total = data_shards as usize + parity_shards as usize;
        if total > MAX_TOTAL_SHARDS {
            return Err(EcError::InvalidConfig {
                reason: "data_shards + parity_shards exceeds MAX_TOTAL_SHARDS",
            });
        }
        Ok(Self { data_shards, parity_shards })
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
    ShardSizeMismatch { index: usize, expected: usize, got: usize },

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
}

/// Pre-computed erasure coding context. Build once; reuse across encode/reconstruct calls.
///
/// Holds the Cauchy encoding matrix and the pre-computed GF multiplication tables
/// for encoding. Reconstruction computes its own per-call tables on the stack.
pub struct ErasureCodec {
    config: EcConfig,
    /// Cauchy encoding matrix, m × k bytes (parity rows only; data rows are identity).
    encode_matrix: Vec<u8>,
    /// Pre-computed GF tables for encoding: 32 * k * m bytes.
    encode_tables: Vec<u8>,
}

impl ErasureCodec {
    /// Build a codec for the given config. Pre-computes encoding matrix and GF tables.
    ///
    /// ZONE_INIT: allocates.
    pub fn new(config: EcConfig) -> Result<Self, EcError> {
        let k = config.data_shards as usize;
        let m = config.parity_shards as usize;
        let total = k + m;

        // gf_gen_cauchy1_matrix(a, total, k) writes the full (k+m)×k systematic
        // encoding matrix: the first k rows are the k×k identity (data shards),
        // and the last m rows are the Cauchy parity coefficients.
        let mut encode_matrix = vec![0u8; total * k];
        let mut encode_tables = vec![0u8; GF_TABLE_ENTRY * k * m];

        unsafe {
            ec_sys::gf_gen_cauchy1_matrix(
                encode_matrix.as_mut_ptr(),
                total as i32,
                k as i32,
            );
            // Parity rows start at offset k*k within encode_matrix.
            ec_sys::ec_init_tables(
                k as i32,
                m as i32,
                encode_matrix[k * k..].as_mut_ptr(),
                encode_tables.as_mut_ptr(),
            );
        }

        Ok(Self { config, encode_matrix, encode_tables })
    }

    /// Returns the config this codec was built for.
    pub fn config(&self) -> EcConfig {
        self.config
    }

    /// Required scratch buffer size (bytes) for `verify` at a given `shard_size`.
    ///
    /// Allocate once and reuse across many `verify` calls:
    /// ```ignore
    /// let mut scratch = vec![0u8; codec.verify_scratch_size(shard_size)];
    /// codec.verify(&data, &parity, &mut scratch)?;
    /// ```
    ///
    /// `shard_size` must satisfy the same `<= i32::MAX` constraint as `verify`.
    /// If the product `m * shard_size` would overflow `usize`, this returns
    /// `usize::MAX`; the subsequent `verify` call will return `ScratchTooSmall`
    /// since no real allocation can satisfy that requirement.
    pub fn verify_scratch_size(&self, shard_size: usize) -> usize {
        (self.config.parity_shards as usize).saturating_mul(shard_size)
    }

// ── Hot path ──────────────────────────────────────────────────────────────

    /// Encode k data shards into m parity shards.
    ///
    /// `data`:   exactly k slices, all of equal length (`shard_size`; may be 0).
    /// `parity`: exactly m mutable slices, each of length `shard_size`.
    ///
    /// **No aliasing**: `data` and `parity` buffers must not overlap. ISA-L reads
    /// from `data` and writes to `parity` concurrently; aliased buffers produce
    /// undefined results.
    ///
    /// ZONE_HOT: no heap allocation.
    pub fn encode(&self, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), EcError> {
        let k = self.config.data_shards as usize;
        let m = self.config.parity_shards as usize;

        if data.len() != k {
            return Err(EcError::ShardCount { expected: k, got: data.len() });
        }
        if parity.len() != m {
            return Err(EcError::ShardCount { expected: m, got: parity.len() });
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

        if shard_size == 0 {
            return Ok(());
        }

        // Build pointer arrays on the stack (k + m ≤ 32 pointers).
        let mut data_ptrs: [*mut u8; MAX_TOTAL_SHARDS] = [std::ptr::null_mut(); MAX_TOTAL_SHARDS];
        let mut parity_ptrs: [*mut u8; MAX_TOTAL_SHARDS] =
            [std::ptr::null_mut(); MAX_TOTAL_SHARDS];

        for (i, s) in data.iter().enumerate() {
            data_ptrs[i] = s.as_ptr() as *mut u8;
        }
        for (i, s) in parity.iter_mut().enumerate() {
            parity_ptrs[i] = s.as_mut_ptr();
        }

        unsafe {
            ec_sys::ec_encode_data(
                shard_size as i32,
                k as i32,
                m as i32,
                self.encode_tables.as_ptr() as *mut u8,
                data_ptrs.as_mut_ptr(),
                parity_ptrs.as_mut_ptr(),
            );
        }

        Ok(())
    }

    /// Verify that parity shards are consistent with data shards.
    ///
    /// Re-encodes `data` into `scratch` and compares against `parity`.
    /// Returns `VerifyResult::Mismatch(idx)` with the (k+i) index of the first
    /// mismatching parity shard.
    ///
    /// `scratch` must be at least `m * shard_size` bytes. The caller allocates
    /// this once and reuses it across many verify calls (e.g. a scrub pass).
    /// Returns `Err(EcError::ScratchTooSmall)` if `scratch` is undersized.
    ///
    /// **No aliasing**: `data`, `parity`, and `scratch` buffers must not overlap.
    /// ISA-L reads from `data` and writes to `scratch`; aliased buffers produce
    /// undefined results.
    ///
    /// ZONE_HOT: no heap allocation.
    pub fn verify(
        &self,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
    ) -> Result<VerifyResult, EcError> {
        let k = self.config.data_shards as usize;
        let m = self.config.parity_shards as usize;

        if data.len() != k {
            return Err(EcError::ShardCount { expected: k, got: data.len() });
        }
        if parity.len() != m {
            return Err(EcError::ShardCount { expected: m, got: parity.len() });
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

        // saturating_mul: if m * shard_size overflows usize (only possible on
        // 32-bit targets since shard_size <= i32::MAX was checked above), the
        // saturated value exceeds any real allocation so ScratchTooSmall is returned.
        let required = m.saturating_mul(shard_size);
        if scratch.len() < required {
            return Err(EcError::ScratchTooSmall { required, provided: scratch.len() });
        }

        if shard_size == 0 {
            return Ok(VerifyResult::Ok);
        }

        // Build pointer arrays into scratch. We use raw pointers to avoid the
        // borrow-checker limitation of taking multiple &mut sub-slices at once.
        let mut tmp_parity_ptrs: [*mut u8; MAX_TOTAL_SHARDS] =
            [std::ptr::null_mut(); MAX_TOTAL_SHARDS];
        for i in 0..m {
            // Safety: each pointer points to a distinct, non-overlapping shard_size
            // region within `scratch`, which is valid for the duration of this scope.
            tmp_parity_ptrs[i] = unsafe { scratch.as_mut_ptr().add(i * shard_size) };
        }

        let mut data_ptrs: [*mut u8; MAX_TOTAL_SHARDS] = [std::ptr::null_mut(); MAX_TOTAL_SHARDS];
        for (i, s) in data.iter().enumerate() {
            // ISA-L only reads from data pointers during encode.
            data_ptrs[i] = s.as_ptr() as *mut u8;
        }

        unsafe {
            ec_sys::ec_encode_data(
                shard_size as i32,
                k as i32,
                m as i32,
                self.encode_tables.as_ptr() as *mut u8,
                data_ptrs.as_mut_ptr(),
                tmp_parity_ptrs.as_mut_ptr(),
            );
        }

        for i in 0..m {
            // Safety: tmp_parity_ptrs[i] points to shard_size bytes within `scratch`.
            let computed =
                unsafe { std::slice::from_raw_parts(tmp_parity_ptrs[i], shard_size) };
            if computed != parity[i] {
                return Ok(VerifyResult::Mismatch(k + i));
            }
        }

        Ok(VerifyResult::Ok)
    }

    /// Reconstruct missing shards from a subset of available shards.
    ///
    /// `present_indices`: sorted, deduplicated shard indices (0..k+m); length >= k.
    /// `present_data`:    one slice per entry in `present_indices`, all of length `shard_size`.
    /// `recover_indices`: shard indices to reconstruct; must not overlap `present_indices`
    ///                    and must not contain duplicates.
    /// `outputs`:         one `&mut [u8]` per entry in `recover_indices`, each `shard_size` bytes.
    ///
    /// If `present_indices.len() > k`, only the first `k` entries are used for the
    /// matrix inversion; the remaining entries are ignored. Callers may pass all
    /// available shards without trimming — the codec selects the first `k`.
    ///
    /// **No aliasing**: `present_data` and `outputs` buffers must not overlap. ISA-L
    /// reads from `present_data` and writes to `outputs`; aliased buffers produce
    /// undefined results.
    ///
    /// ZONE_HOT: no heap allocation. All scratch (matrices, GF tables, pointer arrays)
    /// is stack-allocated and bounded by MAX_TOTAL_SHARDS.
    pub fn reconstruct(
        &self,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), EcError> {
        let k = self.config.data_shards as usize;
        let m = self.config.parity_shards as usize;
        let total = k + m;

        // ── Validate lengths ──────────────────────────────────────────────
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

        // ── Validate and check present_indices ────────────────────────────
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

        // ── Validate recover_indices ──────────────────────────────────────
        // Use a bitset (u64 fits MAX_TOTAL_SHARDS=32, but use two u32s for clarity).
        let mut present_set = 0u64;
        for &idx in present_indices {
            present_set |= 1u64 << idx;
        }
        let mut recover_set = 0u64;
        for &idx in recover_indices.iter() {
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

        // ── Validate shard sizes ──────────────────────────────────────────
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

        // ── Delegate to reconstruction logic ─────────────────────────────
        reconstruct_shards(
            k,
            m,
            &self.encode_matrix,
            present_indices,
            present_data,
            recover_indices,
            outputs,
            shard_size,
        )
    }
}
