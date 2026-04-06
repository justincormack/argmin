const TRACE_TARGET: &str = "ec";

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

/// Parameters for the null backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcConfig {
    /// k: number of data shards.
    pub data_shards: u8,
    /// m: number of parity shards. The null backend only supports zero.
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
        if parity_shards != 0 {
            return Err(EcError::InvalidConfig {
                reason: "null backend only supports parity_shards = 0",
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

    /// Storage overhead multiplier, always 1.0 for the null backend.
    pub fn overhead(self) -> f64 {
        1.0
    }
}

impl Default for EcConfig {
    fn default() -> Self {
        Self {
            data_shards: 4,
            parity_shards: 0,
        }
    }
}

/// Result of a parity verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    /// All parity shards matched.
    Ok,
    /// Included for API compatibility with the real backend.
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

/// Null EC context. It validates inputs but never produces parity.
pub struct ErasureCodec {
    config: EcConfig,
}

impl ErasureCodec {
    pub fn new(config: EcConfig) -> Result<Self, EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::new",
            "k={} m={}",
            config.data_shards,
            config.parity_shards
        );
        Ok(Self { config })
    }

    pub fn config(&self) -> EcConfig {
        self.config
    }

    pub fn verify_scratch_size(&self, shard_size: usize) -> usize {
        let _ = shard_size;
        0
    }

    pub fn encode(&self, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::encode",
            "data_shards={} parity_shards={}",
            data.len(),
            parity.len()
        );
        validate_data_and_parity(self.config.data_shards as usize, data, parity)
    }

    pub fn verify(
        &self,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
    ) -> Result<VerifyResult, EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::verify",
            "data_shards={} parity_shards={} scratch={}",
            data.len(),
            parity.len(),
            scratch.len()
        );
        validate_data_and_parity_const(self.config.data_shards as usize, data, parity)?;
        Ok(VerifyResult::Ok)
    }

    pub fn reconstruct(
        &self,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), EcError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "ErasureCodec::reconstruct",
            "present={} recover={}",
            present_data.len(),
            outputs.len()
        );
        let k = self.config.data_shards as usize;
        let total = k;

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
        for (i, shard) in present_data.iter().enumerate().skip(1) {
            if shard.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: present_indices[i],
                    expected: shard_size,
                    got: shard.len(),
                });
            }
        }
        for (i, shard) in outputs.iter().enumerate() {
            if shard.len() != shard_size {
                return Err(EcError::ShardSizeMismatch {
                    index: recover_indices[i],
                    expected: shard_size,
                    got: shard.len(),
                });
            }
        }

        if recover_indices.is_empty() {
            return Ok(());
        }

        Err(EcError::InsufficientShards {
            need: k,
            have: present_indices.len(),
        })
    }
}

fn validate_data_and_parity(
    k: usize,
    data: &[&[u8]],
    parity: &mut [&mut [u8]],
) -> Result<(), EcError> {
    if data.len() != k {
        return Err(EcError::ShardCount {
            expected: k,
            got: data.len(),
        });
    }
    if !parity.is_empty() {
        return Err(EcError::ShardCount {
            expected: 0,
            got: parity.len(),
        });
    }

    let shard_size = data[0].len();
    check_shard_size(shard_size)?;
    for (i, shard) in data.iter().enumerate().skip(1) {
        if shard.len() != shard_size {
            return Err(EcError::ShardSizeMismatch {
                index: i,
                expected: shard_size,
                got: shard.len(),
            });
        }
    }
    Ok(())
}

fn validate_data_and_parity_const(
    k: usize,
    data: &[&[u8]],
    parity: &[&[u8]],
) -> Result<(), EcError> {
    if data.len() != k {
        return Err(EcError::ShardCount {
            expected: k,
            got: data.len(),
        });
    }
    if !parity.is_empty() {
        return Err(EcError::ShardCount {
            expected: 0,
            got: parity.len(),
        });
    }

    let shard_size = data[0].len();
    check_shard_size(shard_size)?;
    for (i, shard) in data.iter().enumerate().skip(1) {
        if shard.len() != shard_size {
            return Err(EcError::ShardSizeMismatch {
                index: i,
                expected: shard_size,
                got: shard.len(),
            });
        }
    }
    Ok(())
}

pub fn self_test() -> Result<(), EcError> {
    let config = EcConfig::new(4, 0)?;
    let codec = ErasureCodec::new(config)?;
    let data: Vec<Vec<u8>> = (0..config.data_shards as usize)
        .map(|i| {
            (0..256usize)
                .map(|j| ((i * 37 + j * 13 + 7) & 0xFF) as u8)
                .collect()
        })
        .collect();
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity_refs: Vec<&mut [u8]> = Vec::new();
    codec.encode(&data_refs, &mut parity_refs)?;

    let mut scratch = Vec::new();
    match codec.verify(&data_refs, &[], &mut scratch)? {
        VerifyResult::Ok => {}
        VerifyResult::Mismatch(idx) => {
            return Err(EcError::SmokeTestFailed {
                reason: format!("verify mismatch at shard index {}", idx),
            });
        }
    }

    let mut outputs: Vec<&mut [u8]> = Vec::new();
    codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[], &mut outputs)?;
    Ok(())
}
