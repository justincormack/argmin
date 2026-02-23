use crate::{EcConfig, EcError, ErasureCodec, VerifyResult, MAX_TOTAL_SHARDS};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a codec and a set of random-looking (but deterministic) data shards.
fn make_data(k: usize, shard_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..shard_size)
                .map(|j| ((i * 37 + j * 13 + 7) & 0xFF) as u8)
                .collect()
        })
        .collect()
}

/// Encode data → parity, return (data, parity) as owned vecs.
fn encode(codec: &ErasureCodec, data: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let m = codec.config().parity_shards as usize;
    let shard_size = data[0].len();
    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    codec.encode(&data_refs, &mut parity_refs).unwrap();
    parity
}

// ── Config validation ─────────────────────────────────────────────────────────

#[test]
fn config_valid_cases() {
    assert!(EcConfig::new(1, 1).is_ok());
    assert!(EcConfig::new(4, 2).is_ok());
    assert!(EcConfig::new(16, 8).is_ok());
    assert!(EcConfig::new(24, 8).is_ok()); // total = 32 = MAX
}

#[test]
fn config_zero_data_shards() {
    assert_eq!(
        EcConfig::new(0, 2),
        Err(EcError::InvalidConfig { reason: "data_shards must be >= 1" })
    );
}

#[test]
fn config_zero_parity_shards() {
    assert_eq!(
        EcConfig::new(2, 0),
        Err(EcError::InvalidConfig { reason: "parity_shards must be >= 1" })
    );
}

#[test]
fn config_exceeds_max() {
    // 25 + 8 = 33 > 32
    assert!(matches!(
        EcConfig::new(25, 8),
        Err(EcError::InvalidConfig { .. })
    ));
}

#[test]
fn config_total_and_overhead() {
    let c = EcConfig::new(4, 2).unwrap();
    assert_eq!(c.total_shards(), 6);
    assert!((c.overhead() - 1.5).abs() < f64::EPSILON);
}

// ── Encode correctness ────────────────────────────────────────────────────────

#[test]
fn encode_deterministic() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let p1 = encode(&codec, &data);
    let p2 = encode(&codec, &data);
    assert_eq!(p1, p2);
}

#[test]
fn encode_all_zeros_gives_zero_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; 512]).collect();
    let parity = encode(&codec, &data);
    for p in &parity {
        assert!(p.iter().all(|&b| b == 0), "expected zero parity for zero data");
    }
}

#[test]
fn encode_wrong_data_shard_count() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(3, 64); // should be 4
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = (0..2).map(|_| vec![0u8; 64]).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert_eq!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardCount { expected: 4, got: 3 })
    );
}

#[test]
fn encode_wrong_parity_shard_count() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = (0..1).map(|_| vec![0u8; 64]).collect(); // should be 2
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert_eq!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardCount { expected: 2, got: 1 })
    );
}

#[test]
fn encode_mismatched_shard_sizes() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let mut data = make_data(4, 64);
    data[2] = vec![0u8; 32]; // wrong size
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = (0..2).map(|_| vec![0u8; 64]).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert!(matches!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardSizeMismatch { index: 2, .. })
    ));
}

// ── Zero-length shards ────────────────────────────────────────────────────────

#[test]
fn zero_length_encode_is_noop() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data: Vec<Vec<u8>> = (0..4).map(|_| vec![]).collect();
    let mut parity: Vec<Vec<u8>> = (0..2).map(|_| vec![]).collect();
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert!(codec.encode(&data_refs, &mut parity_refs).is_ok());
}

#[test]
fn zero_length_verify_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data: Vec<Vec<u8>> = (0..4).map(|_| vec![]).collect();
    let parity: Vec<Vec<u8>> = (0..2).map(|_| vec![]).collect();
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![]; // shard_size=0, scratch can be empty
    assert_eq!(codec.verify(&data_refs, &parity_refs, &mut scratch).unwrap(), VerifyResult::Ok);
}

#[test]
fn zero_length_reconstruct_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    // present_indices covers all k data shards; recover parity shards (no-op at len 0)
    let present: Vec<Vec<u8>> = (0..4).map(|_| vec![]).collect();
    let present_refs: Vec<&[u8]> = present.iter().map(|v| v.as_slice()).collect();
    let mut out0: Vec<u8> = vec![];
    let mut out1: Vec<u8> = vec![];
    let mut outputs: Vec<&mut [u8]> = vec![out0.as_mut_slice(), out1.as_mut_slice()];
    assert!(codec
        .reconstruct(&[0, 1, 2, 3], &present_refs, &[4, 5], &mut outputs)
        .is_ok());
}

#[test]
fn zero_length_full_roundtrip() {
    // Use (2,2): can reconstruct 2 missing shards from the other 2.
    let codec = ErasureCodec::new(EcConfig::new(2, 2).unwrap()).unwrap();
    let data: Vec<Vec<u8>> = (0..2).map(|_| vec![]).collect();
    let parity = encode(&codec, &data);
    // Reconstruct data from the 2 parity shards (present=[2,3], recover=[0,1]).
    let present: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut out0: Vec<u8> = vec![];
    let mut out1: Vec<u8> = vec![];
    let mut outputs: Vec<&mut [u8]> = vec![out0.as_mut_slice(), out1.as_mut_slice()];
    assert!(codec.reconstruct(&[2, 3], &present, &[0, 1], &mut outputs).is_ok());
    // Both reconstructed shards should be empty.
    assert_eq!(out0, data[0]);
    assert_eq!(out1, data[1]);
}

// ── Verify correctness ────────────────────────────────────────────────────────

#[test]
fn verify_after_encode_passes() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let parity = encode(&codec, &data);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 1024]; // m * shard_size
    assert_eq!(codec.verify(&data_refs, &parity_refs, &mut scratch).unwrap(), VerifyResult::Ok);
}

#[test]
fn verify_detects_corrupt_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let mut parity = encode(&codec, &data);
    parity[0][0] ^= 0xFF; // corrupt first parity shard
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 1024];
    // parity shard 0 has index k+0 = 4
    assert_eq!(codec.verify(&data_refs, &parity_refs, &mut scratch).unwrap(), VerifyResult::Mismatch(4));
}

#[test]
fn verify_detects_corrupt_second_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let mut parity = encode(&codec, &data);
    parity[1][100] ^= 1; // corrupt second parity shard
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 1024];
    assert_eq!(codec.verify(&data_refs, &parity_refs, &mut scratch).unwrap(), VerifyResult::Mismatch(5));
}

#[test]
fn verify_after_restore_passes() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 256);
    let mut parity = encode(&codec, &data);
    let original_byte = parity[0][0];
    parity[0][0] ^= 0xFF;
    parity[0][0] = original_byte; // restore
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 256];
    assert_eq!(codec.verify(&data_refs, &parity_refs, &mut scratch).unwrap(), VerifyResult::Ok);
}

#[test]
fn verify_scratch_too_small() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 256);
    let parity = encode(&codec, &data);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 256 - 1]; // one byte short
    assert!(matches!(
        codec.verify(&data_refs, &parity_refs, &mut scratch),
        Err(EcError::ScratchTooSmall { required: 512, provided: 511 })
    ));
}

#[test]
fn verify_scratch_can_be_reused() {
    // Allocate once, call verify many times — as a scrub pass would do.
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let shard_size = 4096;
    let mut scratch = vec![0u8; codec.verify_scratch_size(shard_size)];
    for _ in 0..10 {
        let data = make_data(4, shard_size);
        let parity = encode(&codec, &data);
        let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
        assert_eq!(codec.verify(&data_refs, &parity_refs, &mut scratch).unwrap(), VerifyResult::Ok);
    }
}

// ── Reconstruct: exhaustive subset enumeration ────────────────────────────────

/// All combinations of `k` items chosen from `0..n`.
fn combinations(n: usize, k: usize) -> Vec<Vec<usize>> {
    let mut result = Vec::new();
    let mut combo = Vec::new();
    combinations_helper(0, n, k, &mut combo, &mut result);
    result
}

fn combinations_helper(
    start: usize,
    n: usize,
    k: usize,
    combo: &mut Vec<usize>,
    result: &mut Vec<Vec<usize>>,
) {
    if combo.len() == k {
        result.push(combo.clone());
        return;
    }
    for i in start..n {
        combo.push(i);
        combinations_helper(i + 1, n, k, combo, result);
        combo.pop();
    }
}

/// For a given config, verify that every combination of k present shards reconstructs
/// all missing shards correctly.
fn exhaustive_reconstruct(data_shards: u8, parity_shards: u8, shard_size: usize) {
    let config = EcConfig::new(data_shards, parity_shards).unwrap();
    let codec = ErasureCodec::new(config).unwrap();
    let k = data_shards as usize;
    let m = parity_shards as usize;
    let total = k + m;

    let data = make_data(k, shard_size);
    let parity = encode(&codec, &data);

    // All shards: 0..k are data, k..k+m are parity.
    let all_shards: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();

    for present_combo in combinations(total, k) {
        let recover: Vec<usize> =
            (0..total).filter(|i| !present_combo.contains(i)).collect();

        let present_data: Vec<&[u8]> =
            present_combo.iter().map(|&i| all_shards[i].as_slice()).collect();
        let mut output_storage: Vec<Vec<u8>> =
            recover.iter().map(|_| vec![0u8; shard_size]).collect();
        let mut outputs: Vec<&mut [u8]> =
            output_storage.iter_mut().map(|v| v.as_mut_slice()).collect();

        codec
            .reconstruct(&present_combo, &present_data, &recover, &mut outputs)
            .unwrap_or_else(|e| {
                panic!(
                    "reconstruct failed for config ({},{}) present={:?} recover={:?}: {}",
                    k, m, present_combo, recover, e
                )
            });

        for (out_idx, &shard_idx) in recover.iter().enumerate() {
            assert_eq!(
                output_storage[out_idx],
                all_shards[shard_idx],
                "config ({},{}) shard {} mismatch, present={:?}",
                k,
                m,
                shard_idx,
                present_combo
            );
        }
    }
}

#[test]
fn reconstruct_exhaustive_2_1() {
    exhaustive_reconstruct(2, 1, 256);
}

#[test]
fn reconstruct_exhaustive_3_2() {
    exhaustive_reconstruct(3, 2, 256);
}

#[test]
fn reconstruct_exhaustive_4_2() {
    exhaustive_reconstruct(4, 2, 256);
}

#[test]
fn reconstruct_exhaustive_6_3() {
    exhaustive_reconstruct(6, 3, 64);
}

#[test]
fn reconstruct_exhaustive_8_4() {
    exhaustive_reconstruct(8, 4, 32);
}

// ── Reconstruct: boundary conditions ─────────────────────────────────────────

#[test]
fn reconstruct_recover_only_parity() {
    // All data present, recover parity — verifies parity recovery path.
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 512);
    let parity = encode(&codec, &data);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut out0 = vec![0u8; 512];
    let mut out1 = vec![0u8; 512];
    let mut outputs: Vec<&mut [u8]> = vec![out0.as_mut_slice(), out1.as_mut_slice()];
    codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[4, 5], &mut outputs).unwrap();
    assert_eq!(out0, parity[0]);
    assert_eq!(out1, parity[1]);
}

#[test]
fn reconstruct_empty_recover_indices_is_noop() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 128);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = vec![];
    assert!(codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[], &mut outputs).is_ok());
}

#[test]
fn reconstruct_more_than_k_present_uses_first_k() {
    // If k+1 shards are present, reconstruction still succeeds using any k.
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 256);
    let parity = encode(&codec, &data);
    // Present: shards 0,1,2,3,4 (5 shards, only need 4)
    let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();
    let present_refs: Vec<&[u8]> = vec![
        all[0].as_slice(),
        all[1].as_slice(),
        all[2].as_slice(),
        all[3].as_slice(),
        all[4].as_slice(),
    ];
    let mut out5 = vec![0u8; 256];
    let mut outputs: Vec<&mut [u8]> = vec![out5.as_mut_slice()];
    codec.reconstruct(&[0, 1, 2, 3, 4], &present_refs, &[5], &mut outputs).unwrap();
    assert_eq!(out5, parity[1]);
}

// ── Reconstruct: error conditions ─────────────────────────────────────────────

#[test]
fn reconstruct_insufficient_shards() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(3, 64); // only 3, need 4
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = vec![];
    assert_eq!(
        codec.reconstruct(&[0, 1, 2], &data_refs, &[], &mut outputs),
        Err(EcError::InsufficientShards { need: 4, have: 3 })
    );
}

#[test]
fn reconstruct_index_out_of_range() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut out = vec![0u8; 64];
    let mut outputs: Vec<&mut [u8]> = vec![out.as_mut_slice()];
    // shard index 6 is out of range for (4,2) which has total=6, valid 0..5
    assert!(matches!(
        codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[6], &mut outputs),
        Err(EcError::ShardIndexOutOfRange { index: 6, total: 6 })
    ));
}

#[test]
fn reconstruct_duplicate_present_index() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = vec![];
    assert!(matches!(
        codec.reconstruct(&[0, 1, 2, 2], &data_refs, &[], &mut outputs),
        Err(EcError::DuplicateShardIndex { index: 2 })
    ));
}

#[test]
fn reconstruct_unsorted_present_indices() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = vec![];
    assert!(matches!(
        codec.reconstruct(&[0, 2, 1, 3], &data_refs, &[], &mut outputs),
        Err(EcError::UnsortedIndices { .. })
    ));
}

#[test]
fn reconstruct_overlapping_indices() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut out = vec![0u8; 64];
    let mut outputs: Vec<&mut [u8]> = vec![out.as_mut_slice()];
    // shard 3 appears in both present and recover
    assert!(matches!(
        codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[3], &mut outputs),
        Err(EcError::OverlappingIndices { index: 3 })
    ));
}

#[test]
fn reconstruct_present_data_length_mismatch() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(3, 64); // 3 slices for 4 indices
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = vec![];
    assert!(matches!(
        codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[], &mut outputs),
        Err(EcError::ShardCount { expected: 4, got: 3 })
    ));
}

#[test]
fn reconstruct_outputs_length_mismatch() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut out = vec![0u8; 64];
    let mut outputs: Vec<&mut [u8]> = vec![out.as_mut_slice()]; // 1 output for 2 recover indices
    assert!(matches!(
        codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[4, 5], &mut outputs),
        Err(EcError::ShardCount { expected: 2, got: 1 })
    ));
}

// ── Single-byte shards (minimum non-zero size) ────────────────────────────────

#[test]
fn single_byte_shards_roundtrip() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1);
    let parity = encode(&codec, &data);
    // Drop shards 0 and 1; reconstruct from shards 2,3,4,5
    let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();
    let present_refs: Vec<&[u8]> =
        vec![all[2].as_slice(), all[3].as_slice(), all[4].as_slice(), all[5].as_slice()];
    let mut out0 = vec![0u8; 1];
    let mut out1 = vec![0u8; 1];
    let mut outputs: Vec<&mut [u8]> = vec![out0.as_mut_slice(), out1.as_mut_slice()];
    codec.reconstruct(&[2, 3, 4, 5], &present_refs, &[0, 1], &mut outputs).unwrap();
    assert_eq!(out0, data[0]);
    assert_eq!(out1, data[1]);
}

// ── Allocation counting (ZONE_HOT compliance) ─────────────────────────────────

#[cfg(test)]
mod alloc_tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    // Thread-local counters: only allocations on the current thread are counted.
    // This allows other test threads to allocate freely without affecting the count.
    thread_local! {
        static COUNTING: Cell<bool> = const { Cell::new(false) };
        static ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    }

    struct CountingAllocator;

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            COUNTING.with(|c| {
                if c.get() {
                    ALLOC_COUNT.with(|a| a.set(a.get() + 1));
                }
            });
            System.alloc(layout)
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout)
        }
    }

    #[global_allocator]
    static A: CountingAllocator = CountingAllocator;

    fn count_allocs<F: FnOnce()>(f: F) -> usize {
        ALLOC_COUNT.with(|a| a.set(0));
        COUNTING.with(|c| c.set(true));
        f();
        COUNTING.with(|c| c.set(false));
        ALLOC_COUNT.with(|a| a.get())
    }

    #[test]
    fn encode_zero_allocs_in_hot_path() {
        let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
        let data = make_data(4, 4096);
        let mut parity: Vec<Vec<u8>> = (0..2).map(|_| vec![0u8; 4096]).collect();

        let allocs = count_allocs(|| {
            let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let mut parity_refs: Vec<&mut [u8]> =
                parity.iter_mut().map(|v| v.as_mut_slice()).collect();
            codec.encode(&data_refs, &mut parity_refs).unwrap();
        });

        // Vec for data_refs and parity_refs are allowed (caller infrastructure, not codec internals).
        // The codec itself must not allocate. We check that the number is bounded and small.
        // (Two Vec::new() calls = 2 allocs for the ref slices above, both by caller.)
        assert_eq!(allocs, 2, "codec encode should not allocate beyond caller ref vecs");
    }

    #[test]
    fn reconstruct_zero_allocs_in_hot_path() {
        let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
        let data = make_data(4, 4096);
        let parity = encode(&codec, &data);
        let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();
        let mut out4 = vec![0u8; 4096];
        let mut out5 = vec![0u8; 4096];

        let allocs = count_allocs(|| {
            let present_refs: Vec<&[u8]> =
                vec![all[0].as_slice(), all[1].as_slice(), all[2].as_slice(), all[3].as_slice()];
            let mut outputs: Vec<&mut [u8]> = vec![out4.as_mut_slice(), out5.as_mut_slice()];
            codec.reconstruct(&[0, 1, 2, 3], &present_refs, &[4, 5], &mut outputs).unwrap();
        });

        assert_eq!(allocs, 2, "codec reconstruct should not allocate beyond caller ref vecs");
    }
}

// ── Property-based tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// For any valid (k, m, shard_size, data), encoding then reconstructing
        /// from any k of k+m shards recovers the original data exactly.
        #[test]
        fn roundtrip_any_k_shards(
            k in 1usize..=6,
            m in 1usize..=3,
            shard_size in 0usize..=512,
            seed in 0u64..=u64::MAX,
        ) {
            prop_assume!(k + m <= MAX_TOTAL_SHARDS);

            let config = EcConfig::new(k as u8, m as u8).unwrap();
            let codec = ErasureCodec::new(config).unwrap();

            // Generate deterministic data from seed.
            let data: Vec<Vec<u8>> = (0..k)
                .map(|i| {
                    (0..shard_size)
                        .map(|j| {
                            let x = seed.wrapping_add(i as u64 * 1000 + j as u64);
                            (x ^ (x >> 32)) as u8
                        })
                        .collect()
                })
                .collect();

            let parity = encode(&codec, &data);
            let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();

            // Test a representative subset of combinations (not all C(k+m,k) to keep runtime bounded).
            // Use the seed to pick a few random subsets.
            let total = k + m;
            let mut rng_state = seed;
            let trials = 5.min(total);
            for _ in 0..trials {
                // Fisher-Yates shuffle of 0..total using rng_state
                let mut indices: Vec<usize> = (0..total).collect();
                for i in (1..total).rev() {
                    rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    let j = (rng_state >> 33) as usize % (i + 1);
                    indices.swap(i, j);
                }
                let mut present_combo: Vec<usize> = indices[..k].to_vec();
                present_combo.sort_unstable();
                let recover: Vec<usize> = (0..total)
                    .filter(|i| !present_combo.contains(i))
                    .collect();

                if recover.is_empty() {
                    continue;
                }

                let present_refs: Vec<&[u8]> =
                    present_combo.iter().map(|&i| all[i].as_slice()).collect();
                let mut output_storage: Vec<Vec<u8>> =
                    recover.iter().map(|_| vec![0u8; shard_size]).collect();
                let mut outputs: Vec<&mut [u8]> =
                    output_storage.iter_mut().map(|v| v.as_mut_slice()).collect();

                codec
                    .reconstruct(&present_combo, &present_refs, &recover, &mut outputs)
                    .unwrap();

                for (out_idx, &shard_idx) in recover.iter().enumerate() {
                    prop_assert_eq!(
                        &output_storage[out_idx],
                        &all[shard_idx],
                        "shard {} mismatch, config ({},{}), present={:?}",
                        shard_idx, k, m, present_combo
                    );
                }
            }
        }
    }
}
