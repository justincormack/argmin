use crate::codec::{supported_backends, Backend};
use crate::self_test;
use crate::{EcConfig, EcError, ErasureCodec, VerifyResult, MAX_TOTAL_SHARDS};

// Helpers

fn make_data(k: usize, shard_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..shard_size)
                .map(|j| ((i * 37 + j * 13 + 7) & 0xFF) as u8)
                .collect()
        })
        .collect()
}

fn encode(codec: &ErasureCodec, data: &[Vec<u8>]) -> Vec<Vec<u8>> {
    encode_with_backend(codec, data, Backend::Scalar)
}

fn encode_with_backend(codec: &ErasureCodec, data: &[Vec<u8>], backend: Backend) -> Vec<Vec<u8>> {
    let m = codec.config().parity_shards as usize;
    let shard_size = data[0].len();
    let mut parity: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    codec
        .encode_with_backend_for_test(&data_refs, &mut parity_refs, backend)
        .unwrap();
    parity
}

fn backend_name(backend: Backend) -> &'static str {
    match backend {
        Backend::Scalar => "scalar",
        #[cfg(target_arch = "x86_64")]
        Backend::Avx2X86_64 => "x86_64-avx2",
    }
}

// Config validation

#[test]
fn config_valid_cases() {
    assert!(EcConfig::new(1, 0).is_ok());
    assert!(EcConfig::new(1, 1).is_ok());
    assert!(EcConfig::new(4, 2).is_ok());
    assert!(EcConfig::new(16, 8).is_ok());
    assert!(EcConfig::new(24, 8).is_ok());
}

#[test]
fn config_zero_data_shards() {
    assert_eq!(
        EcConfig::new(0, 2),
        Err(EcError::InvalidConfig {
            reason: "data_shards must be >= 1"
        })
    );
}

#[test]
fn config_exceeds_max() {
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

// Encode correctness

#[test]
fn encode_deterministic() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let p1 = encode(&codec, &data);
    let p2 = encode(&codec, &data);
    assert_eq!(p1, p2);
}

#[test]
fn supported_backends_match_scalar_encode_and_verify() {
    let codec = ErasureCodec::new(EcConfig::new(6, 3).unwrap()).unwrap();
    let shard_sizes = [
        0usize, 1, 2, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 1024, 4096,
    ];

    for &shard_size in &shard_sizes {
        let data = make_data(6, shard_size);
        let scalar_parity = encode_with_backend(&codec, &data, Backend::Scalar);
        let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        for backend in supported_backends() {
            let parity = encode_with_backend(&codec, &data, backend);
            assert_eq!(
                parity,
                scalar_parity,
                "backend={} shard_size={shard_size}",
                backend_name(backend)
            );

            let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
            let mut scratch = vec![0u8; codec.verify_scratch_size(shard_size)];
            assert_eq!(
                codec
                    .verify_with_backend_for_test(&data_refs, &parity_refs, &mut scratch, backend)
                    .unwrap(),
                VerifyResult::Ok,
                "backend={} shard_size={shard_size}",
                backend_name(backend)
            );
        }
    }
}

#[test]
fn supported_backends_match_scalar_reconstruct() {
    let codec = ErasureCodec::new(EcConfig::new(6, 3).unwrap()).unwrap();
    let shard_sizes = [
        0usize, 1, 2, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 1024, 2048,
    ];
    let cases: &[(&[usize], &[usize])] = &[
        (&[1, 2, 4, 6, 7, 8], &[0, 3, 5]),
        (&[0, 1, 2, 3, 5, 7], &[4, 6, 8]),
        (&[2, 3, 4, 5, 7, 8], &[0, 1, 6]),
    ];

    for &shard_size in &shard_sizes {
        let data = make_data(6, shard_size);
        let parity = encode_with_backend(&codec, &data, Backend::Scalar);
        let all_shards: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();

        for &(present_indices, recover_indices) in cases {
            let present_data: Vec<&[u8]> = present_indices
                .iter()
                .map(|&index| all_shards[index].as_slice())
                .collect();

            for backend in supported_backends() {
                let mut recovered: Vec<Vec<u8>> = recover_indices
                    .iter()
                    .map(|_| vec![0u8; shard_size])
                    .collect();
                let mut outputs: Vec<&mut [u8]> = recovered
                    .iter_mut()
                    .map(|value| value.as_mut_slice())
                    .collect();
                codec
                    .reconstruct_with_backend_for_test(
                        present_indices,
                        &present_data,
                        recover_indices,
                        &mut outputs,
                        backend,
                    )
                    .unwrap();

                for (out_index, &shard_index) in recover_indices.iter().enumerate() {
                    assert_eq!(
                        recovered[out_index],
                        all_shards[shard_index],
                        "backend={} shard_size={shard_size} shard={shard_index}",
                        backend_name(backend)
                    );
                }
            }
        }
    }
}

#[test]
fn runtime_smoke_test_ok() {
    self_test().unwrap();
}

#[test]
fn encode_all_zeros_gives_zero_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; 512]).collect();
    let parity = encode(&codec, &data);
    for p in &parity {
        assert!(p.iter().all(|&b| b == 0));
    }
}

#[test]
fn zero_parity_codec_construction() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    assert_eq!(codec.config().data_shards, 4);
    assert_eq!(codec.config().parity_shards, 0);
    assert_eq!(codec.verify_scratch_size(1024), 0);
}

#[test]
fn zero_parity_encode_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 512);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<&mut [u8]> = Vec::new();
    assert!(codec.encode(&data_refs, &mut parity).is_ok());
}

#[test]
fn zero_parity_verify_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 512);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity: Vec<&[u8]> = Vec::new();
    let mut scratch = Vec::new();
    assert_eq!(
        codec.verify(&data_refs, &parity, &mut scratch).unwrap(),
        VerifyResult::Ok
    );
}

#[test]
fn zero_parity_reconstruct_requires_no_recovery() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let present = make_data(4, 64);
    let present_refs: Vec<&[u8]> = present.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = Vec::new();
    assert!(codec
        .reconstruct(&[0, 1, 2, 3], &present_refs, &[], &mut outputs)
        .is_ok());
}

#[test]
fn encode_wrong_data_shard_count() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(3, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = (0..2).map(|_| vec![0u8; 64]).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert_eq!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardCount {
            expected: 4,
            got: 3
        })
    );
}

#[test]
fn encode_wrong_parity_shard_count() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = (0..1).map(|_| vec![0u8; 64]).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert_eq!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardCount {
            expected: 2,
            got: 1
        })
    );
}

#[test]
fn encode_mismatched_shard_sizes() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let mut data = make_data(4, 64);
    data[2] = vec![0u8; 32];
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = (0..2).map(|_| vec![0u8; 64]).collect();
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert!(matches!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardSizeMismatch { index: 2, .. })
    ));
}

#[test]
fn encode_mismatched_parity_shard_sizes() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<Vec<u8>> = vec![vec![0u8; 64], vec![0u8; 32]];
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
    assert!(matches!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardSizeMismatch { index: 5, .. })
    ));
}

// Zero-length shards

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
    let mut scratch = vec![];
    assert_eq!(
        codec
            .verify(&data_refs, &parity_refs, &mut scratch)
            .unwrap(),
        VerifyResult::Ok
    );
}

#[test]
fn zero_length_reconstruct_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
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
    let codec = ErasureCodec::new(EcConfig::new(2, 2).unwrap()).unwrap();
    let data: Vec<Vec<u8>> = (0..2).map(|_| vec![]).collect();
    let parity = encode(&codec, &data);
    let present: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut out0: Vec<u8> = vec![];
    let mut out1: Vec<u8> = vec![];
    let mut outputs: Vec<&mut [u8]> = vec![out0.as_mut_slice(), out1.as_mut_slice()];
    assert!(codec
        .reconstruct(&[2, 3], &present, &[0, 1], &mut outputs)
        .is_ok());
    assert_eq!(out0, data[0]);
    assert_eq!(out1, data[1]);
}

// Verify correctness

#[test]
fn verify_after_encode_passes() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let parity = encode(&codec, &data);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 1024];
    assert_eq!(
        codec
            .verify(&data_refs, &parity_refs, &mut scratch)
            .unwrap(),
        VerifyResult::Ok
    );
}

#[test]
fn verify_detects_corrupt_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let mut parity = encode(&codec, &data);
    parity[0][0] ^= 0xFF;
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 1024];
    assert_eq!(
        codec
            .verify(&data_refs, &parity_refs, &mut scratch)
            .unwrap(),
        VerifyResult::Mismatch(4)
    );
}

#[test]
fn verify_detects_corrupt_second_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1024);
    let mut parity = encode(&codec, &data);
    parity[1][100] ^= 1;
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 1024];
    assert_eq!(
        codec
            .verify(&data_refs, &parity_refs, &mut scratch)
            .unwrap(),
        VerifyResult::Mismatch(5)
    );
}

#[test]
fn verify_scratch_too_small() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 256);
    let parity = encode(&codec, &data);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 2 * 256 - 1];
    assert!(matches!(
        codec.verify(&data_refs, &parity_refs, &mut scratch),
        Err(EcError::ScratchTooSmall {
            required: 512,
            provided: 511
        })
    ));
}

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

fn exhaustive_reconstruct(data_shards: u8, parity_shards: u8, shard_size: usize) {
    let config = EcConfig::new(data_shards, parity_shards).unwrap();
    let codec = ErasureCodec::new(config).unwrap();
    let k = data_shards as usize;
    let m = parity_shards as usize;
    let total = k + m;

    let data = make_data(k, shard_size);
    let parity = encode(&codec, &data);
    let all_shards: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();

    for present_combo in combinations(total, k) {
        let recover: Vec<usize> = (0..total).filter(|i| !present_combo.contains(i)).collect();
        let present_data: Vec<&[u8]> = present_combo
            .iter()
            .map(|&i| all_shards[i].as_slice())
            .collect();
        let mut output_storage: Vec<Vec<u8>> =
            recover.iter().map(|_| vec![0u8; shard_size]).collect();
        let mut outputs: Vec<&mut [u8]> = output_storage
            .iter_mut()
            .map(|v| v.as_mut_slice())
            .collect();

        codec
            .reconstruct(&present_combo, &present_data, &recover, &mut outputs)
            .unwrap();

        for (out_idx, &shard_idx) in recover.iter().enumerate() {
            assert_eq!(output_storage[out_idx], all_shards[shard_idx]);
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

#[test]
fn reconstruct_more_than_k_present_uses_first_k() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 256);
    let parity = encode(&codec, &data);
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
    codec
        .reconstruct(&[0, 1, 2, 3, 4], &present_refs, &[5], &mut outputs)
        .unwrap();
    assert_eq!(out5, parity[1]);
}

#[test]
fn reconstruct_insufficient_shards() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(3, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = vec![];
    assert_eq!(
        codec.reconstruct(&[0, 1, 2], &data_refs, &[], &mut outputs),
        Err(EcError::InsufficientShards { need: 4, have: 3 })
    );
}

#[test]
fn single_byte_shards_roundtrip() {
    let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
    let data = make_data(4, 1);
    let parity = encode(&codec, &data);
    let all: Vec<Vec<u8>> = data.iter().chain(parity.iter()).cloned().collect();
    let present_refs: Vec<&[u8]> = vec![
        all[2].as_slice(),
        all[3].as_slice(),
        all[4].as_slice(),
        all[5].as_slice(),
    ];
    let mut out0 = vec![0u8; 1];
    let mut out1 = vec![0u8; 1];
    let mut outputs: Vec<&mut [u8]> = vec![out0.as_mut_slice(), out1.as_mut_slice()];
    codec
        .reconstruct(&[2, 3, 4, 5], &present_refs, &[0, 1], &mut outputs)
        .unwrap();
    assert_eq!(out0, data[0]);
    assert_eq!(out1, data[1]);
}

#[cfg(test)]
mod alloc_tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

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
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
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
        assert_eq!(allocs, 2);
    }

    #[test]
    fn verify_zero_allocs_in_hot_path() {
        let codec = ErasureCodec::new(EcConfig::new(4, 2).unwrap()).unwrap();
        let data = make_data(4, 4096);
        let parity = encode(&codec, &data);
        let mut scratch = vec![0u8; codec.verify_scratch_size(4096)];
        let allocs = count_allocs(|| {
            let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let parity_refs: Vec<&[u8]> = parity.iter().map(|v| v.as_slice()).collect();
            codec
                .verify(&data_refs, &parity_refs, &mut scratch)
                .unwrap();
        });
        assert_eq!(allocs, 2);
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
            let present_refs: Vec<&[u8]> = vec![
                all[0].as_slice(),
                all[1].as_slice(),
                all[2].as_slice(),
                all[3].as_slice(),
            ];
            let mut outputs: Vec<&mut [u8]> = vec![out4.as_mut_slice(), out5.as_mut_slice()];
            codec
                .reconstruct(&[0, 1, 2, 3], &present_refs, &[4, 5], &mut outputs)
                .unwrap();
        });
        assert_eq!(allocs, 2);
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
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
            let total = k + m;
            let mut rng_state = seed;
            let trials = 5.min(total);
            for _ in 0..trials {
                let mut indices: Vec<usize> = (0..total).collect();
                for i in (1..total).rev() {
                    rng_state = rng_state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
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
                    prop_assert_eq!(&output_storage[out_idx], &all[shard_idx]);
                }
            }
        }
    }
}
