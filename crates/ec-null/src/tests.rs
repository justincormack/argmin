use crate::codec::check_shard_size;
use crate::self_test;
use crate::{EcConfig, EcError, ErasureCodec, VerifyResult, MAX_TOTAL_SHARDS};

fn make_data(k: usize, shard_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|i| {
            (0..shard_size)
                .map(|j| ((i * 37 + j * 13 + 7) & 0xFF) as u8)
                .collect()
        })
        .collect()
}

#[test]
fn config_valid_cases() {
    assert!(EcConfig::new(1, 0).is_ok());
    assert!(EcConfig::new(4, 0).is_ok());
    assert!(EcConfig::new(MAX_TOTAL_SHARDS as u8, 0).is_ok());
}

#[test]
fn config_zero_data_shards() {
    assert_eq!(
        EcConfig::new(0, 0),
        Err(EcError::InvalidConfig {
            reason: "data_shards must be >= 1"
        })
    );
}

#[test]
fn config_rejects_nonzero_parity() {
    assert_eq!(
        EcConfig::new(4, 1),
        Err(EcError::InvalidConfig {
            reason: "null backend only supports parity_shards = 0"
        })
    );
}

#[test]
fn config_total_and_overhead() {
    let c = EcConfig::new(4, 0).unwrap();
    assert_eq!(c.total_shards(), 4);
    assert!((c.overhead() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn runtime_smoke_test_ok() {
    self_test().unwrap();
}

#[test]
fn zero_parity_codec_construction() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    assert_eq!(codec.config().data_shards, 4);
    assert_eq!(codec.config().parity_shards, 0);
    assert_eq!(codec.verify_scratch_size(1024), 0);
}

#[test]
fn encode_zero_parity_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 512);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<&mut [u8]> = Vec::new();
    assert!(codec.encode(&data_refs, &mut parity).is_ok());
}

#[test]
fn encode_wrong_data_shard_count() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(3, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<&mut [u8]> = Vec::new();
    assert_eq!(
        codec.encode(&data_refs, &mut parity),
        Err(EcError::ShardCount {
            expected: 4,
            got: 3
        })
    );
}

#[test]
fn encode_rejects_non_empty_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity = [Vec::new()];
    let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(Vec::as_mut_slice).collect();
    assert_eq!(
        codec.encode(&data_refs, &mut parity_refs),
        Err(EcError::ShardCount {
            expected: 0,
            got: 1
        })
    );
}

#[test]
fn encode_mismatched_data_shard_sizes() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let mut data = make_data(4, 64);
    data[2] = vec![0u8; 32];
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<&mut [u8]> = Vec::new();
    assert!(matches!(
        codec.encode(&data_refs, &mut parity),
        Err(EcError::ShardSizeMismatch { index: 2, .. })
    ));
}

#[test]
fn verify_zero_parity_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 512);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut scratch = Vec::new();
    assert_eq!(
        codec.verify(&data_refs, &[], &mut scratch).unwrap(),
        VerifyResult::Ok
    );
}

#[test]
fn verify_rejects_non_empty_parity() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let parity = [vec![0u8; 64]];
    let parity_refs: Vec<&[u8]> = parity.iter().map(Vec::as_slice).collect();
    let mut scratch = Vec::new();
    assert_eq!(
        codec.verify(&data_refs, &parity_refs, &mut scratch),
        Err(EcError::ShardCount {
            expected: 0,
            got: 1
        })
    );
}

#[test]
fn verify_allows_extra_scratch() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut scratch = vec![0u8; 1];
    assert_eq!(
        codec.verify(&data_refs, &[], &mut scratch).unwrap(),
        VerifyResult::Ok
    );
}

#[test]
fn reconstruct_empty_recover_is_noop() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = Vec::new();
    assert!(codec
        .reconstruct(&[0, 1, 2, 3], &data_refs, &[], &mut outputs)
        .is_ok());
}

#[test]
fn reconstruct_rejects_missing_data_shard() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(3, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut out = vec![0u8; 64];
    let mut outputs: Vec<&mut [u8]> = vec![out.as_mut_slice()];
    assert_eq!(
        codec.reconstruct(&[0, 1, 2], &data_refs, &[3], &mut outputs),
        Err(EcError::InsufficientShards { need: 4, have: 3 })
    );
}

#[test]
fn reconstruct_rejects_existing_shard_in_recover() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut out = vec![0u8; 64];
    let mut outputs: Vec<&mut [u8]> = vec![out.as_mut_slice()];
    assert_eq!(
        codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[3], &mut outputs),
        Err(EcError::OverlappingIndices { index: 3 })
    );
}

#[test]
fn reconstruct_rejects_out_of_range_recovery_index() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut out = vec![0u8; 64];
    let mut outputs: Vec<&mut [u8]> = vec![out.as_mut_slice()];
    assert_eq!(
        codec.reconstruct(&[0, 1, 2, 3], &data_refs, &[4], &mut outputs),
        Err(EcError::ShardIndexOutOfRange { index: 4, total: 4 })
    );
}

#[test]
fn reconstruct_rejects_unsorted_present_indices() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data = make_data(4, 64);
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut outputs: Vec<&mut [u8]> = Vec::new();
    assert!(matches!(
        codec.reconstruct(&[0, 2, 1, 3], &data_refs, &[], &mut outputs),
        Err(EcError::UnsortedIndices { .. })
    ));
}

#[test]
fn zero_length_paths_are_ok() {
    let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
    let data: Vec<Vec<u8>> = (0..4).map(|_| Vec::new()).collect();
    let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    let mut parity: Vec<&mut [u8]> = Vec::new();
    let mut scratch = Vec::new();
    let mut outputs: Vec<&mut [u8]> = Vec::new();
    assert!(codec.encode(&data_refs, &mut parity).is_ok());
    assert_eq!(
        codec.verify(&data_refs, &[], &mut scratch).unwrap(),
        VerifyResult::Ok
    );
    assert!(codec
        .reconstruct(&[0, 1, 2, 3], &data_refs, &[], &mut outputs)
        .is_ok());
}

#[test]
fn shard_size_too_large_boundary() {
    assert!(check_shard_size(0).is_ok());
    assert!(check_shard_size(i32::MAX as usize).is_ok());
    assert!(matches!(
        check_shard_size(i32::MAX as usize + 1),
        Err(EcError::ShardSizeTooLarge { size }) if size == i32::MAX as usize + 1
    ));
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
        let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
        let data = make_data(4, 4096);

        let allocs = count_allocs(|| {
            let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let mut parity_refs: Vec<&mut [u8]> = Vec::new();
            codec.encode(&data_refs, &mut parity_refs).unwrap();
        });

        assert_eq!(allocs, 1);
    }

    #[test]
    fn verify_zero_allocs_in_hot_path() {
        let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
        let data = make_data(4, 4096);
        let mut scratch = Vec::new();

        let allocs = count_allocs(|| {
            let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            codec.verify(&data_refs, &[], &mut scratch).unwrap();
        });

        assert_eq!(allocs, 1);
    }

    #[test]
    fn reconstruct_zero_allocs_in_hot_path() {
        let codec = ErasureCodec::new(EcConfig::new(4, 0).unwrap()).unwrap();
        let data = make_data(4, 4096);

        let allocs = count_allocs(|| {
            let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
            let mut outputs: Vec<&mut [u8]> = Vec::new();
            codec
                .reconstruct(&[0, 1, 2, 3], &data_refs, &[], &mut outputs)
                .unwrap();
        });

        assert_eq!(allocs, 1);
    }
}
