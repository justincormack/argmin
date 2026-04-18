use crate::cluster::NodeId;
use crate::deterministic_log::{deterministic_log_u53, Unit53};
use rapidhash::v3::{rapidhash_v3_micro_inline, RapidSecrets};

/// Fixed secrets for placement hashing. Using a constant seed gives stable,
/// reproducible output across all invocations and all platforms.
const SECRETS: RapidSecrets = RapidSecrets::seed(0);
const STACK_KEY_LIMIT: usize = 512 + core::mem::size_of::<u32>();

#[inline]
fn hash_bytes(data: &[u8]) -> u64 {
    rapidhash_v3_micro_inline::<true, false>(data, &SECRETS)
}

#[inline]
fn hash_key_node(key: &[u8], node_id: NodeId) -> u64 {
    let node_id_bytes = node_id.as_u32().to_le_bytes();
    let total_len = key.len() + node_id_bytes.len();
    if total_len <= STACK_KEY_LIMIT {
        let mut data = [0u8; STACK_KEY_LIMIT];
        data[..key.len()].copy_from_slice(key);
        data[key.len()..total_len].copy_from_slice(&node_id_bytes);
        return hash_bytes(&data[..total_len]);
    }

    let mut data = Vec::with_capacity(total_len);
    data.extend_from_slice(key);
    data.extend_from_slice(&node_id_bytes);
    hash_bytes(&data)
}

/// Compute the HRW score for a given key and node.
///
/// Score = -log(U) / weight, where U is a uniform random variable on `[2^-53, 1]`
/// derived from hash(key || node_id_bytes).
///
/// Lower score = preferred. Weight must be > 0.0 (caller ensures this).
///
/// The log input is restricted to the placement-owned `Unit53` domain rather
/// than an arbitrary `f64`.
pub(crate) fn score(key: &[u8], node_id: NodeId, weight: f64) -> f64 {
    let h = hash_key_node(key, node_id);
    let u = Unit53::from_hash(h);
    -deterministic_log_u53(u) / weight
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_is_non_negative() {
        let s = score(b"hello", NodeId::new(42), 1.0);
        assert!(s >= 0.0, "score must be non-negative, got {s}");
    }

    #[test]
    fn score_is_deterministic() {
        let a = score(b"key", NodeId::new(7), 1.0);
        let b = score(b"key", NodeId::new(7), 1.0);
        assert_eq!(a.to_bits(), b.to_bits());
    }

    #[test]
    fn different_nodes_produce_different_scores() {
        let s0 = score(b"key", NodeId::new(0), 1.0);
        let s1 = score(b"key", NodeId::new(1), 1.0);
        // Collision probability is ~2^-64; this should never fail.
        assert_ne!(s0.to_bits(), s1.to_bits());
    }

    #[test]
    fn different_keys_produce_different_scores() {
        let s0 = score(b"key0", NodeId::new(0), 1.0);
        let s1 = score(b"key1", NodeId::new(0), 1.0);
        assert_ne!(s0.to_bits(), s1.to_bits());
    }

    #[test]
    fn weight_scales_score_inversely() {
        // Higher weight → lower score (more likely to be selected)
        let s1 = score(b"k", NodeId::new(0), 1.0);
        let s2 = score(b"k", NodeId::new(0), 2.0);
        assert!(s2 < s1, "double weight should halve score: {s2} vs {s1}");
    }

    #[test]
    fn empty_key_is_valid() {
        let s = score(b"", NodeId::new(0), 1.0);
        assert!(s >= 0.0);
    }
}
