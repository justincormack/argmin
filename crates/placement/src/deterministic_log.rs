/// Hash-derived unit-interval values used by placement scoring.
///
/// The placement score path does not accept arbitrary `f64` log inputs. Values
/// are always constructed from a 53-bit integer numerator `n` in `[1, 2^53]`
/// and represent `n / 2^53`, i.e. the closed interval `[2^-53, 1]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Unit53(u64);

impl Unit53 {
    const DENOMINATOR_BITS: u32 = 53;
    const DENOMINATOR: u64 = 1u64 << Self::DENOMINATOR_BITS;

    #[inline]
    pub(crate) fn from_hash(hash: u64) -> Self {
        // hash >> 11 is in [0, 2^53 - 1]; adding 1 gives [1, 2^53].
        Self((hash >> 11) + 1)
    }

    #[inline]
    fn numerator(self) -> u64 {
        debug_assert!((1..=Self::DENOMINATOR).contains(&self.0));
        self.0
    }

    #[inline]
    pub(crate) fn to_f64(self) -> f64 {
        self.numerator() as f64 / Self::DENOMINATOR as f64
    }
}

/// Deterministic natural logarithm for the placement score domain `[2^-53, 1]`.
///
/// This is a dedicated entry point for the placement score path so the domain is
/// explicit in the API. It is still backed by `libm::log` until the restricted
/// binary64 implementation lands.
#[inline]
pub(crate) fn deterministic_log_u53(x: Unit53) -> f64 {
    libm::log(x.to_f64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_hash_maps_zero_to_smallest_value() {
        let x = Unit53::from_hash(0);
        assert_eq!(x.numerator(), 1);
        assert_eq!(x.to_f64().to_bits(), (2.0f64).powi(-53).to_bits());
    }

    #[test]
    fn from_hash_maps_max_to_one() {
        let x = Unit53::from_hash(u64::MAX);
        assert_eq!(x.numerator(), 1u64 << 53);
        assert_eq!(x.to_f64().to_bits(), 1.0f64.to_bits());
    }

    #[test]
    fn deterministic_log_respects_domain_endpoints() {
        let min = deterministic_log_u53(Unit53::from_hash(0));
        let max = deterministic_log_u53(Unit53::from_hash(u64::MAX));

        assert!(min.is_finite());
        assert!(min < 0.0);
        assert_eq!(max.to_bits(), 0.0f64.to_bits());
    }

    #[test]
    fn deterministic_log_is_monotonic_over_hash_domain() {
        let smaller = Unit53::from_hash(0);
        let larger = Unit53::from_hash(1u64 << 63);

        assert!(smaller.to_f64() < larger.to_f64());
        assert!(deterministic_log_u53(smaller) < deterministic_log_u53(larger));
    }
}
