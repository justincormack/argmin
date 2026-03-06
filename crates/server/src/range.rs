/// Byte range parsing for HTTP Range requests (RFC 7233).
///
/// Only single-range support — S3 does not support multi-range requests.
use crate::error::ServerError;

/// A parsed byte range from the Range header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// bytes=start-end (inclusive on both ends)
    Range { start: u64, end: u64 },
    /// bytes=start-
    FromStart { start: u64 },
    /// bytes=-N (last N bytes)
    Suffix { length: u64 },
}

impl ByteRange {
    /// Parse a Range header value (e.g. "bytes=0-99", "bytes=100-", "bytes=-50").
    ///
    /// Only supports a single range. Returns an error for multi-range or malformed values.
    pub fn parse(header: &str) -> Result<Self, ServerError> {
        let spec = header
            .strip_prefix("bytes=")
            .ok_or_else(|| ServerError::InvalidRequest {
                reason: "Range header must start with 'bytes='".to_string(),
            })?;

        // Reject multi-range
        if spec.contains(',') {
            return Err(ServerError::InvalidRequest {
                reason: "multi-range not supported".to_string(),
            });
        }

        let dash_pos = spec.find('-').ok_or_else(|| ServerError::InvalidRequest {
            reason: "invalid range format".to_string(),
        })?;

        let before = &spec[..dash_pos];
        let after = &spec[dash_pos + 1..];

        match (before.is_empty(), after.is_empty()) {
            // bytes=-N (suffix)
            (true, false) => {
                let length = after
                    .parse::<u64>()
                    .map_err(|_| ServerError::InvalidRequest {
                        reason: "invalid suffix range".to_string(),
                    })?;
                if length == 0 {
                    return Err(ServerError::InvalidRequest {
                        reason: "suffix range length must be non-zero".to_string(),
                    });
                }
                Ok(ByteRange::Suffix { length })
            }
            // bytes=start-
            (false, true) => {
                let start = before
                    .parse::<u64>()
                    .map_err(|_| ServerError::InvalidRequest {
                        reason: "invalid range start".to_string(),
                    })?;
                Ok(ByteRange::FromStart { start })
            }
            // bytes=start-end
            (false, false) => {
                let start = before
                    .parse::<u64>()
                    .map_err(|_| ServerError::InvalidRequest {
                        reason: "invalid range start".to_string(),
                    })?;
                let end = after
                    .parse::<u64>()
                    .map_err(|_| ServerError::InvalidRequest {
                        reason: "invalid range end".to_string(),
                    })?;
                if start > end {
                    return Err(ServerError::InvalidRequest {
                        reason: "range start exceeds end".to_string(),
                    });
                }
                Ok(ByteRange::Range { start, end })
            }
            // bytes=- (both empty)
            (true, true) => Err(ServerError::InvalidRequest {
                reason: "invalid range format".to_string(),
            }),
        }
    }

    /// Resolve to concrete (start, end_inclusive) byte offsets for the given object size.
    ///
    /// Returns `None` if the range is unsatisfiable (e.g. start >= size for a non-suffix range).
    pub fn resolve(self, object_size: u64) -> Option<(u64, u64)> {
        if object_size == 0 {
            return None;
        }
        match self {
            ByteRange::Range { start, end } => {
                if start >= object_size {
                    return None;
                }
                let clamped_end = end.min(object_size - 1);
                Some((start, clamped_end))
            }
            ByteRange::FromStart { start } => {
                if start >= object_size {
                    return None;
                }
                Some((start, object_size - 1))
            }
            ByteRange::Suffix { length } => {
                if length >= object_size {
                    Some((0, object_size - 1))
                } else {
                    Some((object_size - length, object_size - 1))
                }
            }
        }
    }
}

/// Parse an `x-amz-copy-source-range` header value.
///
/// AWS only allows the `bytes=start-end` form for copy-source-range (no suffix
/// or open-ended ranges). Returns `(start, end)` inclusive on success.
/// Malformed values produce `InvalidArgument` to match AWS/Ceph behavior.
pub fn parse_copy_source_range(header: &str) -> Result<(u64, u64), ServerError> {
    let range = ByteRange::parse(header).map_err(|_| ServerError::InvalidArgument {
        reason: format!("invalid copy source range: {header}"),
    })?;
    match range {
        ByteRange::Range { start, end } => Ok((start, end)),
        _ => Err(ServerError::InvalidArgument {
            reason: format!("invalid copy source range: {header}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse ──────────────────────────────────────────────────────

    #[test]
    fn parse_range() {
        assert_eq!(
            ByteRange::parse("bytes=0-99").unwrap(),
            ByteRange::Range { start: 0, end: 99 }
        );
    }

    #[test]
    fn parse_from_start() {
        assert_eq!(
            ByteRange::parse("bytes=100-").unwrap(),
            ByteRange::FromStart { start: 100 }
        );
    }

    #[test]
    fn parse_suffix() {
        assert_eq!(
            ByteRange::parse("bytes=-50").unwrap(),
            ByteRange::Suffix { length: 50 }
        );
    }

    #[test]
    fn parse_rejects_no_bytes_prefix() {
        assert!(ByteRange::parse("0-99").is_err());
    }

    #[test]
    fn parse_rejects_multi_range() {
        assert!(ByteRange::parse("bytes=0-50, 100-150").is_err());
    }

    #[test]
    fn parse_rejects_start_greater_than_end() {
        assert!(ByteRange::parse("bytes=100-50").is_err());
    }

    #[test]
    fn parse_rejects_empty_range() {
        assert!(ByteRange::parse("bytes=-").is_err());
    }

    #[test]
    fn parse_rejects_zero_suffix() {
        assert!(ByteRange::parse("bytes=-0").is_err());
    }

    #[test]
    fn parse_rejects_non_numeric() {
        assert!(ByteRange::parse("bytes=abc-def").is_err());
    }

    #[test]
    fn parse_single_byte() {
        assert_eq!(
            ByteRange::parse("bytes=0-0").unwrap(),
            ByteRange::Range { start: 0, end: 0 }
        );
    }

    // ── resolve ────────────────────────────────────────────────────

    #[test]
    fn resolve_range_within_bounds() {
        let r = ByteRange::Range { start: 0, end: 99 };
        assert_eq!(r.resolve(200), Some((0, 99)));
    }

    #[test]
    fn resolve_range_clamps_end() {
        let r = ByteRange::Range { start: 0, end: 999 };
        assert_eq!(r.resolve(100), Some((0, 99)));
    }

    #[test]
    fn resolve_range_unsatisfiable() {
        let r = ByteRange::Range {
            start: 200,
            end: 300,
        };
        assert_eq!(r.resolve(100), None);
    }

    #[test]
    fn resolve_from_start() {
        let r = ByteRange::FromStart { start: 50 };
        assert_eq!(r.resolve(100), Some((50, 99)));
    }

    #[test]
    fn resolve_from_start_unsatisfiable() {
        let r = ByteRange::FromStart { start: 100 };
        assert_eq!(r.resolve(100), None);
    }

    #[test]
    fn resolve_suffix() {
        let r = ByteRange::Suffix { length: 20 };
        assert_eq!(r.resolve(100), Some((80, 99)));
    }

    #[test]
    fn resolve_suffix_exceeds_size() {
        let r = ByteRange::Suffix { length: 999 };
        assert_eq!(r.resolve(100), Some((0, 99)));
    }

    #[test]
    fn resolve_zero_size_object() {
        let r = ByteRange::Range { start: 0, end: 0 };
        assert_eq!(r.resolve(0), None);

        let r = ByteRange::FromStart { start: 0 };
        assert_eq!(r.resolve(0), None);

        let r = ByteRange::Suffix { length: 10 };
        assert_eq!(r.resolve(0), None);
    }
}
