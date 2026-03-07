/// ETag formatting: CRC64-NVME value to/from quoted hex string.
/// Format a CRC64-NVME value as a quoted hex ETag string.
///
/// Example: `format_etag(0xABCDEF1234567890)` → `"\"abcdef1234567890\""`
pub fn format_etag(crc64: u64) -> String {
    format!("\"{:016x}\"", crc64)
}

/// Format an ETag for an ObjectRecord, handling both regular and multipart objects.
///
/// Regular objects (etag_kind=0): `"abcdef1234567890"`
/// Multipart objects (etag_kind=1): `"abcdef1234567890-3"` (with parts_count suffix)
pub fn format_object_etag(etag: &[u8], etag_kind: u8, parts_count: Option<u32>) -> String {
    let crc = etag_bytes_to_crc64(etag).unwrap_or(0);
    if etag_kind == 1 {
        if let Some(count) = parts_count {
            return format!("\"{:016x}-{count}\"", crc);
        }
    }
    format_etag(crc)
}

/// Parse a quoted hex ETag string back to a CRC64-NVME value.
///
/// Accepts both quoted (`"abcdef..."`) and unquoted (`abcdef...`) forms.
pub fn parse_etag(etag: &str) -> Option<u64> {
    let hex = etag.trim_matches('"');
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Convert a CRC64-NVME value to its 8-byte big-endian representation
/// (for storage in the etag field of ObjectRecord).
pub fn crc64_to_etag_bytes(crc64: u64) -> Vec<u8> {
    crc64.to_be_bytes().to_vec()
}

/// Compute a composite multipart ETag.
///
/// The composite ETag is CRC64 of the concatenated per-part CRC64 bytes,
/// formatted as `"<hex>-<parts_count>"`.
pub fn compute_multipart_etag(part_etag_bytes: &[&[u8]]) -> (Vec<u8>, String) {
    let mut concat = Vec::with_capacity(part_etag_bytes.len() * 8);
    for bytes in part_etag_bytes {
        concat.extend_from_slice(bytes);
    }
    let composite_crc = checksum::crc64::checksum(&concat);
    let etag_bytes = crc64_to_etag_bytes(composite_crc);
    let etag_str = format!("\"{:016x}-{}\"", composite_crc, part_etag_bytes.len());
    (etag_bytes, etag_str)
}

/// Convert etag bytes back to a CRC64-NVME value.
pub fn etag_bytes_to_crc64(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    Some(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_etag_basic() {
        assert_eq!(format_etag(0xABCDEF1234567890), "\"abcdef1234567890\"");
    }

    #[test]
    fn format_etag_zero() {
        assert_eq!(format_etag(0), "\"0000000000000000\"");
    }

    #[test]
    fn parse_etag_quoted() {
        assert_eq!(parse_etag("\"abcdef1234567890\""), Some(0xABCDEF1234567890));
    }

    #[test]
    fn parse_etag_unquoted() {
        assert_eq!(parse_etag("abcdef1234567890"), Some(0xABCDEF1234567890));
    }

    #[test]
    fn parse_etag_invalid() {
        assert_eq!(parse_etag("short"), None);
        assert_eq!(parse_etag("zzzzzzzzzzzzzzzz"), None);
    }

    #[test]
    fn round_trip_format_parse() {
        let crc = 0xDEADBEEFCAFE1234u64;
        let etag = format_etag(crc);
        assert_eq!(parse_etag(&etag), Some(crc));
    }

    #[test]
    fn etag_bytes_to_crc64_wrong_length() {
        assert_eq!(etag_bytes_to_crc64(&[]), None);
        assert_eq!(etag_bytes_to_crc64(&[1, 2, 3]), None);
        assert_eq!(etag_bytes_to_crc64(&[0; 9]), None);
    }

    #[test]
    fn crc64_bytes_round_trip() {
        let crc = 0xAE8B14860A799888u64;
        let bytes = crc64_to_etag_bytes(crc);
        assert_eq!(bytes.len(), 8);
        assert_eq!(etag_bytes_to_crc64(&bytes), Some(crc));
    }

    #[test]
    fn compute_multipart_etag_format() {
        let part1 = crc64_to_etag_bytes(checksum::crc64::checksum(b"part1"));
        let part2 = crc64_to_etag_bytes(checksum::crc64::checksum(b"part2"));
        let (bytes, etag_str) = compute_multipart_etag(&[&part1, &part2]);
        assert_eq!(bytes.len(), 8);
        assert!(etag_str.starts_with('"'));
        assert!(etag_str.ends_with("-2\""));
    }

    #[test]
    fn compute_multipart_etag_deterministic() {
        let part1 = crc64_to_etag_bytes(0x1234);
        let (a_bytes, a_str) = compute_multipart_etag(&[&part1]);
        let (b_bytes, b_str) = compute_multipart_etag(&[&part1]);
        assert_eq!(a_bytes, b_bytes);
        assert_eq!(a_str, b_str);
        assert!(a_str.ends_with("-1\""));
    }
}
