/// ETag formatting: CRC64-NVME value to/from quoted hex string.
/// Format a CRC64-NVME value as a quoted hex ETag string.
///
/// Example: `format_etag(0xABCDEF1234567890)` → `"\"abcdef1234567890\""`
pub fn format_etag(crc64: u64) -> String {
    format!("\"{:016x}\"", crc64)
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
}
