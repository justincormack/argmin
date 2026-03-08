/// Canonical request construction per AWS SigV4 spec.
use ring::digest;

/// SHA-256 hash as lowercase hex string.
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = digest::digest(&digest::SHA256, data);
    hex_encode(hash.as_ref())
}

/// Percent-encode a value per SigV4 rules (RFC 3986 unreserved chars only).
/// Encodes everything except A-Z, a-z, 0-9, '-', '_', '.', '~'.
pub fn uri_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push(HEX_UPPER[(byte >> 4) as usize] as char);
                encoded.push(HEX_UPPER[(byte & 0x0f) as usize] as char);
            }
        }
    }
    encoded
}

/// Percent-encode a URI path, preserving '/' separators.
///
/// This preserves any existing %XX sequences (uppercasing hex digits) to avoid
/// double-encoding already-encoded bytes in the raw request path.
pub fn uri_encode_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut encoded = String::with_capacity(path.len());
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'/' {
            encoded.push('/');
            i += 1;
            continue;
        }
        if byte == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                encoded.push('%');
                encoded.push(HEX_UPPER[hi as usize] as char);
                encoded.push(HEX_UPPER[lo as usize] as char);
                i += 3;
                continue;
            }
        }
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push(HEX_UPPER[(byte >> 4) as usize] as char);
                encoded.push(HEX_UPPER[(byte & 0x0f) as usize] as char);
            }
        }
        i += 1;
    }
    encoded
}

/// Build the canonical request string per SigV4 spec.
///
/// Parameters:
/// - `method`: HTTP method (e.g. "GET", "PUT")
/// - `uri`: URI path (e.g. "/mybucket/mykey")
/// - `query`: Canonical query string (sorted key=value pairs joined by &)
/// - `headers`: Canonical headers string (lowercase key:trimmed-value\n for each)
/// - `signed_headers`: Semicolon-separated list of signed header names
/// - `body_hash`: SHA-256 hex hash of the request body
pub fn canonical_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &str,
    signed_headers: &str,
    body_hash: &str,
) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        uri_encode_path(uri),
        query,
        headers,
        signed_headers,
        body_hash
    )
}

/// Build the canonical headers string from a list of (name, value) pairs.
/// Sorts by header name, combines duplicate headers with comma-separated values,
/// and collapses interior whitespace per the SigV4 spec.
pub fn canonical_headers(headers: &[(&str, &str)]) -> String {
    // Sort by header name. sort_by_key is a STABLE sort in Rust, so
    // duplicate header values retain their original request order as
    // required by SigV4.
    let mut sorted: Vec<(&str, &str)> = headers.to_vec();
    sorted.sort_by_key(|(name, _)| *name);

    let mut result = String::new();
    let mut i = 0;
    while i < sorted.len() {
        let name = sorted[i].0;
        result.push_str(name);
        result.push(':');
        // Collect all values for this header name
        result.push_str(&normalize_header_value(sorted[i].1));
        i += 1;
        while i < sorted.len() && sorted[i].0 == name {
            result.push(',');
            result.push_str(&normalize_header_value(sorted[i].1));
            i += 1;
        }
        result.push('\n');
    }
    result
}

/// Trim leading/trailing whitespace and collapse interior runs of whitespace
/// to a single space, per SigV4 canonical header value rules.
fn normalize_header_value(value: &str) -> String {
    let trimmed = value.trim();
    let mut result = String::with_capacity(trimmed.len());
    let mut prev_was_space = false;
    for ch in trimmed.chars() {
        if ch.is_ascii_whitespace() {
            if !prev_was_space {
                result.push(' ');
                prev_was_space = true;
            }
        } else {
            result.push(ch);
            prev_was_space = false;
        }
    }
    result
}

/// Build the canonical query string from raw query string.
/// Per SigV4: percent-decode raw pairs first, then re-encode with SigV4 rules.
/// This avoids double-encoding when the incoming URL already has %XX sequences.
pub fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let val = parts.next().unwrap_or("");
            (
                uri_encode(&percent_decode(key)),
                uri_encode(&percent_decode(val)),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-decode a string (RFC 3986). Does NOT treat + as space.
fn percent_decode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                result.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Build the string-to-sign per SigV4 spec.
///
/// Parameters:
/// - `timestamp`: ISO 8601 timestamp (e.g. "20130524T000000Z")
/// - `scope`: Credential scope string (e.g. "20130524/us-east-1/s3/aws4_request")
/// - `canonical_request_hash`: SHA-256 hex hash of the canonical request
pub fn string_to_sign(timestamp: &str, scope: &str, canonical_request_hash: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        timestamp, scope, canonical_request_hash
    )
}

/// Parse an ISO 8601 SigV4 timestamp ("YYYYMMDDTHHMMSSz") into Unix epoch seconds.
/// Returns None if the format is invalid.
pub fn parse_amz_date(ts: &str) -> Option<u64> {
    // Expected format: "20130524T000000Z" (exactly 16 chars)
    if ts.len() != 16 || ts.as_bytes()[8] != b'T' || ts.as_bytes()[15] != b'Z' {
        return None;
    }
    let year: u32 = ts[0..4].parse().ok()?;
    let month: u32 = ts[4..6].parse().ok()?;
    let day: u32 = ts[6..8].parse().ok()?;
    let hour: u32 = ts[9..11].parse().ok()?;
    let min: u32 = ts[11..13].parse().ok()?;
    let sec: u32 = ts[13..15].parse().ok()?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 59 {
        return None;
    }

    // Days from epoch (1970-01-01) to the given date
    let days = days_since_epoch(year, month, day)?;
    Some(days as u64 * 86400 + hour as u64 * 3600 + min as u64 * 60 + sec as u64)
}

/// Days from 1970-01-01 to the given date.
fn days_since_epoch(year: u32, month: u32, day: u32) -> Option<u64> {
    if year < 1970 {
        return None;
    }
    // Cumulative days before each month (non-leap)
    const MONTH_DAYS: [u32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

    let mut days: u64 = 0;
    // Full years
    for y in 1970..year {
        days += if is_leap(y) { 366 } else { 365 };
    }
    // Months in current year
    days += MONTH_DAYS[(month - 1) as usize] as u64;
    // Leap day adjustment
    if month > 2 && is_leap(year) {
        days += 1;
    }
    days += (day - 1) as u64;
    Some(days)
}

fn is_leap(y: u32) -> bool {
    y.is_multiple_of(4) && (!y.is_multiple_of(100) || y.is_multiple_of(400))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX_LOWER[(b >> 4) as usize] as char);
        s.push(HEX_LOWER[(b & 0x0f) as usize] as char);
    }
    s
}

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";
const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn sha256_empty() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn uri_encode_simple() {
        assert_eq!(uri_encode("hello"), "hello");
        assert_eq!(uri_encode("hello world"), "hello%20world");
        assert_eq!(uri_encode("a+b=c"), "a%2Bb%3Dc");
    }

    #[test]
    fn uri_encode_preserves_unreserved() {
        assert_eq!(uri_encode("test-file_name.txt~"), "test-file_name.txt~");
    }

    #[test]
    fn uri_encode_unicode() {
        // UTF-8 bytes get percent-encoded
        assert_eq!(uri_encode("\u{00e9}"), "%C3%A9");
    }

    #[test]
    fn uri_encode_path_preserves_slashes() {
        assert_eq!(uri_encode_path("/bucket/my key"), "/bucket/my%20key");
    }

    #[test]
    fn uri_encode_path_preserves_percent_encoding() {
        assert_eq!(uri_encode_path("/bucket/1999%23"), "/bucket/1999%23");
        assert_eq!(uri_encode_path("/bucket/%2f"), "/bucket/%2F");
    }

    #[test]
    fn canonical_query_string_sorts() {
        assert_eq!(canonical_query_string("b=2&a=1"), "a=1&b=2");
    }

    #[test]
    fn canonical_query_string_empty() {
        assert_eq!(canonical_query_string(""), "");
    }

    #[test]
    fn canonical_query_string_encodes() {
        assert_eq!(canonical_query_string("key=val ue"), "key=val%20ue");
    }

    #[test]
    fn canonical_headers_sorts_by_name() {
        let headers = [
            ("x-amz-date", "20130524T000000Z"),
            ("host", "example.com"),
            ("content-type", "text/plain"),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(
            result,
            "content-type:text/plain\nhost:example.com\nx-amz-date:20130524T000000Z\n"
        );
    }

    #[test]
    fn canonical_headers_combines_duplicates() {
        let headers = [
            ("host", "example.com"),
            ("x-amz-meta-tag", "alpha"),
            ("x-amz-meta-tag", "beta"),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(result, "host:example.com\nx-amz-meta-tag:alpha,beta\n");
    }

    #[test]
    fn canonical_headers_trims_whitespace() {
        let headers = [
            ("host", "  example.com  "),
            ("content-type", " text/plain "),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(result, "content-type:text/plain\nhost:example.com\n");
    }

    #[test]
    fn canonical_headers_collapses_interior_whitespace() {
        let headers = [
            ("host", "example.com"),
            ("x-amz-meta-desc", "  hello   world  foo  "),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(
            result,
            "host:example.com\nx-amz-meta-desc:hello world foo\n"
        );
    }

    #[test]
    fn canonical_query_string_no_double_encode() {
        // prefix=photos%2F should NOT become prefix=photos%252F
        assert_eq!(
            canonical_query_string("prefix=photos%2F"),
            "prefix=photos%2F"
        );
    }

    #[test]
    fn canonical_query_string_pre_encoded_mixed() {
        // Mix of encoded and unencoded values
        assert_eq!(
            canonical_query_string("key=hello%20world&b=2&a=1"),
            "a=1&b=2&key=hello%20world"
        );
    }

    #[test]
    fn parse_amz_date_valid() {
        // 2013-05-24 00:00:00 UTC
        let epoch = parse_amz_date("20130524T000000Z").unwrap();
        // Known: 2013-05-24 is day 15849 since 1970-01-01
        assert_eq!(epoch, 1369353600);
    }

    #[test]
    fn parse_amz_date_with_time() {
        let epoch = parse_amz_date("20130524T120000Z").unwrap();
        assert_eq!(epoch, 1369353600 + 12 * 3600);
    }

    #[test]
    fn parse_amz_date_invalid_format() {
        assert!(parse_amz_date("").is_none());
        assert!(parse_amz_date("2013-05-24T00:00:00Z").is_none());
        assert!(parse_amz_date("not-a-timestamp").is_none());
        assert!(parse_amz_date("20130524T000000").is_none()); // missing Z
    }

    #[test]
    fn parse_amz_date_invalid_values() {
        assert!(parse_amz_date("20131324T000000Z").is_none()); // month 13
        assert!(parse_amz_date("20130532T000000Z").is_none()); // day 32
        assert!(parse_amz_date("20130524T250000Z").is_none()); // hour 25
    }

    // ── Property-based tests ────────────────────────────────────────

    fn normalize_value_ref(value: &str) -> String {
        let trimmed = value.trim();
        let mut result = String::with_capacity(trimmed.len());
        let mut prev_was_space = false;
        for ch in trimmed.chars() {
            if ch.is_ascii_whitespace() {
                if !prev_was_space {
                    result.push(' ');
                    prev_was_space = true;
                }
            } else {
                result.push(ch);
                prev_was_space = false;
            }
        }
        result
    }

    fn parse_canonical_headers(s: &str) -> Vec<(String, Vec<String>)> {
        let mut out: Vec<(String, Vec<String>)> = Vec::new();
        for line in s.split('\n') {
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(2, ':');
            let name = parts.next().unwrap_or("").to_string();
            let value = parts.next().unwrap_or("");
            let values = value.split(',').map(|v| v.to_string()).collect::<Vec<_>>();
            out.push((name, values));
        }
        out
    }

    #[test]
    fn parse_amz_date_boundary_valid() {
        // Month 1, day 1, time 00:00:00
        assert!(parse_amz_date("19700101T000000Z").is_some());
        // Month 12, day 31, time 23:59:59
        assert!(parse_amz_date("20251231T235959Z").is_some());
    }

    #[test]
    fn parse_amz_date_month_zero() {
        assert!(parse_amz_date("20250024T000000Z").is_none());
    }

    #[test]
    fn parse_amz_date_day_zero() {
        assert!(parse_amz_date("20250100T000000Z").is_none());
    }

    #[test]
    fn parse_amz_date_hour_24() {
        assert!(parse_amz_date("20250101T240000Z").is_none());
    }

    #[test]
    fn parse_amz_date_min_60() {
        assert!(parse_amz_date("20250101T006000Z").is_none());
    }

    #[test]
    fn parse_amz_date_sec_60() {
        assert!(parse_amz_date("20250101T000060Z").is_none());
    }

    #[test]
    fn parse_amz_date_before_1970() {
        assert!(parse_amz_date("19690101T000000Z").is_none());
    }

    #[test]
    fn uri_encode_path_percent_at_end() {
        // % at end of string without enough following chars
        assert_eq!(uri_encode_path("/a%"), "/a%25");
        assert_eq!(uri_encode_path("/a%2"), "/a%252");
    }

    #[test]
    fn uri_encode_path_invalid_percent_hex() {
        // %ZZ is not valid hex — should be re-encoded
        assert_eq!(uri_encode_path("/a%ZZ"), "/a%25ZZ");
    }

    #[test]
    fn uri_encode_path_non_ascii_byte() {
        // Non-unreserved ASCII byte that's not % or /
        assert_eq!(uri_encode_path("/hello world"), "/hello%20world");
        assert_eq!(uri_encode_path("/a+b"), "/a%2Bb");
    }

    #[test]
    fn percent_decode_upper_and_lower_hex() {
        assert_eq!(percent_decode("%2f"), "/");
        assert_eq!(percent_decode("%2F"), "/");
    }

    #[test]
    fn percent_decode_percent_at_end() {
        assert_eq!(percent_decode("abc%"), "abc%");
        assert_eq!(percent_decode("abc%2"), "abc%2");
    }

    #[test]
    fn percent_decode_invalid_hex() {
        assert_eq!(percent_decode("%GG"), "%GG");
    }

    #[test]
    fn hex_val_boundaries() {
        assert_eq!(hex_val(b'/'), None); // just before '0'
        assert_eq!(hex_val(b':'), None); // just after '9'
        assert_eq!(hex_val(b'`'), None); // just before 'a'
        assert_eq!(hex_val(b'g'), None); // just after 'f'
        assert_eq!(hex_val(b'@'), None); // just before 'A'
        assert_eq!(hex_val(b'G'), None); // just after 'F'
    }

    #[test]
    fn is_leap_coverage() {
        assert!(is_leap(2000)); // divisible by 400
        assert!(!is_leap(1900)); // divisible by 100 but not 400
        assert!(is_leap(2024)); // divisible by 4 but not 100
        assert!(!is_leap(2023)); // not divisible by 4
    }

    #[test]
    fn days_since_epoch_leap_year_feb() {
        // 2024-03-01 should include the leap day
        let days_mar1 = days_since_epoch(2024, 3, 1).unwrap();
        let days_feb28 = days_since_epoch(2024, 2, 28).unwrap();
        assert_eq!(days_mar1 - days_feb28, 2); // Feb 29 + Mar 1
    }

    #[test]
    fn canonical_headers_empty() {
        assert_eq!(canonical_headers(&[]), "");
    }

    #[test]
    fn canonical_query_string_key_only_no_equals() {
        assert_eq!(canonical_query_string("lifecycle"), "lifecycle=");
    }

    proptest! {
        #[test]
        fn prop_canonical_query_order_independent(
            pairs in proptest::collection::vec(
                (
                    proptest::string::string_regex(r"[A-Za-z0-9._~% -]{0,16}").unwrap(),
                    proptest::string::string_regex(r"[A-Za-z0-9._~% -]{0,16}").unwrap(),
                ),
                0..=16
            ),
        ) {
            let make_query = |pairs: &[(String, String)]| {
                pairs
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("&")
            };

            let mut reversed = pairs.clone();
            reversed.reverse();

            let q1 = make_query(&pairs);
            let q2 = make_query(&reversed);
            prop_assert_eq!(canonical_query_string(&q1), canonical_query_string(&q2));
        }

        #[test]
        fn prop_canonical_query_no_double_encode(
            pairs in proptest::collection::vec(
                (
                    proptest::string::string_regex(r"[A-Za-z0-9._~ -]{0,16}").unwrap(),
                    proptest::string::string_regex(r"[A-Za-z0-9._~ -]{0,16}").unwrap(),
                ),
                0..=16
            ),
        ) {
            let make_query = |pairs: &[(String, String)]| {
                pairs
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join("&")
            };

            let raw = make_query(&pairs);
            let encoded_pairs: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (uri_encode(k), uri_encode(v)))
                .collect();
            let encoded = make_query(&encoded_pairs);

            prop_assert_eq!(canonical_query_string(&raw), canonical_query_string(&encoded));
        }

        #[test]
        fn prop_canonical_headers_sorted_and_stable(
            headers in proptest::collection::vec(
                (
                    proptest::string::string_regex(r"[a-z0-9-]{1,16}").unwrap(),
                    proptest::string::string_regex(r"[A-Za-z0-9 \t]{0,32}").unwrap(),
                ),
                0..=32
            ),
        ) {
            let canonical = canonical_headers(
                &headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<Vec<_>>()
            );
            let parsed = parse_canonical_headers(&canonical);

            // Names must be sorted ascending.
            for w in parsed.windows(2) {
                prop_assert!(w[0].0 <= w[1].0);
            }

            // For each header name, values must appear in original order after normalization.
            let mut expected: std::collections::BTreeMap<String, Vec<String>> = std::collections::BTreeMap::new();
            for (name, value) in &headers {
                expected.entry(name.clone()).or_default().push(normalize_value_ref(value));
            }

            for (name, values) in parsed {
                let exp = expected.remove(&name).unwrap_or_default();
                prop_assert_eq!(values, exp);
            }
        }
    }
}
