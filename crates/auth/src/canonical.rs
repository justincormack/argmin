/// Canonical request construction per AWS SigV4 spec.
use crate::encoding::{hex_encode_lower, hex_val, percent_decode_lossy};

/// SHA-256 hash as lowercase hex string.
pub fn sha256_hex(data: &[u8]) -> String {
    hex_encode_lower(&checksum::sha256::digest(data))
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
/// This preserves any existing %XX sequences verbatim to avoid double-encoding
/// already-encoded bytes in the raw request path.
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
        if byte == b'%'
            && i + 2 < bytes.len()
            && hex_val(bytes[i + 1]).is_some()
            && hex_val(bytes[i + 2]).is_some()
        {
            encoded.push('%');
            encoded.push(bytes[i + 1] as char);
            encoded.push(bytes[i + 2] as char);
            i += 3;
            continue;
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

/// Trim leading/trailing spaces and collapse interior runs of spaces to a
/// single space, matching AWS SigV4 canonical header normalization.
fn normalize_header_value(value: &str) -> String {
    let trimmed = value.trim_matches(' ');
    let mut result = String::with_capacity(trimmed.len());
    let mut prev_was_space = false;
    for ch in trimmed.chars() {
        if ch == ' ' {
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
///
/// AWS S3 canonicalizes percent-decoded invalid UTF-8 through U+FFFD rather
/// than rejecting it or preserving the invalid bytes. This lossy conversion
/// is therefore intentional: changing it would alter SigV4 verification.
/// `test_presigned_invalid_utf8_query_value_matches_replacement_character`
/// pins the behavior against both AWS and the local server.
fn percent_decode(s: &str) -> String {
    percent_decode_lossy(s).into_owned()
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Iso8601UtcOptions {
    pub trim_whitespace: bool,
    pub require_fixed_width_fields: bool,
}

pub fn parse_iso8601_utc_seconds_with_options(
    value: &str,
    options: Iso8601UtcOptions,
) -> Option<u64> {
    let value = if options.trim_whitespace {
        value.trim()
    } else {
        value
    };

    let datetime = value.strip_suffix('Z')?;
    let (datetime, fractional) = datetime
        .split_once('.')
        .map_or((datetime, None), |(prefix, suffix)| (prefix, Some(suffix)));
    if fractional.is_some_and(|part| !part.bytes().all(|b| b.is_ascii_digit())) {
        return None;
    }

    let (date, time) = datetime.split_once('T')?;
    let (year, month, day) = parse_iso8601_date(date, options.require_fixed_width_fields)?;
    let (hour, minute, second) = parse_iso8601_time(time, options.require_fixed_width_fields)?;
    date_time_to_epoch_seconds(year, month, day, hour, minute, second)
}

fn parse_iso8601_date(date: &str, require_fixed_width_fields: bool) -> Option<(i64, u32, u32)> {
    let mut parts = date.split('-');
    let year_part = parts.next()?;
    let month_part = parts.next()?;
    let day_part = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    if require_fixed_width_fields
        && (year_part.len() != 4 || month_part.len() != 2 || day_part.len() != 2)
    {
        return None;
    }

    let year = year_part.parse::<i64>().ok()?;
    let month = month_part.parse::<u32>().ok()?;
    let day = day_part.parse::<u32>().ok()?;
    Some((year, month, day))
}

fn parse_iso8601_time(time: &str, require_fixed_width_fields: bool) -> Option<(u32, u32, u32)> {
    let mut parts = time.split(':');
    let hour_part = parts.next()?;
    let minute_part = parts.next()?;
    let second_part = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    if require_fixed_width_fields
        && (hour_part.len() != 2 || minute_part.len() != 2 || second_part.len() != 2)
    {
        return None;
    }

    let hour = hour_part.parse::<u32>().ok()?;
    let minute = minute_part.parse::<u32>().ok()?;
    let second = second_part.parse::<u32>().ok()?;
    Some((hour, minute, second))
}

fn date_time_to_epoch_seconds(
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<u64> {
    let max_day = days_in_month_i64(year, month)?;
    if day == 0 || day > max_day || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let days = date_to_days_i64(year, month, day)?;
    if days < 0 {
        return None;
    }

    let seconds = days
        .checked_mul(86_400)?
        .checked_add(i64::from(hour) * 3_600)?
        .checked_add(i64::from(minute) * 60)?
        .checked_add(i64::from(second))?;
    u64::try_from(seconds).ok()
}

fn days_in_month_i64(year: i64, month: u32) -> Option<u32> {
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_i64(year) => 29,
        2 => 28,
        _ => return None,
    })
}

fn date_to_days_i64(year: i64, month: u32, day: u32) -> Option<i64> {
    let year = year.checked_sub(i64::from(month <= 2))?;
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year.checked_sub(era.checked_mul(400)?)?;
    let month = i64::from(month);
    let day = i64::from(day);
    let month_offset = month.checked_add(if month > 2 { -3 } else { 9 })?;
    let doy = month_offset
        .checked_mul(153)?
        .checked_add(2)?
        .checked_div(5)?
        .checked_add(day)?
        .checked_sub(1)?;
    let doe = yoe
        .checked_mul(365)?
        .checked_add(yoe / 4)?
        .checked_sub(yoe / 100)?
        .checked_add(doy)?;
    era.checked_mul(146097)?
        .checked_add(doe)?
        .checked_sub(719468)
}

fn is_leap_i64(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Parse an ISO 8601 SigV4 timestamp ("YYYYMMDDTHHMMSSz") into Unix epoch seconds.
/// Returns None if the format is invalid.
pub fn parse_amz_date(ts: &str) -> Option<u64> {
    let bytes = ts.as_bytes();
    // Expected format: "20130524T000000Z" (exactly 16 ASCII bytes)
    if bytes.len() != 16 || bytes[8] != b'T' || bytes[15] != b'Z' {
        return None;
    }
    let days = parse_yyyymmdd_ascii(&bytes[0..8])?;
    let hour = parse_fixed_width_u32_ascii(&bytes[9..11])?;
    let min = parse_fixed_width_u32_ascii(&bytes[11..13])?;
    let sec = parse_fixed_width_u32_ascii(&bytes[13..15])?;

    if hour > 23 || min > 59 || sec > 59 {
        return None;
    }

    Some(days * 86400 + hour as u64 * 3600 + min as u64 * 60 + sec as u64)
}

pub(crate) fn parse_amz_date_stamp(date_stamp: &str) -> Option<u64> {
    parse_yyyymmdd_ascii(date_stamp.as_bytes())
}

pub(crate) fn amz_date_matches_date_stamp(timestamp: &str, date_stamp: &str) -> bool {
    parse_amz_date(timestamp).is_some()
        && parse_amz_date_stamp(date_stamp).is_some()
        && timestamp.as_bytes().get(..8) == Some(date_stamp.as_bytes())
}

fn parse_yyyymmdd_ascii(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }

    let year = parse_fixed_width_u32_ascii(&bytes[0..4])?;
    let month = parse_fixed_width_u32_ascii(&bytes[4..6])?;
    let day = parse_fixed_width_u32_ascii(&bytes[6..8])?;
    days_since_epoch(year, month, day)
}

/// Days from 1970-01-01 to the given date.
fn days_since_epoch(year: u32, month: u32, day: u32) -> Option<u64> {
    if year < 1970 {
        return None;
    }
    if !(1..=12).contains(&month) {
        return None;
    }
    // Cumulative days before each month (non-leap)
    const MONTH_DAYS: [u32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    const DAYS_PER_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let mut max_day = DAYS_PER_MONTH[(month - 1) as usize];
    if month == 2 && is_leap(year) {
        max_day += 1;
    }
    if day == 0 || day > max_day {
        return None;
    }

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

fn parse_fixed_width_u32_ascii(bytes: &[u8]) -> Option<u32> {
    let mut value = 0u32;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u32::from(b - b'0'))?;
    }
    Some(value)
}

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
        assert_eq!(uri_encode_path("/bucket/%2f"), "/bucket/%2f");
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
    fn canonical_headers_preserves_tabs() {
        let headers = [
            ("host", "example.com"),
            ("x-amz-meta-desc", "\thello\tworld\t"),
        ];
        let result = canonical_headers(&headers);
        assert_eq!(
            result,
            "host:example.com\nx-amz-meta-desc:\thello\tworld\t\n"
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
    fn parse_iso8601_utc_seconds_strict_fixed_width() {
        let epoch = parse_iso8601_utc_seconds_with_options(
            "2025-01-01T00:00:00.123Z",
            Iso8601UtcOptions {
                trim_whitespace: false,
                require_fixed_width_fields: true,
            },
        )
        .unwrap();
        assert_eq!(epoch, 1735689600);
        assert!(parse_iso8601_utc_seconds_with_options(
            "2025-1-1T0:0:0Z",
            Iso8601UtcOptions {
                trim_whitespace: false,
                require_fixed_width_fields: true,
            },
        )
        .is_none());
    }

    #[test]
    fn parse_iso8601_utc_seconds_permissive_trimmed() {
        let epoch = parse_iso8601_utc_seconds_with_options(
            " 2025-1-1T0:0:0Z ",
            Iso8601UtcOptions {
                trim_whitespace: true,
                require_fixed_width_fields: false,
            },
        )
        .unwrap();
        assert_eq!(epoch, 1735689600);
    }

    #[test]
    fn parse_iso8601_utc_seconds_rejects_invalid_fractional_and_pre_epoch() {
        assert!(parse_iso8601_utc_seconds_with_options(
            "2025-01-01T00:00:00.xyzZ",
            Iso8601UtcOptions {
                trim_whitespace: false,
                require_fixed_width_fields: true,
            },
        )
        .is_none());
        assert!(parse_iso8601_utc_seconds_with_options(
            "1969-12-31T23:59:59Z",
            Iso8601UtcOptions {
                trim_whitespace: false,
                require_fixed_width_fields: true,
            },
        )
        .is_none());
    }

    #[test]
    fn parse_iso8601_utc_seconds_rejects_overflowing_year_math() {
        assert!(parse_iso8601_utc_seconds_with_options(
            "72000020000000000-2-2T0:0:0Z",
            Iso8601UtcOptions {
                trim_whitespace: true,
                require_fixed_width_fields: false,
            },
        )
        .is_none());
        assert!(date_to_days_i64(72_000_020_000_000_000, 2, 2).is_none());
    }

    #[test]
    fn parse_amz_date_stamp_valid() {
        assert_eq!(parse_amz_date_stamp("20130524"), Some(15849));
    }

    #[test]
    fn parse_amz_date_stamp_invalid() {
        assert!(parse_amz_date_stamp("2013052").is_none());
        assert!(parse_amz_date_stamp("2013052X").is_none());
        assert!(parse_amz_date_stamp("20131324").is_none());
        assert!(parse_amz_date_stamp("20130431").is_none());
    }

    #[test]
    fn amz_date_matches_date_stamp_requires_valid_and_matching_inputs() {
        assert!(amz_date_matches_date_stamp("20130524T000000Z", "20130524"));
        assert!(!amz_date_matches_date_stamp("20130524T000000Z", "20130525"));
        assert!(!amz_date_matches_date_stamp("20130524T000000Z", "2013052X"));
        assert!(!amz_date_matches_date_stamp("bad", "20130524"));
    }

    #[test]
    fn parse_amz_date_invalid_format() {
        assert!(parse_amz_date("").is_none());
        assert!(parse_amz_date("2013-05-24T00:00:00Z").is_none());
        assert!(parse_amz_date("not-a-timestamp").is_none());
        assert!(parse_amz_date("20130524T000000").is_none()); // missing Z
        assert!(parse_amz_date("20130524X000000Z").is_none()); // bad separator
        assert!(parse_amz_date("20130524T000000X").is_none()); // bad suffix
    }

    #[test]
    fn parse_amz_date_invalid_values() {
        assert!(parse_amz_date("20131324T000000Z").is_none()); // month 13
        assert!(parse_amz_date("20130532T000000Z").is_none()); // day 32
        assert!(parse_amz_date("20130431T000000Z").is_none()); // Apr 31
        assert!(parse_amz_date("20130524T250000Z").is_none()); // hour 25
    }

    #[test]
    fn parse_amz_date_rejects_non_ascii_boundary_case() {
        assert!(parse_amz_date("2025010éT000000Z").is_none());
    }

    // ── Property-based tests ────────────────────────────────────────

    fn normalize_value_ref(value: &str) -> String {
        let trimmed = value.trim_matches(' ');
        let mut result = String::with_capacity(trimmed.len());
        let mut prev_was_space = false;
        for ch in trimmed.chars() {
            if ch == ' ' {
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
