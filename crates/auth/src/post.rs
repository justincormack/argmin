/// S3 POST Object authentication (SigV4 form-based).
///
/// SigV4 uses form fields: `x-amz-algorithm`, `x-amz-credential`, `x-amz-date`,
/// `policy`, `x-amz-signature`
use crate::credential::CredentialStore;
use crate::error::AuthError;
use crate::request::{AuthContext, AuthMode};
use crate::sigv4;

const TRACE_TARGET: &str = "auth";

/// Authenticate a POST Object request using SigV4 form fields.
///
/// SigV4 POST signs the base64-encoded policy directly (no canonical request).
/// Returns `Ok(AuthContext)` on success.
pub fn authenticate_post_sigv4(
    algorithm: &str,
    credential: &str,
    date: &str,
    policy_b64: &str,
    signature_hex: &str,
    store: &CredentialStore,
) -> Result<AuthContext, AuthError> {
    observability::trace_scope!(
        TRACE_TARGET,
        "authenticate_post_sigv4",
        "algorithm={} credential={}",
        algorithm,
        credential
    );
    // Validate algorithm
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(AuthError::MalformedAuth);
    }

    // Parse credential: AKID/YYYYMMDD/region/service/aws4_request
    let parts: Vec<&str> = credential.splitn(5, '/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(AuthError::MalformedAuth);
    }
    let access_key_id = parts[0];
    if access_key_id.is_empty() {
        return Err(AuthError::MalformedAuth);
    }
    let cred_date = parts[1];
    let region = parts[2];
    let service = parts[3];

    // Validate date in credential matches the short date from x-amz-date
    // x-amz-date is YYYYMMDDTHHMMSSZ, short date is first 8 chars
    if date.len() < 8 || &date[..8] != cred_date {
        return Err(AuthError::MalformedAuth);
    }

    // Look up the secret key
    let record = store
        .get_record(access_key_id)
        .ok_or(AuthError::UnknownAccessKey)?;
    if !record.enabled {
        return Err(AuthError::UnknownAccessKey);
    }

    // Derive signing key and compute expected signature
    let signing_key = sigv4::derive_signing_key(&record.secret_key, cred_date, region, service);
    let expected_sig = sigv4::hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes());
    let expected_hex = sigv4::hex_encode(expected_sig.as_ref());

    // Constant-time comparison to prevent timing attacks on signature values.
    if !crate::constant_time_eq(expected_hex.as_bytes(), signature_hex.as_bytes()) {
        return Err(AuthError::SignatureMismatch);
    }

    Ok(AuthContext {
        mode: AuthMode::HeaderSigV4,
        access_key_id: Some(access_key_id.to_string()),
        account: Some(record.account.clone()),
        request_epoch_secs: None,
        signing_region: Some(region.to_string()),
        streaming: None,
    })
}

/// Error from POST policy validation.
///
/// These are distinct from `AuthError` — policy violations are 400 (bad request),
/// not 403 (access denied).
#[derive(Debug, thiserror::Error)]
pub enum PostPolicyError {
    #[error("malformed policy: {0}")]
    Malformed(&'static str),
    #[error("policy expired")]
    Expired,
    #[error("policy condition failed: {0}")]
    ConditionFailed(&'static str),
}

/// Validate a POST policy document.
///
/// Checks expiration and condition matching.
/// Returns `PostPolicyError` (maps to HTTP 400) on failure.
pub fn validate_post_policy(
    policy_b64: &str,
    form_fields: &[(&str, &str)],
    file_size: usize,
    bucket: &str,
    now_epoch_secs: u64,
) -> Result<(), PostPolicyError> {
    use base64::Engine;

    let policy_bytes = base64::engine::general_purpose::STANDARD
        .decode(policy_b64)
        .map_err(|_| PostPolicyError::Malformed("invalid base64"))?;

    let policy_str = std::str::from_utf8(&policy_bytes)
        .map_err(|_| PostPolicyError::Malformed("invalid UTF-8"))?;

    let policy: serde_json::Value =
        serde_json::from_str(policy_str).map_err(|_| PostPolicyError::Malformed("invalid JSON"))?;

    // Check expiration (case-sensitive key per S3 spec)
    let expiration = policy
        .get("expiration")
        .and_then(|v| v.as_str())
        .ok_or(PostPolicyError::Malformed("missing expiration"))?;

    check_expiration(expiration, now_epoch_secs)?;

    // Check conditions (case-sensitive key per S3 spec)
    let conditions = policy
        .get("conditions")
        .and_then(|v| v.as_array())
        .ok_or(PostPolicyError::Malformed("missing conditions"))?;

    // Empty conditions array is invalid — bucket/key must be constrained
    if conditions.is_empty() {
        return Err(PostPolicyError::Malformed("empty conditions"));
    }

    // Track which field names are covered by policy conditions
    let mut covered_fields = std::collections::HashSet::new();

    for condition in conditions {
        if let Some(obj) = condition.as_object() {
            // Empty condition object is invalid
            if obj.is_empty() {
                return Err(PostPolicyError::Malformed("empty condition object"));
            }
            // Exact match: {"field": "value"}
            for (key, val) in obj {
                let expected = val
                    .as_str()
                    .ok_or(PostPolicyError::Malformed("condition value must be string"))?;
                // "bucket" is a special virtual condition key (not a form field)
                // and must be lowercase — "Bucket" is treated as a regular field.
                if key == "bucket" {
                    covered_fields.insert("bucket".to_string());
                    if bucket != expected {
                        return Err(PostPolicyError::ConditionFailed("bucket"));
                    }
                } else {
                    // Form field condition keys are case-insensitive
                    let field_name = key.to_ascii_lowercase();
                    covered_fields.insert(field_name.clone());
                    let form_val = find_field(form_fields, &field_name);
                    if form_val != Some(expected) {
                        return Err(PostPolicyError::ConditionFailed("exact match"));
                    }
                }
            }
        } else if let Some(arr) = condition.as_array() {
            // Validate content-length-range arg count before the len==3 check
            if let Some(op) = arr.first().and_then(|v| v.as_str()) {
                if op == "content-length-range" && arr.len() != 3 {
                    return Err(PostPolicyError::Malformed(
                        "content-length-range requires exactly 2 arguments",
                    ));
                }
            }
            if arr.len() == 3 {
                let op = arr[0].as_str().ok_or(PostPolicyError::Malformed(
                    "condition operator must be string",
                ))?;
                if op.eq_ignore_ascii_case("starts-with") {
                    let field_ref = arr[1].as_str().ok_or(PostPolicyError::Malformed(
                        "starts-with field must be string",
                    ))?;
                    let prefix = arr[2].as_str().ok_or(PostPolicyError::Malformed(
                        "starts-with prefix must be string",
                    ))?;
                    let field_name = field_ref
                        .strip_prefix('$')
                        .ok_or(PostPolicyError::Malformed(
                            "field reference must start with $",
                        ))?
                        .to_ascii_lowercase();
                    covered_fields.insert(field_name.clone());
                    let form_val = find_field(form_fields, &field_name).unwrap_or("");
                    if !form_val.starts_with(prefix) {
                        return Err(PostPolicyError::ConditionFailed("starts-with"));
                    }
                } else if op.eq_ignore_ascii_case("eq") {
                    let field_ref = arr[1]
                        .as_str()
                        .ok_or(PostPolicyError::Malformed("eq field must be string"))?;
                    let expected = arr[2]
                        .as_str()
                        .ok_or(PostPolicyError::Malformed("eq value must be string"))?;
                    let field_name = field_ref
                        .strip_prefix('$')
                        .ok_or(PostPolicyError::Malformed(
                            "field reference must start with $",
                        ))?
                        .to_ascii_lowercase();
                    covered_fields.insert(field_name.clone());
                    let form_val = find_field(form_fields, &field_name);
                    if form_val != Some(expected) {
                        return Err(PostPolicyError::ConditionFailed("eq"));
                    }
                } else if op == "content-length-range" {
                    // Reject negative values (as_i64 check) and non-integer values
                    let min_i = arr[1].as_i64().ok_or(PostPolicyError::Malformed(
                        "content-length-range min must be integer",
                    ))?;
                    let max_i = arr[2].as_i64().ok_or(PostPolicyError::Malformed(
                        "content-length-range max must be integer",
                    ))?;
                    if min_i < 0 || max_i < 0 {
                        return Err(PostPolicyError::Malformed(
                            "content-length-range values must be non-negative",
                        ));
                    }
                    let min = min_i as u64;
                    let max = max_i as u64;
                    let size = file_size as u64;
                    if size < min || size > max {
                        return Err(PostPolicyError::ConditionFailed("content-length-range"));
                    }
                }
            }
        }
    }

    // Check that every non-exempt form field has a covering condition
    for (field_name, _) in form_fields {
        let lower = field_name.to_ascii_lowercase();
        match lower.as_str() {
            // These fields are part of the auth mechanism itself, not user data.
            // x-ignore-* fields are explicitly exempt per AWS docs.
            "policy" | "x-amz-signature" | "signature" | "awsaccesskeyid" | "file" => {}
            f if f.starts_with("x-ignore-") => {}
            _ => {
                if !covered_fields.contains(&lower) {
                    return Err(PostPolicyError::ConditionFailed(
                        "form field not covered by policy",
                    ));
                }
            }
        }
    }

    Ok(())
}

fn find_field<'a>(fields: &[(&'a str, &'a str)], name: &str) -> Option<&'a str> {
    fields
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| *v)
}

/// Check if a policy expiration timestamp is still valid.
/// Accepts ISO 8601 format: "2025-12-30T12:00:00Z" or "2025-12-30T12:00:00.000Z"
///
/// Returns `Err(Malformed)` for unparseable dates, `Err(Expired)` for past dates.
fn check_expiration(expiration: &str, now_epoch_secs: u64) -> Result<(), PostPolicyError> {
    let epoch = parse_iso8601(expiration)
        .ok_or(PostPolicyError::Malformed("invalid expiration date format"))?;
    if now_epoch_secs > epoch {
        return Err(PostPolicyError::Expired);
    }
    Ok(())
}

/// Parse an ISO 8601 date to epoch seconds.
/// Accepts "YYYY-MM-DDTHH:MM:SSZ" or "YYYY-MM-DDTHH:MM:SS.sssZ".
fn parse_iso8601(s: &str) -> Option<u64> {
    // Must end with 'Z' (UTC)
    let s = s.strip_suffix('Z')?;
    let s = s.split('.').next().unwrap_or(s); // Strip fractional seconds
    let (date, time) = s.split_once('T')?;
    let date_parts: Vec<&str> = date.split('-').collect();
    let time_parts: Vec<&str> = time.split(':').collect();
    if date_parts.len() != 3 || time_parts.len() != 3 {
        return None;
    }

    // Keep the accepted format narrow and bounded so malformed inputs are
    // rejected before conversion and cannot trigger panics or huge loops.
    if date_parts[0].len() != 4
        || date_parts[1].len() != 2
        || date_parts[2].len() != 2
        || time_parts[0].len() != 2
        || time_parts[1].len() != 2
        || time_parts[2].len() != 2
    {
        return None;
    }

    let year: u64 = date_parts[0].parse().ok()?;
    let month: u64 = date_parts[1].parse().ok()?;
    let day: u64 = date_parts[2].parse().ok()?;
    let hour: u64 = time_parts[0].parse().ok()?;
    let min: u64 = time_parts[1].parse().ok()?;
    let sec: u64 = time_parts[2].parse().ok()?;

    if !(1..=12).contains(&month) || hour > 23 || min > 59 || sec > 59 {
        return None;
    }

    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => unreachable!("month range checked above"),
    };
    if day == 0 || day > max_day {
        return None;
    }

    date_to_epoch(year, month, day, hour, min, sec)
}

/// Convert a validated UTC date to epoch seconds.
fn date_to_epoch(year: u64, month: u64, day: u64, hour: u64, min: u64, sec: u64) -> Option<u64> {
    let year = i64::try_from(year).ok()?;
    let month = u32::try_from(month).ok()?;
    let day = u32::try_from(day).ok()?;

    let adjust = if month <= 2 { 1 } else { 0 };
    let y = year.checked_sub(adjust)?;
    let era = if y >= 0 { y } else { y.checked_sub(399)? } / 400;
    let yoe = y - era * 400;
    let month_i = i64::from(month);
    let day_i = i64::from(day);
    let doy = (153 * (month_i + if month > 2 { -3 } else { 9 }) + 2) / 5 + day_i - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let days = u64::try_from(days).ok()?;

    days.checked_mul(86_400)?
        .checked_add(hour.checked_mul(3_600)?)?
        .checked_add(min.checked_mul(60)?)?
        .checked_add(sec)
}

fn is_leap(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::SecretKey;
    use s3_types::AccountIdentity;

    fn test_store() -> CredentialStore {
        let mut store = CredentialStore::new();
        store.add(
            "testAccessKey123".to_string(),
            SecretKey::new("testSecretKey456".to_string()),
        );
        store
    }

    #[test]
    fn check_expiration_future() {
        assert!(check_expiration("2099-12-31T23:59:59Z", 0).is_ok());
    }

    #[test]
    fn check_expiration_past() {
        assert!(matches!(
            check_expiration("2020-01-01T00:00:00Z", 1700000000),
            Err(PostPolicyError::Expired)
        ));
    }

    #[test]
    fn check_expiration_with_millis() {
        assert!(check_expiration("2099-12-31T23:59:59.000Z", 0).is_ok());
    }

    #[test]
    fn check_expiration_invalid_format() {
        assert!(matches!(
            check_expiration("2020-01-01 00:00:00+00:00", 0),
            Err(PostPolicyError::Malformed(_))
        ));
    }

    #[test]
    fn check_expiration_invalid_month_rejected() {
        assert!(matches!(
            check_expiration("2025-14-01T00:00:00Z", 0),
            Err(PostPolicyError::Malformed(_))
        ));
    }

    #[test]
    fn check_expiration_invalid_day_rejected() {
        assert!(matches!(
            check_expiration("2025-02-29T00:00:00Z", 0),
            Err(PostPolicyError::Malformed(_))
        ));
    }

    #[test]
    fn check_expiration_invalid_time_rejected() {
        assert!(matches!(
            check_expiration("2025-01-01T24:00:00Z", 0),
            Err(PostPolicyError::Malformed(_))
        ));
    }

    #[test]
    fn check_expiration_unbounded_year_rejected() {
        assert!(matches!(
            check_expiration("12345-01-01T00:00:00Z", 0),
            Err(PostPolicyError::Malformed(_))
        ));
    }

    #[test]
    fn sigv4_post_valid() {
        let store = test_store();
        let policy_b64 = "eyJleHBpcmF0aW9uIjoiMjAzMC0wMS0wMVQwMDowMDowMFoiLCJjb25kaXRpb25zIjpbXX0=";
        let date = "20250101T000000Z";
        let credential = "testAccessKey123/20250101/us-east-1/s3/aws4_request";

        // Compute expected signature
        let signing_key = crate::sigv4::derive_signing_key(
            &SecretKey::new("testSecretKey456".to_string()),
            "20250101",
            "us-east-1",
            "s3",
        );
        let sig = crate::sigv4::hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes());
        let sig_hex = crate::sigv4::hex_encode(sig.as_ref());

        let ctx = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            credential,
            date,
            policy_b64,
            &sig_hex,
            &store,
        )
        .unwrap();
        assert_eq!(ctx.access_key_id.as_deref(), Some("testAccessKey123"));
    }

    #[test]
    fn sigv4_post_bad_algorithm() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA1",
            "testAccessKey123/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_bad_credential_format() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/bad",
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_date_mismatch() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/s3/aws4_request",
            "20250102T000000Z",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_unknown_key() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "BADKEY/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey));
    }

    #[test]
    fn sigv4_post_bad_signature() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            "policy",
            "0000000000000000000000000000000000000000000000000000000000000000",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::SignatureMismatch));
    }

    fn future_policy_b64(conditions: &[serde_json::Value]) -> String {
        use base64::Engine;
        let policy = serde_json::json!({
            "expiration": "2099-12-31T23:59:59Z",
            "conditions": conditions,
        });
        base64::engine::general_purpose::STANDARD.encode(policy.to_string().as_bytes())
    }

    #[test]
    fn policy_rejects_uncovered_form_field() {
        let policy_b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "my-bucket"}),
            serde_json::json!({"key": "obj"}),
        ]);
        let form_fields = vec![
            ("key", "obj"),
            ("Content-Type", "text/plain"), // no condition for this
        ];
        let err = validate_post_policy(&policy_b64, &form_fields, 0, "my-bucket", 0).unwrap_err();
        assert!(
            matches!(err, PostPolicyError::ConditionFailed(_)),
            "expected ConditionFailed, got {:?}",
            err
        );
    }

    #[test]
    fn policy_allows_exempt_fields() {
        let policy_b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "my-bucket"}),
            serde_json::json!({"key": "obj"}),
        ]);
        // Exempt fields should not require conditions
        let form_fields = vec![
            ("key", "obj"),
            ("policy", "abc"),
            ("x-amz-signature", "deadbeef"),
        ];
        validate_post_policy(&policy_b64, &form_fields, 0, "my-bucket", 0).unwrap();
    }

    #[test]
    fn policy_allows_x_ignore_fields() {
        let policy_b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "my-bucket"}),
            serde_json::json!({"key": "obj"}),
        ]);
        // x-ignore-* fields are exempt from policy coverage per AWS docs
        let form_fields = vec![
            ("key", "obj"),
            ("x-ignore-foo", "bar"),
            ("x-ignore-something", "else"),
        ];
        validate_post_policy(&policy_b64, &form_fields, 0, "my-bucket", 0).unwrap();
    }

    // ── validate_post_policy error paths ──────────────────────────────

    #[test]
    fn policy_invalid_base64() {
        let err = validate_post_policy("!!!not-base64!!!", &[], 0, "b", 0).unwrap_err();
        assert!(matches!(err, PostPolicyError::Malformed("invalid base64")));
    }

    #[test]
    fn policy_invalid_utf8() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode([0xFF, 0xFE]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(err, PostPolicyError::Malformed("invalid UTF-8")));
    }

    #[test]
    fn policy_invalid_json() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(b"not json");
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(err, PostPolicyError::Malformed("invalid JSON")));
    }

    #[test]
    fn policy_missing_expiration() {
        use base64::Engine;
        let json = serde_json::json!({"conditions": [{"bucket": "b"}]});
        let b64 = base64::engine::general_purpose::STANDARD.encode(json.to_string().as_bytes());
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("missing expiration")
        ));
    }

    #[test]
    fn policy_missing_conditions() {
        use base64::Engine;
        let json = serde_json::json!({"expiration": "2099-12-31T23:59:59Z"});
        let b64 = base64::engine::general_purpose::STANDARD.encode(json.to_string().as_bytes());
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("missing conditions")
        ));
    }

    #[test]
    fn policy_empty_conditions() {
        let b64 = future_policy_b64(&[]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("empty conditions")
        ));
    }

    #[test]
    fn policy_empty_condition_object() {
        let b64 = future_policy_b64(&[serde_json::json!({})]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("empty condition object")
        ));
    }

    #[test]
    fn policy_condition_value_not_string() {
        let b64 = future_policy_b64(&[serde_json::json!({"bucket": 42})]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("condition value must be string")
        ));
    }

    #[test]
    fn policy_bucket_mismatch() {
        let b64 = future_policy_b64(&[serde_json::json!({"bucket": "other"})]);
        let err = validate_post_policy(&b64, &[], 0, "my-bucket", 0).unwrap_err();
        assert!(matches!(err, PostPolicyError::ConditionFailed("bucket")));
    }

    #[test]
    fn policy_exact_match_field_mismatch() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!({"key": "expected"}),
        ]);
        let form_fields = vec![("key", "wrong")];
        let err = validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::ConditionFailed("exact match")
        ));
    }

    #[test]
    fn policy_exact_match_field_missing() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!({"key": "expected"}),
        ]);
        let form_fields: Vec<(&str, &str)> = vec![];
        let err = validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::ConditionFailed("exact match")
        ));
    }

    // ── Array-form conditions ─────────────────────────────────────────

    #[test]
    fn policy_starts_with_success() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", "$key", "uploads/"]),
        ]);
        let form_fields = vec![("key", "uploads/photo.jpg")];
        validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_starts_with_failure() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", "$key", "uploads/"]),
        ]);
        let form_fields = vec![("key", "other/photo.jpg")];
        let err = validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::ConditionFailed("starts-with")
        ));
    }

    #[test]
    fn policy_starts_with_missing_dollar() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", "key", "uploads/"]),
        ]);
        let form_fields = vec![("key", "uploads/x")];
        let err = validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("field reference must start with $")
        ));
    }

    #[test]
    fn policy_starts_with_field_not_string() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", 42, "prefix"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("starts-with field must be string")
        ));
    }

    #[test]
    fn policy_starts_with_prefix_not_string() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", "$key", 42]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("starts-with prefix must be string")
        ));
    }

    #[test]
    fn policy_starts_with_missing_field() {
        // starts-with on a field not in the form — form_val defaults to ""
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", "$key", ""]),
        ]);
        // Empty prefix matches anything including empty string
        validate_post_policy(&b64, &[], 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_non_three_element_array_condition_ignored() {
        // Non content-length-range array conditions with len != 3 are ignored.
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", "$key"]),
        ]);
        validate_post_policy(&b64, &[], 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_eq_success() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["eq", "$key", "exact-value"]),
        ]);
        let form_fields = vec![("key", "exact-value")];
        validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_eq_failure() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["eq", "$key", "expected"]),
        ]);
        let form_fields = vec![("key", "wrong")];
        let err = validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap_err();
        assert!(matches!(err, PostPolicyError::ConditionFailed("eq")));
    }

    #[test]
    fn policy_eq_field_not_string() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["eq", 42, "val"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("eq field must be string")
        ));
    }

    #[test]
    fn policy_eq_value_not_string() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["eq", "$key", 42]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("eq value must be string")
        ));
    }

    #[test]
    fn policy_eq_missing_dollar() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["eq", "key", "val"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("field reference must start with $")
        ));
    }

    // ── content-length-range ──────────────────────────────────────────

    #[test]
    fn policy_content_length_range_success() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 0, 1024]),
        ]);
        validate_post_policy(&b64, &[], 512, "b", 0).unwrap();
    }

    #[test]
    fn policy_content_length_range_too_small() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 100, 1024]),
        ]);
        let err = validate_post_policy(&b64, &[], 50, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::ConditionFailed("content-length-range")
        ));
    }

    #[test]
    fn policy_content_length_range_too_large() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 0, 100]),
        ]);
        let err = validate_post_policy(&b64, &[], 200, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::ConditionFailed("content-length-range")
        ));
    }

    #[test]
    fn policy_content_length_range_negative_min() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", -1, 100]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("content-length-range values must be non-negative")
        ));
    }

    #[test]
    fn policy_content_length_range_negative_max() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 0, -1]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("content-length-range values must be non-negative")
        ));
    }

    #[test]
    fn policy_content_length_range_non_integer_min() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", "abc", 100]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("content-length-range min must be integer")
        ));
    }

    #[test]
    fn policy_content_length_range_non_integer_max() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 0, "abc"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("content-length-range max must be integer")
        ));
    }

    #[test]
    fn policy_content_length_range_wrong_arg_count() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 0]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("content-length-range requires exactly 2 arguments")
        ));
    }

    #[test]
    fn policy_array_operator_not_string() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!([42, "$key", "val"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("condition operator must be string")
        ));
    }

    #[test]
    fn policy_case_insensitive_starts_with() {
        // "Starts-With" should match case-insensitively
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["Starts-With", "$key", ""]),
        ]);
        let form_fields = vec![("key", "anything")];
        validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_case_insensitive_eq() {
        // "EQ" should match case-insensitively
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["EQ", "$key", "val"]),
        ]);
        let form_fields = vec![("key", "val")];
        validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_expired() {
        let b64 = future_policy_b64(&[serde_json::json!({"bucket": "b"})]);
        // future_policy_b64 uses 2099 expiration, so use a huge now value
        let err = validate_post_policy(&b64, &[], 0, "b", 99_999_999_999).unwrap_err();
        assert!(matches!(err, PostPolicyError::Expired));
    }

    #[test]
    fn policy_expiration_invalid_format() {
        use base64::Engine;
        let json = serde_json::json!({
            "expiration": "not-a-date",
            "conditions": [{"bucket": "b"}],
        });
        let b64 = base64::engine::general_purpose::STANDARD.encode(json.to_string().as_bytes());
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert!(matches!(
            err,
            PostPolicyError::Malformed("invalid expiration date format")
        ));
    }

    // ── authenticate_post_sigv4 edge cases ────────────────────────────

    #[test]
    fn sigv4_post_credential_wrong_suffix() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/s3/wrong",
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_empty_access_key() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_date_too_short() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/s3/aws4_request",
            "short",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_disabled_key() {
        let mut store = CredentialStore::new();
        store.add_record(crate::credential::CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("secret".to_string()),
            account: AccountIdentity::from_principal("p"),
            session_token: None,
            expires_at_epoch_secs: None,
            enabled: false,
        });
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "AKID/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey));
    }

    // ── parse_iso8601 edge cases ──────────────────────────────────────

    #[test]
    fn parse_iso8601_no_z_suffix() {
        assert!(parse_iso8601("2025-01-01T00:00:00").is_none());
    }

    #[test]
    fn parse_iso8601_no_t_separator() {
        assert!(parse_iso8601("2025-01-01 00:00:00Z").is_none());
    }

    #[test]
    fn parse_iso8601_bad_date_parts() {
        assert!(parse_iso8601("2025-01T00:00:00Z").is_none());
    }

    #[test]
    fn parse_iso8601_bad_time_parts() {
        assert!(parse_iso8601("2025-01-01T00:00Z").is_none());
    }

    #[test]
    fn parse_iso8601_non_numeric() {
        assert!(parse_iso8601("abcd-ef-ghTij:kl:mnZ").is_none());
    }

    #[test]
    fn parse_iso8601_rejects_pre_epoch_year() {
        assert!(parse_iso8601("1969-12-31T23:59:59Z").is_none());
    }

    // ── find_field ────────────────────────────────────────────────────

    #[test]
    fn find_field_case_insensitive() {
        let fields = vec![("Content-Type", "text/plain")];
        assert_eq!(find_field(&fields, "content-type"), Some("text/plain"));
    }

    #[test]
    fn find_field_missing() {
        let fields: Vec<(&str, &str)> = vec![];
        assert_eq!(find_field(&fields, "key"), None);
    }

    // ── Exempt form fields ────────────────────────────────────────────

    #[test]
    fn policy_allows_signature_field() {
        let b64 = future_policy_b64(&[serde_json::json!({"bucket": "b"})]);
        let form_fields = vec![("signature", "old-sig")];
        validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_allows_awsaccesskeyid_field() {
        let b64 = future_policy_b64(&[serde_json::json!({"bucket": "b"})]);
        let form_fields = vec![("AWSAccessKeyId", "AKID")];
        validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap();
    }

    #[test]
    fn policy_allows_file_field() {
        let b64 = future_policy_b64(&[serde_json::json!({"bucket": "b"})]);
        let form_fields = vec![("file", "data")];
        validate_post_policy(&b64, &form_fields, 0, "b", 0).unwrap();
    }

    // ── Unknown operator in 3-element array is silently ignored ───────

    #[test]
    fn policy_unknown_operator_ignored() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["unknown-op", "$key", "val"]),
        ]);
        // The unknown op is skipped, but "key" form field has no covering condition
        // so it should fail if present
        validate_post_policy(&b64, &[], 0, "b", 0).unwrap();
    }

    // ── Condition that is neither object nor array is silently ignored ─

    #[test]
    fn policy_scalar_condition_ignored() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!("just a string"),
        ]);
        validate_post_policy(&b64, &[], 0, "b", 0).unwrap();
    }

    // ── date_to_epoch and is_leap coverage ────────────────────────────

    #[test]
    fn date_to_epoch_leap_year() {
        // 2000-03-01 (2000 is a leap year divisible by 400)
        let epoch = date_to_epoch(2000, 3, 1, 0, 0, 0).unwrap();
        // 2000-01-01 = day 10957 from 1970-01-01
        // Jan: 31, Feb: 29 (leap), so Mar 1 = 10957 + 31 + 29 = 11017
        assert_eq!(epoch, 11017 * 86400);
    }

    #[test]
    fn date_to_epoch_non_leap_century() {
        // 1900 is divisible by 100 but not 400, so not a leap year
        // Test via is_leap directly
        assert!(!is_leap(1900));
        assert!(is_leap(2000));
        assert!(is_leap(2024));
        assert!(!is_leap(2023));
    }
}
