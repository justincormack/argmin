/// S3 POST Object authentication (SigV2 and SigV4 form-based).
///
/// SigV2 uses form fields: `AWSAccessKeyId`, `policy`, `signature`
/// SigV4 uses form fields: `x-amz-algorithm`, `x-amz-credential`, `x-amz-date`,
///   `policy`, `x-amz-signature`
use ring::hmac;

use crate::credential::CredentialStore;
use crate::error::AuthError;
use crate::request::{AuthContext, AuthMode};
use crate::sigv4;

/// Authenticate a POST Object request using form fields.
///
/// Returns `Ok(AuthContext)` on success. If no auth fields are present,
/// returns anonymous context.
pub fn authenticate_post(
    access_key_id: Option<&str>,
    policy_b64: Option<&str>,
    signature_b64: Option<&str>,
    store: &CredentialStore,
) -> Result<AuthContext, AuthError> {
    // If no auth fields, treat as anonymous
    let (akid, policy, sig) = match (access_key_id, policy_b64, signature_b64) {
        (None, None, None) => {
            return Ok(AuthContext {
                mode: AuthMode::Anonymous,
                access_key_id: None,
                principal: None,
                request_epoch_secs: None,
            });
        }
        (Some(akid), Some(policy), Some(sig)) => (akid, policy, sig),
        // Partial auth fields → missing signature/policy
        _ => {
            return Err(AuthError::MissingAuth);
        }
    };

    // Look up the secret key
    let record = store.get_record(akid).ok_or(AuthError::UnknownAccessKey)?;
    if !record.enabled {
        return Err(AuthError::UnknownAccessKey);
    }

    // Verify SigV2: signature = base64(HMAC-SHA1(secret_key, base64_policy))
    let expected_mac = hmac::sign(
        &hmac::Key::new(
            hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
            record.secret_key.0.as_bytes(),
        ),
        policy.as_bytes(),
    );

    use base64::Engine;
    let expected_b64 = base64::engine::general_purpose::STANDARD.encode(expected_mac.as_ref());

    let sig_decoded = sig.trim();
    if sig_decoded != expected_b64 {
        return Err(AuthError::SignatureMismatch);
    }

    Ok(AuthContext {
        mode: AuthMode::HeaderSigV4, // Reuse existing mode; could add PostSigV2 later
        access_key_id: Some(akid.to_string()),
        principal: Some(record.principal.clone()),
        request_epoch_secs: None,
    })
}

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

    if expected_hex != signature_hex {
        return Err(AuthError::SignatureMismatch);
    }

    Ok(AuthContext {
        mode: AuthMode::HeaderSigV4,
        access_key_id: Some(access_key_id.to_string()),
        principal: Some(record.principal.clone()),
        request_epoch_secs: None,
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
                let field_name = key.to_ascii_lowercase();
                if field_name == "bucket" {
                    covered_fields.insert("bucket".to_string());
                    if bucket != expected {
                        return Err(PostPolicyError::ConditionFailed("bucket"));
                    }
                } else {
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
    let year: u64 = date_parts[0].parse().ok()?;
    let month: u64 = date_parts[1].parse().ok()?;
    let day: u64 = date_parts[2].parse().ok()?;
    let hour: u64 = time_parts[0].parse().ok()?;
    let min: u64 = time_parts[1].parse().ok()?;
    let sec: u64 = time_parts[2].parse().ok()?;
    Some(date_to_epoch(year, month, day, hour, min, sec))
}

/// Convert a date to approximate epoch seconds.
fn date_to_epoch(year: u64, month: u64, day: u64, hour: u64, min: u64, sec: u64) -> u64 {
    // Days from epoch (1970-01-01) using a simplified calculation
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if is_leap(y) { 366 } else { 365 };
    }
    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        days += month_days[m as usize] as i64;
        if m == 2 && is_leap(year) {
            days += 1;
        }
    }
    days += day as i64 - 1;
    (days as u64) * 86400 + hour * 3600 + min * 60 + sec
}

fn is_leap(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::SecretKey;

    fn test_store() -> CredentialStore {
        let mut store = CredentialStore::new();
        store.add(
            "testAccessKey123".to_string(),
            SecretKey("testSecretKey456".to_string()),
        );
        store
    }

    #[test]
    fn anonymous_when_no_fields() {
        let store = test_store();
        let ctx = authenticate_post(None, None, None, &store).unwrap();
        assert_eq!(ctx.mode, AuthMode::Anonymous);
    }

    #[test]
    fn valid_sigv2_auth() {
        use base64::Engine;
        let store = test_store();
        let policy_b64 = "eyJleHBpcmF0aW9uIjoiMjAzMC0wMS0wMVQwMDowMDowMFoiLCJjb25kaXRpb25zIjpbXX0=";

        // Compute expected signature
        let mac = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"testSecretKey456"),
            policy_b64.as_bytes(),
        );
        let sig = base64::engine::general_purpose::STANDARD.encode(mac.as_ref());

        let ctx = authenticate_post(
            Some("testAccessKey123"),
            Some(policy_b64),
            Some(&sig),
            &store,
        )
        .unwrap();
        assert_eq!(ctx.access_key_id.as_deref(), Some("testAccessKey123"));
    }

    #[test]
    fn bad_access_key() {
        let store = test_store();
        let err =
            authenticate_post(Some("badkey"), Some("policy"), Some("sig"), &store).unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey));
    }

    #[test]
    fn bad_signature() {
        let store = test_store();
        let err = authenticate_post(
            Some("testAccessKey123"),
            Some("policy"),
            Some("badsig=="),
            &store,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::SignatureMismatch));
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
    fn sigv4_post_valid() {
        let store = test_store();
        let policy_b64 = "eyJleHBpcmF0aW9uIjoiMjAzMC0wMS0wMVQwMDowMDowMFoiLCJjb25kaXRpb25zIjpbXX0=";
        let date = "20250101T000000Z";
        let credential = "testAccessKey123/20250101/us-east-1/s3/aws4_request";

        // Compute expected signature
        let signing_key = crate::sigv4::derive_signing_key(
            &SecretKey("testSecretKey456".to_string()),
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
}
