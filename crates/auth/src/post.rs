/// S3 POST Object authentication (SigV4 form-based).
///
/// SigV4 uses form fields: `x-amz-algorithm`, `x-amz-credential`, `x-amz-date`,
/// `policy`, `x-amz-signature`
use crate::credential::parse_credential_scope_ref;
use crate::error::AuthError;
use crate::request::{
    validate_static_credential_has_no_token, validate_static_record_expiry, AuthContext, AuthMode,
    ExpectedSigningRegion,
};
use crate::sigv4;

const TRACE_TARGET: &str = "auth";

#[derive(Clone, Debug, PartialEq, Eq)]
enum PostPolicyCondition {
    BucketExact(String),
    FieldExact { field: String, expected: String },
    StartsWith { field: String, prefix: String },
    Eq { field: String, expected: String },
    ContentLengthRange { min: u64, max: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedPostPolicy {
    conditions: Vec<PostPolicyCondition>,
}

#[derive(Clone, Copy, Debug)]
pub struct ExpectedCredentialScope<'a> {
    region: ExpectedSigningRegion<'a>,
    service: &'a str,
}

impl<'a> ExpectedCredentialScope<'a> {
    #[must_use]
    pub fn new(region: ExpectedSigningRegion<'a>, service: &'a str) -> Self {
        Self { region, service }
    }
}

impl PreparedPostPolicy {
    #[must_use]
    pub fn max_content_length(&self) -> Option<u64> {
        self.conditions
            .iter()
            .filter_map(|condition| match condition {
                PostPolicyCondition::ContentLengthRange { max, .. } => Some(*max),
                _ => None,
            })
            .min()
    }
}

/// SigV4 POST Object form authentication fields.
#[derive(Clone, Copy, Debug)]
pub struct PostSigV4Request<'a> {
    pub algorithm: &'a str,
    pub credential: &'a str,
    pub date: &'a str,
    pub policy_b64: &'a str,
    pub signature_hex: &'a str,
    pub security_token: Option<&'a str>,
}

/// Authenticate a POST Object request using SigV4 form fields.
///
/// SigV4 POST signs the base64-encoded policy directly (no canonical request).
/// Returns `Ok(AuthContext)` on success.
pub fn authenticate_post_sigv4(
    request: PostSigV4Request<'_>,
    provider: &crate::IdentityProvider,
    expected_scope: ExpectedCredentialScope<'_>,
    now_epoch_secs: u64,
) -> Result<AuthContext, AuthError> {
    observability::trace_scope!(
        TRACE_TARGET,
        "authenticate_post_sigv4",
        "algorithm={} credential={}",
        request.algorithm,
        observability::redacted("sigv4_credential")
    );
    // Validate algorithm
    if request.algorithm != "AWS4-HMAC-SHA256" {
        return Err(AuthError::MalformedAuth);
    }

    let credential =
        parse_credential_scope_ref(request.credential).ok_or(AuthError::MalformedAuth)?;
    if let Some(region) = expected_scope.region.exact() {
        if credential.region != region {
            return Err(AuthError::InvalidCredentialScopeRegion {
                param: "X-Amz-Credential",
                credential: request.credential.to_string(),
                provided_region: credential.region.to_string(),
                expected_region: region.to_string(),
            });
        }
    }
    if credential.service != expected_scope.service {
        return Err(AuthError::InvalidCredentialScopeService {
            param: "X-Amz-Credential",
            credential: request.credential.to_string(),
            provided_service: credential.service.to_string(),
            expected_service: expected_scope.service.to_string(),
        });
    }

    if !crate::canonical::amz_date_matches_date_stamp(request.date, credential.date) {
        return Err(AuthError::MalformedAuth);
    }
    let request_epoch_secs =
        crate::canonical::parse_amz_date(request.date).ok_or(AuthError::MalformedAuth)?;

    // Look up the secret key
    let record = provider
        .lookup_long_lived_credential(credential.access_key_id)
        .map_err(AuthError::IdentityProviderFailure)?
        .ok_or_else(|| AuthError::UnknownAccessKey {
            access_key_id: credential.access_key_id.to_string(),
        })?;
    if !record.is_enabled() {
        return Err(AuthError::UnknownAccessKey {
            access_key_id: credential.access_key_id.to_string(),
        });
    }
    validate_static_record_expiry(&record, now_epoch_secs)?;

    // Derive signing key and compute expected signature
    let signing_key = sigv4::derive_signing_key(
        record.secret_key(),
        credential.date,
        credential.region,
        credential.service,
    );
    let expected_sig = sigv4::hmac_sha256(signing_key.as_ref(), request.policy_b64.as_bytes());
    let expected_hex = sigv4::hex_encode(expected_sig.as_ref());

    // Constant-time comparison to prevent timing attacks on signature values.
    if !crate::constant_time_eq(expected_hex.as_bytes(), request.signature_hex.as_bytes()) {
        return Err(AuthError::SignatureMismatch {
            diagnostics: Some(Box::new(crate::SignatureMismatchDiagnostics {
                access_key_id: credential.access_key_id.to_string(),
                string_to_sign: request.policy_b64.to_string(),
                signature_provided: request.signature_hex.to_string(),
                canonical_request: None,
            })),
        });
    }
    validate_static_credential_has_no_token(request.security_token)?;

    Ok(AuthContext {
        mode: AuthMode::PostSigV4,
        access_key_id: Some(credential.access_key_id.to_string()),
        identity: Some(record.identity().clone()),
        authorization_profile: record.authorization_profile(),
        request_epoch_secs: Some(request_epoch_secs),
        signing_region: Some(credential.region.to_string()),
        streaming: None,
    })
}

/// Error from POST policy validation.
///
/// These are distinct from `AuthError` — policy violations are 400 (bad request),
/// not 403 (access denied).
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum PostPolicyError {
    #[error("malformed policy: {0}")]
    Malformed(&'static str),
    #[error("{0}")]
    InvalidDocument(String),
    #[error("policy expired")]
    Expired,
    #[error("policy condition failed: {condition}")]
    ConditionFailed {
        condition: &'static str,
        field: Option<String>,
    },
}

fn parse_post_policy(
    policy_b64: &str,
    now_epoch_secs: u64,
) -> Result<PreparedPostPolicy, PostPolicyError> {
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

    let mut parsed_conditions = Vec::with_capacity(conditions.len());

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
                    parsed_conditions.push(PostPolicyCondition::BucketExact(expected.to_string()));
                } else {
                    parsed_conditions.push(PostPolicyCondition::FieldExact {
                        field: key.to_ascii_lowercase(),
                        expected: expected.to_string(),
                    });
                }
            }
        } else if let Some(arr) = condition.as_array() {
            let op = arr
                .first()
                .and_then(|v| v.as_str())
                .ok_or(PostPolicyError::Malformed(
                    "condition operator must be string",
                ))?;
            if op.eq_ignore_ascii_case("starts-with") {
                if arr.len() != 3 {
                    return Err(PostPolicyError::InvalidDocument(
                        "Invalid Policy: Invalid starts-with: wrong number of arguments."
                            .to_string(),
                    ));
                }
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
                parsed_conditions.push(PostPolicyCondition::StartsWith {
                    field: field_name,
                    prefix: prefix.to_string(),
                });
            } else if op.eq_ignore_ascii_case("eq") {
                if arr.len() != 3 {
                    return Err(PostPolicyError::InvalidDocument(
                        "Invalid Policy: Invalid Eq: wrong number of arguments.".to_string(),
                    ));
                }
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
                parsed_conditions.push(PostPolicyCondition::Eq {
                    field: field_name,
                    expected: expected.to_string(),
                });
            } else if op == "content-length-range" {
                if arr.len() != 3 {
                    return Err(PostPolicyError::InvalidDocument(
                        "Invalid Policy: Invalid content-length-range: wrong number of arguments."
                            .to_string(),
                    ));
                }
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
                parsed_conditions.push(PostPolicyCondition::ContentLengthRange {
                    min: min_i as u64,
                    max: max_i as u64,
                });
            } else {
                return Err(PostPolicyError::InvalidDocument(format!(
                    "Invalid Policy: Invalid Condition: unknown operation '{op}'."
                )));
            }
        } else {
            return Err(PostPolicyError::InvalidDocument(
                "Invalid Policy: Invalid condition test: must be a List or Object.".to_string(),
            ));
        }
    }

    Ok(PreparedPostPolicy {
        conditions: parsed_conditions,
    })
}

/// Decode and validate a POST policy before file ingestion begins.
///
/// This checks all field-dependent constraints and expiration, but does not
/// check `content-length-range` against the final file size.
pub fn prepare_post_policy(
    policy_b64: &str,
    form_fields: &[(&str, &str)],
    bucket: &str,
    now_epoch_secs: u64,
) -> Result<PreparedPostPolicy, PostPolicyError> {
    let prepared = parse_post_policy(policy_b64, now_epoch_secs)?;

    // Track which field names are covered by policy conditions
    let mut covered_fields = std::collections::HashSet::new();

    for condition in &prepared.conditions {
        match condition {
            PostPolicyCondition::BucketExact(expected) => {
                covered_fields.insert("bucket".to_string());
                if bucket != expected {
                    return Err(PostPolicyError::ConditionFailed {
                        condition: "bucket",
                        field: None,
                    });
                }
            }
            PostPolicyCondition::FieldExact { field, expected } => {
                covered_fields.insert(field.clone());
                if find_field(form_fields, field) != Some(expected.as_str()) {
                    return Err(PostPolicyError::ConditionFailed {
                        condition: "exact match",
                        field: Some(field.clone()),
                    });
                }
            }
            PostPolicyCondition::StartsWith { field, prefix } => {
                covered_fields.insert(field.clone());
                let form_val = find_field(form_fields, field).unwrap_or("");
                if !form_val.starts_with(prefix) {
                    return Err(PostPolicyError::ConditionFailed {
                        condition: "starts-with",
                        field: Some(field.clone()),
                    });
                }
            }
            PostPolicyCondition::Eq { field, expected } => {
                covered_fields.insert(field.clone());
                if find_field(form_fields, field) != Some(expected.as_str()) {
                    return Err(PostPolicyError::ConditionFailed {
                        condition: "eq",
                        field: Some(field.clone()),
                    });
                }
            }
            PostPolicyCondition::ContentLengthRange { .. } => {}
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
                    return Err(PostPolicyError::ConditionFailed {
                        condition: "form field not covered by policy",
                        field: Some(lower),
                    });
                }
            }
        }
    }

    Ok(prepared)
}

/// Validate the final uploaded file size against a previously prepared policy.
pub fn validate_prepared_post_policy_size(
    prepared: &PreparedPostPolicy,
    file_size: usize,
) -> Result<(), PostPolicyError> {
    let size = file_size as u64;
    for condition in &prepared.conditions {
        if let PostPolicyCondition::ContentLengthRange { min, max } = condition {
            if size < *min || size > *max {
                return Err(PostPolicyError::ConditionFailed {
                    condition: "content-length-range",
                    field: None,
                });
            }
        }
    }
    Ok(())
}

/// Validate a POST policy document.
///
/// Checks expiration, condition matching, and final size constraints.
pub fn validate_post_policy(
    policy_b64: &str,
    form_fields: &[(&str, &str)],
    file_size: usize,
    bucket: &str,
    now_epoch_secs: u64,
) -> Result<(), PostPolicyError> {
    let prepared = prepare_post_policy(policy_b64, form_fields, bucket, now_epoch_secs)?;
    validate_prepared_post_policy_size(&prepared, file_size)
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
    crate::canonical::parse_iso8601_utc_seconds_with_options(
        s,
        crate::canonical::Iso8601UtcOptions {
            trim_whitespace: false,
            require_fixed_width_fields: true,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::{CredentialStore, SecretKey, StoredCredential};
    use s3_types::AccountIdentity;
    use std::sync::Arc;

    struct FailingIdentityProvider(crate::IdentityProviderError);

    impl crate::IdentityProviderBackend for FailingIdentityProvider {
        fn lookup_long_lived_credential(
            &self,
            _access_key_id: &str,
        ) -> Result<Option<Arc<StoredCredential>>, crate::IdentityProviderError> {
            Err(self.0)
        }

        fn lookup_live_role_identity(
            &self,
            _stable_role_id: &crate::StableRoleId,
        ) -> Result<Option<Arc<crate::LiveRoleIdentity>>, crate::IdentityProviderError> {
            Err(self.0)
        }

        fn find_account_by_canonical_user_id(
            &self,
            _canonical_user_id: &s3_types::CanonicalUserId,
        ) -> Result<Option<AccountIdentity>, crate::IdentityProviderError> {
            Err(self.0)
        }
    }

    fn assert_split_validation_matches(
        policy_b64: &str,
        form_fields: &[(&str, &str)],
        file_size: usize,
        bucket: &str,
        now_epoch_secs: u64,
    ) {
        let wrapper =
            validate_post_policy(policy_b64, form_fields, file_size, bucket, now_epoch_secs);
        let split = prepare_post_policy(policy_b64, form_fields, bucket, now_epoch_secs)
            .and_then(|prepared| validate_prepared_post_policy_size(&prepared, file_size));
        assert_eq!(wrapper, split, "wrapper and split validation diverged");
    }

    fn test_store() -> crate::IdentityProvider {
        let mut store = CredentialStore::new();
        store
            .add(
                "testAccessKey123".to_string(),
                SecretKey::new("testSecretKey456".to_string()),
            )
            .unwrap();
        crate::IdentityProvider::in_memory(store).unwrap()
    }

    fn configured_record(
        access_key_id: &str,
        secret_key: &str,
        principal: &str,
        expires_at_epoch_secs: Option<u64>,
        enabled: bool,
    ) -> crate::credential::StoredCredential {
        crate::credential::StoredCredential::configured(
            access_key_id.to_string(),
            SecretKey::new(secret_key.to_string()),
            AccountIdentity::from_principal(principal),
            crate::ConfiguredPrincipalIdentity::new(principal),
            crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs,
            enabled,
        )
    }

    fn authenticate_post_sigv4(
        algorithm: &str,
        credential: &str,
        date: &str,
        policy_b64: &str,
        signature_hex: &str,
        provider: &crate::IdentityProvider,
        expected_scope: ExpectedCredentialScope<'_>,
    ) -> Result<AuthContext, AuthError> {
        super::authenticate_post_sigv4(
            PostSigV4Request {
                algorithm,
                credential,
                date,
                policy_b64,
                signature_hex,
                security_token: None,
            },
            provider,
            expected_scope,
            0,
        )
    }

    fn signed_test_policy() -> (String, String) {
        let policy_b64 =
            "eyJleHBpcmF0aW9uIjoiMjAzMC0wMS0wMVQwMDowMDowMFoiLCJjb25kaXRpb25zIjpbXX0=".to_string();
        let signing_key = crate::sigv4::derive_signing_key(
            &SecretKey::new("testSecretKey456".to_string()),
            "20250101",
            "us-east-1",
            "s3",
        );
        let sig = crate::sigv4::hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes());
        let sig_hex = crate::sigv4::hex_encode(sig.as_ref());
        (policy_b64, sig_hex)
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
        let (policy_b64, sig_hex) = signed_test_policy();
        let date = "20250101T000000Z";
        let credential = "testAccessKey123/20250101/us-east-1/s3/aws4_request";

        let ctx = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            credential,
            date,
            &policy_b64,
            &sig_hex,
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap();
        assert_eq!(ctx.mode, AuthMode::PostSigV4);
        assert_eq!(ctx.access_key_id.as_deref(), Some("testAccessKey123"));
    }

    #[test]
    fn sigv4_post_provider_failure_is_not_unknown_access_key() {
        let provider = crate::IdentityProvider::new(FailingIdentityProvider(
            crate::IdentityProviderError::Unavailable,
        ))
        .unwrap();
        let (policy_b64, sig_hex) = signed_test_policy();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            &policy_b64,
            &sig_hex,
            &provider,
            ExpectedCredentialScope::new(
                ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
                "s3",
            ),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::IdentityProviderFailure(crate::IdentityProviderError::Unavailable)
        ));
    }

    #[test]
    fn sigv4_post_invalid_provider_record_preserves_failure_kind() {
        let provider = crate::IdentityProvider::new(FailingIdentityProvider(
            crate::IdentityProviderError::InvalidRecord,
        ))
        .unwrap();
        let (policy_b64, sig_hex) = signed_test_policy();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            &policy_b64,
            &sig_hex,
            &provider,
            ExpectedCredentialScope::new(
                ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
                "s3",
            ),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::IdentityProviderFailure(crate::IdentityProviderError::InvalidRecord)
        ));
    }

    #[test]
    fn sigv4_post_unexpected_security_token() {
        let store = test_store();
        let (policy_b64, sig_hex) = signed_test_policy();
        let err = super::authenticate_post_sigv4(
            PostSigV4Request {
                algorithm: "AWS4-HMAC-SHA256",
                credential: "testAccessKey123/20250101/us-east-1/s3/aws4_request",
                date: "20250101T000000Z",
                policy_b64: &policy_b64,
                signature_hex: &sig_hex,
                security_token: Some("unexpected"),
            },
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnexpectedSecurityToken { .. }));
    }

    #[test]
    fn sigv4_post_expired_token() {
        let mut store = CredentialStore::new();
        store
            .add_record(configured_record(
                "testAccessKey123",
                "testSecretKey456",
                "u1",
                Some(100),
                true,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
        let (policy_b64, sig_hex) = signed_test_policy();
        let err = super::authenticate_post_sigv4(
            PostSigV4Request {
                algorithm: "AWS4-HMAC-SHA256",
                credential: "testAccessKey123/20250101/us-east-1/s3/aws4_request",
                date: "20250101T000000Z",
                policy_b64: &policy_b64,
                signature_hex: &sig_hex,
                security_token: None,
            },
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
            101,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::ExpiredToken));
    }

    #[test]
    fn sigv4_post_expired_token_with_bad_signature_reports_expired_token() {
        let mut store = CredentialStore::new();
        store
            .add_record(configured_record(
                "testAccessKey123",
                "testSecretKey456",
                "u1",
                Some(100),
                true,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
        let (policy_b64, _) = signed_test_policy();
        let err = super::authenticate_post_sigv4(
            PostSigV4Request {
                algorithm: "AWS4-HMAC-SHA256",
                credential: "testAccessKey123/20250101/us-east-1/s3/aws4_request",
                date: "20250101T000000Z",
                policy_b64: &policy_b64,
                signature_hex: "0000000000000000000000000000000000000000000000000000000000000000",
                security_token: None,
            },
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
            101,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::ExpiredToken));
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey { .. }));
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
    }

    #[test]
    fn sigv4_post_wrong_service_scope_rejected() {
        let store = test_store();
        let policy_b64 = "eyJleHBpcmF0aW9uIjoiMjAzMC0wMS0wMVQwMDowMDowMFoiLCJjb25kaXRpb25zIjpbXX0=";
        let signing_key = crate::sigv4::derive_signing_key(
            &SecretKey::new("testSecretKey456".to_string()),
            "20250101",
            "us-east-1",
            "execute-api",
        );
        let sig = crate::sigv4::hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes());
        let sig_hex = crate::sigv4::hex_encode(sig.as_ref());

        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/execute-api/aws4_request",
            "20250101T000000Z",
            policy_b64,
            &sig_hex,
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidCredentialScopeService {
                param: "X-Amz-Credential",
                provided_service,
                expected_service,
                ..
            } if provided_service == "execute-api" && expected_service == "s3"
        ));
    }

    #[test]
    fn sigv4_post_wrong_expected_region_rejected() {
        let store = test_store();
        let policy_b64 = "eyJleHBpcmF0aW9uIjoiMjAzMC0wMS0wMVQwMDowMDowMFoiLCJjb25kaXRpb25zIjpbXX0=";
        let signing_key = crate::sigv4::derive_signing_key(
            &SecretKey::new("testSecretKey456".to_string()),
            "20250101",
            "us-west-2",
            "s3",
        );
        let sig = crate::sigv4::hmac_sha256(signing_key.as_ref(), policy_b64.as_bytes());
        let sig_hex = crate::sigv4::hex_encode(sig.as_ref());

        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-west-2/s3/aws4_request",
            "20250101T000000Z",
            policy_b64,
            &sig_hex,
            &store,
            ExpectedCredentialScope::new(
                ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
                "s3",
            ),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidCredentialScopeRegion {
                param: "X-Amz-Credential",
                provided_region,
                expected_region,
                ..
            } if provided_region == "us-west-2" && expected_region == "us-east-1"
        ));
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
            matches!(err, PostPolicyError::ConditionFailed { .. }),
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
    fn split_validation_matches_wrapper_for_established_cases() {
        let uncovered_fields = vec![("key", "obj"), ("acl", "public-read")];
        let uncovered_policy = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!({"key": "obj"}),
        ]);
        let multiple_ranges_policy = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 0, 1024]),
            serde_json::json!(["content-length-range", 10, 100]),
        ]);
        let min_gt_max_policy = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 100, 10]),
        ]);
        let malformed_args_policy = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["content-length-range", 0]),
        ]);
        let expired_policy = future_policy_b64(&[serde_json::json!({"bucket": "b"})]);

        let cases = [
            (&multiple_ranges_policy, Vec::new(), 5usize, "b", 0u64),
            (&multiple_ranges_policy, Vec::new(), 50usize, "b", 0u64),
            (&multiple_ranges_policy, Vec::new(), 150usize, "b", 0u64),
            (&min_gt_max_policy, Vec::new(), 50usize, "b", 0u64),
            (&malformed_args_policy, Vec::new(), 0usize, "b", 0u64),
            (&uncovered_policy, uncovered_fields, 0usize, "b", 0u64),
            (&expired_policy, Vec::new(), 0usize, "b", 99_999_999_999u64),
        ];

        for (policy_b64, form_fields, file_size, bucket, now_epoch_secs) in cases {
            assert_split_validation_matches(
                policy_b64,
                &form_fields,
                file_size,
                bucket,
                now_epoch_secs,
            );
        }
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
        assert!(matches!(
            err,
            PostPolicyError::ConditionFailed {
                condition: "bucket",
                field: None
            }
        ));
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
            PostPolicyError::ConditionFailed {
                condition: "exact match",
                ..
            }
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
            PostPolicyError::ConditionFailed {
                condition: "exact match",
                ..
            }
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
            PostPolicyError::ConditionFailed {
                condition: "starts-with",
                ..
            }
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
    fn policy_short_starts_with_array_condition_is_invalid_document() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["starts-with", "$key"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert_eq!(
            err,
            PostPolicyError::InvalidDocument(
                "Invalid Policy: Invalid starts-with: wrong number of arguments.".to_string()
            )
        );
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
        assert!(matches!(
            err,
            PostPolicyError::ConditionFailed {
                condition: "eq",
                ..
            }
        ));
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

    #[test]
    fn policy_short_eq_array_condition_is_invalid_document() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["eq", "$key"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert_eq!(
            err,
            PostPolicyError::InvalidDocument(
                "Invalid Policy: Invalid Eq: wrong number of arguments.".to_string()
            )
        );
    }

    #[test]
    fn policy_long_eq_array_condition_is_invalid_document() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["eq", "$key", "val", "extra"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert_eq!(
            err,
            PostPolicyError::InvalidDocument(
                "Invalid Policy: Invalid Eq: wrong number of arguments.".to_string()
            )
        );
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
            PostPolicyError::ConditionFailed {
                condition: "content-length-range",
                field: None
            }
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
            PostPolicyError::ConditionFailed {
                condition: "content-length-range",
                field: None
            }
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
        assert_eq!(
            err,
            PostPolicyError::InvalidDocument(
                "Invalid Policy: Invalid content-length-range: wrong number of arguments."
                    .to_string()
            )
        );
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_credential_too_long() {
        let store = test_store();
        let access_key = "A".repeat(crate::MAX_ACCESS_KEY_ID_LEN + 1);
        let credential = format!("{access_key}/20250101/us-east-1/s3/aws4_request");
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            &credential,
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
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
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_date_invalid_char_boundary() {
        let store = test_store();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "testAccessKey123/20250101/us-east-1/s3/aws4_request",
            "2025010éT000000Z",
            "policy",
            "sig",
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn sigv4_post_disabled_key() {
        let mut store = CredentialStore::new();
        store
            .add_record(configured_record("AKID", "secret", "p", None, false))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
        let err = authenticate_post_sigv4(
            "AWS4-HMAC-SHA256",
            "AKID/20250101/us-east-1/s3/aws4_request",
            "20250101T000000Z",
            "policy",
            "sig",
            &store,
            ExpectedCredentialScope::new(ExpectedSigningRegion::DeferredToBucketRouting, "s3"),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey { .. }));
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

    // ── Invalid condition forms ───────────────────────────────────────

    #[test]
    fn policy_unknown_operator_is_invalid_document() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!(["unknown-op", "$key", "val"]),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert_eq!(
            err,
            PostPolicyError::InvalidDocument(
                "Invalid Policy: Invalid Condition: unknown operation 'unknown-op'.".to_string()
            )
        );
    }

    #[test]
    fn policy_scalar_condition_is_invalid_document() {
        let b64 = future_policy_b64(&[
            serde_json::json!({"bucket": "b"}),
            serde_json::json!("just a string"),
        ]);
        let err = validate_post_policy(&b64, &[], 0, "b", 0).unwrap_err();
        assert_eq!(
            err,
            PostPolicyError::InvalidDocument(
                "Invalid Policy: Invalid condition test: must be a List or Object.".to_string()
            )
        );
    }
}
