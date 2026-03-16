/// High-level request authentication entrypoint.
use ring::hmac;

use crate::canonical::{
    canonical_headers, canonical_query_string, canonical_request, parse_amz_date, sha256_hex,
    string_to_sign,
};
use crate::credential::{CredentialScope, CredentialStore};
use crate::error::AuthError;
use crate::sigv4::{derive_signing_key, parse_auth_header, verify_request};
use crate::{
    MAX_ACCESS_KEY_ID_LEN, MAX_AUTHORIZATION_HEADER_LEN, MAX_CREDENTIAL_LEN,
    MAX_PRESIGNED_QUERY_LEN, MAX_SESSION_TOKEN_LEN, MAX_SIGNED_HEADERS_LEN,
    MAX_SIGNED_HEADER_COUNT,
};

const TRACE_TARGET: &str = "auth";

/// Authentication mode used by the incoming request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    HeaderSigV4,
    PresignedSigV4,
    Anonymous,
}

/// Signing context needed for verifying aws-chunked streaming signatures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingSigningContext {
    /// The derived signing key (32 bytes).
    pub signing_key: [u8; 32],
    /// The seed signature from the Authorization header (hex string).
    pub seed_signature: String,
    /// The credential scope string (e.g. "20130524/us-east-1/s3/aws4_request").
    pub scope: String,
    /// The request timestamp (e.g. "20130524T000000Z").
    pub timestamp: String,
}

/// Authenticated request context shared with higher layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub mode: AuthMode,
    pub access_key_id: Option<String>,
    pub principal: Option<String>,
    pub request_epoch_secs: Option<u64>,
    /// Present when the request uses STREAMING-AWS4-HMAC-SHA256-* content hash.
    pub streaming: Option<StreamingSigningContext>,
}

/// Authenticate a request and return identity context.
///
/// Phase 1 behavior: header-based SigV4 only.
#[allow(clippy::too_many_arguments)]
pub fn authenticate_request(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    store: &CredentialStore,
    expected_region: &str,
    expected_service: &str,
    now_epoch_secs: u64,
) -> Result<AuthContext, AuthError> {
    observability::trace_scope!(
        TRACE_TARGET,
        "authenticate_request",
        "method={} path={} query={}",
        method,
        path,
        query_string
    );
    let authorization_headers: Vec<&str> = headers
        .iter()
        .filter_map(|(name, value)| (*name == "authorization").then_some(*value))
        .collect();
    if authorization_headers.len() > 1 {
        return Err(AuthError::DuplicateAuthorizationHeader);
    }

    if let Some(auth_header) = authorization_headers.first().copied() {
        // Empty Authorization header → AccessDenied (AWS behavior)
        if auth_header.trim().is_empty() {
            return Err(AuthError::AccessDenied);
        }
        if auth_header.len() > MAX_AUTHORIZATION_HEADER_LEN {
            return Err(AuthError::MalformedAuth);
        }
        return authenticate_header(
            method,
            path,
            query_string,
            headers,
            body,
            store,
            expected_region,
            expected_service,
            now_epoch_secs,
            auth_header,
        );
    }

    if query_param(query_string, "X-Amz-Algorithm").is_some() {
        if query_string.len() > MAX_PRESIGNED_QUERY_LEN {
            return Err(AuthError::InvalidQueryParam {
                param: "X-Amz-Algorithm",
            });
        }
        return authenticate_presigned(
            method,
            path,
            query_string,
            headers,
            body,
            store,
            expected_region,
            expected_service,
            now_epoch_secs,
        );
    }

    // If x-amz-date is present without Authorization or presigned params,
    // AWS treats this as an incomplete signed request and returns 403.
    if header_value(headers, "x-amz-date").is_some() {
        return Err(AuthError::AccessDenied);
    }

    Err(AuthError::MissingAuth)
}

#[allow(clippy::too_many_arguments)]
fn authenticate_header(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    store: &CredentialStore,
    expected_region: &str,
    expected_service: &str,
    now_epoch_secs: u64,
    auth_header: &str,
) -> Result<AuthContext, AuthError> {
    let parsed = parse_auth_header(auth_header)?;
    if parsed.credential.region != expected_region || parsed.credential.service != expected_service
    {
        return Err(AuthError::MalformedAuth);
    }
    let body_hash = match header_value(headers, "x-amz-content-sha256") {
        Some("UNSIGNED-PAYLOAD") => "UNSIGNED-PAYLOAD".to_string(),
        Some(hash) => hash.to_string(),
        None => sha256_hex(body),
    };
    let access_key_id = verify_request(
        method,
        path,
        query_string,
        headers,
        &body_hash,
        &parsed,
        store,
    )?;
    let record = store
        .get_record(&access_key_id)
        .ok_or(AuthError::UnknownAccessKey)?;

    validate_record_token_and_expiry(
        record,
        header_value(headers, "x-amz-security-token"),
        now_epoch_secs,
    )?;

    // Build streaming signing context for STREAMING-AWS4-HMAC-SHA256-* requests.
    let streaming = if body_hash.starts_with("STREAMING-AWS4-HMAC-SHA256") {
        let timestamp = header_value(headers, "x-amz-date")
            .unwrap_or("")
            .to_string();
        let scope = format!(
            "{}/{}/{}/aws4_request",
            parsed.credential.date, parsed.credential.region, parsed.credential.service
        );
        let signing_key = derive_signing_key(
            &record.secret_key,
            &parsed.credential.date,
            &parsed.credential.region,
            &parsed.credential.service,
        );
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(signing_key.as_ref());
        Some(StreamingSigningContext {
            signing_key: key_bytes,
            seed_signature: parsed.signature.clone(),
            scope,
            timestamp,
        })
    } else {
        None
    };

    Ok(AuthContext {
        mode: AuthMode::HeaderSigV4,
        access_key_id: Some(access_key_id),
        principal: Some(record.principal.clone()),
        request_epoch_secs: header_value(headers, "x-amz-date").and_then(parse_amz_date),
        streaming,
    })
}

#[allow(clippy::too_many_arguments)]
fn authenticate_presigned(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &[(&str, &str)],
    _body: &[u8],
    store: &CredentialStore,
    expected_region: &str,
    expected_service: &str,
    now_epoch_secs: u64,
) -> Result<AuthContext, AuthError> {
    let algorithm =
        query_param(query_string, "X-Amz-Algorithm").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Algorithm",
        })?;
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Algorithm",
        });
    }

    let credential_raw =
        query_param(query_string, "X-Amz-Credential").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Credential",
        })?;
    if credential_raw.is_empty() || credential_raw.len() > MAX_CREDENTIAL_LEN {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }
    let credential = parse_credential_scope(&credential_raw)?;
    if credential.region != expected_region {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }
    if credential.service != expected_service {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }

    let signed_headers_raw =
        query_param(query_string, "X-Amz-SignedHeaders").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-SignedHeaders",
        })?;
    if signed_headers_raw.is_empty() || signed_headers_raw.len() > MAX_SIGNED_HEADERS_LEN {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-SignedHeaders",
        });
    }
    let signed_headers: Vec<String> = signed_headers_raw
        .split(';')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if signed_headers.is_empty() || signed_headers.len() > MAX_SIGNED_HEADER_COUNT {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-SignedHeaders",
        });
    }

    let request_date =
        query_param(query_string, "X-Amz-Date").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Date",
        })?;
    let request_epoch = parse_amz_date(&request_date).ok_or(AuthError::InvalidQueryParam {
        param: "X-Amz-Date",
    })?;

    let expires = query_param(query_string, "X-Amz-Expires")
        .ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Expires",
        })?
        .parse::<u64>()
        .map_err(|_| AuthError::InvalidQueryParam {
            param: "X-Amz-Expires",
        })?;
    if expires > 604_800 {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Expires",
        });
    }
    if expires == 0 || now_epoch_secs > request_epoch.saturating_add(expires) {
        return Err(AuthError::RequestExpired);
    }

    let signature =
        query_param(query_string, "X-Amz-Signature").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Signature",
        })?;
    // AWS treats malformed-looking presigned signature values as a signature
    // mismatch rather than rejecting them at query parsing time.

    let record = store
        .get_record(&credential.access_key_id)
        .ok_or(AuthError::UnknownAccessKey)?;
    if !record.enabled {
        return Err(AuthError::UnknownAccessKey);
    }
    let token = query_param(query_string, "X-Amz-Security-Token")
        .or_else(|| header_value(headers, "x-amz-security-token").map(str::to_string));
    validate_record_token_and_expiry(record, token.as_deref(), now_epoch_secs)?;

    let signed_header_pairs = collect_signed_headers(&signed_headers, headers)?;
    let canonical_hdrs = canonical_headers(&signed_header_pairs);
    let signed_headers_joined = signed_headers.join(";");
    let canonical_qs = canonical_query_string(&query_without_signature(query_string));
    // If the request includes x-amz-content-sha256 as a signed header, use
    // its value (allows presigned PUTs with a known body hash). Otherwise
    // default to UNSIGNED-PAYLOAD (the common case for presigned URLs where
    // the body is unknown at signing time).
    let body_hash = match header_value(headers, "x-amz-content-sha256") {
        Some(hash) => hash.to_string(),
        None => "UNSIGNED-PAYLOAD".to_string(),
    };
    let canonical_req = canonical_request(
        method,
        path,
        &canonical_qs,
        &canonical_hdrs,
        &signed_headers_joined,
        &body_hash,
    );
    let canonical_hash = sha256_hex(canonical_req.as_bytes());
    let scope = format!(
        "{}/{}/{}/aws4_request",
        credential.date, credential.region, credential.service
    );
    let sts = string_to_sign(&request_date, &scope, &canonical_hash);
    let signing_key = derive_signing_key(
        &record.secret_key,
        &credential.date,
        &credential.region,
        &credential.service,
    );
    let expected_sig = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
        sts.as_bytes(),
    );
    // Constant-time comparison to prevent timing attacks on signature values.
    let expected_hex = hex_encode_lower(expected_sig.as_ref());
    if !crate::constant_time_eq(expected_hex.as_bytes(), signature.as_bytes()) {
        return Err(AuthError::SignatureMismatch);
    }

    Ok(AuthContext {
        mode: AuthMode::PresignedSigV4,
        access_key_id: Some(credential.access_key_id),
        principal: Some(record.principal.clone()),
        request_epoch_secs: Some(request_epoch),
        streaming: None,
    })
}

fn parse_credential_scope(value: &str) -> Result<CredentialScope, AuthError> {
    if value.is_empty() || value.len() > MAX_CREDENTIAL_LEN {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }
    let parts: Vec<&str> = value.splitn(5, '/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }
    if parts[0].is_empty() || parts[0].len() > MAX_ACCESS_KEY_ID_LEN {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }
    Ok(CredentialScope {
        access_key_id: parts[0].to_string(),
        date: parts[1].to_string(),
        region: parts[2].to_string(),
        service: parts[3].to_string(),
    })
}

fn collect_signed_headers<'a>(
    signed_headers: &[String],
    headers: &[(&'a str, &'a str)],
) -> Result<Vec<(&'a str, &'a str)>, AuthError> {
    let mut out: Vec<(&str, &str)> = Vec::new();
    for signed_name in signed_headers {
        let mut found = false;
        for (name, value) in headers {
            if *name == signed_name.as_str() {
                out.push((name, value));
                found = true;
            }
        }
        if !found {
            return Err(AuthError::MissingSignedHeader {
                header: signed_name.clone(),
            });
        }
    }
    Ok(out)
}

fn validate_record_token_and_expiry(
    record: &crate::credential::CredentialRecord,
    request_token: Option<&str>,
    now_epoch_secs: u64,
) -> Result<(), AuthError> {
    if let Some(expiry) = record.expires_at_epoch_secs {
        if now_epoch_secs != 0 && now_epoch_secs > expiry {
            return Err(AuthError::ExpiredToken);
        }
    }

    if let Some(expected_token) = record.session_token.as_deref() {
        if request_token.is_some_and(|t| t.len() > MAX_SESSION_TOKEN_LEN) {
            return Err(AuthError::InvalidToken);
        }
        // Constant-time comparison to prevent timing attacks on session tokens.
        let matches = match request_token {
            Some(t) => crate::constant_time_eq(t.as_bytes(), expected_token.as_bytes()),
            None => false,
        };
        if !matches {
            return Err(AuthError::InvalidToken);
        }
    }

    Ok(())
}

fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').filter(|s| !s.is_empty()).find_map(|pair| {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next()?;
        if key != name {
            return None;
        }
        let val = parts.next().unwrap_or("");
        Some(percent_decode(val))
    })
}

fn query_without_signature(query: &str) -> String {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter(|pair| pair.split('=').next().unwrap_or("") != "X-Amz-Signature")
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn header_value<'a>(headers: &[(&'a str, &'a str)], name: &str) -> Option<&'a str> {
    headers.iter().find(|(k, _)| *k == name).map(|(_, v)| *v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::{CredentialRecord, SecretKey};

    fn example_store() -> CredentialStore {
        let mut store = CredentialStore::new();
        store.add(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
        );
        store
    }

    #[test]
    fn authenticate_header_sigv4_success() {
        let store = example_store();
        let headers = [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let ctx = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap();
        assert_eq!(ctx.mode, AuthMode::HeaderSigV4);
        assert_eq!(ctx.access_key_id.as_deref(), Some("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(ctx.principal.as_deref(), Some("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(ctx.request_epoch_secs, Some(1_369_353_600));
    }

    #[test]
    fn authenticate_missing_auth_header() {
        let store = example_store();
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let err = authenticate_request("GET", "/", "", &headers, b"", &store, "us-east-1", "s3", 0)
            .unwrap_err();
        assert!(matches!(err, AuthError::MissingAuth));
    }

    #[test]
    fn authenticate_amz_date_without_auth_is_denied() {
        let store = example_store();
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let err = authenticate_request("GET", "/", "", &headers, b"", &store, "us-east-1", "s3", 0)
            .unwrap_err();
        assert!(matches!(err, AuthError::AccessDenied));
    }

    #[test]
    fn authenticate_presigned_success() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host";
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let query_for_sig = canonical_query_string(query);
        let canonical_req = canonical_request(
            "GET",
            "/hello.txt",
            &query_for_sig,
            "host:examplebucket.s3.amazonaws.com\n",
            "host",
            "UNSIGNED-PAYLOAD",
        );
        let canonical_hash = sha256_hex(canonical_req.as_bytes());
        let scope = "20240201/us-east-1/s3/aws4_request";
        let sts = string_to_sign("20240201T120000Z", scope, &canonical_hash);
        let key = derive_signing_key(
            &SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20240201",
            "us-east-1",
            "s3",
        );
        let sig = hex_encode_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );

        let full_query = format!("{query}&X-Amz-Signature={sig}");
        let ctx = authenticate_request(
            "GET",
            "/hello.txt",
            &full_query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            parse_amz_date("20240201T120500Z").unwrap(),
        )
        .unwrap();
        assert_eq!(ctx.mode, AuthMode::PresignedSigV4);
        assert_eq!(ctx.access_key_id.as_deref(), Some("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn authenticate_presigned_success_with_signed_content_sha256() {
        let store = example_store();
        let body_hash = sha256_hex(b"known presigned payload");
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host;x-amz-content-sha256";
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", body_hash.as_str()),
        ];
        let query_for_sig = canonical_query_string(query);
        let canonical_req = canonical_request(
            "PUT",
            "/hello.txt",
            &query_for_sig,
            &format!(
                "host:examplebucket.s3.amazonaws.com\nx-amz-content-sha256:{}\n",
                body_hash
            ),
            "host;x-amz-content-sha256",
            &body_hash,
        );
        let canonical_hash = sha256_hex(canonical_req.as_bytes());
        let scope = "20240201/us-east-1/s3/aws4_request";
        let sts = string_to_sign("20240201T120000Z", scope, &canonical_hash);
        let key = derive_signing_key(
            &SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20240201",
            "us-east-1",
            "s3",
        );
        let sig = hex_encode_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );

        let full_query = format!("{query}&X-Amz-Signature={sig}");
        let ctx = authenticate_request(
            "PUT",
            "/hello.txt",
            &full_query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            parse_amz_date("20240201T120500Z").unwrap(),
        )
        .unwrap();
        assert_eq!(ctx.mode, AuthMode::PresignedSigV4);
    }

    #[test]
    fn authenticate_presigned_expired() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=1&X-Amz-SignedHeaders=host";
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let query_for_sig = canonical_query_string(query);
        let canonical_req = canonical_request(
            "GET",
            "/hello.txt",
            &query_for_sig,
            "host:examplebucket.s3.amazonaws.com\n",
            "host",
            "UNSIGNED-PAYLOAD",
        );
        let canonical_hash = sha256_hex(canonical_req.as_bytes());
        let scope = "20240201/us-east-1/s3/aws4_request";
        let sts = string_to_sign("20240201T120000Z", scope, &canonical_hash);
        let key = derive_signing_key(
            &SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20240201",
            "us-east-1",
            "s3",
        );
        let sig = hex_encode_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );

        let full_query = format!("{query}&X-Amz-Signature={sig}");
        let err = authenticate_request(
            "GET",
            "/hello.txt",
            &full_query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            parse_amz_date("20240201T120500Z").unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::RequestExpired));
    }

    #[test]
    fn authenticate_presigned_missing_param() {
        let store = example_store();
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let err = authenticate_request(
            "GET",
            "/hello.txt",
            "X-Amz-Algorithm=AWS4-HMAC-SHA256",
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingQueryParam {
                param: "X-Amz-Credential"
            }
        ));
    }

    #[test]
    fn authenticate_presigned_signature_mismatch() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000";
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let err = authenticate_request(
            "GET",
            "/hello.txt",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            parse_amz_date("20240201T120500Z").unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::SignatureMismatch));
    }

    #[test]
    fn authenticate_header_unsigned_security_token_rejected() {
        // AWS requires x-amz-security-token to be signed; unsigned → UnsignedHeaders.
        let mut store = example_store();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            principal: "u1".to_string(),
            session_token: Some("expected".to_string()),
            expires_at_epoch_secs: None,
            enabled: true,
        });
        let headers = [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-security-token", "wrong"),
        ];
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnsignedHeaders { .. }));
    }

    #[test]
    fn authenticate_header_expired_token() {
        let mut store = example_store();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            principal: "u1".to_string(),
            session_token: None,
            expires_at_epoch_secs: Some(5),
            enabled: true,
        });
        let headers = [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            10,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::ExpiredToken));
    }

    // ── Empty authorization header ────────────────────────────────────

    #[test]
    fn authenticate_empty_auth_header() {
        let store = example_store();
        let headers = [("authorization", "   "), ("host", "example.com")];
        let err = authenticate_request("GET", "/", "", &headers, b"", &store, "us-east-1", "s3", 0)
            .unwrap_err();
        assert!(matches!(err, AuthError::AccessDenied));
    }

    // ── Presigned: invalid algorithm ──────────────────────────────────

    #[test]
    fn presigned_invalid_algorithm() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA1&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Algorithm"
            }
        ));
    }

    // ── Presigned: bad credential scope ───────────────────────────────

    #[test]
    fn presigned_bad_credential_scope() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2Fbad&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Credential"
            }
        ));
    }

    #[test]
    fn presigned_credential_scope_wrong_terminator() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Fnot_aws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Credential"
            }
        ));
    }

    // ── Presigned: region mismatch ────────────────────────────────────

    #[test]
    fn presigned_region_mismatch() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Credential"
            }
        ));
    }

    // ── Presigned: service mismatch ───────────────────────────────────

    #[test]
    fn presigned_service_mismatch() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fiam%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Credential"
            }
        ));
    }

    // ── Presigned: empty signed headers ───────────────────────────────

    #[test]
    fn presigned_empty_signed_headers() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-SignedHeaders"
            }
        ));
    }

    // ── Presigned: invalid date format ────────────────────────────────

    #[test]
    fn presigned_invalid_date() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=not-a-date&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Date"
            }
        ));
    }

    // ── Presigned: non-numeric expires ─────────────────────────────────

    #[test]
    fn presigned_non_numeric_expires() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=abc&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Expires"
            }
        ));
    }

    // ── Presigned: expires > 604800 ───────────────────────────────────

    #[test]
    fn presigned_expires_too_large() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=604801&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryParam {
                param: "X-Amz-Expires"
            }
        ));
    }

    // ── Presigned: expires = 0 ────────────────────────────────────────

    #[test]
    fn presigned_expires_zero() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=0&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::RequestExpired));
    }

    // ── Presigned: missing individual params ──────────────────────────

    #[test]
    fn presigned_missing_signed_headers() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingQueryParam {
                param: "X-Amz-SignedHeaders"
            }
        ));
    }

    #[test]
    fn presigned_missing_date() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingQueryParam {
                param: "X-Amz-Date"
            }
        ));
    }

    #[test]
    fn presigned_missing_expires() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingQueryParam {
                param: "X-Amz-Expires"
            }
        ));
    }

    #[test]
    fn presigned_missing_signature() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingQueryParam {
                param: "X-Amz-Signature"
            }
        ));
    }

    // ── Presigned: disabled key ───────────────────────────────────────

    #[test]
    fn presigned_disabled_key() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("secret".to_string()),
            principal: "p".to_string(),
            session_token: None,
            expires_at_epoch_secs: None,
            enabled: false,
        });
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey));
    }

    // ── Presigned: unknown key ────────────────────────────────────────

    #[test]
    fn presigned_unknown_key() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=BADKEY%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey));
    }

    // ── validate_record_token_and_expiry ──────────────────────────────

    #[test]
    fn token_valid_match() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            principal: "p".to_string(),
            session_token: Some("tok123".to_string()),
            expires_at_epoch_secs: None,
            enabled: true,
        };
        validate_record_token_and_expiry(&record, Some("tok123"), 0).unwrap();
    }

    #[test]
    fn token_mismatch() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            principal: "p".to_string(),
            session_token: Some("expected".to_string()),
            expires_at_epoch_secs: None,
            enabled: true,
        };
        let err = validate_record_token_and_expiry(&record, Some("wrong"), 0).unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn token_missing_when_required() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            principal: "p".to_string(),
            session_token: Some("expected".to_string()),
            expires_at_epoch_secs: None,
            enabled: true,
        };
        let err = validate_record_token_and_expiry(&record, None, 0).unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn expiry_not_checked_when_now_zero() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            principal: "p".to_string(),
            session_token: None,
            expires_at_epoch_secs: Some(5),
            enabled: true,
        };
        // now_epoch_secs == 0 skips expiry check
        validate_record_token_and_expiry(&record, None, 0).unwrap();
    }

    #[test]
    fn no_token_no_expiry_ok() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            principal: "p".to_string(),
            session_token: None,
            expires_at_epoch_secs: None,
            enabled: true,
        };
        validate_record_token_and_expiry(&record, None, 100).unwrap();
    }

    #[test]
    fn expiry_not_yet_expired_with_nonzero_now() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            principal: "p".to_string(),
            session_token: None,
            expires_at_epoch_secs: Some(100),
            enabled: true,
        };
        validate_record_token_and_expiry(&record, None, 100).unwrap();
    }

    // ── Helper function unit tests ────────────────────────────────────

    #[test]
    fn query_param_basic() {
        assert_eq!(query_param("a=1&b=2", "b"), Some("2".to_string()));
        assert_eq!(query_param("a=1&b=2", "c"), None);
        assert_eq!(query_param("", "a"), None);
    }

    #[test]
    fn query_param_percent_encoded() {
        assert_eq!(
            query_param("key=hello%20world", "key"),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn query_param_no_value() {
        assert_eq!(query_param("key", "key"), Some("".to_string()));
    }

    #[test]
    fn query_without_signature_removes_sig() {
        assert_eq!(
            query_without_signature("a=1&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&b=2"),
            "a=1&b=2"
        );
    }

    #[test]
    fn query_without_signature_no_sig() {
        assert_eq!(query_without_signature("a=1&b=2"), "a=1&b=2");
    }

    #[test]
    fn percent_decode_basic() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("no-encoding"), "no-encoding");
        assert_eq!(percent_decode("%2F"), "/");
    }

    #[test]
    fn percent_decode_invalid_hex() {
        // %ZZ is not valid hex — should be passed through literally
        assert_eq!(percent_decode("%ZZ"), "%ZZ");
    }

    #[test]
    fn percent_decode_truncated() {
        // % at end of string — not enough chars for hex pair
        assert_eq!(percent_decode("abc%"), "abc%");
        assert_eq!(percent_decode("abc%2"), "abc%2");
    }

    #[test]
    fn hex_val_coverage() {
        assert_eq!(hex_val(b'0'), Some(0));
        assert_eq!(hex_val(b'9'), Some(9));
        assert_eq!(hex_val(b'a'), Some(10));
        assert_eq!(hex_val(b'f'), Some(15));
        assert_eq!(hex_val(b'A'), Some(10));
        assert_eq!(hex_val(b'F'), Some(15));
        assert_eq!(hex_val(b'g'), None);
        assert_eq!(hex_val(b'G'), None);
        assert_eq!(hex_val(b' '), None);
    }

    #[test]
    fn hex_encode_lower_empty() {
        assert_eq!(hex_encode_lower(&[]), "");
    }

    #[test]
    fn hex_encode_lower_bytes() {
        assert_eq!(hex_encode_lower(&[0x00, 0xff, 0xab]), "00ffab");
    }

    #[test]
    fn header_value_found() {
        let headers = [("host", "example.com"), ("x-amz-date", "20130524T000000Z")];
        assert_eq!(header_value(&headers, "host"), Some("example.com"));
        assert_eq!(header_value(&headers, "missing"), None);
    }

    #[test]
    fn collect_signed_headers_optional_missing_is_rejected() {
        let signed_headers = vec!["x-custom-header".to_string()];
        let headers = [("host", "example.com")];
        let err = collect_signed_headers(&signed_headers, &headers).unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingSignedHeader { header } if header == "x-custom-header"
        ));
    }

    // ── Streaming signing context ─────────────────────────────────────

    #[test]
    fn header_auth_streaming_body_hash() {
        let store = example_store();
        // Use STREAMING-AWS4-HMAC-SHA256-PAYLOAD body hash
        let body_hash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", body_hash),
            ("x-amz-date", "20130524T000000Z"),
        ];
        // Build a valid signature for this request
        let signed_headers_str = "host;x-amz-content-sha256;x-amz-date";
        let canonical_hdrs =
            canonical_headers(&headers.iter().map(|&(k, v)| (k, v)).collect::<Vec<_>>());
        let canonical_qs = canonical_query_string("");
        let creq = crate::canonical::canonical_request(
            "PUT",
            "/test.txt",
            &canonical_qs,
            &canonical_hdrs,
            signed_headers_str,
            body_hash,
        );
        let creq_hash = sha256_hex(creq.as_bytes());
        let scope = "20130524/us-east-1/s3/aws4_request";
        let sts = crate::canonical::string_to_sign("20130524T000000Z", scope, &creq_hash);
        let key = crate::sigv4::derive_signing_key(
            &SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20130524",
            "us-east-1",
            "s3",
        );
        let sig = hex_encode_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );

        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders={}, Signature={}",
            signed_headers_str, sig
        );
        let headers_with_auth: Vec<(&str, &str)> = vec![
            ("authorization", &auth_header),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", body_hash),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let ctx = authenticate_request(
            "PUT",
            "/test.txt",
            "",
            &headers_with_auth,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap();
        assert!(ctx.streaming.is_some());
        let streaming = ctx.streaming.unwrap();
        assert_eq!(streaming.timestamp, "20130524T000000Z");
        assert_eq!(streaming.scope, "20130524/us-east-1/s3/aws4_request");
        assert_eq!(streaming.seed_signature, sig);
    }

    // ── Header auth: body hash from header vs computed ────────────────

    #[test]
    fn header_auth_no_content_sha256_computes_hash() {
        // When x-amz-content-sha256 is absent, body hash is computed from body
        let store = example_store();
        let body = b"test body";
        let body_hash = sha256_hex(body);
        let headers_for_sig = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let signed_headers_str = "host;x-amz-date";
        let canonical_hdrs = canonical_headers(&headers_for_sig);
        let creq = crate::canonical::canonical_request(
            "PUT",
            "/test.txt",
            "",
            &canonical_hdrs,
            signed_headers_str,
            &body_hash,
        );
        let creq_hash = sha256_hex(creq.as_bytes());
        let scope = "20130524/us-east-1/s3/aws4_request";
        let sts = crate::canonical::string_to_sign("20130524T000000Z", scope, &creq_hash);
        let key = crate::sigv4::derive_signing_key(
            &SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20130524",
            "us-east-1",
            "s3",
        );
        let sig = hex_encode_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );
        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders={}, Signature={}",
            signed_headers_str, sig
        );
        let headers_with_auth: Vec<(&str, &str)> = vec![
            ("authorization", &auth_header),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let ctx = authenticate_request(
            "PUT",
            "/test.txt",
            "",
            &headers_with_auth,
            body,
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap();
        assert!(ctx.streaming.is_none());
        assert_eq!(ctx.mode, AuthMode::HeaderSigV4);
    }

    // ── Presigned: security token in query string ─────────────────────

    #[test]
    fn presigned_security_token_in_query() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            principal: "u1".to_string(),
            session_token: Some("session-token-123".to_string()),
            expires_at_epoch_secs: None,
            enabled: true,
        });
        let base_query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-Security-Token=session-token-123&X-Amz-SignedHeaders=host";
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let query_for_sig = canonical_query_string(base_query);
        let canonical_req = canonical_request(
            "GET",
            "/hello.txt",
            &query_for_sig,
            "host:examplebucket.s3.amazonaws.com\n",
            "host",
            "UNSIGNED-PAYLOAD",
        );
        let canonical_hash = sha256_hex(canonical_req.as_bytes());
        let scope = "20240201/us-east-1/s3/aws4_request";
        let sts = string_to_sign("20240201T120000Z", scope, &canonical_hash);
        let key = crate::sigv4::derive_signing_key(
            &SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20240201",
            "us-east-1",
            "s3",
        );
        let sig = hex_encode_lower(
            hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()),
                sts.as_bytes(),
            )
            .as_ref(),
        );
        let full_query = format!("{base_query}&X-Amz-Signature={sig}");
        let ctx = authenticate_request(
            "GET",
            "/hello.txt",
            &full_query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            parse_amz_date("20240201T120500Z").unwrap(),
        )
        .unwrap();
        assert_eq!(ctx.mode, AuthMode::PresignedSigV4);
    }

    // ── Presigned: token mismatch via query ───────────────────────────

    #[test]
    fn presigned_token_mismatch_in_query() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            principal: "u1".to_string(),
            session_token: Some("expected-token".to_string()),
            expires_at_epoch_secs: None,
            enabled: true,
        });
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-Security-Token=wrong-token&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::InvalidToken));
    }

    // ── Presigned: expired token ──────────────────────────────────────

    #[test]
    fn presigned_expired_token() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            principal: "u1".to_string(),
            session_token: None,
            expires_at_epoch_secs: Some(100),
            enabled: true,
        });
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            200,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::ExpiredToken));
    }

    // ── Presigned: collect_signed_headers missing host ─────────────────

    #[test]
    fn presigned_signed_header_host_missing() {
        let store = example_store();
        // host is in SignedHeaders but not in actual headers
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers: [(&str, &str); 0] = [];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingSignedHeader { header } if header == "host"
        ));
    }

    #[test]
    fn presigned_signed_header_amz_date_missing() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host;x-amz-date&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")]; // x-amz-date missing from actual headers
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingSignedHeader { header } if header == "x-amz-date"
        ));
    }

    #[test]
    fn presigned_signed_header_content_sha256_missing() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host;x-amz-content-sha256&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")]; // x-amz-content-sha256 missing
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingSignedHeader { header } if header == "x-amz-content-sha256"
        ));
    }

    #[test]
    fn authenticate_duplicate_authorization_header_rejected() {
        let store = example_store();
        let headers = [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::DuplicateAuthorizationHeader));
    }

    #[test]
    fn authenticate_header_region_mismatch() {
        let store = example_store();
        let headers = [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/eu-west-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn authenticate_header_service_mismatch() {
        let store = example_store();
        let headers = [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/iam/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ];
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            "us-east-1",
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }
}
