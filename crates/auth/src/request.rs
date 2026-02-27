/// High-level request authentication entrypoint.
use ring::hmac;

use crate::canonical::{
    canonical_headers, canonical_query_string, canonical_request, parse_amz_date, sha256_hex,
    string_to_sign,
};
use crate::credential::{CredentialScope, CredentialStore};
use crate::error::AuthError;
use crate::sigv4::{derive_signing_key, parse_auth_header, verify_request};

/// Authentication mode used by the incoming request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    HeaderSigV4,
    PresignedSigV4,
    Anonymous,
}

/// Authenticated request context shared with higher layers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub mode: AuthMode,
    pub access_key_id: Option<String>,
    pub principal: Option<String>,
    pub request_epoch_secs: Option<u64>,
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
    if let Some(auth_header) = header_value(headers, "authorization") {
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
    _expected_region: &str,
    _expected_service: &str,
    now_epoch_secs: u64,
    auth_header: &str,
) -> Result<AuthContext, AuthError> {
    let parsed = parse_auth_header(auth_header)?;
    let body_hash = match header_value(headers, "x-amz-content-sha256") {
        Some("UNSIGNED-PAYLOAD") => "UNSIGNED-PAYLOAD".to_string(),
        Some(hash) => hash.to_string(),
        None => sha256_hex(body),
    };
    let access_key_id =
        verify_request(method, path, query_string, headers, &body_hash, &parsed, store)?;
    let record = store
        .get_record(&access_key_id)
        .ok_or(AuthError::UnknownAccessKey)?;

    validate_record_token_and_expiry(
        record,
        header_value(headers, "x-amz-security-token"),
        now_epoch_secs,
    )?;

    Ok(AuthContext {
        mode: AuthMode::HeaderSigV4,
        access_key_id: Some(access_key_id),
        principal: Some(record.principal.clone()),
        request_epoch_secs: header_value(headers, "x-amz-date").and_then(parse_amz_date),
    })
}

#[allow(clippy::too_many_arguments)]
fn authenticate_presigned(
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
    let algorithm = query_param(query_string, "X-Amz-Algorithm").ok_or(
        AuthError::MissingQueryParam {
            param: "X-Amz-Algorithm",
        },
    )?;
    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Algorithm",
        });
    }

    let credential_raw = query_param(query_string, "X-Amz-Credential").ok_or(
        AuthError::MissingQueryParam {
            param: "X-Amz-Credential",
        },
    )?;
    let credential = parse_credential_scope(&credential_raw)?;
    if !expected_region.is_empty() && credential.region != expected_region {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }
    if !expected_service.is_empty() && credential.service != expected_service {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }

    let signed_headers_raw = query_param(query_string, "X-Amz-SignedHeaders").ok_or(
        AuthError::MissingQueryParam {
            param: "X-Amz-SignedHeaders",
        },
    )?;
    let signed_headers: Vec<String> = signed_headers_raw
        .split(';')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if signed_headers.is_empty() {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-SignedHeaders",
        });
    }

    let request_date = query_param(query_string, "X-Amz-Date").ok_or(
        AuthError::MissingQueryParam {
            param: "X-Amz-Date",
        },
    )?;
    let request_epoch =
        parse_amz_date(&request_date).ok_or(AuthError::InvalidQueryParam {
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
    if now_epoch_secs > request_epoch.saturating_add(expires) {
        return Err(AuthError::RequestExpired);
    }

    let signature = query_param(query_string, "X-Amz-Signature").ok_or(
        AuthError::MissingQueryParam {
            param: "X-Amz-Signature",
        },
    )?;

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
    let canonical_qs =
        canonical_query_string(&query_without_signature(query_string));
    let body_hash = match header_value(headers, "x-amz-content-sha256") {
        Some("UNSIGNED-PAYLOAD") => "UNSIGNED-PAYLOAD".to_string(),
        Some(hash) => hash.to_string(),
        None => {
            if body.is_empty() {
                "UNSIGNED-PAYLOAD".to_string()
            } else {
                sha256_hex(body)
            }
        }
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
    if hex_encode_lower(expected_sig.as_ref()) != signature {
        return Err(AuthError::SignatureMismatch);
    }

    Ok(AuthContext {
        mode: AuthMode::PresignedSigV4,
        access_key_id: Some(credential.access_key_id),
        principal: Some(record.principal.clone()),
        request_epoch_secs: Some(request_epoch),
    })
}

fn parse_credential_scope(value: &str) -> Result<CredentialScope, AuthError> {
    let parts: Vec<&str> = value.splitn(5, '/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
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
            if signed_name == "host" {
                return Err(AuthError::MissingSignedHeader { header: "host" });
            }
            if signed_name == "x-amz-date" {
                return Err(AuthError::MissingSignedHeader {
                    header: "x-amz-date",
                });
            }
            if signed_name == "x-amz-content-sha256" {
                return Err(AuthError::MissingSignedHeader {
                    header: "x-amz-content-sha256",
                });
            }
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
        if request_token != Some(expected_token) {
            return Err(AuthError::InvalidToken);
        }
    }

    Ok(())
}

fn query_param(query: &str, name: &str) -> Option<String> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .find_map(|pair| {
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
    headers
        .iter()
        .find(|(k, _)| *k == name)
        .map(|(_, v)| *v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::{CredentialRecord, SecretKey};

    fn example_store() -> CredentialStore {
        let mut store = CredentialStore::new();
        store.add(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            SecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
        assert_eq!(
            ctx.access_key_id.as_deref(),
            Some("AKIAIOSFODNN7EXAMPLE")
        );
        assert_eq!(ctx.principal.as_deref(), Some("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(ctx.request_epoch_secs, Some(1_369_353_600));
    }

    #[test]
    fn authenticate_missing_auth_header() {
        let store = example_store();
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let err = authenticate_request(
            "GET", "/", "", &headers, b"", &store, "us-east-1", "s3", 0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MissingAuth));
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
            &SecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20240201",
            "us-east-1",
            "s3",
        );
        let sig =
            hex_encode_lower(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()), sts.as_bytes()).as_ref());

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
        assert_eq!(
            ctx.access_key_id.as_deref(),
            Some("AKIAIOSFODNN7EXAMPLE")
        );
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
            &SecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            "20240201",
            "us-east-1",
            "s3",
        );
        let sig =
            hex_encode_lower(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key.as_ref()), sts.as_bytes()).as_ref());

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
    fn authenticate_header_invalid_token() {
        let mut store = example_store();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
        assert!(matches!(err, AuthError::InvalidToken));
    }

    #[test]
    fn authenticate_header_expired_token() {
        let mut store = example_store();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
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
}
