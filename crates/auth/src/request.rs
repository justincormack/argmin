/// High-level request authentication entrypoint.
use std::borrow::Cow;

use ring::hmac;
use s3_types::AccountIdentity;

use crate::canonical::{
    amz_date_matches_date_stamp, canonical_headers, canonical_query_string, canonical_request,
    parse_amz_date, sha256_hex, string_to_sign,
};
use crate::credential::{parse_credential_scope_ref, CredentialStore};
use crate::error::AuthError;
use crate::sigv4::{
    derive_signing_key, parse_auth_header, unsigned_required_headers, verify_request_record,
};
use crate::{
    MAX_AUTHORIZATION_HEADER_LEN, MAX_PRESIGNED_QUERY_LEN, MAX_SIGNED_HEADERS_LEN,
    MAX_SIGNED_HEADER_COUNT,
};

const TRACE_TARGET: &str = "auth";

/// Authentication mode used by the incoming request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    HeaderSigV4,
    PostSigV4,
    PresignedSigV4,
    Anonymous,
}

/// Expected SigV4 signing region for credential-scope validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedSigningRegion<'a> {
    /// The request is authenticated at a regional endpoint and must use that
    /// endpoint's region in the SigV4 credential scope.
    ExactEndpointRegion(&'a str),
    /// A bucket-aware caller will compare the signing region after
    /// authentication, once S3 routing and missing-bucket semantics are known.
    DeferredToBucketRouting,
}

impl<'a> ExpectedSigningRegion<'a> {
    pub(crate) fn exact(self) -> Option<&'a str> {
        match self {
            Self::ExactEndpointRegion(region) => Some(region),
            Self::DeferredToBucketRouting => None,
        }
    }
}

/// Signing context needed for verifying aws-chunked streaming signatures.
#[derive(Clone, PartialEq, Eq)]
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

impl std::fmt::Debug for StreamingSigningContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingSigningContext")
            .field("signing_key", &observability::redacted("sigv4_signing_key"))
            .field(
                "seed_signature",
                &observability::redacted("sigv4_seed_signature"),
            )
            .field("scope", &observability::escaped(&self.scope))
            .field("timestamp", &observability::escaped(&self.timestamp))
            .finish()
    }
}

/// Authenticated request context shared with higher layers.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub mode: AuthMode,
    pub access_key_id: Option<String>,
    pub account: Option<AccountIdentity>,
    pub authorization_profile: crate::AuthorizationProfile,
    pub request_epoch_secs: Option<u64>,
    pub signing_region: Option<String>,
    /// Present when the request uses STREAMING-AWS4-HMAC-SHA256-* content hash.
    pub streaming: Option<StreamingSigningContext>,
}

impl std::fmt::Debug for AuthContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let access_key_id = self.access_key_id.as_deref().map(observability::escaped);
        let signing_region = self.signing_region.as_deref().map(observability::escaped);
        f.debug_struct("AuthContext")
            .field("mode", &self.mode)
            .field("access_key_id", &access_key_id)
            .field("account", &self.account)
            .field("authorization_profile", &self.authorization_profile)
            .field("request_epoch_secs", &self.request_epoch_secs)
            .field("signing_region", &signing_region)
            .field("streaming", &self.streaming)
            .finish()
    }
}

impl AuthContext {
    #[must_use]
    pub const fn anonymous() -> Self {
        Self {
            mode: AuthMode::Anonymous,
            access_key_id: None,
            account: None,
            authorization_profile: crate::AuthorizationProfile::Standard,
            request_epoch_secs: None,
            signing_region: None,
            streaming: None,
        }
    }

    #[must_use]
    pub fn principal(&self) -> Option<&str> {
        self.account.as_ref().map(AccountIdentity::principal)
    }
}

/// Borrowed access to lowercased request headers.
pub trait HeaderSource {
    fn first_value<'a>(&'a self, name: &str) -> Option<&'a str>;

    fn visit<'a, F>(&'a self, f: F)
    where
        F: FnMut(&'a str, &'a str);
}

impl<'h> HeaderSource for [(&'h str, &'h str)] {
    fn first_value<'a>(&'a self, name: &str) -> Option<&'a str> {
        self.iter().find(|(k, _)| *k == name).map(|(_, v)| *v)
    }

    fn visit<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a str, &'a str),
    {
        for (name, value) in self {
            f(name, value);
        }
    }
}

impl<'h, const N: usize> HeaderSource for [(&'h str, &'h str); N] {
    fn first_value<'a>(&'a self, name: &str) -> Option<&'a str> {
        self.as_slice().first_value(name)
    }

    fn visit<'a, F>(&'a self, f: F)
    where
        F: FnMut(&'a str, &'a str),
    {
        self.as_slice().visit(f);
    }
}

impl<'h> HeaderSource for Vec<(&'h str, &'h str)> {
    fn first_value<'a>(&'a self, name: &str) -> Option<&'a str> {
        self.as_slice().first_value(name)
    }

    fn visit<'a, F>(&'a self, f: F)
    where
        F: FnMut(&'a str, &'a str),
    {
        self.as_slice().visit(f);
    }
}

impl HeaderSource for [(String, String)] {
    fn first_value<'a>(&'a self, name: &str) -> Option<&'a str> {
        self.iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    fn visit<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a str, &'a str),
    {
        for (name, value) in self {
            f(name.as_str(), value.as_str());
        }
    }
}

/// Authenticate a SigV4 request and return identity context.
///
/// Pass `ExactEndpointRegion(region)` for normal request authentication. Pass
/// `DeferredToBucketRouting` only when a higher-level caller will validate the
/// SigV4 region after authentication, such as bucket-aware endpoint routing.
#[allow(clippy::too_many_arguments)]
pub fn authenticate_request<H: HeaderSource + ?Sized>(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &H,
    body: &[u8],
    store: &CredentialStore,
    expected_region: ExpectedSigningRegion<'_>,
    expected_service: &str,
    now_epoch_secs: u64,
) -> Result<AuthContext, AuthError> {
    let query = observability::query_summary(query_string);
    observability::trace_scope!(
        TRACE_TARGET,
        "authenticate_request",
        "method={} path={} has_query={} query_params={} sigv4_query={}",
        observability::escaped(method),
        observability::escaped(path),
        query.has_query(),
        query.param_count(),
        query.has_sigv4_params()
    );
    let mut authorization_header = None;
    let mut authorization_count = 0usize;
    headers.visit(|name, value| {
        if name == "authorization" {
            authorization_count += 1;
            if authorization_count == 1 {
                authorization_header = Some(value);
            }
        }
    });
    if authorization_count > 1 {
        return Err(AuthError::DuplicateAuthorizationHeader);
    }

    if let Some(auth_header) = authorization_header {
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

    if query_param_lossy(query_string, "X-Amz-Algorithm").is_some() {
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
    if headers.first_value("x-amz-date").is_some() {
        return Err(AuthError::AccessDenied);
    }

    Err(AuthError::MissingAuth)
}

#[allow(clippy::too_many_arguments)]
fn authenticate_header<H: HeaderSource + ?Sized>(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &H,
    body: &[u8],
    store: &CredentialStore,
    expected_region: ExpectedSigningRegion<'_>,
    expected_service: &str,
    now_epoch_secs: u64,
    auth_header: &str,
) -> Result<AuthContext, AuthError> {
    let parsed = parse_auth_header(auth_header)?;
    if expected_region
        .exact()
        .is_some_and(|region| parsed.credential.region != region)
        || parsed.credential.service != expected_service
    {
        return Err(AuthError::MalformedAuth);
    }
    let request_epoch_secs = headers.first_value("x-amz-date").and_then(parse_amz_date);
    if request_epoch_secs.is_some_and(|request_epoch| {
        now_epoch_secs.abs_diff(request_epoch) > crate::SIGV4_CLOCK_SKEW_SECS
    }) {
        return Err(AuthError::RequestExpired);
    }
    let body_hash = match headers.first_value("x-amz-content-sha256") {
        Some("UNSIGNED-PAYLOAD") => Cow::Borrowed("UNSIGNED-PAYLOAD"),
        Some(hash) => Cow::Borrowed(hash),
        None => Cow::Owned(sha256_hex(body)),
    };
    let record = verify_request_record(
        method,
        path,
        query_string,
        headers,
        body_hash.as_ref(),
        &parsed,
        store,
    )?;

    validate_static_record_token_and_expiry(
        record,
        headers.first_value("x-amz-security-token"),
        now_epoch_secs,
    )?;

    let crate::sigv4::SigV4Auth {
        credential,
        signed_headers: _,
        signature,
    } = parsed;

    // Build streaming signing context for STREAMING-AWS4-HMAC-SHA256-* requests.
    let streaming = if body_hash.as_ref().starts_with("STREAMING-AWS4-HMAC-SHA256") {
        let timestamp = headers.first_value("x-amz-date").unwrap_or("").to_owned();
        let scope = format!(
            "{}/{}/{}/aws4_request",
            credential.date, credential.region, credential.service
        );
        let signing_key = derive_signing_key(
            &record.secret_key,
            &credential.date,
            &credential.region,
            &credential.service,
        );
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(signing_key.as_ref());
        Some(StreamingSigningContext {
            signing_key: key_bytes,
            seed_signature: signature,
            scope,
            timestamp,
        })
    } else {
        None
    };

    Ok(AuthContext {
        mode: AuthMode::HeaderSigV4,
        access_key_id: Some(credential.access_key_id),
        account: Some(record.account.clone()),
        authorization_profile: record.authorization_profile,
        request_epoch_secs,
        signing_region: Some(credential.region),
        streaming,
    })
}

#[allow(clippy::too_many_arguments)]
fn authenticate_presigned<H: HeaderSource + ?Sized>(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &H,
    _body: &[u8],
    store: &CredentialStore,
    expected_region: ExpectedSigningRegion<'_>,
    expected_service: &str,
    now_epoch_secs: u64,
) -> Result<AuthContext, AuthError> {
    let algorithm =
        query_param_lossy(query_string, "X-Amz-Algorithm").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Algorithm",
        })?;
    if algorithm.as_ref() != "AWS4-HMAC-SHA256" {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Algorithm",
        });
    }

    let credential_raw = query_param_lossy(query_string, "X-Amz-Credential").ok_or(
        AuthError::MissingQueryParam {
            param: "X-Amz-Credential",
        },
    )?;
    let credential = parse_credential_scope_ref(credential_raw.as_ref()).ok_or(
        AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        },
    )?;
    if let Some(region) = expected_region.exact() {
        if credential.region != region {
            return Err(AuthError::InvalidQueryCredentialRegion {
                param: "X-Amz-Credential",
                provided_region: credential.region.to_string(),
                expected_region: region.to_string(),
            });
        }
    }
    if credential.service != expected_service {
        return Err(AuthError::InvalidQueryCredentialService {
            param: "X-Amz-Credential",
            provided_service: credential.service.to_string(),
            expected_service: expected_service.to_string(),
        });
    }

    let signed_headers_raw = query_param_lossy(query_string, "X-Amz-SignedHeaders").ok_or(
        AuthError::MissingQueryParam {
            param: "X-Amz-SignedHeaders",
        },
    )?;
    if signed_headers_raw.is_empty() || signed_headers_raw.len() > MAX_SIGNED_HEADERS_LEN {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-SignedHeaders",
        });
    }
    let signed_headers: Vec<&str> = signed_headers_raw
        .split(';')
        .filter(|s| !s.is_empty())
        .collect();
    if signed_headers.is_empty() || signed_headers.len() > MAX_SIGNED_HEADER_COUNT {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-SignedHeaders",
        });
    }

    let request_date =
        query_param_lossy(query_string, "X-Amz-Date").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Date",
        })?;
    let request_epoch =
        parse_amz_date(request_date.as_ref()).ok_or(AuthError::InvalidQueryParam {
            param: "X-Amz-Date",
        })?;
    if !amz_date_matches_date_stamp(request_date.as_ref(), credential.date) {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
    }

    let expires = query_param_lossy(query_string, "X-Amz-Expires")
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
    if request_epoch > now_epoch_secs.saturating_add(crate::SIGV4_CLOCK_SKEW_SECS) {
        return Err(AuthError::RequestNotYetValid);
    }
    if expires == 0 || now_epoch_secs > request_epoch.saturating_add(expires) {
        return Err(AuthError::PresignedRequestExpired);
    }

    let signature =
        query_param_lossy(query_string, "X-Amz-Signature").ok_or(AuthError::MissingQueryParam {
            param: "X-Amz-Signature",
        })?;
    // AWS treats malformed-looking presigned signature values as a signature
    // mismatch rather than rejecting them at query parsing time.

    let unsigned_headers = presigned_unsigned_required_headers(&signed_headers, headers);
    if !unsigned_headers.is_empty() {
        return Err(AuthError::UnsignedHeaders {
            headers: unsigned_headers,
        });
    }

    let record = store
        .get_record(credential.access_key_id)
        .ok_or(AuthError::UnknownAccessKey)?;
    if !record.enabled {
        return Err(AuthError::UnknownAccessKey);
    }
    let token = query_param_lossy(query_string, "X-Amz-Security-Token");
    let signed_header_token = signed_headers
        .iter()
        .any(|signed_header| signed_header == &"x-amz-security-token")
        .then(|| headers.first_value("x-amz-security-token"))
        .flatten();

    let signed_header_pairs = collect_signed_headers(&signed_headers, headers)?;
    let canonical_hdrs = canonical_headers(&signed_header_pairs);
    let signed_headers_joined = signed_headers.join(";");
    let canonical_qs = canonical_query_string(&query_without_signature(query_string));
    // AWS treats x-amz-content-sha256 as a presigned payload-hash override
    // even when it is not listed in X-Amz-SignedHeaders: UNSIGNED-PAYLOAD is
    // accepted, while another value participates in signature verification.
    // Other unsigned x-amz-* headers are rejected before this point.
    let body_hash = match headers.first_value("x-amz-content-sha256") {
        Some(hash) => Cow::Borrowed(hash),
        None => Cow::Borrowed("UNSIGNED-PAYLOAD"),
    };
    let canonical_req = canonical_request(
        method,
        path,
        &canonical_qs,
        &canonical_hdrs,
        &signed_headers_joined,
        body_hash.as_ref(),
    );
    let canonical_hash = sha256_hex(canonical_req.as_bytes());
    let scope = format!(
        "{}/{}/{}/aws4_request",
        credential.date, credential.region, credential.service
    );
    let sts = string_to_sign(request_date.as_ref(), &scope, &canonical_hash);
    let signing_key = derive_signing_key(
        &record.secret_key,
        credential.date,
        credential.region,
        credential.service,
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
    validate_static_record_token_and_expiry(
        record,
        token.as_deref().or(signed_header_token),
        now_epoch_secs,
    )?;

    Ok(AuthContext {
        mode: AuthMode::PresignedSigV4,
        access_key_id: Some(credential.access_key_id.to_owned()),
        account: Some(record.account.clone()),
        authorization_profile: record.authorization_profile,
        request_epoch_secs: Some(request_epoch),
        signing_region: Some(credential.region.to_owned()),
        streaming: None,
    })
}

fn presigned_unsigned_required_headers<H, S>(signed_headers: &[S], headers: &H) -> Vec<String>
where
    H: HeaderSource + ?Sized,
    S: AsRef<str>,
{
    let mut unsigned_headers = unsigned_required_headers(signed_headers, headers);
    // AWS treats an unsigned x-amz-content-sha256 on presigned URLs as part of
    // signature verification, producing SignatureDoesNotMatch rather than
    // HeadersNotSigned. Other unsigned x-amz-* headers are rejected directly.
    unsigned_headers.retain(|header| header != "x-amz-content-sha256");
    unsigned_headers
}

fn collect_signed_headers<'a, H, S>(
    signed_headers: &[S],
    headers: &'a H,
) -> Result<Vec<(&'a str, &'a str)>, AuthError>
where
    H: HeaderSource + ?Sized,
    S: AsRef<str>,
{
    let mut out: Vec<(&str, &str)> = Vec::new();
    for signed_name in signed_headers {
        let signed_name = signed_name.as_ref();
        let mut found = false;
        headers.visit(|name, value| {
            if name == signed_name {
                out.push((name, value));
                found = true;
            }
        });
        if !found {
            return Err(AuthError::MissingSignedHeader {
                header: signed_name.to_owned(),
            });
        }
    }
    Ok(out)
}

pub(crate) fn validate_static_record_token_and_expiry(
    record: &crate::credential::CredentialRecord,
    request_token: Option<&str>,
    now_epoch_secs: u64,
) -> Result<(), AuthError> {
    if let Some(expiry) = record.expires_at_epoch_secs {
        if now_epoch_secs > expiry {
            return Err(AuthError::ExpiredToken);
        }
    }

    if let Some(token) = request_token {
        return Err(AuthError::UnexpectedSecurityToken {
            token: token.to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
fn query_param(query: &str, name: &str) -> Option<String> {
    query_param_lossy(query, name).map(Cow::into_owned)
}

fn query_param_lossy<'a>(query: &'a str, name: &str) -> Option<Cow<'a, str>> {
    query.split('&').filter(|s| !s.is_empty()).find_map(|pair| {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next()?;
        if key != name {
            return None;
        }
        let val = parts.next().unwrap_or("");
        Some(percent_decode_lossy(val))
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

fn percent_decode_lossy(s: &str) -> Cow<'_, str> {
    if !s.as_bytes().contains(&b'%') {
        return Cow::Borrowed(s);
    }
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
    Cow::Owned(String::from_utf8_lossy(&out).into_owned())
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

#[cfg(test)]
fn header_value<'a, H: HeaderSource + ?Sized>(headers: &'a H, name: &str) -> Option<&'a str> {
    headers.first_value(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::{CredentialRecord, SecretKey};
    use s3_types::AccountIdentity;

    fn account(principal: &str) -> AccountIdentity {
        AccountIdentity::from_principal(principal)
    }

    fn example_store() -> CredentialStore {
        let mut store = CredentialStore::new();
        store.add(
            "AKIAIOSFODNN7EXAMPLE".to_string(),
            SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
        );
        store
    }

    fn aws_example_time() -> u64 {
        parse_amz_date("20130524T000000Z").unwrap()
    }

    fn presigned_example_time() -> u64 {
        parse_amz_date("20240201T120500Z").unwrap()
    }

    fn aws_example_signed_headers() -> [(&'static str, &'static str); 5] {
        [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"),
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
            ("x-amz-date", "20130524T000000Z"),
        ]
    }

    #[test]
    fn authenticate_header_sigv4_success() {
        let store = example_store();
        let headers = aws_example_signed_headers();
        let ctx = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
        .unwrap();
        assert_eq!(ctx.mode, AuthMode::HeaderSigV4);
        assert_eq!(ctx.access_key_id.as_deref(), Some("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(ctx.principal(), Some("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(ctx.request_epoch_secs, Some(1_369_353_600));
    }

    #[test]
    fn authenticate_header_sigv4_rejects_past_skew() {
        let store = example_store();
        let headers = aws_example_signed_headers();
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time() + crate::SIGV4_CLOCK_SKEW_SECS + 1,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::RequestExpired));
    }

    #[test]
    fn authenticate_header_sigv4_rejects_zero_now_skew() {
        let store = example_store();
        let headers = aws_example_signed_headers();
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::RequestExpired));
    }

    #[test]
    fn authenticate_header_sigv4_rejects_future_skew() {
        let store = example_store();
        let headers = aws_example_signed_headers();
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time() - crate::SIGV4_CLOCK_SKEW_SECS - 1,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::RequestExpired));
    }

    #[test]
    fn authenticate_header_sigv4_accepts_max_skew() {
        let store = example_store();
        let headers = aws_example_signed_headers();
        authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time() + crate::SIGV4_CLOCK_SKEW_SECS,
        )
        .unwrap();
    }

    #[test]
    fn authenticate_missing_auth_header() {
        let store = example_store();
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let err = authenticate_request(
            "GET",
            "/",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
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
        let err = authenticate_request(
            "GET",
            "/",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            parse_amz_date("20240201T120500Z").unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::PresignedRequestExpired));
    }

    #[test]
    fn authenticate_presigned_rejects_future_skew() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T122001Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host";
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
        let sts = string_to_sign("20240201T122001Z", scope, &canonical_hash);
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            parse_amz_date("20240201T120500Z").unwrap(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::RequestNotYetValid));
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            account: account("u1"),
            authorization_profile: crate::AuthorizationProfile::Standard,
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
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
            account: account("u1"),
            authorization_profile: crate::AuthorizationProfile::Standard,
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::ExpiredToken));
    }

    // ── Empty authorization header ────────────────────────────────────

    #[test]
    fn authenticate_empty_auth_header() {
        let store = example_store();
        let headers = [("authorization", "   "), ("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            "",
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
    fn presigned_credential_scope_invalid_date() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F2024020X%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
    fn presigned_credential_scope_date_mismatch() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240202%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryCredentialRegion {
                param: "X-Amz-Credential",
                provided_region,
                expected_region,
            } if provided_region == "eu-west-1" && expected_region == "us-east-1"
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidQueryCredentialService {
                param: "X-Amz-Credential",
                provided_service,
                expected_service,
            } if provided_service == "iam" && expected_service == "s3"
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::PresignedRequestExpired));
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            account: account("p"),
            authorization_profile: crate::AuthorizationProfile::Standard,
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::UnknownAccessKey));
    }

    // ── validate_static_record_token_and_expiry ───────────────────────

    #[test]
    fn unexpected_token_rejected_for_static_credential() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            account: account("p"),
            authorization_profile: crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs: None,
            enabled: true,
        };
        let err =
            validate_static_record_token_and_expiry(&record, Some("unexpected"), 0).unwrap_err();
        assert!(matches!(
            err,
            AuthError::UnexpectedSecurityToken { token } if token == "unexpected"
        ));
    }

    #[test]
    fn overlong_unexpected_token_rejected_like_other_static_credential_tokens() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            account: account("p"),
            authorization_profile: crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs: None,
            enabled: true,
        };
        let token = "x".repeat(4097);
        let err = validate_static_record_token_and_expiry(&record, Some(&token), 0).unwrap_err();
        assert!(matches!(
            err,
            AuthError::UnexpectedSecurityToken { token: returned } if returned == token
        ));
    }

    #[test]
    fn epoch_time_before_future_expiry_is_not_expired() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            account: account("p"),
            authorization_profile: crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs: Some(5),
            enabled: true,
        };
        validate_static_record_token_and_expiry(&record, None, 0).unwrap();
    }

    #[test]
    fn no_token_no_expiry_ok() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            account: account("p"),
            authorization_profile: crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs: None,
            enabled: true,
        };
        validate_static_record_token_and_expiry(&record, None, 100).unwrap();
    }

    #[test]
    fn expiry_not_yet_expired_with_nonzero_now() {
        let record = CredentialRecord {
            access_key_id: "AKID".to_string(),
            secret_key: SecretKey::new("s".to_string()),
            account: account("p"),
            authorization_profile: crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs: Some(100),
            enabled: true,
        };
        validate_static_record_token_and_expiry(&record, None, 100).unwrap();
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
        assert_eq!(percent_decode_lossy("hello%20world"), "hello world");
        assert_eq!(percent_decode_lossy("no-encoding"), "no-encoding");
        assert_eq!(percent_decode_lossy("%2F"), "/");
    }

    #[test]
    fn percent_decode_invalid_hex() {
        // %ZZ is not valid hex — should be passed through literally
        assert_eq!(percent_decode_lossy("%ZZ"), "%ZZ");
    }

    #[test]
    fn percent_decode_truncated() {
        // % at end of string — not enough chars for hex pair
        assert_eq!(percent_decode_lossy("abc%"), "abc%");
        assert_eq!(percent_decode_lossy("abc%2"), "abc%2");
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
        .unwrap();
        assert!(ctx.streaming.is_none());
        assert_eq!(ctx.mode, AuthMode::HeaderSigV4);
    }

    // ── Presigned: static credentials reject security token in query ───

    #[test]
    fn presigned_security_token_in_query_with_bad_signature_rejects_signature_first() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            account: account("u1"),
            authorization_profile: crate::AuthorizationProfile::Standard,
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::SignatureMismatch));
    }

    #[test]
    fn presigned_signed_security_token_header_with_bad_signature_rejects_signature_first() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host%3Bx-amz-security-token&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com"), ("x-amz-security-token", "token")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::SignatureMismatch));
    }

    // ── Presigned: expired token ──────────────────────────────────────

    #[test]
    fn presigned_expired_token() {
        let mut store = CredentialStore::new();
        store.add_record(CredentialRecord {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            account: account("u1"),
            authorization_profile: crate::AuthorizationProfile::Standard,
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::MissingSignedHeader { header } if header == "host"
        ));
    }

    #[test]
    fn presigned_unsigned_host_header_rejected() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=x-amz-content-sha256&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [
            ("host", "example.com"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
        ];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::UnsignedHeaders { headers } if headers == ["host"]
        ));
    }

    #[test]
    fn presigned_unsigned_amz_header_rejected() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com"), ("x-amz-meta-unsigned", "value")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::UnsignedHeaders { headers } if headers == ["x-amz-meta-unsigned"]
        ));
    }

    #[test]
    fn presigned_unsigned_security_token_header_rejected() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [("host", "example.com"), ("x-amz-security-token", "token")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::UnsignedHeaders { headers } if headers == ["x-amz-security-token"]
        ));
    }

    #[test]
    fn presigned_unsigned_content_sha256_is_signature_mismatch() {
        let store = example_store();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let headers = [
            ("host", "example.com"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
        ];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            b"",
            &store,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::SignatureMismatch));
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
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
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            0,
        )
        .unwrap_err();
        assert!(matches!(err, AuthError::MalformedAuth));
    }

    #[test]
    fn auth_context_debug_redacts_streaming_secrets_and_escapes_text() {
        let ctx = AuthContext {
            mode: AuthMode::HeaderSigV4,
            access_key_id: Some("AK\r\nID".into()),
            account: Some(AccountIdentity::from_principal("user-123")),
            authorization_profile: crate::AuthorizationProfile::Standard,
            request_epoch_secs: Some(1234),
            signing_region: Some("us-\neast-1".into()),
            streaming: Some(StreamingSigningContext {
                signing_key: [7u8; 32],
                seed_signature: "feedface".into(),
                scope: "20250101/us-east-1/s3/aws4_request".into(),
                timestamp: "20250101T000000Z".into(),
            }),
        };

        let debug = format!("{ctx:?}");
        assert!(debug.contains(r#""AK\r\nID""#));
        assert!(debug.contains(r#""us-\neast-1""#));
        assert!(debug.contains("<redacted:sigv4_signing_key>"));
        assert!(debug.contains("<redacted:sigv4_seed_signature>"));
        assert!(!debug.contains("feedface"));
    }
}
