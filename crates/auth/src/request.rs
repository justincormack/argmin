/// High-level request authentication entrypoint.
use std::borrow::Cow;

use crate::canonical::{
    amz_date_matches_date_stamp, canonical_headers, canonical_query_string, canonical_request,
    parse_amz_date, sha256_hex, string_to_sign,
};
use crate::credential::parse_credential_scope_ref;
use crate::encoding::{hex_encode_lower, percent_decode_lossy};
use crate::error::AuthError;
use crate::sigv4::{
    derive_signing_key, parse_auth_header, unsigned_required_headers, verify_request_credential,
    HeaderSigningTimestamp, VerifyRequestCredentialInput,
};
use crate::{
    MAX_AUTHORIZATION_HEADER_LEN, MAX_PRESIGNED_QUERY_LEN, MAX_SIGNED_HEADERS_LEN,
    MAX_SIGNED_HEADER_COUNT,
};
use ring::hmac;

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
#[derive(Clone)]
pub struct StreamingSigningContext {
    /// The derived signing key (32 bytes).
    pub signing_key: [u8; 32],
    /// The seed signature from the Authorization header (hex string).
    pub seed_signature: String,
    /// The credential scope string (e.g. "20130524/us-east-1/s3/aws4_request").
    pub scope: String,
    /// The request timestamp (e.g. "20130524T000000Z").
    pub timestamp: String,
    /// The access key, echoed in chunk SignatureDoesNotMatch bodies.
    pub access_key_id: String,
    /// The seed request's canonical request, echoed in chunk
    /// SignatureDoesNotMatch bodies like AWS does.
    pub seed_canonical_request: String,
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
            .field(
                "access_key_id",
                &observability::escaped(&self.access_key_id),
            )
            .field(
                "seed_canonical_request",
                &observability::redacted("sigv4_seed_canonical_request"),
            )
            .field(
                "seed_canonical_request_len",
                &self.seed_canonical_request.len(),
            )
            .finish()
    }
}

/// Authenticated request context shared with higher layers.
#[derive(Clone)]
pub struct AuthContext {
    pub mode: AuthMode,
    pub access_key_id: Option<String>,
    pub identity: Option<crate::AuthenticatedIdentity>,
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
            .field("identity", &self.identity)
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
            identity: None,
            authorization_profile: crate::AuthorizationProfile::Standard,
            request_epoch_secs: None,
            signing_region: None,
            streaming: None,
        }
    }

    #[must_use]
    pub fn configured_principal(&self) -> Option<&str> {
        self.identity
            .as_ref()?
            .configured_principal()
            .map(crate::ConfiguredPrincipalIdentity::principal)
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
    provider: &crate::IdentityProvider,
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
        if query_param_lossy(query_string, "X-Amz-Algorithm").is_some()
            || query_param_lossy(query_string, "Signature").is_some()
        {
            return Err(AuthError::MultipleAuthMechanisms {
                authorization: auth_header.to_string(),
            });
        }
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
            provider,
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
            provider,
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
    provider: &crate::IdentityProvider,
    expected_region: ExpectedSigningRegion<'_>,
    expected_service: &str,
    now_epoch_secs: u64,
    auth_header: &str,
) -> Result<AuthContext, AuthError> {
    let parsed = parse_auth_header(auth_header)?;
    // S3 uses x-amz-date when present and otherwise permits an ISO-8601 basic
    // Date header. Keep this single selection for skew validation, seed
    // signature verification, and aws-chunked chunk/trailer verification.
    let selected_timestamp = headers
        .first_value("x-amz-date")
        .map(|value| HeaderSigningTimestamp {
            header_name: "x-amz-date",
            value,
        })
        .or_else(|| {
            headers
                .first_value("date")
                .map(|value| HeaderSigningTimestamp {
                    header_name: "date",
                    value,
                })
        });
    let request_epoch_secs = selected_timestamp
        .map(|timestamp| timestamp.value)
        .and_then(parse_amz_date);
    if request_epoch_secs.is_some_and(|request_epoch| {
        now_epoch_secs.abs_diff(request_epoch) > crate::SIGV4_CLOCK_SKEW_SECS
    }) {
        return Err(AuthError::RequestExpired);
    }
    if let Some(region) = expected_region.exact() {
        if parsed.credential.region != region {
            return Err(AuthError::InvalidHeaderCredentialRegion {
                provided_region: parsed.credential.region.to_string(),
                expected_region: region.to_string(),
            });
        }
    }
    if parsed.credential.service != expected_service {
        return Err(AuthError::InvalidHeaderCredentialService {
            provided_service: parsed.credential.service.to_string(),
            expected_service: expected_service.to_string(),
        });
    }
    let body_hash = match headers.first_value("x-amz-content-sha256") {
        Some("UNSIGNED-PAYLOAD") => Cow::Borrowed("UNSIGNED-PAYLOAD"),
        Some(hash) => Cow::Borrowed(hash),
        None => Cow::Owned(sha256_hex(body)),
    };
    let credential = resolve_header_credential(&parsed, headers, provider, now_epoch_secs)?;
    let seed_canonical_request = verify_request_credential(
        VerifyRequestCredentialInput {
            method,
            uri: path,
            query_string,
            headers,
            body_hash: body_hash.as_ref(),
            auth: &parsed,
            timestamp: selected_timestamp,
        },
        &credential,
    )?;

    if credential.long_lived().is_some() {
        validate_static_credential_has_no_token(headers.first_value("x-amz-security-token"))?;
    }

    let crate::sigv4::SigV4Auth {
        credential: credential_scope,
        signed_headers: _,
        signature,
    } = parsed;

    // Build streaming signing context for STREAMING-AWS4-HMAC-SHA256-* requests.
    let streaming = if body_hash.as_ref().starts_with("STREAMING-AWS4-HMAC-SHA256") {
        let timestamp = selected_timestamp
            .map(|timestamp| timestamp.value)
            .expect("successful header SigV4 verification selected a timestamp")
            .to_owned();
        let scope = format!(
            "{}/{}/{}/aws4_request",
            credential_scope.date, credential_scope.region, credential_scope.service
        );
        let signing_key = derive_signing_key(
            credential.secret_key(),
            &credential_scope.date,
            &credential_scope.region,
            &credential_scope.service,
        );
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(signing_key.as_ref());
        Some(StreamingSigningContext {
            signing_key: key_bytes,
            seed_signature: signature,
            scope,
            timestamp,
            access_key_id: credential.access_key_id().to_string(),
            seed_canonical_request,
        })
    } else {
        None
    };

    Ok(AuthContext {
        mode: AuthMode::HeaderSigV4,
        access_key_id: Some(credential_scope.access_key_id),
        identity: Some(credential.identity().clone()),
        authorization_profile: credential.long_lived().map_or(
            crate::AuthorizationProfile::Standard,
            crate::StoredCredential::authorization_profile,
        ),
        request_epoch_secs,
        signing_region: Some(credential_scope.region),
        streaming,
    })
}

fn resolve_header_credential<H: HeaderSource + ?Sized>(
    auth: &crate::SigV4Auth,
    headers: &H,
    provider: &crate::IdentityProvider,
    now_epoch_secs: u64,
) -> Result<crate::AuthenticatedCredential, AuthError> {
    if !crate::is_reserved_session_access_key_id(&auth.credential.access_key_id) {
        let record = provider
            .lookup_long_lived_credential(&auth.credential.access_key_id)
            .map_err(AuthError::IdentityProviderFailure)?
            .ok_or_else(|| unknown_access_key(&auth.credential.access_key_id))?;
        if !record.is_enabled() {
            return Err(unknown_access_key(&auth.credential.access_key_id));
        }
        validate_static_record_expiry(&record, now_epoch_secs)?;
        return Ok(crate::AuthenticatedCredential::LongLived(record));
    }

    let unsigned_headers = unsigned_required_headers(&auth.signed_headers, headers);
    if !unsigned_headers.is_empty() {
        return Err(AuthError::UnsignedHeaders {
            headers: unsigned_headers,
        });
    }
    let token_selection = select_header_session_token(headers, &auth.credential.access_key_id)?;
    authenticate_selected_s3_session_credential(
        provider,
        &auth.credential.access_key_id,
        token_selection,
        now_epoch_secs,
    )
}

fn map_s3_session_authentication_error(
    access_key_id: &str,
    token_selection: &SessionTokenSelection,
    error: crate::SessionCredentialAuthenticationError,
) -> AuthError {
    match error {
        crate::SessionCredentialAuthenticationError::InvalidCredential => {
            unknown_access_key(access_key_id)
        }
        crate::SessionCredentialAuthenticationError::InvalidToken => {
            token_selection.selected.as_deref().map_or_else(
                || unknown_access_key(access_key_id),
                |token| AuthError::UnexpectedSecurityToken {
                    token: token.to_string(),
                },
            )
        }
        crate::SessionCredentialAuthenticationError::ExpiredToken => {
            debug_assert!(token_selection.selected.is_some());
            AuthError::ExpiredSessionToken {
                tokens: token_selection.presented.clone(),
            }
        }
        crate::SessionCredentialAuthenticationError::KeyRingUnavailable => {
            AuthError::SessionTokenKeyRingUnavailable
        }
        crate::SessionCredentialAuthenticationError::IdentityProvider(error) => {
            AuthError::IdentityProviderFailure(error)
        }
    }
}

struct SessionTokenSelection {
    selected: Option<String>,
    presented: Vec<String>,
}

fn select_header_session_token<H: HeaderSource + ?Sized>(
    headers: &H,
    access_key_id: &str,
) -> Result<SessionTokenSelection, AuthError> {
    let mut presented = Vec::new();
    headers.visit(|name, value| {
        if name == "x-amz-security-token" {
            presented.push(value.to_string());
        }
    });
    session_token_selection(presented, access_key_id)
}

fn session_token_selection(
    presented: Vec<String>,
    access_key_id: &str,
) -> Result<SessionTokenSelection, AuthError> {
    let selected = presented.first().cloned();
    if selected
        .as_ref()
        .is_some_and(|first| presented.iter().skip(1).any(|token| token != first))
    {
        return Err(unknown_access_key(access_key_id));
    }
    Ok(SessionTokenSelection {
        selected: selected.filter(|token| !token.is_empty()),
        presented,
    })
}

pub(crate) fn authenticate_presented_s3_session_credential(
    provider: &crate::IdentityProvider,
    access_key_id: &str,
    presented_tokens: Vec<String>,
    now_epoch_secs: u64,
) -> Result<crate::AuthenticatedCredential, AuthError> {
    let token_selection = session_token_selection(presented_tokens, access_key_id)?;
    authenticate_selected_s3_session_credential(
        provider,
        access_key_id,
        token_selection,
        now_epoch_secs,
    )
}

fn authenticate_selected_s3_session_credential(
    provider: &crate::IdentityProvider,
    access_key_id: &str,
    token_selection: SessionTokenSelection,
    now_epoch_secs: u64,
) -> Result<crate::AuthenticatedCredential, AuthError> {
    provider
        .authenticate_session_credential(
            access_key_id,
            token_selection.selected.as_deref(),
            now_epoch_secs,
        )
        .map_err(|error| {
            map_s3_session_authentication_error(access_key_id, &token_selection, error)
        })
}

fn unknown_access_key(access_key_id: &str) -> AuthError {
    AuthError::UnknownAccessKey {
        access_key_id: access_key_id.to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
fn authenticate_presigned<H: HeaderSource + ?Sized>(
    method: &str,
    path: &str,
    query_string: &str,
    headers: &H,
    _body: &[u8],
    provider: &crate::IdentityProvider,
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
        return Err(AuthError::RequestNotYetValid {
            amz_date_epoch_millis: request_epoch.saturating_mul(1000),
            expires_epoch: request_epoch.saturating_add(expires),
            server_time_epoch: now_epoch_secs,
        });
    }
    if expires == 0 || now_epoch_secs > request_epoch.saturating_add(expires) {
        return Err(AuthError::PresignedRequestExpired {
            x_amz_expires: expires,
            expires_epoch: request_epoch.saturating_add(expires),
            server_time_epoch: now_epoch_secs,
        });
    }
    if !amz_date_matches_date_stamp(request_date.as_ref(), credential.date) {
        return Err(AuthError::InvalidQueryParam {
            param: "X-Amz-Credential",
        });
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

    let authenticated_credential = resolve_presigned_credential(
        credential.access_key_id,
        query_string,
        &signed_headers,
        headers,
        provider,
        now_epoch_secs,
    )?;

    let signed_header_pairs = collect_signed_headers(&signed_headers, headers);
    let signed_header_pair_refs = signed_header_pairs
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    let canonical_hdrs = canonical_headers(&signed_header_pair_refs);
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
        authenticated_credential.secret_key(),
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
        return Err(AuthError::SignatureMismatch {
            diagnostics: Some(Box::new(crate::SignatureMismatchDiagnostics {
                access_key_id: credential.access_key_id.to_string(),
                string_to_sign: sts,
                signature_provided: signature.to_string(),
                canonical_request: Some(canonical_req),
            })),
        });
    }
    if authenticated_credential.long_lived().is_some() {
        let token = query_param_lossy(query_string, "X-Amz-Security-Token");
        let signed_header_token = signed_headers
            .iter()
            .any(|signed_header| signed_header == &"x-amz-security-token")
            .then(|| headers.first_value("x-amz-security-token"))
            .flatten();
        validate_static_credential_has_no_token(token.as_deref().or(signed_header_token))?;
    }

    Ok(AuthContext {
        mode: AuthMode::PresignedSigV4,
        access_key_id: Some(credential.access_key_id.to_owned()),
        identity: Some(authenticated_credential.identity().clone()),
        authorization_profile: authenticated_credential.long_lived().map_or(
            crate::AuthorizationProfile::Standard,
            crate::StoredCredential::authorization_profile,
        ),
        request_epoch_secs: Some(request_epoch),
        signing_region: Some(credential.region.to_owned()),
        streaming: None,
    })
}

fn resolve_presigned_credential<H: HeaderSource + ?Sized>(
    access_key_id: &str,
    query_string: &str,
    signed_headers: &[&str],
    headers: &H,
    provider: &crate::IdentityProvider,
    now_epoch_secs: u64,
) -> Result<crate::AuthenticatedCredential, AuthError> {
    if !crate::is_reserved_session_access_key_id(access_key_id) {
        let record = provider
            .lookup_long_lived_credential(access_key_id)
            .map_err(AuthError::IdentityProviderFailure)?
            .ok_or_else(|| unknown_access_key(access_key_id))?;
        if !record.is_enabled() {
            return Err(unknown_access_key(access_key_id));
        }
        validate_static_record_expiry(&record, now_epoch_secs)?;
        return Ok(crate::AuthenticatedCredential::LongLived(record));
    }

    let token_selection =
        select_presigned_session_token(query_string, signed_headers, headers, access_key_id)?;
    authenticate_selected_s3_session_credential(
        provider,
        access_key_id,
        token_selection,
        now_epoch_secs,
    )
}

fn select_presigned_session_token<H: HeaderSource + ?Sized>(
    query_string: &str,
    signed_headers: &[&str],
    headers: &H,
    access_key_id: &str,
) -> Result<SessionTokenSelection, AuthError> {
    let mut presented = Vec::new();
    // AWS makes a declared signed header authoritative over the query
    // location. A declared-but-missing header therefore does not fall back to
    // X-Amz-Security-Token.
    if signed_headers.contains(&"x-amz-security-token") {
        headers.visit(|name, value| {
            if name == "x-amz-security-token" {
                presented.push(value.to_string());
            }
        });
    } else {
        presented.extend(
            query_params_lossy(query_string, "X-Amz-Security-Token")
                .into_iter()
                .map(Cow::into_owned),
        );
    }
    session_token_selection(presented, access_key_id)
}

fn presigned_unsigned_required_headers<H, S>(signed_headers: &[S], headers: &H) -> Vec<String>
where
    H: HeaderSource + ?Sized,
    S: AsRef<str>,
{
    unsigned_required_headers(signed_headers, headers)
}

fn collect_signed_headers<H, S>(signed_headers: &[S], headers: &H) -> Vec<(String, String)>
where
    H: HeaderSource + ?Sized,
    S: AsRef<str>,
{
    let mut out = Vec::new();
    for signed_name in signed_headers {
        let signed_name = signed_name.as_ref();
        let mut found = false;
        headers.visit(|name, value| {
            if name == signed_name {
                out.push((name.to_string(), value.to_string()));
                found = true;
            }
        });
        if !found {
            out.push((signed_name.to_owned(), String::new()));
        }
    }
    out
}

pub(crate) fn validate_static_record_expiry(
    record: &crate::credential::StoredCredential,
    now_epoch_secs: u64,
) -> Result<(), AuthError> {
    if let Some(expiry) = record.expires_at_epoch_secs() {
        if now_epoch_secs > expiry {
            return Err(AuthError::ExpiredToken);
        }
    }
    Ok(())
}

pub(crate) fn validate_static_credential_has_no_token(
    request_token: Option<&str>,
) -> Result<(), AuthError> {
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

fn query_params_lossy<'a>(query: &'a str, name: &str) -> Vec<Cow<'a, str>> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter_map(move |pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            if key != name {
                return None;
            }
            let val = parts.next().unwrap_or("");
            Some(percent_decode_lossy(val))
        })
        .collect()
}

fn query_without_signature(query: &str) -> String {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .filter(|pair| pair.split('=').next().unwrap_or("") != "X-Amz-Signature")
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
fn header_value<'a, H: HeaderSource + ?Sized>(headers: &'a H, name: &str) -> Option<&'a str> {
    headers.first_value(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::{CredentialStore, SecretKey, StoredCredential};
    use s3_types::AccountIdentity;
    use std::sync::{Arc, RwLock};

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

    enum SessionRoleState {
        Present(Arc<crate::LiveRoleIdentity>),
        Missing,
        Failure(crate::IdentityProviderError),
    }

    struct SessionRoleProvider {
        role: Arc<RwLock<SessionRoleState>>,
    }

    impl crate::IdentityProviderBackend for SessionRoleProvider {
        fn lookup_long_lived_credential(
            &self,
            _access_key_id: &str,
        ) -> Result<Option<Arc<StoredCredential>>, crate::IdentityProviderError> {
            Ok(None)
        }

        fn lookup_live_role_identity(
            &self,
            stable_role_id: &crate::StableRoleId,
        ) -> Result<Option<Arc<crate::LiveRoleIdentity>>, crate::IdentityProviderError> {
            let state = self
                .role
                .read()
                .map_err(|_| crate::IdentityProviderError::Unavailable)?;
            match &*state {
                SessionRoleState::Present(role) if role.role().stable_id() == stable_role_id => {
                    Ok(Some(Arc::clone(role)))
                }
                SessionRoleState::Present(_) | SessionRoleState::Missing => Ok(None),
                SessionRoleState::Failure(error) => Err(*error),
            }
        }

        fn find_account_by_canonical_user_id(
            &self,
            _canonical_user_id: &s3_types::CanonicalUserId,
        ) -> Result<Option<AccountIdentity>, crate::IdentityProviderError> {
            Ok(None)
        }
    }

    struct HeaderSessionFixture {
        provider: crate::IdentityProvider,
        role_state: Arc<RwLock<SessionRoleState>>,
        access_key_id: String,
        secret_key: SecretKey,
        token: String,
        now_epoch_secs: u64,
    }

    fn header_session_fixture(expires_at_offset_secs: i64) -> HeaderSessionFixture {
        session_fixture_with_identity(
            expires_at_offset_secs,
            "test-role",
            "test-session",
            Some("source-user"),
        )
    }

    fn session_fixture_with_identity(
        expires_at_offset_secs: i64,
        role_name: &str,
        session_name: &str,
        source_identity: Option<&str>,
    ) -> HeaderSessionFixture {
        let now_epoch_secs = parse_amz_date("20260720T120000Z").unwrap();
        let stable_role_id = crate::StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap();
        let role = crate::IamRoleIdentity::new(
            crate::AwsAccountId::new("123456789012").unwrap(),
            stable_role_id.clone(),
            crate::RoleName::new(role_name).unwrap(),
            crate::IamPath::new("/test/").unwrap(),
        );
        let live_role = crate::LiveRoleIdentity::new(
            AccountIdentity::new(
                "123456789012",
                s3_types::CanonicalUserId::from_principal("123456789012"),
                "test account",
            ),
            role,
        )
        .unwrap();
        let role_state = Arc::new(RwLock::new(SessionRoleState::Present(Arc::new(live_role))));
        let provider = crate::IdentityProvider::new(SessionRoleProvider {
            role: Arc::clone(&role_state),
        })
        .unwrap();
        let issuer = provider
            .lookup_live_role_identity(&stable_role_id)
            .unwrap()
            .unwrap();
        let access_key_id = "ARGS0123456789ABCDEFGHIJ".to_string();
        let secret_key = SecretKey::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN".to_string());
        let expires_at_epoch_secs = i64::try_from(now_epoch_secs).unwrap() + expires_at_offset_secs;
        let token = provider
            .seal_session_credential_v1(
                crate::GeneratedSessionCredentialMaterial::from_parts_for_test(
                    access_key_id.clone(),
                    secret_key.clone(),
                ),
                &issuer,
                crate::RoleSessionName::new(session_name).unwrap(),
                crate::SessionLifetime::new(
                    i64::try_from(now_epoch_secs).unwrap() - 60,
                    expires_at_epoch_secs,
                )
                .unwrap(),
                source_identity.map(|value| crate::SourceIdentity::new(value).unwrap()),
            )
            .unwrap();
        HeaderSessionFixture {
            provider,
            role_state,
            access_key_id,
            secret_key,
            token,
            now_epoch_secs,
        }
    }

    fn issue_header_session_token(
        fixture: &HeaderSessionFixture,
        access_key_id: &str,
        secret_key: SecretKey,
    ) -> String {
        let stable_role_id = crate::StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap();
        let issuer = fixture
            .provider
            .lookup_live_role_identity(&stable_role_id)
            .unwrap()
            .unwrap();
        fixture
            .provider
            .seal_session_credential_v1(
                crate::GeneratedSessionCredentialMaterial::from_parts_for_test(
                    access_key_id.to_string(),
                    secret_key,
                ),
                &issuer,
                crate::RoleSessionName::new("other-session").unwrap(),
                crate::SessionLifetime::new(
                    i64::try_from(fixture.now_epoch_secs).unwrap() - 60,
                    i64::try_from(fixture.now_epoch_secs).unwrap() + 3_600,
                )
                .unwrap(),
                None,
            )
            .unwrap()
    }

    fn signed_header_session_request(
        access_key_id: &str,
        signing_secret: &SecretKey,
        tokens: &[&str],
        cover_token: bool,
        region: &str,
        service: &str,
        valid_signature: bool,
    ) -> Vec<(String, String)> {
        let mut headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("x-amz-date".to_string(), "20260720T120000Z".to_string()),
        ];
        headers.extend(
            tokens
                .iter()
                .map(|token| ("x-amz-security-token".to_string(), (*token).to_string())),
        );
        let signed_headers = if cover_token {
            "host;x-amz-date;x-amz-security-token"
        } else {
            "host;x-amz-date"
        };
        let canonical_pairs: Vec<_> = headers
            .iter()
            .filter(|(name, _)| {
                name == "host"
                    || name == "x-amz-date"
                    || (cover_token && name == "x-amz-security-token")
            })
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let canonical_request = canonical_request(
            "GET",
            "/",
            "",
            &canonical_headers(&canonical_pairs),
            signed_headers,
            &sha256_hex(b""),
        );
        let scope = format!("20260720/{region}/{service}/aws4_request");
        let string_to_sign = string_to_sign(
            "20260720T120000Z",
            &scope,
            &sha256_hex(canonical_request.as_bytes()),
        );
        let signature = if valid_signature {
            let signing_key = derive_signing_key(signing_secret, "20260720", region, service);
            hex_encode_lower(
                hmac::sign(
                    &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                    string_to_sign.as_bytes(),
                )
                .as_ref(),
            )
        } else {
            "0".repeat(64)
        };
        headers.push((
            "authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key_id}/20260720/{region}/{service}/aws4_request, SignedHeaders={signed_headers}, Signature={signature}"
            ),
        ));
        headers
    }

    fn signed_streaming_session_request(
        access_key_id: &str,
        signing_secret: &SecretKey,
        tokens: &[&str],
        cover_token: bool,
        region: &str,
        service: &str,
        valid_seed_signature: bool,
    ) -> Vec<(String, String)> {
        let body_hash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
        let mut headers = vec![
            ("content-encoding".to_string(), "aws-chunked".to_string()),
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("x-amz-content-sha256".to_string(), body_hash.to_string()),
            ("x-amz-date".to_string(), "20260720T120000Z".to_string()),
            ("x-amz-decoded-content-length".to_string(), "5".to_string()),
        ];
        headers.extend(
            tokens
                .iter()
                .map(|token| ("x-amz-security-token".to_string(), (*token).to_string())),
        );
        let signed_headers = if cover_token {
            "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-security-token"
        } else {
            "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"
        };
        let canonical_pairs: Vec<_> = headers
            .iter()
            .filter(|(name, _)| {
                name == "content-encoding"
                    || name == "host"
                    || name == "x-amz-content-sha256"
                    || name == "x-amz-date"
                    || name == "x-amz-decoded-content-length"
                    || (cover_token && name == "x-amz-security-token")
            })
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let canonical_request = canonical_request(
            "PUT",
            "/streaming",
            "",
            &canonical_headers(&canonical_pairs),
            signed_headers,
            body_hash,
        );
        let scope = format!("20260720/{region}/{service}/aws4_request");
        let string_to_sign = string_to_sign(
            "20260720T120000Z",
            &scope,
            &sha256_hex(canonical_request.as_bytes()),
        );
        let signature = if valid_seed_signature {
            let signing_key = derive_signing_key(signing_secret, "20260720", region, service);
            hex_encode_lower(
                hmac::sign(
                    &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                    string_to_sign.as_bytes(),
                )
                .as_ref(),
            )
        } else {
            "0".repeat(64)
        };
        headers.push((
            "authorization".to_string(),
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key_id}/20260720/{region}/{service}/aws4_request, SignedHeaders={signed_headers}, Signature={signature}"
            ),
        ));
        headers
    }

    fn signed_presigned_session_request(
        access_key_id: &str,
        signing_secret: &SecretKey,
        query_tokens: &[&str],
        header_tokens: &[&str],
        sign_token_header: bool,
        scope: (&str, &str),
        valid_signature: bool,
    ) -> (String, Vec<(String, String)>) {
        let (region, service) = scope;
        let host = "examplebucket.s3.amazonaws.com";
        let signed_headers = if sign_token_header {
            "host;x-amz-security-token"
        } else {
            "host"
        };
        let credential = format!("{access_key_id}/20260720/{region}/{service}/aws4_request");
        let mut query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date=20260720T120000Z&X-Amz-Expires=900",
            crate::canonical::uri_encode(&credential)
        );
        for token in query_tokens {
            query.push_str("&X-Amz-Security-Token=");
            query.push_str(&crate::canonical::uri_encode(token));
        }
        query.push_str("&X-Amz-SignedHeaders=");
        query.push_str(&crate::canonical::uri_encode(signed_headers));

        let mut headers = vec![("host".to_string(), host.to_string())];
        headers.extend(
            header_tokens
                .iter()
                .map(|token| ("x-amz-security-token".to_string(), (*token).to_string())),
        );
        let canonical_pairs = headers
            .iter()
            .filter(|(name, _)| {
                name == "host" || (sign_token_header && name == "x-amz-security-token")
            })
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let canonical_request = canonical_request(
            "GET",
            "/",
            &canonical_query_string(&query),
            &canonical_headers(&canonical_pairs),
            signed_headers,
            "UNSIGNED-PAYLOAD",
        );
        let scope = format!("20260720/{region}/{service}/aws4_request");
        let string_to_sign = string_to_sign(
            "20260720T120000Z",
            &scope,
            &sha256_hex(canonical_request.as_bytes()),
        );
        let signature = if valid_signature {
            let signing_key = derive_signing_key(signing_secret, "20260720", region, service);
            hex_encode_lower(
                hmac::sign(
                    &hmac::Key::new(hmac::HMAC_SHA256, signing_key.as_ref()),
                    string_to_sign.as_bytes(),
                )
                .as_ref(),
            )
        } else {
            "0".repeat(64)
        };
        query.push_str("&X-Amz-Signature=");
        query.push_str(&signature);
        (query, headers)
    }

    fn presigned_token_location<'a>(
        sign_token_header: bool,
        tokens: &'a [&'a str],
    ) -> (&'a [&'a str], &'a [&'a str]) {
        if sign_token_header {
            (&[], tokens)
        } else {
            (tokens, &[])
        }
    }

    fn account(principal: &str) -> AccountIdentity {
        AccountIdentity::from_principal(principal)
    }

    fn configured_record(
        access_key_id: &str,
        secret_key: &str,
        account: AccountIdentity,
        expires_at_epoch_secs: Option<u64>,
        enabled: bool,
    ) -> StoredCredential {
        let principal = crate::ConfiguredPrincipalIdentity::new(account.principal());
        StoredCredential::configured(
            access_key_id.to_string(),
            SecretKey::new(secret_key.to_string()),
            account,
            principal,
            crate::AuthorizationProfile::Standard,
            expires_at_epoch_secs,
            enabled,
        )
    }

    fn example_store() -> crate::IdentityProvider {
        let mut store = CredentialStore::new();
        store
            .add(
                "AKIAIOSFODNN7EXAMPLE".to_string(),
                SecretKey::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            )
            .unwrap();
        crate::IdentityProvider::in_memory(store).unwrap()
    }

    fn aws_example_time() -> u64 {
        parse_amz_date("20130524T000000Z").unwrap()
    }

    fn presigned_example_time() -> u64 {
        parse_amz_date("20240201T120500Z").unwrap()
    }

    fn sign_test_presigned_query(method: &str, path: &str, query: &str, host: &str) -> String {
        let query_for_sig = canonical_query_string(query);
        let canonical_req = canonical_request(
            method,
            path,
            &query_for_sig,
            &format!("host:{host}\n"),
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
        format!("{query}&X-Amz-Signature={sig}")
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
        assert_eq!(ctx.configured_principal(), Some("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(ctx.request_epoch_secs, Some(1_369_353_600));
    }

    fn authenticate_header_session(
        fixture: &HeaderSessionFixture,
        headers: &[(String, String)],
    ) -> Result<AuthContext, AuthError> {
        authenticate_request(
            "GET",
            "/",
            "",
            headers,
            b"",
            &fixture.provider,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            fixture.now_epoch_secs,
        )
    }

    fn authenticate_streaming_session(
        fixture: &HeaderSessionFixture,
        headers: &[(String, String)],
    ) -> Result<AuthContext, AuthError> {
        authenticate_request(
            "PUT",
            "/streaming",
            "",
            headers,
            b"",
            &fixture.provider,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            fixture.now_epoch_secs,
        )
    }

    fn authenticate_presigned_session(
        fixture: &HeaderSessionFixture,
        query: &str,
        headers: &[(String, String)],
    ) -> Result<AuthContext, AuthError> {
        authenticate_request(
            "GET",
            "/",
            query,
            headers,
            b"",
            &fixture.provider,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            fixture.now_epoch_secs,
        )
    }

    #[test]
    fn authenticate_header_session_credential_returns_typed_role_identity() {
        let fixture = header_session_fixture(3_600);
        let headers = signed_header_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            true,
            "us-east-1",
            "s3",
            true,
        );
        let context = authenticate_header_session(&fixture, &headers).unwrap();

        assert_eq!(context.mode, AuthMode::HeaderSigV4);
        assert_eq!(
            context.access_key_id.as_deref(),
            Some(fixture.access_key_id.as_str())
        );
        assert_eq!(
            context
                .identity
                .as_ref()
                .unwrap()
                .role_session()
                .unwrap()
                .session_name()
                .as_str(),
            "test-session"
        );
        assert!(context.configured_principal().is_none());
        assert_eq!(
            context.authorization_profile,
            crate::AuthorizationProfile::Standard
        );
    }

    #[test]
    fn header_session_token_structure_and_binding_precede_signature_verification() {
        let fixture = header_session_fixture(3_600);
        let other_token = issue_header_session_token(
            &fixture,
            "ARGS1123456789ABCDEFGHIJ",
            SecretKey::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn".to_string()),
        );

        for tokens in [Vec::new(), vec![""], vec![other_token.as_str()]] {
            let headers = signed_header_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &tokens,
                !tokens.is_empty(),
                "us-east-1",
                "s3",
                false,
            );
            assert!(matches!(
                authenticate_header_session(&fixture, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        let malformed = "ARGST1.not-a-canonical-token";
        let headers = signed_header_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_header_session(&fixture, &headers),
            Err(AuthError::UnexpectedSecurityToken { token }) if token == malformed
        ));

        let headers = signed_header_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token, &fixture.token],
            true,
            "us-east-1",
            "s3",
            true,
        );
        authenticate_header_session(&fixture, &headers).unwrap();

        for valid_signature in [true, false] {
            let headers = signed_header_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &[&fixture.token, &other_token],
                true,
                "us-east-1",
                "s3",
                valid_signature,
            );
            assert!(matches!(
                authenticate_header_session(&fixture, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        let headers = signed_header_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_header_session(&fixture, &headers),
            Err(AuthError::SignatureMismatch { .. })
        ));
    }

    #[test]
    fn header_session_scope_and_coverage_precede_token_validation() {
        let fixture = header_session_fixture(3_600);
        let malformed = "ARGST1.not-a-canonical-token";

        let wrong_region = signed_header_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            true,
            "us-west-2",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_header_session(&fixture, &wrong_region),
            Err(AuthError::InvalidHeaderCredentialRegion {
                provided_region,
                expected_region,
            }) if provided_region == "us-west-2" && expected_region == "us-east-1"
        ));

        let wrong_service = signed_header_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            true,
            "us-east-1",
            "sts",
            false,
        );
        assert!(matches!(
            authenticate_header_session(&fixture, &wrong_service),
            Err(AuthError::InvalidHeaderCredentialService {
                provided_service,
                expected_service,
            }) if provided_service == "sts" && expected_service == "s3"
        ));

        let unsigned = signed_header_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            false,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_header_session(&fixture, &unsigned),
            Err(AuthError::UnsignedHeaders { headers })
                if headers == ["x-amz-security-token"]
        ));
    }

    #[test]
    fn header_session_expiry_and_issuer_liveness_precede_signature_verification() {
        let expired = header_session_fixture(0);
        for valid_signature in [true, false] {
            let headers = signed_header_session_request(
                &expired.access_key_id,
                &expired.secret_key,
                &[&expired.token],
                true,
                "us-east-1",
                "s3",
                valid_signature,
            );
            assert!(matches!(
                authenticate_header_session(&expired, &headers),
                Err(AuthError::ExpiredSessionToken { tokens })
                    if tokens.len() == 1 && tokens[0] == expired.token
            ));
        }

        for valid_signature in [true, false] {
            let headers = signed_header_session_request(
                &expired.access_key_id,
                &expired.secret_key,
                &[&expired.token, &expired.token],
                true,
                "us-east-1",
                "s3",
                valid_signature,
            );
            assert!(matches!(
                authenticate_header_session(&expired, &headers),
                Err(AuthError::ExpiredSessionToken { tokens })
                    if tokens.len() == 2
                        && tokens.iter().all(|token| token == &expired.token)
            ));
        }

        let missing = header_session_fixture(3_600);
        *missing.role_state.write().unwrap() = SessionRoleState::Missing;
        let headers = signed_header_session_request(
            &missing.access_key_id,
            &missing.secret_key,
            &[&missing.token],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_header_session(&missing, &headers),
            Err(AuthError::UnknownAccessKey { .. })
        ));

        let unavailable = header_session_fixture(3_600);
        *unavailable.role_state.write().unwrap() =
            SessionRoleState::Failure(crate::IdentityProviderError::Unavailable);
        let headers = signed_header_session_request(
            &unavailable.access_key_id,
            &unavailable.secret_key,
            &[&unavailable.token],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_header_session(&unavailable, &headers),
            Err(AuthError::IdentityProviderFailure(
                crate::IdentityProviderError::Unavailable
            ))
        ));
    }

    #[test]
    fn streaming_authenticates_maximum_issued_session_token_without_debug_leakage() {
        let role_name = "r".repeat(crate::identity::ROLE_NAME_MAX_LEN);
        let session_name = "s".repeat(crate::identity::ROLE_SESSION_NAME_MAX_LEN);
        let source_identity = "i".repeat(crate::identity::SOURCE_IDENTITY_MAX_LEN);
        let fixture =
            session_fixture_with_identity(3_600, &role_name, &session_name, Some(&source_identity));
        assert_eq!(
            fixture.token.len(),
            crate::session_token::MAX_ISSUED_V1_TOKEN_LEN
        );
        let headers = signed_streaming_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            true,
            "us-east-1",
            "s3",
            true,
        );
        let request_head_len = "PUT /streaming HTTP/1.1\r\n".len()
            + headers
                .iter()
                .map(|(name, value)| name.len() + 2 + value.len() + 2)
                .sum::<usize>()
            + 2;
        assert!(request_head_len <= 8_192);

        let context = authenticate_streaming_session(&fixture, &headers).unwrap();
        assert_eq!(context.mode, AuthMode::HeaderSigV4);
        assert_eq!(
            context.access_key_id.as_deref(),
            Some(fixture.access_key_id.as_str())
        );
        assert_eq!(
            context
                .identity
                .as_ref()
                .unwrap()
                .role_session()
                .unwrap()
                .session_name()
                .as_str(),
            session_name
        );
        assert_eq!(
            context.authorization_profile,
            crate::AuthorizationProfile::Standard
        );
        let streaming = context.streaming.as_ref().unwrap();
        assert_eq!(streaming.timestamp, "20260720T120000Z");
        assert_eq!(streaming.scope, "20260720/us-east-1/s3/aws4_request");
        assert!(streaming.seed_canonical_request.contains(&fixture.token));

        let debug = format!("{context:?}");
        assert!(debug.contains("<redacted:sigv4_seed_canonical_request>"));
        assert!(!debug.contains(&fixture.token));
        assert!(!debug.contains(fixture.secret_key.as_str()));
    }

    #[test]
    fn streaming_token_selection_and_coverage_precede_seed_signature() {
        let fixture = header_session_fixture(3_600);
        let other_token = issue_header_session_token(
            &fixture,
            "ARGS1123456789ABCDEFGHIJ",
            SecretKey::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn".to_string()),
        );

        for tokens in [Vec::new(), vec![""], vec![other_token.as_str()]] {
            let headers = signed_streaming_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &tokens,
                !tokens.is_empty(),
                "us-east-1",
                "s3",
                false,
            );
            assert!(matches!(
                authenticate_streaming_session(&fixture, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        let malformed = "ARGST1.not-a-canonical-token";
        let headers = signed_streaming_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_streaming_session(&fixture, &headers),
            Err(AuthError::UnexpectedSecurityToken { token }) if token == malformed
        ));

        for tokens in [
            vec![fixture.token.as_str(), other_token.as_str()],
            vec![other_token.as_str(), fixture.token.as_str()],
        ] {
            for valid_seed_signature in [true, false] {
                let headers = signed_streaming_session_request(
                    &fixture.access_key_id,
                    &fixture.secret_key,
                    &tokens,
                    true,
                    "us-east-1",
                    "s3",
                    valid_seed_signature,
                );
                assert!(matches!(
                    authenticate_streaming_session(&fixture, &headers),
                    Err(AuthError::UnknownAccessKey { .. })
                ));
            }
        }

        let identical = [fixture.token.as_str(), fixture.token.as_str()];
        let headers = signed_streaming_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &identical,
            true,
            "us-east-1",
            "s3",
            true,
        );
        authenticate_streaming_session(&fixture, &headers).unwrap();
        let headers = signed_streaming_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &identical,
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_streaming_session(&fixture, &headers),
            Err(AuthError::SignatureMismatch { .. })
        ));

        *fixture.role_state.write().unwrap() = SessionRoleState::Missing;
        let headers = signed_streaming_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            false,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_streaming_session(&fixture, &headers),
            Err(AuthError::UnsignedHeaders { headers })
                if headers == ["x-amz-security-token"]
        ));
    }

    #[test]
    fn streaming_scope_precedes_token_coverage_structure_and_seed_signature() {
        let fixture = header_session_fixture(3_600);
        let malformed = "ARGST1.not-a-canonical-token";

        for (tokens, cover_token) in [
            (Vec::new(), false),
            (vec![""], true),
            (vec![malformed], true),
            (vec![fixture.token.as_str()], false),
            (vec![fixture.token.as_str(), malformed], true),
        ] {
            let wrong_region = signed_streaming_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &tokens,
                cover_token,
                "us-west-2",
                "sts",
                false,
            );
            assert!(matches!(
                authenticate_streaming_session(&fixture, &wrong_region),
                Err(AuthError::InvalidHeaderCredentialRegion {
                    provided_region,
                    expected_region,
                }) if provided_region == "us-west-2" && expected_region == "us-east-1"
            ));

            let wrong_service = signed_streaming_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &tokens,
                cover_token,
                "us-east-1",
                "sts",
                false,
            );
            assert!(matches!(
                authenticate_streaming_session(&fixture, &wrong_service),
                Err(AuthError::InvalidHeaderCredentialService {
                    provided_service,
                    expected_service,
                }) if provided_service == "sts" && expected_service == "s3"
            ));
        }
    }

    #[test]
    fn streaming_token_opening_expiry_and_liveness_precede_seed_signature() {
        let expired = header_session_fixture(0);
        let other_token = issue_header_session_token(
            &expired,
            "ARGS1123456789ABCDEFGHIJ",
            SecretKey::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn".to_string()),
        );
        let malformed = "ARGST1.not-a-canonical-token";

        for tokens in [Vec::new(), vec![""], vec![other_token.as_str()]] {
            let headers = signed_streaming_session_request(
                &expired.access_key_id,
                &expired.secret_key,
                &tokens,
                !tokens.is_empty(),
                "us-east-1",
                "s3",
                false,
            );
            assert!(matches!(
                authenticate_streaming_session(&expired, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }
        let headers = signed_streaming_session_request(
            &expired.access_key_id,
            &expired.secret_key,
            &[malformed],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_streaming_session(&expired, &headers),
            Err(AuthError::UnexpectedSecurityToken { token }) if token == malformed
        ));
        for tokens in [
            vec![expired.token.as_str(), other_token.as_str()],
            vec![other_token.as_str(), expired.token.as_str()],
        ] {
            let headers = signed_streaming_session_request(
                &expired.access_key_id,
                &expired.secret_key,
                &tokens,
                true,
                "us-east-1",
                "s3",
                false,
            );
            assert!(matches!(
                authenticate_streaming_session(&expired, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        for tokens in [
            vec![expired.token.as_str()],
            vec![expired.token.as_str(), expired.token.as_str()],
        ] {
            for valid_seed_signature in [true, false] {
                let headers = signed_streaming_session_request(
                    &expired.access_key_id,
                    &expired.secret_key,
                    &tokens,
                    true,
                    "us-east-1",
                    "s3",
                    valid_seed_signature,
                );
                assert!(matches!(
                    authenticate_streaming_session(&expired, &headers),
                    Err(AuthError::ExpiredSessionToken { tokens: presented })
                        if presented == tokens
                ));
            }
        }

        let missing = header_session_fixture(3_600);
        *missing.role_state.write().unwrap() = SessionRoleState::Missing;
        let headers = signed_streaming_session_request(
            &missing.access_key_id,
            &missing.secret_key,
            &[&missing.token],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_streaming_session(&missing, &headers),
            Err(AuthError::UnknownAccessKey { .. })
        ));

        let unavailable = header_session_fixture(3_600);
        *unavailable.role_state.write().unwrap() =
            SessionRoleState::Failure(crate::IdentityProviderError::Unavailable);
        let headers = signed_streaming_session_request(
            &unavailable.access_key_id,
            &unavailable.secret_key,
            &[&unavailable.token],
            true,
            "us-east-1",
            "s3",
            false,
        );
        assert!(matches!(
            authenticate_streaming_session(&unavailable, &headers),
            Err(AuthError::IdentityProviderFailure(
                crate::IdentityProviderError::Unavailable
            ))
        ));
    }

    #[test]
    fn authenticate_presigned_session_credential_returns_typed_role_identity() {
        let fixture = header_session_fixture(3_600);
        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            &[],
            false,
            ("us-east-1", "s3"),
            true,
        );
        let context = authenticate_presigned_session(&fixture, &query, &headers).unwrap();

        assert_eq!(context.mode, AuthMode::PresignedSigV4);
        assert_eq!(
            context.access_key_id.as_deref(),
            Some(fixture.access_key_id.as_str())
        );
        assert_eq!(
            context
                .identity
                .as_ref()
                .unwrap()
                .role_session()
                .unwrap()
                .session_name()
                .as_str(),
            "test-session"
        );
        assert!(context.configured_principal().is_none());
        assert_eq!(
            context.authorization_profile,
            crate::AuthorizationProfile::Standard
        );
    }

    #[test]
    fn presigned_authenticates_maximum_issued_session_token_within_query_limit() {
        let role_name = "r".repeat(crate::identity::ROLE_NAME_MAX_LEN);
        let session_name = "s".repeat(crate::identity::ROLE_SESSION_NAME_MAX_LEN);
        let source_identity = "i".repeat(crate::identity::SOURCE_IDENTITY_MAX_LEN);
        let fixture =
            session_fixture_with_identity(3_600, &role_name, &session_name, Some(&source_identity));
        assert_eq!(
            fixture.token.len(),
            crate::session_token::MAX_ISSUED_V1_TOKEN_LEN
        );

        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            &[],
            false,
            ("us-east-1", "s3"),
            true,
        );
        assert!(query.len() <= MAX_PRESIGNED_QUERY_LEN);
        let context = authenticate_presigned_session(&fixture, &query, &headers).unwrap();
        assert_eq!(
            context
                .identity
                .unwrap()
                .role_session()
                .unwrap()
                .session_name()
                .as_str(),
            session_name
        );
    }

    #[test]
    fn presigned_query_session_token_structure_and_binding_precede_signature() {
        let fixture = header_session_fixture(3_600);
        let other_token = issue_header_session_token(
            &fixture,
            "ARGS1123456789ABCDEFGHIJ",
            SecretKey::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn".to_string()),
        );

        for tokens in [Vec::new(), vec![""], vec![other_token.as_str()]] {
            let (query, headers) = signed_presigned_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &tokens,
                &[],
                false,
                ("us-east-1", "s3"),
                false,
            );
            assert!(matches!(
                authenticate_presigned_session(&fixture, &query, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        let malformed = "ARGST1.not-a-canonical-token";
        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            &[],
            false,
            ("us-east-1", "s3"),
            false,
        );
        assert!(matches!(
            authenticate_presigned_session(&fixture, &query, &headers),
            Err(AuthError::UnexpectedSecurityToken { token }) if token == malformed
        ));

        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token, &fixture.token],
            &[],
            false,
            ("us-east-1", "s3"),
            true,
        );
        authenticate_presigned_session(&fixture, &query, &headers).unwrap();

        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            &[],
            false,
            ("us-east-1", "s3"),
            false,
        );
        assert!(matches!(
            authenticate_presigned_session(&fixture, &query, &headers),
            Err(AuthError::SignatureMismatch { .. })
        ));

        for valid_signature in [true, false] {
            for tokens in [
                [fixture.token.as_str(), other_token.as_str()],
                [other_token.as_str(), fixture.token.as_str()],
            ] {
                let (query, headers) = signed_presigned_session_request(
                    &fixture.access_key_id,
                    &fixture.secret_key,
                    &tokens,
                    &[],
                    false,
                    ("us-east-1", "s3"),
                    valid_signature,
                );
                assert!(matches!(
                    authenticate_presigned_session(&fixture, &query, &headers),
                    Err(AuthError::UnknownAccessKey { .. })
                ));
            }
        }
    }

    #[test]
    fn presigned_signed_session_token_header_is_authoritative() {
        let fixture = header_session_fixture(3_600);
        let other_token = issue_header_session_token(
            &fixture,
            "ARGS1123456789ABCDEFGHIJ",
            SecretKey::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn".to_string()),
        );

        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&other_token],
            &[&fixture.token],
            true,
            ("us-east-1", "s3"),
            true,
        );
        authenticate_presigned_session(&fixture, &query, &headers).unwrap();

        for valid_signature in [true, false] {
            let (query, headers) = signed_presigned_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &[&fixture.token],
                &[&other_token],
                true,
                ("us-east-1", "s3"),
                valid_signature,
            );
            assert!(matches!(
                authenticate_presigned_session(&fixture, &query, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        for header_tokens in [Vec::new(), vec![""]] {
            let (query, headers) = signed_presigned_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &[&fixture.token],
                &header_tokens,
                true,
                ("us-east-1", "s3"),
                false,
            );
            assert!(matches!(
                authenticate_presigned_session(&fixture, &query, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        let malformed = "ARGST1.not-a-canonical-token";
        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            &[malformed],
            true,
            ("us-east-1", "s3"),
            false,
        );
        assert!(matches!(
            authenticate_presigned_session(&fixture, &query, &headers),
            Err(AuthError::UnexpectedSecurityToken { token }) if token == malformed
        ));

        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&other_token],
            &[&fixture.token, &fixture.token],
            true,
            ("us-east-1", "s3"),
            true,
        );
        authenticate_presigned_session(&fixture, &query, &headers).unwrap();

        for valid_signature in [true, false] {
            let (query, headers) = signed_presigned_session_request(
                &fixture.access_key_id,
                &fixture.secret_key,
                &[&fixture.token],
                &[&fixture.token, &other_token],
                true,
                ("us-east-1", "s3"),
                valid_signature,
            );
            assert!(matches!(
                authenticate_presigned_session(&fixture, &query, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }
    }

    #[test]
    fn presigned_unsigned_session_token_header_precedes_selected_query_token() {
        let fixture = header_session_fixture(3_600);
        let malformed = "ARGST1.not-a-canonical-token";
        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            &[malformed],
            false,
            ("us-east-1", "s3"),
            false,
        );
        assert!(matches!(
            authenticate_presigned_session(&fixture, &query, &headers),
            Err(AuthError::UnsignedHeaders { headers })
                if headers == ["x-amz-security-token"]
        ));
    }

    #[test]
    fn presigned_session_scope_precedes_token_selection_and_signature() {
        let fixture = header_session_fixture(3_600);
        let malformed = "ARGST1.not-a-canonical-token";

        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[malformed],
            &[],
            false,
            ("us-west-2", "s3"),
            false,
        );
        assert!(matches!(
            authenticate_presigned_session(&fixture, &query, &headers),
            Err(AuthError::InvalidQueryCredentialRegion {
                provided_region,
                expected_region,
                ..
            }) if provided_region == "us-west-2" && expected_region == "us-east-1"
        ));

        let (query, headers) = signed_presigned_session_request(
            &fixture.access_key_id,
            &fixture.secret_key,
            &[&fixture.token],
            &[&fixture.token, malformed],
            true,
            ("us-east-1", "sts"),
            false,
        );
        assert!(matches!(
            authenticate_presigned_session(&fixture, &query, &headers),
            Err(AuthError::InvalidQueryCredentialService {
                provided_service,
                expected_service,
                ..
            }) if provided_service == "sts" && expected_service == "s3"
        ));
    }

    #[test]
    fn presigned_session_expiry_and_liveness_precede_signature() {
        let expired = header_session_fixture(0);
        let live_mismatched_token = issue_header_session_token(
            &expired,
            "ARGS1123456789ABCDEFGHIJ",
            SecretKey::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmn".to_string()),
        );
        for sign_token_header in [false, true] {
            let duplicate_tokens = [expired.token.as_str(), expired.token.as_str()];
            let (query_tokens, header_tokens) =
                presigned_token_location(sign_token_header, &duplicate_tokens);
            let (query, headers) = signed_presigned_session_request(
                &expired.access_key_id,
                &expired.secret_key,
                query_tokens,
                header_tokens,
                sign_token_header,
                ("us-east-1", "s3"),
                false,
            );
            assert!(matches!(
                authenticate_presigned_session(&expired, &query, &headers),
                Err(AuthError::ExpiredSessionToken { tokens })
                    if tokens.len() == 2
                        && tokens.iter().all(|token| token == &expired.token)
            ));

            for tokens in [Vec::new(), vec![""]] {
                let (query_tokens, header_tokens) =
                    presigned_token_location(sign_token_header, &tokens);
                let (query, headers) = signed_presigned_session_request(
                    &expired.access_key_id,
                    &expired.secret_key,
                    query_tokens,
                    header_tokens,
                    sign_token_header,
                    ("us-east-1", "s3"),
                    false,
                );
                assert!(matches!(
                    authenticate_presigned_session(&expired, &query, &headers),
                    Err(AuthError::UnknownAccessKey { .. })
                ));
            }

            let malformed = "ARGST1.not-a-canonical-token";
            let malformed_tokens = [malformed];
            let (query_tokens, header_tokens) =
                presigned_token_location(sign_token_header, &malformed_tokens);
            let (query, headers) = signed_presigned_session_request(
                &expired.access_key_id,
                &expired.secret_key,
                query_tokens,
                header_tokens,
                sign_token_header,
                ("us-east-1", "s3"),
                false,
            );
            assert!(matches!(
                authenticate_presigned_session(&expired, &query, &headers),
                Err(AuthError::UnexpectedSecurityToken { token }) if token == malformed
            ));

            for tokens in [
                [expired.token.as_str(), live_mismatched_token.as_str()],
                [live_mismatched_token.as_str(), expired.token.as_str()],
            ] {
                let (query_tokens, header_tokens) =
                    presigned_token_location(sign_token_header, &tokens);
                let (query, headers) = signed_presigned_session_request(
                    &expired.access_key_id,
                    &expired.secret_key,
                    query_tokens,
                    header_tokens,
                    sign_token_header,
                    ("us-east-1", "s3"),
                    false,
                );
                assert!(matches!(
                    authenticate_presigned_session(&expired, &query, &headers),
                    Err(AuthError::UnknownAccessKey { .. })
                ));
            }
        }

        let (query, headers) = signed_presigned_session_request(
            &expired.access_key_id,
            &expired.secret_key,
            &[&live_mismatched_token],
            &[&expired.token],
            true,
            ("us-east-1", "s3"),
            false,
        );
        assert!(matches!(
            authenticate_presigned_session(&expired, &query, &headers),
            Err(AuthError::ExpiredSessionToken { tokens })
                if tokens == [expired.token.clone()]
        ));

        for (region, service) in [("us-west-2", "s3"), ("us-east-1", "sts")] {
            for sign_token_header in [false, true] {
                let tokens = [expired.token.as_str()];
                let (query_tokens, header_tokens) =
                    presigned_token_location(sign_token_header, &tokens);
                let (query, headers) = signed_presigned_session_request(
                    &expired.access_key_id,
                    &expired.secret_key,
                    query_tokens,
                    header_tokens,
                    sign_token_header,
                    (region, service),
                    false,
                );
                let error = authenticate_presigned_session(&expired, &query, &headers).unwrap_err();
                if region != "us-east-1" {
                    assert!(matches!(
                        error,
                        AuthError::InvalidQueryCredentialRegion { .. }
                    ));
                } else {
                    assert!(matches!(
                        error,
                        AuthError::InvalidQueryCredentialService { .. }
                    ));
                }
            }
        }

        let missing = header_session_fixture(3_600);
        *missing.role_state.write().unwrap() = SessionRoleState::Missing;
        for sign_token_header in [false, true] {
            let tokens = [missing.token.as_str()];
            let (query_tokens, header_tokens) =
                presigned_token_location(sign_token_header, &tokens);
            let (query, headers) = signed_presigned_session_request(
                &missing.access_key_id,
                &missing.secret_key,
                query_tokens,
                header_tokens,
                sign_token_header,
                ("us-east-1", "s3"),
                false,
            );
            assert!(matches!(
                authenticate_presigned_session(&missing, &query, &headers),
                Err(AuthError::UnknownAccessKey { .. })
            ));
        }

        let unavailable = header_session_fixture(3_600);
        *unavailable.role_state.write().unwrap() =
            SessionRoleState::Failure(crate::IdentityProviderError::Unavailable);
        for sign_token_header in [false, true] {
            let tokens = [unavailable.token.as_str()];
            let (query_tokens, header_tokens) =
                presigned_token_location(sign_token_header, &tokens);
            let (query, headers) = signed_presigned_session_request(
                &unavailable.access_key_id,
                &unavailable.secret_key,
                query_tokens,
                header_tokens,
                sign_token_header,
                ("us-east-1", "s3"),
                false,
            );
            assert!(matches!(
                authenticate_presigned_session(&unavailable, &query, &headers),
                Err(AuthError::IdentityProviderFailure(
                    crate::IdentityProviderError::Unavailable
                ))
            ));
        }
    }

    #[test]
    fn header_session_key_ring_failure_maps_to_distinct_internal_auth_error() {
        let token_selection = SessionTokenSelection {
            selected: Some("ARGST1.redacted".to_string()),
            presented: vec!["ARGST1.redacted".to_string()],
        };
        assert!(matches!(
            map_s3_session_authentication_error(
                "ARGS0123456789ABCDEFGHIJ",
                &token_selection,
                crate::SessionCredentialAuthenticationError::KeyRingUnavailable,
            ),
            AuthError::SessionTokenKeyRingUnavailable
        ));
    }

    #[test]
    fn header_provider_failure_is_not_unknown_access_key() {
        let provider = crate::IdentityProvider::new(FailingIdentityProvider(
            crate::IdentityProviderError::Unavailable,
        ))
        .unwrap();
        let headers = aws_example_signed_headers();
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            &[],
            &provider,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::IdentityProviderFailure(crate::IdentityProviderError::Unavailable)
        ));
    }

    #[test]
    fn header_invalid_provider_record_preserves_failure_kind() {
        let provider = crate::IdentityProvider::new(FailingIdentityProvider(
            crate::IdentityProviderError::InvalidRecord,
        ))
        .unwrap();
        let headers = aws_example_signed_headers();
        let err = authenticate_request(
            "GET",
            "/test.txt",
            "",
            &headers,
            &[],
            &provider,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            aws_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::IdentityProviderFailure(crate::IdentityProviderError::InvalidRecord)
        ));
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
    fn presigned_provider_failure_is_not_unknown_access_key() {
        let provider = crate::IdentityProvider::new(FailingIdentityProvider(
            crate::IdentityProviderError::Unavailable,
        ))
        .unwrap();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000";
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            &[],
            &provider,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::IdentityProviderFailure(crate::IdentityProviderError::Unavailable)
        ));
    }

    #[test]
    fn presigned_invalid_provider_record_preserves_failure_kind() {
        let provider = crate::IdentityProvider::new(FailingIdentityProvider(
            crate::IdentityProviderError::InvalidRecord,
        ))
        .unwrap();
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000";
        let headers = [("host", "examplebucket.s3.amazonaws.com")];
        let err = authenticate_request(
            "GET",
            "/",
            query,
            &headers,
            &[],
            &provider,
            ExpectedSigningRegion::ExactEndpointRegion("us-east-1"),
            "s3",
            presigned_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::IdentityProviderFailure(crate::IdentityProviderError::InvalidRecord)
        ));
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
        assert!(matches!(err, AuthError::PresignedRequestExpired { .. }));
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
        assert!(matches!(err, AuthError::RequestNotYetValid { .. }));
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
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
    }

    #[test]
    fn authenticate_header_unsigned_security_token_rejected() {
        // AWS requires x-amz-security-token to be signed; unsigned → UnsignedHeaders.
        let store = example_store();
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
        let mut store = CredentialStore::new();
        store
            .add_record(configured_record(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                account("u1"),
                Some(5),
                true,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
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

    #[test]
    fn authenticate_header_expired_token_with_bad_signature_reports_expired_token() {
        let mut store = CredentialStore::new();
        store
            .add_record(configured_record(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                account("u1"),
                Some(5),
                true,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
        let headers = [
            ("authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000"),
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
        assert!(matches!(err, AuthError::PresignedRequestExpired { .. }));
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
        store
            .add_record(configured_record(
                "AKID",
                "secret",
                account("p"),
                None,
                false,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
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
        assert!(matches!(err, AuthError::UnknownAccessKey { .. }));
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
        assert!(matches!(err, AuthError::UnknownAccessKey { .. }));
    }

    // ── static credential token and expiry helpers ────────────────────

    #[test]
    fn unexpected_token_rejected_for_static_credential() {
        let err = validate_static_credential_has_no_token(Some("unexpected")).unwrap_err();
        assert!(matches!(
            err,
            AuthError::UnexpectedSecurityToken { token } if token == "unexpected"
        ));
    }

    #[test]
    fn overlong_unexpected_token_rejected_like_other_static_credential_tokens() {
        let token = "x".repeat(4097);
        let err = validate_static_credential_has_no_token(Some(&token)).unwrap_err();
        assert!(matches!(
            err,
            AuthError::UnexpectedSecurityToken { token: returned } if returned == token
        ));
    }

    #[test]
    fn epoch_time_before_future_expiry_is_not_expired() {
        let record = configured_record("AKID", "s", account("p"), Some(5), true);
        validate_static_record_expiry(&record, 0).unwrap();
    }

    #[test]
    fn no_token_no_expiry_ok() {
        let record = configured_record("AKID", "s", account("p"), None, true);
        validate_static_record_expiry(&record, 100).unwrap();
        validate_static_credential_has_no_token(None).unwrap();
    }

    #[test]
    fn expiry_not_yet_expired_with_nonzero_now() {
        let record = configured_record("AKID", "s", account("p"), Some(100), true);
        validate_static_record_expiry(&record, 100).unwrap();
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
    fn query_params_preserve_duplicate_order_and_decode_each_value() {
        assert_eq!(
            query_params_lossy("key=first%20value&other=x&key=second%2Bvalue", "key"),
            ["first value", "second+value"]
        );
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
        use crate::encoding::hex_val;

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
    fn collect_signed_headers_includes_missing_headers_with_empty_values() {
        let signed_headers = vec!["x-custom-header".to_string()];
        let headers = [("host", "example.com")];
        assert_eq!(
            collect_signed_headers(&signed_headers, &headers),
            [("x-custom-header".to_string(), String::new())]
        );
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
        store
            .add_record(configured_record(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                account("u1"),
                None,
                true,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
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
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
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
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
    }

    // ── Presigned: expired token ──────────────────────────────────────

    #[test]
    fn presigned_expired_token() {
        let mut store = CredentialStore::new();
        store
            .add_record(configured_record(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                account("u1"),
                Some(100),
                true,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
        let query = sign_test_presigned_query(
            "GET",
            "/",
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20240201%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240201T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host",
            "example.com",
        );
        let headers = [("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            &query,
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

    #[test]
    fn presigned_expired_token_with_bad_signature_reports_expired_token() {
        let mut store = CredentialStore::new();
        store
            .add_record(configured_record(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                account("u1"),
                Some(100),
                true,
            ))
            .unwrap();
        let store = crate::IdentityProvider::in_memory(store).unwrap();
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
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
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
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
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
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
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
        assert!(matches!(err, AuthError::SignatureMismatch { .. }));
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
    fn authenticate_header_and_query_auth_rejected_before_parsing() {
        let store = example_store();
        let authorization = "not-even-valid-header-auth";
        let headers = [("authorization", authorization), ("host", "example.com")];
        let err = authenticate_request(
            "GET",
            "/",
            "X-Amz-Algorithm=not-even-valid-query-auth",
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
            AuthError::MultipleAuthMechanisms { authorization: value }
                if value == authorization
        ));
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
            aws_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidHeaderCredentialRegion {
                provided_region,
                expected_region,
            } if provided_region == "eu-west-1" && expected_region == "us-east-1"
        ));
    }

    #[test]
    fn authenticate_header_skew_precedes_region_mismatch() {
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
        assert!(matches!(err, AuthError::RequestExpired));
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
            aws_example_time(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidHeaderCredentialService {
                provided_service,
                expected_service,
            } if provided_service == "iam" && expected_service == "s3"
        ));
    }

    #[test]
    fn auth_context_debug_redacts_streaming_secrets_and_escapes_text() {
        let account = AccountIdentity::from_principal("user-123");
        let ctx = AuthContext {
            mode: AuthMode::HeaderSigV4,
            access_key_id: Some("AK\r\nID".into()),
            identity: Some(crate::AuthenticatedIdentity::configured(
                account,
                crate::ConfiguredPrincipalIdentity::new("user-123"),
            )),
            authorization_profile: crate::AuthorizationProfile::Standard,
            request_epoch_secs: Some(1234),
            signing_region: Some("us-\neast-1".into()),
            streaming: Some(StreamingSigningContext {
                signing_key: [7u8; 32],
                seed_signature: "feedface".into(),
                scope: "20250101/us-east-1/s3/aws4_request".into(),
                timestamp: "20250101T000000Z".into(),
                access_key_id: "AKIDEXAMPLE".into(),
                seed_canonical_request: "GET\n/\n\nhost:h\n\nhost\nUNSIGNED-PAYLOAD".into(),
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
