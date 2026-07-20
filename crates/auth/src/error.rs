/// Authentication error types.
#[derive(thiserror::Error)]
pub enum AuthError {
    #[error("missing Authorization header")]
    MissingAuth,
    #[error("malformed Authorization header")]
    MalformedAuth,
    #[error("Authorization header does not contain exactly the required components")]
    MalformedAuthComponents,
    #[error("Authorization SignedHeaders component is empty")]
    MalformedSignedHeaders,
    #[error("unsupported Authorization type")]
    UnsupportedAuthType,
    #[error("missing query auth parameter: {param}")]
    MissingQueryParam { param: &'static str },
    #[error("invalid query auth parameter: {param}")]
    InvalidQueryParam { param: &'static str },
    #[error("invalid query credential region: {param}")]
    InvalidQueryCredentialRegion {
        param: &'static str,
        provided_region: String,
        expected_region: String,
    },
    #[error("invalid query credential service: {param}")]
    InvalidQueryCredentialService {
        param: &'static str,
        provided_service: String,
        expected_service: String,
    },
    #[error("invalid Authorization credential region")]
    InvalidHeaderCredentialRegion {
        provided_region: String,
        expected_region: String,
    },
    #[error("invalid Authorization credential service")]
    InvalidHeaderCredentialService {
        provided_service: String,
        expected_service: String,
    },
    #[error("invalid credential scope: {param}")]
    InvalidCredentialScope { param: &'static str },
    #[error("invalid credential scope region: {param}")]
    InvalidCredentialScopeRegion {
        param: &'static str,
        credential: String,
        provided_region: String,
        expected_region: String,
    },
    #[error("invalid credential scope service: {param}")]
    InvalidCredentialScopeService {
        param: &'static str,
        credential: String,
        provided_service: String,
        expected_service: String,
    },
    #[error("unknown access key id")]
    UnknownAccessKey { access_key_id: String },
    #[error("identity provider failure")]
    IdentityProviderFailure(crate::IdentityProviderError),
    #[error("session-token key ring unavailable")]
    SessionTokenKeyRingUnavailable,
    #[error("duplicate Authorization header")]
    DuplicateAuthorizationHeader,
    #[error("multiple authentication mechanisms supplied")]
    MultipleAuthMechanisms { authorization: String },
    #[error("access denied")]
    AccessDenied,
    #[error("signature mismatch")]
    SignatureMismatch {
        /// SigV4 verification inputs echoed by AWS in the error body;
        /// absent for POST policy signatures.
        diagnostics: Option<Box<SignatureMismatchDiagnostics>>,
    },
    #[error("unexpected security token")]
    UnexpectedSecurityToken { token: String },
    #[error("token expired")]
    ExpiredToken,
    #[error("session token expired")]
    ExpiredSessionToken { tokens: Vec<String> },
    #[error("missing required signed header: {header}")]
    MissingSignedHeader { header: String },
    #[error("request timestamp is too far from server time")]
    RequestExpired,
    #[error("Request has expired")]
    PresignedRequestExpired {
        /// The X-Amz-Expires parameter value, echoed in the error body.
        x_amz_expires: u64,
        /// Epoch seconds when the URL expired.
        expires_epoch: u64,
        /// Epoch seconds of the server clock at rejection.
        server_time_epoch: u64,
    },
    #[error("Request is not yet valid")]
    RequestNotYetValid {
        /// The X-Amz-Date parameter, echoed as epoch milliseconds.
        amz_date_epoch_millis: u64,
        /// Epoch seconds when the URL would expire.
        expires_epoch: u64,
        /// Epoch seconds of the server clock at rejection.
        server_time_epoch: u64,
    },
    #[error("there were headers present in the request which were not signed")]
    UnsignedHeaders { headers: Vec<String> },
}

impl std::fmt::Debug for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingAuth => f.write_str("MissingAuth"),
            Self::MalformedAuth => f.write_str("MalformedAuth"),
            Self::MalformedAuthComponents => f.write_str("MalformedAuthComponents"),
            Self::MalformedSignedHeaders => f.write_str("MalformedSignedHeaders"),
            Self::UnsupportedAuthType => f.write_str("UnsupportedAuthType"),
            Self::MissingQueryParam { param } => f
                .debug_struct("MissingQueryParam")
                .field("param", &param)
                .finish(),
            Self::InvalidQueryParam { param } => f
                .debug_struct("InvalidQueryParam")
                .field("param", &param)
                .finish(),
            Self::InvalidQueryCredentialRegion {
                param,
                provided_region,
                expected_region,
            } => f
                .debug_struct("InvalidQueryCredentialRegion")
                .field("param", &param)
                .field("provided_region", &observability::escaped(provided_region))
                .field("expected_region", &observability::escaped(expected_region))
                .finish(),
            Self::InvalidQueryCredentialService {
                param,
                provided_service,
                expected_service,
            } => f
                .debug_struct("InvalidQueryCredentialService")
                .field("param", &param)
                .field(
                    "provided_service",
                    &observability::escaped(provided_service),
                )
                .field(
                    "expected_service",
                    &observability::escaped(expected_service),
                )
                .finish(),
            Self::InvalidHeaderCredentialRegion {
                provided_region,
                expected_region,
            } => f
                .debug_struct("InvalidHeaderCredentialRegion")
                .field("provided_region", &observability::escaped(provided_region))
                .field("expected_region", &observability::escaped(expected_region))
                .finish(),
            Self::InvalidHeaderCredentialService {
                provided_service,
                expected_service,
            } => f
                .debug_struct("InvalidHeaderCredentialService")
                .field(
                    "provided_service",
                    &observability::escaped(provided_service),
                )
                .field(
                    "expected_service",
                    &observability::escaped(expected_service),
                )
                .finish(),
            Self::InvalidCredentialScope { param } => f
                .debug_struct("InvalidCredentialScope")
                .field("param", &param)
                .finish(),
            Self::InvalidCredentialScopeRegion {
                param,
                credential,
                provided_region,
                expected_region,
            } => f
                .debug_struct("InvalidCredentialScopeRegion")
                .field("param", &param)
                .field("credential", &observability::redacted("sigv4_credential"))
                .field("credential_len", &credential.len())
                .field("provided_region", &observability::escaped(provided_region))
                .field("expected_region", &observability::escaped(expected_region))
                .finish(),
            Self::InvalidCredentialScopeService {
                param,
                credential,
                provided_service,
                expected_service,
            } => f
                .debug_struct("InvalidCredentialScopeService")
                .field("param", &param)
                .field("credential", &observability::redacted("sigv4_credential"))
                .field("credential_len", &credential.len())
                .field(
                    "provided_service",
                    &observability::escaped(provided_service),
                )
                .field(
                    "expected_service",
                    &observability::escaped(expected_service),
                )
                .finish(),
            Self::UnknownAccessKey { access_key_id } => f
                .debug_struct("UnknownAccessKey")
                .field("access_key_id", &observability::escaped(access_key_id))
                .finish(),
            Self::IdentityProviderFailure(error) => f
                .debug_tuple("IdentityProviderFailure")
                .field(error)
                .finish(),
            Self::SessionTokenKeyRingUnavailable => f.write_str("SessionTokenKeyRingUnavailable"),
            Self::DuplicateAuthorizationHeader => f.write_str("DuplicateAuthorizationHeader"),
            Self::MultipleAuthMechanisms { authorization } => f
                .debug_struct("MultipleAuthMechanisms")
                .field(
                    "authorization",
                    &observability::redacted("authorization_header"),
                )
                .field("authorization_len", &authorization.len())
                .finish(),
            Self::AccessDenied => f.write_str("AccessDenied"),
            Self::SignatureMismatch { .. } => f.write_str("SignatureMismatch"),
            Self::UnexpectedSecurityToken { .. } => f
                .debug_struct("UnexpectedSecurityToken")
                .field("token", &observability::redacted("security_token"))
                .finish(),
            Self::ExpiredToken => f.write_str("ExpiredToken"),
            Self::ExpiredSessionToken { tokens } => f
                .debug_struct("ExpiredSessionToken")
                .field(
                    "tokens",
                    &observability::redacted("presented_session_tokens"),
                )
                .field("token_count", &tokens.len())
                .finish(),
            Self::MissingSignedHeader { header } => f
                .debug_struct("MissingSignedHeader")
                .field("header", &observability::escaped(header))
                .finish(),
            Self::RequestExpired => f.write_str("RequestExpired"),
            Self::PresignedRequestExpired { .. } => f.write_str("PresignedRequestExpired"),
            Self::RequestNotYetValid { .. } => f.write_str("RequestNotYetValid"),
            Self::UnsignedHeaders { headers } => {
                let headers: Vec<_> = headers
                    .iter()
                    .map(|header| observability::escaped(header))
                    .collect();
                f.debug_struct("UnsignedHeaders")
                    .field("headers", &headers)
                    .finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthError, SignatureMismatchDiagnostics};

    #[test]
    fn unexpected_security_token_debug_is_redacted() {
        let err = AuthError::UnexpectedSecurityToken {
            token: "tok\nen".to_string(),
        };
        let debug = format!("{err:?}");
        assert!(debug.contains("<redacted:security_token>"));
        assert!(!debug.contains("tok\nen"));
    }

    #[test]
    fn unexpected_security_token_display_is_redacted() {
        let err = AuthError::UnexpectedSecurityToken {
            token: "tok\nen".to_string(),
        };
        let display = err.to_string();
        assert_eq!(display, "unexpected security token");
        assert!(!display.contains("tok\nen"));
    }

    #[test]
    fn expired_session_token_debug_and_display_are_redacted() {
        let err = AuthError::ExpiredSessionToken {
            tokens: vec![
                "first-secret-token".to_string(),
                "second-secret-token".to_string(),
            ],
        };
        let debug = format!("{err:?}");
        assert!(debug.contains("<redacted:presented_session_tokens>"));
        assert!(debug.contains("token_count"));
        assert!(!debug.contains("first-secret-token"));
        assert!(!debug.contains("second-secret-token"));

        let display = err.to_string();
        assert_eq!(display, "session token expired");
        assert!(!display.contains("secret-token"));
    }

    #[test]
    fn multiple_auth_mechanisms_debug_redacts_authorization() {
        let err = AuthError::MultipleAuthMechanisms {
            authorization: "AWS4-HMAC-SHA256 secret-signature".to_string(),
        };
        let debug = format!("{err:?}");
        assert!(debug.contains("<redacted:authorization_header>"));
        assert!(!debug.contains("secret-signature"));
    }

    #[test]
    fn unsigned_headers_debug_escapes_header_names() {
        let err = AuthError::UnsignedHeaders {
            headers: vec!["x-amz-meta-\nname".to_string()],
        };
        let debug = format!("{err:?}");
        assert!(debug.contains(r#""x-amz-meta-\nname""#));
        assert!(!debug.contains("x-amz-meta-\nname"));
    }

    #[test]
    fn signature_mismatch_diagnostics_debug_redacts_protocol_values() {
        let diagnostics = SignatureMismatchDiagnostics {
            access_key_id: "ARGS0123456789ABCDEFGHIJ".to_string(),
            string_to_sign: "policy-containing-secret-token".to_string(),
            signature_provided: "client-signature".to_string(),
            canonical_request: Some("canonical-request-containing-secret-token".to_string()),
        };
        let debug = format!("{diagnostics:?}");

        assert!(debug.contains("<redacted:sigv4_string_to_sign>"));
        assert!(debug.contains("<redacted:sigv4_signature>"));
        assert!(debug.contains("<redacted:sigv4_canonical_request>"));
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("client-signature"));
    }
}

/// The SigV4 verification inputs AWS echoes in a `SignatureDoesNotMatch`
/// error body.
///
/// The raw values are retained only for the protocol renderer. A canonical
/// request or POST string-to-sign can contain a bearer session token, so this
/// type's diagnostic representation must remain redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct SignatureMismatchDiagnostics {
    pub access_key_id: String,
    pub string_to_sign: String,
    pub signature_provided: String,
    /// Absent for POST policy signatures, which have no canonical request;
    /// chunk signatures echo the seed request's canonical form.
    pub canonical_request: Option<String>,
}

impl std::fmt::Debug for SignatureMismatchDiagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignatureMismatchDiagnostics")
            .field(
                "access_key_id",
                &observability::escaped(&self.access_key_id),
            )
            .field(
                "string_to_sign",
                &observability::redacted("sigv4_string_to_sign"),
            )
            .field("string_to_sign_len", &self.string_to_sign.len())
            .field(
                "signature_provided",
                &observability::redacted("sigv4_signature"),
            )
            .field("signature_provided_len", &self.signature_provided.len())
            .field(
                "canonical_request",
                &self
                    .canonical_request
                    .as_ref()
                    .map(|_| observability::redacted("sigv4_canonical_request")),
            )
            .field(
                "canonical_request_len",
                &self.canonical_request.as_ref().map(String::len),
            )
            .finish()
    }
}
