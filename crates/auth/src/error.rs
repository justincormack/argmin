/// Authentication error types.
#[derive(thiserror::Error)]
pub enum AuthError {
    #[error("missing Authorization header")]
    MissingAuth,
    #[error("malformed Authorization header")]
    MalformedAuth,
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
    UnknownAccessKey,
    #[error("duplicate Authorization header")]
    DuplicateAuthorizationHeader,
    #[error("access denied")]
    AccessDenied,
    #[error("signature mismatch")]
    SignatureMismatch,
    #[error("invalid session token")]
    InvalidToken,
    #[error("unexpected security token")]
    UnexpectedSecurityToken { token: String },
    #[error("token expired")]
    ExpiredToken,
    #[error("missing required signed header: {header}")]
    MissingSignedHeader { header: String },
    #[error("request timestamp is too far from server time")]
    RequestExpired,
    #[error("Request has expired")]
    PresignedRequestExpired,
    #[error("Request is not yet valid")]
    RequestNotYetValid,
    #[error("there were headers present in the request which were not signed")]
    UnsignedHeaders { headers: Vec<String> },
}

impl std::fmt::Debug for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingAuth => f.write_str("MissingAuth"),
            Self::MalformedAuth => f.write_str("MalformedAuth"),
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
            Self::UnknownAccessKey => f.write_str("UnknownAccessKey"),
            Self::DuplicateAuthorizationHeader => f.write_str("DuplicateAuthorizationHeader"),
            Self::AccessDenied => f.write_str("AccessDenied"),
            Self::SignatureMismatch => f.write_str("SignatureMismatch"),
            Self::InvalidToken => f.write_str("InvalidToken"),
            Self::UnexpectedSecurityToken { .. } => f
                .debug_struct("UnexpectedSecurityToken")
                .field("token", &observability::redacted("security_token"))
                .finish(),
            Self::ExpiredToken => f.write_str("ExpiredToken"),
            Self::MissingSignedHeader { header } => f
                .debug_struct("MissingSignedHeader")
                .field("header", &observability::escaped(header))
                .finish(),
            Self::RequestExpired => f.write_str("RequestExpired"),
            Self::PresignedRequestExpired => f.write_str("PresignedRequestExpired"),
            Self::RequestNotYetValid => f.write_str("RequestNotYetValid"),
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
    use super::AuthError;

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
    fn unsigned_headers_debug_escapes_header_names() {
        let err = AuthError::UnsignedHeaders {
            headers: vec!["x-amz-meta-\nname".to_string()],
        };
        let debug = format!("{err:?}");
        assert!(debug.contains(r#""x-amz-meta-\nname""#));
        assert!(!debug.contains("x-amz-meta-\nname"));
    }
}
