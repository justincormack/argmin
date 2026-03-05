/// Authentication error types.

#[derive(Debug, thiserror::Error)]
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
    #[error("unknown access key id")]
    UnknownAccessKey,
    #[error("access denied")]
    AccessDenied,
    #[error("signature mismatch")]
    SignatureMismatch,
    #[error("invalid session token")]
    InvalidToken,
    #[error("token expired")]
    ExpiredToken,
    #[error("missing required signed header: {header}")]
    MissingSignedHeader { header: &'static str },
    #[error("request timestamp is too far from server time")]
    RequestExpired,
    #[error("there were headers present in the request which were not signed")]
    UnsignedHeaders { headers: Vec<String> },
}
