/// Authentication error types.

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing Authorization header")]
    MissingAuth,
    #[error("malformed Authorization header")]
    MalformedAuth,
    #[error("unknown access key id")]
    UnknownAccessKey,
    #[error("signature mismatch")]
    SignatureMismatch,
    #[error("missing required signed header: {header}")]
    MissingSignedHeader { header: &'static str },
}
