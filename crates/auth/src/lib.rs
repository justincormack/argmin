pub mod canonical;
pub mod credential;
pub mod error;
pub mod sigv4;

pub use credential::{CredentialScope, CredentialStore, SecretKey};
pub use error::AuthError;
pub use sigv4::{parse_auth_header, verify_request, SigV4Auth};
